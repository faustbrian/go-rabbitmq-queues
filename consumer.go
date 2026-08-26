package rabbitmqqueue

import (
	"context"
	"io"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumerChannel interface {
	Qos(int, int, bool) error
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(string, bool) error
	Ack(uint64, bool) error
	Nack(uint64, bool, bool) error
	Reject(uint64, bool) error
	Close() error
}

// DeliveryHandler processes one owned delivery and returns its explicit manual settlement.
type DeliveryHandler func(context.Context, Delivery) (Settlement, error)

type consumerEnvelope struct {
	delivery Delivery
	tag      uint64
}

// Consumer owns one manual-acknowledgement AMQP channel and a bounded worker pool.
type Consumer struct {
	config   ConsumerConfig
	handler  DeliveryHandler
	channel  consumerChannel
	resource io.Closer

	lifetimeContext context.Context
	stopLifetime    context.CancelFunc
	jobs            chan consumerEnvelope
	done            chan struct{}

	stateMu      sync.Mutex
	stopping     bool
	terminalErr  error
	cancelOnce   sync.Once
	cancelErr    error
	resourceOnce sync.Once
	resourceErr  error
}

func newConsumerFromChannel(
	ctx context.Context,
	config ConsumerConfig,
	handler DeliveryHandler,
	channel consumerChannel,
	resource io.Closer,
) (*Consumer, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if handler == nil || channel == nil || resource == nil {
		return nil, ErrInvalidConsumer
	}
	setupContext, cancelSetup := context.WithTimeout(ctx, config.HandlerTimeout)
	defer cancelSetup()
	if err := boundedConsumerSetup(setupContext, config.HandlerTimeout, resource, channel, func() error {
		return channel.Qos(config.Prefetch, 0, false)
	}); err != nil {
		return nil, err
	}
	lifetimeContext, stopLifetime := context.WithCancel(context.Background())
	type consumeResult struct {
		deliveries <-chan amqp.Delivery
		err        error
	}
	consumed := make(chan consumeResult, 1)
	go func() {
		deliveries, err := channel.Consume(
			config.Queue.Name,
			config.Name,
			false,
			false,
			false,
			false,
			nil,
		)
		consumed <- consumeResult{deliveries: deliveries, err: err}
	}()
	var deliveries <-chan amqp.Delivery
	select {
	case result := <-consumed:
		if result.err != nil || result.deliveries == nil {
			stopLifetime()
			_ = boundedCloseConsumerResources(resource, channel, deadlineFor(setupContext, config.HandlerTimeout))
			return nil, ErrConsumerUnavailable
		}
		deliveries = result.deliveries
	case <-setupContext.Done():
		stopLifetime()
		_ = boundedCloseConsumerResources(resource, channel, deadlineFor(setupContext, config.HandlerTimeout))
		return nil, ErrConsumerUnavailable
	}
	consumer := &Consumer{
		config: config, handler: handler, channel: channel, resource: resource,
		lifetimeContext: lifetimeContext, stopLifetime: stopLifetime,
		jobs: make(chan consumerEnvelope, config.Prefetch), done: make(chan struct{}),
	}
	go consumer.run(deliveries)
	return consumer, nil
}

func boundedConsumerSetup(
	ctx context.Context,
	fallback time.Duration,
	resource io.Closer,
	channel consumerChannel,
	setup func() error,
) error {
	result := make(chan error, 1)
	go func() { result <- setup() }()
	select {
	case err := <-result:
		if err == nil {
			return nil
		}
	case <-ctx.Done():
	}
	_ = boundedCloseConsumerResources(resource, channel, deadlineFor(ctx, fallback))
	return ErrConsumerUnavailable
}

func (consumer *Consumer) run(deliveries <-chan amqp.Delivery) {
	var workers sync.WaitGroup
	workers.Add(consumer.config.Concurrency)
	for range consumer.config.Concurrency {
		go func() {
			defer workers.Done()
			for envelope := range consumer.jobs {
				consumer.handle(envelope)
			}
		}()
	}

consume:
	for {
		select {
		case source, open := <-deliveries:
			if !open {
				if !consumer.isStopping() {
					consumer.setTerminal(ErrConsumerUnavailable)
				}
				break consume
			}
			delivery, err := deliveryFromAMQP(source, consumer.config)
			if err != nil {
				if consumer.settle(source.DeliveryTag, Reject(false)) != nil {
					consumer.setTerminal(ErrConsumerUnavailable)
					break consume
				}
				continue
			}
			select {
			case consumer.jobs <- consumerEnvelope{delivery: delivery, tag: source.DeliveryTag}:
			case <-consumer.lifetimeContext.Done():
				break consume
			}
		case <-consumer.lifetimeContext.Done():
			break consume
		}
	}
	close(consumer.jobs)
	workers.Wait()
	close(consumer.done)
}

func (consumer *Consumer) handle(envelope consumerEnvelope) {
	handlerContext, cancel := context.WithTimeout(consumer.lifetimeContext, consumer.config.HandlerTimeout)
	requested, err := consumer.handler(handlerContext, envelope.delivery)
	contextErr := handlerContext.Err()
	cancel()
	if err != nil || contextErr != nil || requested.Validate() != nil {
		requested = consumer.config.Failure
	}
	if requested.Method == SettlementDelegate {
		return
	}
	requested = boundedSettlement(envelope.delivery, requested, consumer.config)
	if err := consumer.settle(envelope.tag, requested); err != nil {
		consumer.setTerminal(ErrConsumerUnavailable)
	}
}

func (consumer *Consumer) settle(tag uint64, settlement Settlement) error {
	settled := make(chan error, 1)
	go func() { settled <- consumer.applySettlement(tag, settlement) }()
	ctx, cancel := context.WithTimeout(consumer.lifetimeContext, consumer.config.HandlerTimeout)
	defer cancel()
	select {
	case err := <-settled:
		return err
	case <-ctx.Done():
		consumer.closeOwnedResources(deadlineFor(ctx, consumer.config.HandlerTimeout))
		return ErrConsumerUnavailable
	}
}

func (consumer *Consumer) applySettlement(tag uint64, settlement Settlement) error {
	switch settlement.Method {
	case SettlementAcknowledge:
		return consumer.channel.Ack(tag, false)
	case SettlementNegativeAcknowledge:
		return consumer.channel.Nack(tag, false, settlement.Requeue)
	case SettlementReject:
		return consumer.channel.Reject(tag, settlement.Requeue)
	default:
		return ErrInvalidSettlement
	}
}

func (consumer *Consumer) isStopping() bool {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumer.stopping
}

func (consumer *Consumer) setTerminal(err error) {
	consumer.stateMu.Lock()
	if consumer.terminalErr == nil {
		consumer.terminalErr = err
	}
	consumer.stateMu.Unlock()
	consumer.stopLifetime()
	consumer.closeOwnedResources(time.Now().Add(consumer.config.HandlerTimeout))
}

// Done closes after broker intake stops and all admitted handlers return.
func (consumer *Consumer) Done() <-chan struct{} { return consumer.done }

// Err reports a sanitized unexpected terminal consumer failure.
func (consumer *Consumer) Err() error {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumer.terminalErr
}

// Drain cancels broker intake and waits for admitted handlers without closing
// the healthy owned connection. A drain deadline forces the connection closed.
func (consumer *Consumer) Drain(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	drainContext, cancelDrain := context.WithDeadline(ctx, deadlineFor(ctx, consumer.config.HandlerTimeout))
	defer cancelDrain()
	consumer.stateMu.Lock()
	consumer.stopping = true
	consumer.stateMu.Unlock()
	consumer.cancelOnce.Do(func() {
		cancelled := make(chan error, 1)
		go func() { cancelled <- consumer.channel.Cancel(consumer.config.Name, false) }()
		select {
		case err := <-cancelled:
			if err != nil {
				consumer.cancelErr = ErrConsumerUnavailable
				consumer.stopLifetime()
			}
		case <-drainContext.Done():
			consumer.stopLifetime()
			consumer.closeOwnedResources(deadlineFor(drainContext, consumer.config.HandlerTimeout))
			consumer.cancelErr = drainContext.Err()
		}
	})
	if consumer.cancelErr != nil {
		return consumer.cancelErr
	}
	select {
	case <-consumer.done:
		return nil
	case <-drainContext.Done():
		consumer.stopLifetime()
		consumer.closeOwnedResources(deadlineFor(drainContext, consumer.config.HandlerTimeout))
		return drainContext.Err()
	}
}

// Close drains admitted handlers, then closes owned resources. If cancellation
// or the drain deadline fails, resources are still closed for redelivery.
func (consumer *Consumer) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	drainErr := consumer.Drain(ctx)
	consumer.closeOwnedResources(deadlineFor(ctx, consumer.config.HandlerTimeout))
	if drainErr != nil {
		return drainErr
	}
	return consumer.resourceErr
}

func (consumer *Consumer) closeOwnedResources(deadline time.Time) {
	consumer.resourceOnce.Do(func() {
		if err := boundedCloseConsumerResources(consumer.resource, consumer.channel, deadline); err != nil {
			consumer.resourceErr = ErrConsumerUnavailable
		}
	})
}

func boundedCloseConsumerResources(resource io.Closer, channel io.Closer, deadline time.Time) error {
	failed := false
	if resource != nil {
		result := startConsumerClose(resource, deadline)
		err, completed := waitForConsumerClose(result, deadline)
		if !completed {
			if channel != nil {
				_ = startConsumerClose(channel, deadline)
			}
			return ErrConsumerUnavailable
		}
		failed = err != nil
	}
	if channel != nil {
		result := startConsumerClose(channel, deadline)
		err, completed := waitForConsumerClose(result, deadline)
		if !completed {
			return ErrConsumerUnavailable
		}
		failed = failed || err != nil
	}
	if failed {
		return ErrConsumerUnavailable
	}
	return nil
}

func startConsumerClose(closer io.Closer, deadline time.Time) <-chan error {
	result := make(chan error, 1)
	go func() { result <- closeWithDeadline(closer, deadline) }()
	return result
}

func waitForConsumerClose(result <-chan error, deadline time.Time) (error, bool) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, false
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case err := <-result:
		return err, true
	case <-timer.C:
		return nil, false
	}
}
