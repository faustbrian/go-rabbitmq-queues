package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumerChannel interface {
	ExchangeDeclarePassive(string, string, bool, bool, bool, bool, amqp.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	QueueBind(string, string, string, bool, amqp.Table) error
	NotifyCancel(chan string) chan string
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

type consumerGeneration struct {
	channel       consumerChannel
	resource      io.Closer
	deliveries    <-chan amqp.Delivery
	cancellations <-chan string
	failure       chan struct{}
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
	closed        atomic.Bool
	cancelOnce    sync.Once
	cancelErr     error
	delegated     atomic.Bool
}

type consumerEnvelope struct {
	delivery   Delivery
	tag        uint64
	generation *consumerGeneration
}

// Consumer owns one active manual-acknowledgement generation and a bounded
// worker pool. Runtime recovery replaces the complete connection/channel/
// consumer generation before admitting more broker deliveries.
type Consumer struct {
	config   ConsumerConfig
	handler  DeliveryHandler
	recovery *consumerRecovery

	lifetimeContext  context.Context
	stopLifetime     context.CancelFunc
	recoveryContext  context.Context
	stopRecovery     context.CancelFunc
	jobs             chan consumerEnvelope
	done             chan struct{}
	observations     *observationStream
	admissionChanged chan struct{}

	admissionMu sync.Mutex
	paused      bool
	resume      chan struct{}

	stateMu     sync.Mutex
	stopping    bool
	recovering  bool
	stopped     bool
	terminalErr error
	generation  *consumerGeneration
	drainErr    error
	resourceErr error
}

func newConsumerFromChannel(
	ctx context.Context,
	config ConsumerConfig,
	handler DeliveryHandler,
	channel consumerChannel,
	resource io.Closer,
) (*Consumer, error) {
	return newConsumerFromChannelWithRecovery(ctx, config, handler, channel, resource, nil)
}

func newConsumerFromChannelWithRecovery(
	ctx context.Context,
	config ConsumerConfig,
	handler DeliveryHandler,
	channel consumerChannel,
	resource io.Closer,
	recovery *consumerRecovery,
) (*Consumer, error) {
	if !contextProvided(ctx) {
		return nil, ErrContextRequired
	}
	config = ownConsumerConfig(config)
	configAccepted, configErr := acceptedConsumerConfig(config.Validate())
	if !configAccepted {
		return nil, configErr
	}
	if !consumerInputsPresent(handler, channel, resource) {
		return nil, ErrInvalidConsumer
	}
	generation, setupErr := setupConsumerGeneration(ctx, config, channel, resource)
	setupAccepted, setupErr := acceptedConsumerGeneration(generation, setupErr)
	if !setupAccepted {
		return nil, setupErr
	}
	lifetimeContext, stopLifetime := context.WithCancel(context.Background())
	recoveryContext, stopRecovery := context.WithCancel(context.Background())
	consumer := &Consumer{
		config: config, handler: handler, recovery: recovery,
		lifetimeContext: lifetimeContext, stopLifetime: stopLifetime,
		recoveryContext: recoveryContext, stopRecovery: stopRecovery,
		jobs: make(chan consumerEnvelope, config.Prefetch), done: make(chan struct{}),
		observations:     newObservationStream(ObservationConsumer, observationBufferSize),
		admissionChanged: make(chan struct{}, 1),
		generation:       generation,
	}
	consumer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationConnected})
	go consumer.run(generation)
	return consumer, nil
}

func acceptedConsumerConfig(err error) (bool, error) {
	return err == nil, err
}

func consumerInputsPresent(handler DeliveryHandler, channel consumerChannel, resource io.Closer) bool {
	return handler != nil && channel != nil && resource != nil
}

func acceptedConsumerGeneration(generation *consumerGeneration, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if generation == nil {
		return false, ErrConsumerUnavailable
	}
	return true, nil
}

func setupConsumerGeneration(
	ctx context.Context,
	config ConsumerConfig,
	channel consumerChannel,
	resource io.Closer,
) (*consumerGeneration, error) {
	setupContext, cancelSetup := context.WithTimeout(ctx, config.HandlerTimeout)
	defer cancelSetup()
	queueName := config.Queue.Name
	if config.Queue.Transient != nil {
		var err error
		queueName, err = setupTransientQueue(
			setupContext, config.HandlerTimeout, config.Queue.Transient, channel, resource,
		)
		if err != nil {
			return nil, err
		}
	}
	if err := boundedConsumerSetup(setupContext, config.HandlerTimeout, resource, channel, func() error {
		return channel.Qos(config.Prefetch, 0, false)
	}); err != nil {
		return nil, err
	}
	cancellations := channel.NotifyCancel(make(chan string, 1))
	if cancellations == nil {
		_ = boundedCloseConsumerResources(
			resource, channel, consumerCleanupDeadline(setupContext, config.HandlerTimeout),
		)
		return nil, ErrConsumerUnavailable
	}
	type consumeResult struct {
		deliveries <-chan amqp.Delivery
		err        error
	}
	consumed := make(chan consumeResult, 1)
	go func() {
		deliveries, err := channel.Consume(
			queueName,
			config.Name,
			false,
			config.Exclusive,
			false,
			false,
			consumerArguments(config),
		)
		consumed <- consumeResult{deliveries: deliveries, err: err}
	}()
	select {
	case result := <-consumed:
		if result.err != nil || result.deliveries == nil {
			_ = boundedCloseConsumerResources(resource, channel, consumerCleanupDeadline(setupContext, config.HandlerTimeout))
			return nil, ErrConsumerUnavailable
		}
		return &consumerGeneration{
			channel: channel, resource: resource, deliveries: result.deliveries,
			cancellations: cancellations,
			failure:       make(chan struct{}, 1), closeDone: make(chan struct{}),
		}, nil
	case <-setupContext.Done():
		_ = boundedCloseConsumerResources(resource, channel, consumerCleanupDeadline(setupContext, config.HandlerTimeout))
		return nil, ErrConsumerUnavailable
	}
}

func setupTransientQueue(
	ctx context.Context,
	fallback time.Duration,
	transient *TransientQueue,
	channel consumerChannel,
	resource io.Closer,
) (string, error) {
	if err := boundedConsumerSetup(ctx, fallback, resource, channel, func() error {
		return channel.ExchangeDeclarePassive(
			transient.Exchange.Name,
			string(transient.Exchange.Kind),
			transient.Exchange.Durable,
			transient.Exchange.AutoDelete,
			transient.Exchange.Internal,
			false,
			nil,
		)
	}); err != nil {
		return "", err
	}
	var declared amqp.Queue
	if err := boundedConsumerSetup(ctx, fallback, resource, channel, func() error {
		var err error
		declared, err = channel.QueueDeclare(
			"", false, true, true, false,
			amqp.Table{"x-queue-type": string(QueueClassic)},
		)
		return err
	}); err != nil {
		return "", err
	}
	if invalidIdentity(declared.Name, 255) {
		_ = boundedCloseConsumerResources(
			resource, channel, consumerCleanupDeadline(ctx, fallback),
		)
		return "", ErrConsumerUnavailable
	}
	if err := boundedConsumerSetup(ctx, fallback, resource, channel, func() error {
		return channel.QueueBind(
			declared.Name,
			transient.RoutingKey,
			transient.Exchange.Name,
			false,
			bindingArguments(transient.Arguments),
		)
	}); err != nil {
		return "", err
	}
	return declared.Name, nil
}

func consumerArguments(config ConsumerConfig) amqp.Table {
	if config.Priority == nil {
		return nil
	}
	return amqp.Table{"x-priority": *config.Priority}
}

func boundedConsumerSetup(ctx context.Context, fallback time.Duration, resource io.Closer, channel consumerChannel, setup func() error) error {
	result := make(chan error, 1)
	go func() { result <- setup() }()
	select {
	case err := <-result:
		if err == nil {
			return nil
		}
	case <-ctx.Done():
	}
	_ = boundedCloseConsumerResources(resource, channel, consumerCleanupDeadline(ctx, fallback))
	return ErrConsumerUnavailable
}

func consumerCleanupDeadline(ctx context.Context, fallback time.Duration) time.Time {
	deadline := deadlineFor(ctx, fallback)
	if positiveDuration(time.Until(deadline)) {
		return deadline
	}
	return time.Now().Add(fallback)
}

func (consumer *Consumer) run(initial *consumerGeneration) {
	var workers sync.WaitGroup
	workers.Add(consumer.config.Concurrency)
	for range consumer.config.Concurrency {
		go func() {
			defer workers.Done()
			for envelope := range consumer.jobs {
				consumer.handle(envelope)
				consumer.signalAdmissionChanged()
			}
		}()
	}

	generation := initial
	for {
		if !consumer.consumeGeneration(generation) {
			consumer.finishRun(&workers)
			return
		}
		consumer.beginRecovery()
		if err := consumer.closeGeneration(generation, time.Now().Add(consumer.config.HandlerTimeout)); consumerOperationFailed(err) {
			consumer.setResourceError(err)
			consumer.setTerminalError(ErrConsumerUnavailable)
			break
		}
		next, ok := consumer.recoverRuntime()
		if !ok {
			if !consumer.isStopping() {
				consumer.setTerminalError(ErrConsumerUnavailable)
			}
			break
		}
		generation = next
		consumer.finishRecovery()
	}
	consumer.finishRun(&workers)
}

func (consumer *Consumer) finishRun(workers *sync.WaitGroup) {
	consumer.stopRecovery()
	if consumer.Err() != nil {
		consumer.stopLifetime()
		_ = consumer.closeGeneration(consumer.currentGeneration(), time.Now().Add(consumer.config.HandlerTimeout))
	}
	close(consumer.jobs)
	workers.Wait()
	generation := consumer.currentGeneration()
	if consumer.drainError() != nil || generation.delegated.Load() {
		consumer.setResourceError(consumer.closeGeneration(
			generation, time.Now().Add(consumer.config.HandlerTimeout),
		))
	}
	consumer.stateMu.Lock()
	consumer.recovering = false
	consumer.stopped = true
	consumer.stateMu.Unlock()
	consumer.observe(Observation{Kind: ObservationShutdown, Outcome: ObservationShutdownCompleted})
	consumer.observations.close()
	close(consumer.done)
}

func (consumer *Consumer) consumeGeneration(generation *consumerGeneration) bool {
	// RabbitMQ limits unsettled deliveries to Prefetch. The extra slot keeps the
	// delivery stream selectable so its closure remains visible while paused.
	pending := make([]consumerEnvelope, consumerPendingCapacity(consumer.config.Prefetch))
	pendingHead := 0
	pendingCount := 0
	cancellations := generation.cancellations
	recoveryDone := consumer.recoveryContext.Done()
	lifetimeDone := consumer.lifetimeContext.Done()
	draining := false
	deliveriesClosed := false
	for {
		consumer.admissionMu.Lock()
		paused := consumerAdmissionPaused(consumer.paused, draining)
		resume := consumer.resume
		if pendingCount > 0 && !paused {
			select {
			case consumer.jobs <- pending[pendingHead]:
				pending[pendingHead] = consumerEnvelope{}
				pendingHead = (pendingHead + 1) % len(pending)
				pendingCount--
				consumer.admissionMu.Unlock()
				continue
			default:
			}
		}
		consumer.admissionMu.Unlock()
		if !paused {
			resume = nil
		}
		deliveries := generation.deliveries
		if suspendConsumerDeliveries(deliveriesClosed, paused, pendingCount, len(pending)) {
			deliveries = nil
		}
		if consumerDrainComplete(draining, deliveriesClosed, pendingCount) {
			return false
		}

		select {
		case source, open := <-deliveries:
			if !open {
				consumer.observePendingCancellation(cancellations)
				deliveriesClosed = true
				if !consumer.isStopping() {
					return true
				}
				draining = true
				recoveryDone = nil
			} else {
				consumer.observe(Observation{Kind: ObservationDelivery, Outcome: ObservationDelivered})
				if source.Redelivered {
					consumer.observe(Observation{Kind: ObservationRedelivery, Outcome: ObservationRedelivered})
				}
				delivery, err := deliveryFromAMQP(source, consumer.config)
				if consumerOperationFailed(err) {
					if consumerOperationFailed(consumer.settle(generation, source.DeliveryTag, Reject(false))) {
						return true
					}
				} else {
					if hasDeadLetterHistory(delivery.Deaths) {
						consumer.observe(Observation{Kind: ObservationDeadLetter, Outcome: ObservationDeadLettered})
					}
					if consumerBacklogFull(pendingCount, len(consumer.jobs), consumer.config.Prefetch) {
						consumer.observe(Observation{Kind: ObservationBacklogPressure, Outcome: ObservationBacklogFull})
					}
					pending[(pendingHead+pendingCount)%len(pending)] = consumerEnvelope{
						delivery: delivery, tag: source.DeliveryTag, generation: generation,
					}
					pendingCount++
				}
			}
		case <-resume:
		case <-consumer.admissionChanged:
		case tag, open := <-cancellations:
			if !open {
				cancellations = nil
			} else if matchingConsumerCancellation(tag, consumer.config.Name) {
				consumer.observe(Observation{
					Kind: ObservationConsumerCancellation, Outcome: ObservationCancelled,
				})
				if consumer.isStopping() {
					draining = true
					recoveryDone = nil
					cancellations = nil
				} else {
					return true
				}
			}
		case <-generation.failure:
			return true
		case <-recoveryDone:
			if !consumer.isStopping() {
				return false
			}
			draining = true
			recoveryDone = nil
		case <-lifetimeDone:
			return false
		}
	}
}

func suspendConsumerDeliveries(deliveriesClosed, paused bool, pendingCount, capacity int) bool {
	return deliveriesClosed || (!paused && pendingCount > 0) || pendingCount == capacity
}

func consumerPendingCapacity(prefetch int) int {
	return prefetch + 1
}

func consumerAdmissionPaused(paused, draining bool) bool {
	return paused && !draining
}

func consumerDrainComplete(draining, deliveriesClosed bool, pendingCount int) bool {
	return draining && deliveriesClosed && pendingCount == 0
}

func hasDeadLetterHistory(deaths []Death) bool {
	return len(deaths) != 0
}

func consumerBacklogFull(pendingCount, admittedCount, prefetch int) bool {
	return pendingCount+admittedCount >= prefetch
}

func matchingConsumerCancellation(tag, name string) bool {
	return tag == name
}

func (consumer *Consumer) observePendingCancellation(cancellations <-chan string) {
	select {
	case tag, open := <-cancellations:
		if open && matchingConsumerCancellation(tag, consumer.config.Name) {
			consumer.observe(Observation{
				Kind: ObservationConsumerCancellation, Outcome: ObservationCancelled,
			})
		}
	default:
	}
}

func (consumer *Consumer) handle(envelope consumerEnvelope) {
	handlerContext, cancel := context.WithTimeout(consumer.lifetimeContext, consumer.config.HandlerTimeout)
	requested, err := consumer.handler(handlerContext, envelope.delivery)
	contextErr := handlerContext.Err()
	cancel()
	if invalidConsumerHandlerOutcome(err, contextErr, requested) {
		consumer.observe(Observation{Kind: ObservationHandlerFailure, Outcome: ObservationHandlerFailed})
		requested = consumer.config.Failure
	}
	if delegatedSettlement(requested.Method) {
		envelope.generation.delegated.Store(true)
		envelope.delivery.completeSettlement(ErrSettlementResultUnavailable)
		return
	}
	requested = boundedSettlement(envelope.delivery, requested, consumer.config)
	settlementErr := consumer.settle(envelope.generation, envelope.tag, requested)
	settlementResult := settlementErr
	if consumerOperationFailed(settlementResult) {
		settlementResult = ErrConsumerUnavailable
	}
	envelope.delivery.completeSettlement(settlementResult)
	if consumerOperationFailed(settlementErr) {
		consumer.failGeneration(envelope.generation)
	}
}

func invalidConsumerHandlerOutcome(handlerErr, contextErr error, requested Settlement) bool {
	return handlerErr != nil || contextErr != nil || requested.Validate() != nil
}

func delegatedSettlement(method SettlementMethod) bool {
	return method == SettlementDelegate
}

func consumerOperationFailed(err error) bool {
	return err != nil
}

func (consumer *Consumer) settle(generation *consumerGeneration, tag uint64, settlement Settlement) error {
	settled := make(chan error, 1)
	go func() { settled <- applySettlement(generation.channel, tag, settlement) }()
	ctx, cancel := context.WithTimeout(consumer.lifetimeContext, consumer.config.HandlerTimeout)
	defer cancel()
	select {
	case err := <-settled:
		if err == nil {
			outcome := ObservationRejected
			switch settlement.Method {
			case SettlementAcknowledge:
				outcome = ObservationAcknowledged
			case SettlementNegativeAcknowledge:
				outcome = ObservationNegativeAcknowledged
			}
			consumer.observe(Observation{Kind: ObservationSettlement, Outcome: outcome})
			if isAcknowledgement(settlement.Method) {
				consumer.observe(Observation{Kind: ObservationAcknowledgement, Outcome: outcome})
			}
		}
		return err
	case <-ctx.Done():
		_ = consumer.closeGeneration(generation, deadlineFor(ctx, consumer.config.HandlerTimeout))
		return ErrConsumerUnavailable
	}
}

func isAcknowledgement(method SettlementMethod) bool {
	return method == SettlementAcknowledge
}

func applySettlement(channel consumerChannel, tag uint64, settlement Settlement) error {
	switch settlement.Method {
	case SettlementAcknowledge:
		return channel.Ack(tag, false)
	case SettlementNegativeAcknowledge:
		return channel.Nack(tag, false, settlement.Requeue)
	case SettlementReject:
		return channel.Reject(tag, settlement.Requeue)
	default:
		return ErrInvalidSettlement
	}
}

func (consumer *Consumer) failGeneration(generation *consumerGeneration) {
	consumer.stateMu.Lock()
	current := consumer.generation == generation
	stopping := consumer.stopping
	if current && stopping && consumer.drainErr == nil {
		consumer.drainErr = ErrConsumerUnavailable
	}
	changed := consumerRecoveryAllowed(current, stopping, consumer.terminalErr, consumer.recovering)
	if changed {
		consumer.recovering = true
	}
	consumer.stateMu.Unlock()
	if !consumerGenerationCanSignal(current, stopping) {
		return
	}
	if changed {
		consumer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationRecovering})
	}
	select {
	case generation.failure <- struct{}{}:
	default:
	}
}

func (consumer *Consumer) beginRecovery() {
	consumer.stateMu.Lock()
	changed := consumerRecoveryAllowed(true, consumer.stopping, consumer.terminalErr, consumer.recovering)
	if changed {
		consumer.recovering = true
	}
	consumer.stateMu.Unlock()
	if changed {
		consumer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationRecovering})
	}
}

func consumerRecoveryAllowed(current, stopping bool, terminal error, recovering bool) bool {
	return current && !stopping && terminal == nil && !recovering
}

func consumerGenerationCanSignal(current, stopping bool) bool {
	return current && !stopping
}

func (consumer *Consumer) finishRecovery() {
	consumer.stateMu.Lock()
	consumer.recovering = false
	consumer.stateMu.Unlock()
	consumer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationRecovered})
}

func (consumer *Consumer) isStopping() bool {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumer.stopping
}

func (consumer *Consumer) setTerminalError(err error) {
	consumer.stateMu.Lock()
	changed := consumerTerminalUnset(consumer.terminalErr)
	if changed {
		consumer.terminalErr = err
	}
	consumer.stateMu.Unlock()
	if changed {
		consumer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationUnavailable})
	}
}

func consumerTerminalUnset(err error) bool {
	return err == nil
}

func (consumer *Consumer) setResourceError(err error) {
	if err == nil {
		return
	}
	consumer.stateMu.Lock()
	consumer.resourceErr = ErrConsumerUnavailable
	consumer.stateMu.Unlock()
}

func (consumer *Consumer) drainError() error {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	if consumer.drainErr != nil || consumer.resourceErr != nil {
		return ErrConsumerUnavailable
	}
	return nil
}

func (consumer *Consumer) currentGeneration() *consumerGeneration {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumer.generation
}

// Done closes after broker intake stops and all admitted handlers return.
func (consumer *Consumer) Done() <-chan struct{} { return consumer.done }

// Err reports a sanitized unexpected terminal consumer failure.
func (consumer *Consumer) Err() error {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumer.terminalErr
}

// Pause stops new handler admission without cancelling the active broker
// consumer. Already admitted handlers continue through settlement. Up to the
// configured prefetch may be held unsettled until Resume. Pause is idempotent.
func (consumer *Consumer) Pause() error {
	consumer.admissionMu.Lock()
	defer consumer.admissionMu.Unlock()
	if consumer.closed() {
		return ErrConsumerClosed
	}
	if !consumer.paused {
		consumer.paused = true
		consumer.resume = make(chan struct{})
		consumer.signalAdmissionChanged()
	}
	return nil
}

// Resume permits handler admission after Pause. It is idempotent.
func (consumer *Consumer) Resume() error {
	consumer.admissionMu.Lock()
	defer consumer.admissionMu.Unlock()
	if consumer.closed() {
		return ErrConsumerClosed
	}
	if consumer.paused {
		consumer.paused = false
		close(consumer.resume)
		consumer.resume = nil
		consumer.signalAdmissionChanged()
	}
	return nil
}

func (consumer *Consumer) closed() bool {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumerLifecycleClosed(consumer.stopping, consumer.stopped, consumer.terminalErr != nil)
}

func consumerLifecycleClosed(stopping, stopped, terminal bool) bool {
	return stopping || stopped || terminal
}

func (consumer *Consumer) signalAdmissionChanged() {
	select {
	case consumer.admissionChanged <- struct{}{}:
	default:
	}
}

// Drain cancels broker intake and waits for every delivery already received
// from the broker. It leaves the healthy owned connection open after complete
// settlement; delegated work or a drain deadline closes the connection.
func (consumer *Consumer) Drain(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	drainContext, cancelDrain := context.WithDeadline(ctx, deadlineFor(ctx, consumer.config.HandlerTimeout))
	defer cancelDrain()
	consumer.stateMu.Lock()
	firstDrain := !consumer.stopping
	consumer.stopping = true
	generation := consumer.generation
	consumer.stateMu.Unlock()
	if firstDrain {
		consumer.observe(Observation{Kind: ObservationShutdown, Outcome: ObservationShutdownStarted})
	}
	consumer.stopRecovery()
	if err := consumer.cancelGeneration(drainContext, generation); err != nil {
		consumer.stopLifetime()
		_ = consumer.closeGeneration(generation, deadlineFor(drainContext, consumer.config.HandlerTimeout))
		return err
	}
	select {
	case <-consumer.done:
		return consumer.drainError()
	case <-drainContext.Done():
		consumer.stopLifetime()
		_ = consumer.closeGeneration(generation, deadlineFor(drainContext, consumer.config.HandlerTimeout))
		return drainContext.Err()
	}
}

// Observations returns the bounded best-effort consumer event stream. It closes
// after broker intake and all admitted handlers stop.
func (consumer *Consumer) Observations() <-chan Observation {
	return consumer.observations.channel
}

func (consumer *Consumer) observe(observation Observation) {
	consumer.observations.emit(observation)
}

func (consumer *Consumer) cancelGeneration(ctx context.Context, generation *consumerGeneration) error {
	if generation == nil {
		return nil
	}
	if !consumerGenerationCanCancel(generation.closed.Load()) {
		return nil
	}
	generation.cancelOnce.Do(func() {
		cancelled := make(chan error, 1)
		go func() { cancelled <- generation.channel.Cancel(consumer.config.Name, false) }()
		select {
		case err := <-cancelled:
			if err != nil {
				generation.cancelErr = ErrConsumerUnavailable
			}
		case <-ctx.Done():
			generation.cancelErr = ctx.Err()
		}
	})
	return generation.cancelErr
}

func consumerGenerationCanCancel(closed bool) bool {
	return !closed
}

// Close drains admitted handlers, then closes owned resources. If cancellation
// or the drain deadline fails, resources are still closed for redelivery.
func (consumer *Consumer) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	drainErr := consumer.Drain(ctx)
	consumer.setResourceError(consumer.closeGeneration(consumer.currentGeneration(), deadlineFor(ctx, consumer.config.HandlerTimeout)))
	if drainErr != nil {
		return drainErr
	}
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	return consumer.resourceErr
}

func (consumer *Consumer) closeGeneration(generation *consumerGeneration, deadline time.Time) error {
	if generation == nil {
		return nil
	}
	generation.closeOnce.Do(func() {
		generation.closed.Store(true)
		go func() {
			generation.closeErr = boundedCloseConsumerResources(generation.resource, generation.channel, deadline)
			close(generation.closeDone)
		}()
	})
	remaining := time.Until(deadline)
	if !positiveDuration(remaining) {
		return ErrConsumerUnavailable
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-generation.closeDone:
		return generation.closeErr
	case <-timer.C:
		return ErrConsumerUnavailable
	}
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
		failed = consumerCloseFailed(err)
	}
	if channel != nil {
		result := startConsumerClose(channel, deadline)
		err, completed := waitForConsumerClose(result, deadline)
		if !completed {
			return ErrConsumerUnavailable
		}
		failed = failed || consumerCloseFailed(err)
	}
	if failed {
		return ErrConsumerUnavailable
	}
	return nil
}

func consumerCloseFailed(err error) bool {
	return err != nil && !errors.Is(err, amqp.ErrClosed)
}

func startConsumerClose(closer io.Closer, deadline time.Time) <-chan error {
	result := make(chan error, 1)
	go func() { result <- closeWithDeadline(closer, deadline) }()
	return result
}

func waitForConsumerClose(result <-chan error, deadline time.Time) (error, bool) {
	remaining := time.Until(deadline)
	if !positiveDuration(remaining) {
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

func positiveDuration(duration time.Duration) bool {
	return duration > 0
}
