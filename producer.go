package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const MaxOutstandingConfirms = 4096

// ProducerConfig bounds synchronous producer work and confirmation state.
type ProducerConfig struct {
	Limits         Limits
	MaxOutstanding int
	PublishTimeout time.Duration
}

// Validate rejects unbounded producer policy.
func (config ProducerConfig) Validate() error {
	if !config.Limits.valid() || config.MaxOutstanding < 1 ||
		config.MaxOutstanding > MaxOutstandingConfirms || config.PublishTimeout <= 0 ||
		config.PublishTimeout > maximumDialTimeout {
		return ErrInvalidBounds
	}
	return nil
}

type producerChannel interface {
	Confirm(bool) error
	NotifyReturn(chan amqp.Return) chan amqp.Return
	NotifyPublish(chan amqp.Confirmation) chan amqp.Confirmation
	GetNextPublishSeqNo() uint64
	PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error
	Close() error
}

// Producer owns one confirm-enabled AMQP channel and never creates consumers.
// Publish is safe for concurrent use. Close prevents new work, drains bounded
// active calls, and then releases the channel and connection resource.
type Producer struct {
	config        ProducerConfig
	session       string
	channel       producerChannel
	resource      io.Closer
	tracker       *publishTracker
	returns       <-chan amqp.Return
	confirms      <-chan amqp.Confirmation
	eventsContext context.Context
	stopEvents    context.CancelFunc
	eventsDone    chan struct{}

	publishMu    sync.Mutex
	stateMu      sync.Mutex
	closed       bool
	unavailable  bool
	active       int
	drained      chan struct{}
	drainedOnce  sync.Once
	resourceOnce sync.Once
	resourceErr  error
}

func newProducerFromChannel(
	config ProducerConfig,
	session string,
	channel producerChannel,
	resource io.Closer,
) (*Producer, error) {
	return newProducerFromChannelWithContext(context.Background(), config, session, channel, resource)
}

func newProducerFromChannelWithContext(
	ctx context.Context,
	config ProducerConfig,
	session string,
	channel producerChannel,
	resource io.Closer,
) (*Producer, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if invalidIdentity(session, 128) || channel == nil || resource == nil {
		return nil, ErrProducerUnavailable
	}
	setupContext, cancel := context.WithTimeout(ctx, config.PublishTimeout)
	defer cancel()
	confirmed := make(chan error, 1)
	go func() { confirmed <- channel.Confirm(false) }()
	var confirmErr error
	select {
	case confirmErr = <-confirmed:
	case <-setupContext.Done():
		_ = closeWithDeadline(resource, deadlineFor(setupContext, config.PublishTimeout))
		_ = channel.Close()
		<-confirmed
		return nil, ErrProducerUnavailable
	}
	if confirmErr != nil {
		_ = closeWithDeadline(resource, deadlineFor(setupContext, config.PublishTimeout))
		_ = channel.Close()
		return nil, ErrProducerUnavailable
	}
	eventsContext, stopEvents := context.WithCancel(context.Background())
	producer := &Producer{
		config:        config,
		session:       session,
		channel:       channel,
		resource:      resource,
		tracker:       newPublishTracker(config.MaxOutstanding),
		eventsContext: eventsContext,
		stopEvents:    stopEvents,
		eventsDone:    make(chan struct{}),
		drained:       make(chan struct{}),
	}
	producer.returns = channel.NotifyReturn(make(chan amqp.Return, config.MaxOutstanding))
	producer.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, config.MaxOutstanding))
	go producer.runEvents()

	return producer, nil
}

func deadlineFor(ctx context.Context, fallback time.Duration) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now().Add(fallback)
}

// Publish sends one publication and waits for its exact mandatory-return and
// confirmation outcome. A timeout after transmission is always ambiguous.
func (producer *Producer) Publish(ctx context.Context, publication Publication) (PublishResult, error) {
	if ctx == nil {
		return PublishResult{State: PublishNotSent}, ErrContextRequired
	}
	if err := publication.Validate(producer.config.Limits); err != nil {
		return PublishResult{State: PublishNotSent}, err
	}
	if err := producer.admit(); err != nil {
		return PublishResult{State: PublishNotSent}, err
	}
	defer producer.release()

	publishContext, cancel := context.WithTimeout(ctx, producer.config.PublishTimeout)
	defer cancel()
	select {
	case <-publishContext.Done():
		return PublishResult{State: PublishNotSent}, publishContext.Err()
	default:
	}

	producer.publishMu.Lock()
	if producer.isUnavailable() {
		producer.publishMu.Unlock()
		return PublishResult{State: PublishNotSent}, ErrProducerUnavailable
	}
	sequence := producer.channel.GetNextPublishSeqNo()
	token := producer.session + "/" + strconv.FormatUint(sequence, 10)
	attempt, err := producer.tracker.register(sequence, token)
	if err != nil {
		producer.publishMu.Unlock()
		return PublishResult{State: PublishNotSent}, err
	}
	sent := make(chan error, 1)
	go func() {
		sent <- producer.channel.PublishWithContext(
			publishContext,
			publication.Exchange,
			publication.RoutingKey,
			publication.Mandatory,
			false,
			amqpPublishing(publication.Message, publication.DeliveryMode, token),
		)
	}()
	var transmissionTimeout error
	select {
	case err = <-sent:
	case <-publishContext.Done():
		select {
		case err = <-sent:
		default:
			transmissionTimeout = publishContext.Err()
			producer.markUnavailable()
			producer.closeOwnedResources(deadlineFor(publishContext, producer.config.PublishTimeout))
			err = <-sent
		}
	}
	contextErr := publishContext.Err()
	preflightCancellation := err != nil && contextErr != nil && errors.Is(err, contextErr)
	if err != nil && !preflightCancellation {
		producer.markUnavailable()
	}
	producer.publishMu.Unlock()
	if err != nil {
		if preflightCancellation {
			producer.tracker.abandon(attempt.sequence, PublishNotSent)
			return PublishResult{State: PublishNotSent}, contextErr
		}
		publishErr := error(ErrPublishAmbiguous)
		if transmissionTimeout != nil {
			publishErr = errors.Join(ErrPublishAmbiguous, transmissionTimeout)
		}
		return producer.completePublishError(attempt, PublishAmbiguous, publishErr)
	}
	if transmissionTimeout != nil {
		return producer.completePublishError(
			attempt,
			PublishAmbiguous,
			errors.Join(ErrPublishAmbiguous, transmissionTimeout),
		)
	}

	return producer.waitForOutcome(publishContext, attempt)
}

func (producer *Producer) completePublishError(
	attempt *publishAttempt,
	state PublishState,
	err error,
) (PublishResult, error) {
	if producer.tracker.abandon(attempt.sequence, state) {
		return PublishResult{State: state}, err
	}
	result := <-attempt.outcome
	if result.State == state {
		return result, err
	}
	return result, publishResultError(result)
}

func (producer *Producer) admit() error {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	if producer.closed {
		return ErrProducerClosed
	}
	if producer.unavailable {
		return ErrProducerUnavailable
	}
	producer.active++
	return nil
}

func (producer *Producer) markUnavailable() {
	producer.stateMu.Lock()
	producer.unavailable = true
	producer.stateMu.Unlock()
}

func (producer *Producer) isUnavailable() bool {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	return producer.unavailable
}

func (producer *Producer) release() {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	producer.active--
	if producer.closed && producer.active == 0 {
		producer.drainedOnce.Do(func() { close(producer.drained) })
	}
}

func (producer *Producer) waitForOutcome(
	ctx context.Context,
	attempt *publishAttempt,
) (PublishResult, error) {
	select {
	case result := <-attempt.outcome:
		return result, publishResultError(result)
	default:
	}
	select {
	case result := <-attempt.outcome:
		return result, publishResultError(result)
	case <-ctx.Done():
		select {
		case result := <-attempt.outcome:
			return result, publishResultError(result)
		default:
		}
		if producer.tracker.abandon(attempt.sequence, PublishAmbiguous) {
			return PublishResult{State: PublishAmbiguous}, errors.Join(ErrPublishAmbiguous, ctx.Err())
		}
		result := <-attempt.outcome
		return result, publishResultError(result)
	}
}

func publishResultError(result PublishResult) error {
	switch result.State {
	case PublishConfirmed:
		return nil
	case PublishReturned:
		return ErrPublishReturned
	case PublishRejected:
		return ErrPublishRejected
	case PublishAmbiguous:
		return ErrPublishAmbiguous
	default:
		return ErrProducerUnavailable
	}
}

func (producer *Producer) runEvents() {
	defer close(producer.eventsDone)
	returns := producer.returns
	for {
		select {
		case returned, open := <-returns:
			if !open {
				returns = nil
				continue
			}
			producer.applyReturn(returned)
		case confirmation, open := <-producer.confirms:
			if !open {
				producer.markUnavailable()
				producer.tracker.failAll(PublishAmbiguous)
				return
			}
			producer.drainReturns(returns)
			producer.tracker.confirm(confirmation.DeliveryTag, confirmation.Ack)
		case <-producer.eventsContext.Done():
			producer.tracker.failAll(PublishAmbiguous)
			return
		}
	}
}

func (producer *Producer) drainReturns(returns <-chan amqp.Return) {
	for returns != nil {
		select {
		case returned, open := <-returns:
			if !open {
				return
			}
			producer.applyReturn(returned)
		default:
			return
		}
	}
}

func (producer *Producer) applyReturn(returned amqp.Return) {
	token, ok := returned.Headers[publishTokenHeader].(string)
	if !ok {
		return
	}
	reason := returned.ReplyText
	if len(reason) > 255 || containsControl(reason) {
		reason = ""
	}
	producer.tracker.returned(token, Return{
		Code:       returned.ReplyCode,
		Reason:     reason,
		Exchange:   returned.Exchange,
		RoutingKey: returned.RoutingKey,
	})
}

// Close prevents new publications, waits for active bounded calls, and closes
// owned AMQP resources. A drain deadline forces the owned connection closed,
// making any still-active publication ambiguous.
func (producer *Producer) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	producer.stateMu.Lock()
	producer.closed = true
	if producer.active == 0 {
		producer.drainedOnce.Do(func() { close(producer.drained) })
	}
	drained := producer.drained
	producer.stateMu.Unlock()
	select {
	case <-ctx.Done():
		producer.closeOwnedResources(producer.closeDeadline(ctx))
		return ctx.Err()
	case <-drained:
	}

	producer.closeOwnedResources(producer.closeDeadline(ctx))
	return producer.resourceErr
}

func (producer *Producer) closeDeadline(ctx context.Context) time.Time {
	return deadlineFor(ctx, producer.config.PublishTimeout)
}

func (producer *Producer) closeOwnedResources(deadline time.Time) {
	producer.resourceOnce.Do(func() {
		if err := closeWithDeadline(producer.resource, deadline); err != nil {
			producer.resourceErr = ErrProducerUnavailable
		}
		if err := producer.channel.Close(); err != nil {
			producer.resourceErr = ErrProducerUnavailable
		}
		producer.stopEvents()
		<-producer.eventsDone
	})
}

type deadlineCloser interface {
	CloseDeadline(time.Time) error
}

func closeWithDeadline(resource io.Closer, deadline time.Time) error {
	if resource, ok := resource.(deadlineCloser); ok {
		return resource.CloseDeadline(deadline)
	}
	return resource.Close()
}

func amqpPublishing(message Message, mode DeliveryMode, token string) amqp.Publishing {
	headers := make(amqp.Table, len(message.Headers)+1)
	for _, header := range message.Headers {
		switch header.Kind {
		case HeaderString:
			headers[header.Key] = header.String
		case HeaderBool:
			headers[header.Key] = header.Bool
		case HeaderInt64:
			headers[header.Key] = header.Int64
		case HeaderBytes:
			headers[header.Key] = append([]byte(nil), header.Bytes...)
		}
	}
	headers[publishTokenHeader] = token
	priority := uint8(0)
	if message.Priority != nil {
		priority = uint8(*message.Priority)
	}
	expiration := ""
	if message.Expiration > 0 {
		expiration = strconv.FormatInt(message.Expiration.Milliseconds(), 10)
	}
	return amqp.Publishing{
		Headers:         headers,
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    uint8(mode),
		Priority:        priority,
		CorrelationId:   message.CorrelationID,
		Expiration:      expiration,
		MessageId:       message.MessageID,
		Timestamp:       message.Timestamp,
		Type:            message.Type,
		AppId:           message.AppID,
		Body:            append([]byte(nil), message.Body...),
	}
}
