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

const (
	MaxOutstandingConfirms = 4096
	MaxPublishBatchSize    = 1024
)

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

// Producer owns one active confirm-enabled AMQP generation and never creates consumers.
// Publish is safe for concurrent use. Close prevents new work, drains bounded
// active calls, cancels recovery, and then releases the channel and connection resource.
type Producer struct {
	config            ProducerConfig
	session           string
	channel           producerChannel
	resource          io.Closer
	tracker           *publishTracker
	returns           <-chan amqp.Return
	confirms          <-chan amqp.Confirmation
	connectionClosed  <-chan *amqp.Error
	connectionBlocked <-chan amqp.Blocking
	generationClose   *sync.Once
	recovery          *producerRecovery
	eventsContext     context.Context
	stopEvents        context.CancelFunc
	eventsDone        chan struct{}
	failure           chan struct{}
	blockedEvents     chan ConnectionBlockedState
	observations      *observationStream

	publishMu    sync.Mutex
	stateMu      sync.Mutex
	closed       bool
	unavailable  bool
	recovering   bool
	terminal     bool
	stopped      bool
	blocked      bool
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
	return newProducerFromChannelWithRecovery(ctx, config, session, channel, resource, nil)
}

func newProducerFromChannelWithRecovery(
	ctx context.Context,
	config ProducerConfig,
	session string,
	channel producerChannel,
	resource io.Closer,
	recovery *producerRecovery,
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
	returns, confirms, connectionClosed, connectionBlocked, err := setupProducerChannel(ctx, config, channel, resource)
	if err != nil {
		return nil, ErrProducerUnavailable
	}
	eventsContext, stopEvents := context.WithCancel(context.Background())
	producer := &Producer{
		config:          config,
		session:         session,
		channel:         channel,
		resource:        resource,
		tracker:         newPublishTracker(config.MaxOutstanding),
		generationClose: &sync.Once{},
		recovery:        recovery,
		eventsContext:   eventsContext,
		stopEvents:      stopEvents,
		eventsDone:      make(chan struct{}),
		failure:         make(chan struct{}, 1),
		blockedEvents:   make(chan ConnectionBlockedState, 1),
		observations:    newObservationStream(ObservationProducer, observationBufferSize),
		drained:         make(chan struct{}),
	}
	producer.returns = returns
	producer.confirms = confirms
	producer.connectionClosed = connectionClosed
	producer.connectionBlocked = connectionBlocked
	producer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationConnected})
	go producer.runEvents(
		producer.returns,
		producer.confirms,
		producer.connectionClosed,
		producer.connectionBlocked,
		producer.tracker,
		producer.failure,
		producer.channel,
		producer.resource,
		producer.generationClose,
	)
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
	return producer.publishAdmitted(ctx, publication)
}

// PublishAsync admits one bounded publication and returns a channel that emits
// exactly one terminal outcome. Admission failures do not create goroutines.
func (producer *Producer) PublishAsync(ctx context.Context, publication Publication) (<-chan PublishOutcome, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if err := publication.Validate(producer.config.Limits); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if err := producer.admit(); err != nil {
		return nil, err
	}
	publication = ownPublication(publication)
	future := make(chan PublishOutcome, 1)
	go func() {
		defer producer.release()
		result, err := producer.publishAdmitted(ctx, publication)
		future <- PublishOutcome{Result: result, Err: err}
		close(future)
	}()
	return future, nil
}

func ownPublication(publication Publication) Publication {
	publication.Message.Body = append([]byte(nil), publication.Message.Body...)
	publication.Message.Headers = append([]Header(nil), publication.Message.Headers...)
	for index := range publication.Message.Headers {
		publication.Message.Headers[index].Bytes = append([]byte(nil), publication.Message.Headers[index].Bytes...)
	}
	if publication.Message.Priority != nil {
		priority := *publication.Message.Priority
		publication.Message.Priority = &priority
	}
	return publication
}

// PublishBatch validates the complete bounded batch before publishing each
// item. Outcomes preserve input order; the batch is not an atomic broker unit.
func (producer *Producer) PublishBatch(ctx context.Context, publications []Publication) ([]PublishOutcome, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if len(publications) == 0 || len(publications) > MaxPublishBatchSize {
		return nil, ErrInvalidBatch
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	for _, publication := range publications {
		if err := publication.Validate(producer.config.Limits); err != nil {
			return nil, errors.Join(ErrInvalidBatch, err)
		}
	}
	outcomes := make([]PublishOutcome, len(publications))
	for index, publication := range publications {
		result, err := producer.Publish(ctx, publication)
		outcomes[index] = PublishOutcome{Result: result, Err: err}
	}
	return outcomes, nil
}

func (producer *Producer) publishAdmitted(ctx context.Context, publication Publication) (result PublishResult, resultErr error) {
	var confirmationStarted time.Time
	defer func() { producer.observePublish(result, confirmationStarted) }()
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
	channel := producer.channel
	tracker := producer.tracker
	resource := producer.resource
	generationClose := producer.generationClose
	failure := producer.failure
	sequence := channel.GetNextPublishSeqNo()
	token := producer.session + "/" + strconv.FormatUint(sequence, 10)
	attempt, err := tracker.register(sequence, token, publication.Exchange, publication.RoutingKey)
	if err != nil {
		producer.publishMu.Unlock()
		return PublishResult{State: PublishNotSent}, err
	}
	sent := make(chan error, 1)
	confirmationStarted = time.Now()
	go func() {
		sent <- channel.PublishWithContext(
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
			producer.failGeneration(tracker, failure)
			_ = closeProducerGeneration(
				channel,
				resource,
				generationClose,
				deadlineFor(publishContext, producer.config.PublishTimeout),
			)
			err = <-sent
		}
	}
	contextErr := publishContext.Err()
	preflightCancellation := err != nil && contextErr != nil && errors.Is(err, contextErr)
	if err != nil && !preflightCancellation {
		producer.failGeneration(tracker, failure)
	}
	producer.publishMu.Unlock()
	if err != nil {
		if preflightCancellation {
			tracker.abandon(attempt.sequence, PublishNotSent)
			return PublishResult{State: PublishNotSent}, contextErr
		}
		publishErr := error(ErrPublishAmbiguous)
		if transmissionTimeout != nil {
			publishErr = errors.Join(ErrPublishAmbiguous, transmissionTimeout)
		}
		return producer.completePublishError(tracker, attempt, PublishAmbiguous, publishErr)
	}
	if transmissionTimeout != nil {
		return producer.completePublishError(
			tracker,
			attempt,
			PublishAmbiguous,
			errors.Join(ErrPublishAmbiguous, transmissionTimeout),
		)
	}

	return producer.waitForOutcome(publishContext, tracker, attempt)
}

func (producer *Producer) completePublishError(
	tracker *publishTracker,
	attempt *publishAttempt,
	state PublishState,
	err error,
) (PublishResult, error) {
	if tracker.abandon(attempt.sequence, state) {
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
	if producer.active >= producer.config.MaxOutstanding {
		producer.observe(Observation{Kind: ObservationBacklogPressure, Outcome: ObservationBacklogFull})
		return ErrOutstandingConfirmLimit
	}
	producer.active++
	return nil
}

func (producer *Producer) markRecovering() {
	producer.stateMu.Lock()
	changed := !producer.closed && !producer.recovering
	if changed {
		producer.unavailable = true
		producer.recovering = true
	}
	producer.stateMu.Unlock()
	if changed {
		producer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationRecovering})
	}
}

func (producer *Producer) finishRecovery(recovered bool) {
	producer.stateMu.Lock()
	producer.recovering = false
	terminal := !recovered && !producer.closed
	if terminal {
		producer.terminal = true
	}
	producer.stateMu.Unlock()
	if recovered {
		producer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationRecovered})
	} else if terminal {
		producer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationUnavailable})
	}
}

// failGeneration is called while publishMu owns the generation snapshot.
func (producer *Producer) failGeneration(tracker *publishTracker, failure chan<- struct{}) {
	if producer.tracker != tracker {
		return
	}
	producer.markRecovering()
	select {
	case failure <- struct{}{}:
	default:
	}
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
	tracker *publishTracker,
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
		if tracker.abandon(attempt.sequence, PublishAmbiguous) {
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

func (producer *Producer) runEvents(
	returns <-chan amqp.Return,
	confirms <-chan amqp.Confirmation,
	connectionClosed <-chan *amqp.Error,
	connectionBlocked <-chan amqp.Blocking,
	tracker *publishTracker,
	failure <-chan struct{},
	channel producerChannel,
	resource io.Closer,
	generationClose *sync.Once,
) {
	defer func() {
		producer.setBlocked(false)
		close(producer.blockedEvents)
		close(producer.eventsDone)
	}()
	for {
		if !producer.runGeneration(returns, confirms, connectionClosed, connectionBlocked, tracker, failure) {
			return
		}
		producer.setBlocked(false)
		producer.markRecovering()
		tracker.failAll(PublishAmbiguous)
		_ = closeProducerGeneration(channel, resource, generationClose, time.Now().Add(producer.config.PublishTimeout))
		select {
		case <-failure:
		default:
		}
		if !producer.recoverRuntime() {
			producer.finishRecovery(false)
			return
		}
		producer.finishRecovery(true)
		returns = producer.returns
		confirms = producer.confirms
		connectionClosed = producer.connectionClosed
		connectionBlocked = producer.connectionBlocked
		tracker = producer.tracker
		failure = producer.failure
		channel = producer.channel
		resource = producer.resource
		generationClose = producer.generationClose
	}
}

func (producer *Producer) runGeneration(
	returns <-chan amqp.Return,
	confirms <-chan amqp.Confirmation,
	connectionClosed <-chan *amqp.Error,
	connectionBlocked <-chan amqp.Blocking,
	tracker *publishTracker,
	failure <-chan struct{},
) bool {
	for {
		select {
		case returned, open := <-returns:
			if !open {
				returns = nil
				continue
			}
			if !producer.applyReturn(tracker, returned) {
				return true
			}
		case confirmation, open := <-confirms:
			if !open {
				return true
			}
			if !producer.drainReturns(tracker, returns) {
				return true
			}
			outcome := ObservationRejected
			if confirmation.Ack {
				outcome = ObservationConfirmed
			}
			producer.observe(Observation{Kind: ObservationConfirm, Outcome: outcome})
			tracker.confirm(confirmation.DeliveryTag, confirmation.Ack)
		case <-connectionClosed:
			return true
		case blocking, open := <-connectionBlocked:
			if !open {
				connectionBlocked = nil
				continue
			}
			producer.setBlocked(blocking.Active)
		case <-failure:
			return true
		case <-producer.eventsContext.Done():
			tracker.failAll(PublishAmbiguous)
			return false
		}
	}
}

// IsBlocked reports the latest sanitized RabbitMQ connection-blocked state.
func (producer *Producer) IsBlocked() bool {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	return producer.blocked
}

// BlockedNotifications emits coalesced sanitized state transitions. The
// channel closes when the producer lifecycle ends.
func (producer *Producer) BlockedNotifications() <-chan ConnectionBlockedState {
	return producer.blockedEvents
}

func (producer *Producer) setBlocked(active bool) {
	producer.stateMu.Lock()
	if producer.blocked == active {
		producer.stateMu.Unlock()
		return
	}
	producer.blocked = active
	producer.stateMu.Unlock()
	outcome := ObservationUnblocked
	if active {
		outcome = ObservationBlocked
	}
	producer.observe(Observation{Kind: ObservationConnectionBlocked, Outcome: outcome})
	state := ConnectionBlockedState{Active: active}
	select {
	case producer.blockedEvents <- state:
		return
	default:
	}
	select {
	case <-producer.blockedEvents:
	default:
	}
	select {
	case producer.blockedEvents <- state:
	default:
	}
}

func (producer *Producer) drainReturns(tracker *publishTracker, returns <-chan amqp.Return) bool {
	for returns != nil {
		select {
		case returned, open := <-returns:
			if !open {
				return true
			}
			if !producer.applyReturn(tracker, returned) {
				return false
			}
		default:
			return true
		}
	}
	return true
}

func (producer *Producer) applyReturn(tracker *publishTracker, returned amqp.Return) bool {
	producer.observe(Observation{Kind: ObservationReturn, Outcome: ObservationReturned})
	token, ok := returned.Headers[publishTokenHeader].(string)
	if !ok {
		return false
	}
	reason := returned.ReplyText
	if len(reason) > 255 || containsControl(reason) {
		reason = ""
	}
	return tracker.returned(token, Return{
		Code:   returned.ReplyCode,
		Reason: reason,
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
	firstClose := !producer.closed
	producer.closed = true
	if producer.active == 0 {
		producer.drainedOnce.Do(func() { close(producer.drained) })
	}
	drained := producer.drained
	producer.stateMu.Unlock()
	if firstClose {
		producer.observe(Observation{Kind: ObservationShutdown, Outcome: ObservationShutdownStarted})
	}
	select {
	case <-ctx.Done():
		producer.closeOwnedResources(producer.closeDeadline(ctx))
		producer.stateMu.Lock()
		producer.stopped = true
		producer.stateMu.Unlock()
		producer.observe(Observation{Kind: ObservationShutdown, Outcome: ObservationShutdownCompleted})
		producer.observations.close()
		return ctx.Err()
	case <-drained:
	}

	producer.closeOwnedResources(producer.closeDeadline(ctx))
	producer.stateMu.Lock()
	producer.stopped = true
	producer.stateMu.Unlock()
	producer.observe(Observation{Kind: ObservationShutdown, Outcome: ObservationShutdownCompleted})
	producer.observations.close()
	return producer.resourceErr
}

// Observations returns the bounded best-effort producer event stream. The
// stream closes after Close completes; terminal recovery alone does not release
// caller-owned observation consumption.
func (producer *Producer) Observations() <-chan Observation {
	return producer.observations.channel
}

func (producer *Producer) observe(observation Observation) {
	producer.observations.emit(observation)
}

func (producer *Producer) observePublish(result PublishResult, confirmationStarted time.Time) {
	outcome := publishObservationOutcome(result.State)
	producer.observe(Observation{Kind: ObservationPublish, Outcome: outcome})
	if result.State == PublishAmbiguous {
		producer.observe(Observation{Kind: ObservationAmbiguous, Outcome: ObservationAmbiguousOutcome})
	}
	if confirmationStarted.IsZero() || (result.State != PublishConfirmed && result.State != PublishRejected && result.State != PublishReturned) {
		return
	}
	producer.observe(Observation{
		Kind: ObservationConfirmationLatency, Outcome: outcome,
		Duration: time.Since(confirmationStarted),
	})
}

func (producer *Producer) closeDeadline(ctx context.Context) time.Time {
	return deadlineFor(ctx, producer.config.PublishTimeout)
}

func (producer *Producer) closeOwnedResources(deadline time.Time) {
	producer.resourceOnce.Do(func() {
		producer.stopEvents()
		if err := producer.closeCurrentGeneration(deadline); err != nil {
			producer.resourceErr = ErrProducerUnavailable
		}
		<-producer.eventsDone
	})
}

func (producer *Producer) closeCurrentGeneration(deadline time.Time) error {
	producer.stateMu.Lock()
	channel := producer.channel
	resource := producer.resource
	generationClose := producer.generationClose
	producer.stateMu.Unlock()
	return closeProducerGeneration(channel, resource, generationClose, deadline)
}

func closeProducerGeneration(
	channel producerChannel,
	resource io.Closer,
	once *sync.Once,
	deadline time.Time,
) error {
	if once == nil {
		return nil
	}
	var closeErr error
	once.Do(func() {
		if resource != nil {
			if err := closeWithDeadline(resource, deadline); err != nil {
				closeErr = ErrProducerUnavailable
			}
		}
		if channel != nil {
			if err := channel.Close(); err != nil {
				closeErr = ErrProducerUnavailable
			}
		}
	})
	return closeErr
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
		ReplyTo:         message.ReplyTo,
		Expiration:      expiration,
		MessageId:       message.MessageID,
		Timestamp:       message.Timestamp,
		Type:            message.Type,
		AppId:           message.AppID,
		Body:            append([]byte(nil), message.Body...),
	}
}
