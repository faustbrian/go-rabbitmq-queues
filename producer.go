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
	if !config.Limits.valid() || !validProducerBounds(config.MaxOutstanding, config.PublishTimeout) {
		return ErrInvalidBounds
	}
	return nil
}

func validProducerBounds(maxOutstanding int, publishTimeout time.Duration) bool {
	return maxOutstanding >= 1 && maxOutstanding <= MaxOutstandingConfirms &&
		publishTimeout > 0 && publishTimeout <= maximumDialTimeout
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
	generationClose   *producerGenerationClose
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
	if !contextProvided(ctx) {
		return nil, ErrContextRequired
	}
	if err := config.Validate(); consumerOperationFailed(err) {
		return nil, err
	}
	if !producerInputsPresent(session, channel, resource) {
		return nil, ErrProducerUnavailable
	}
	returns, confirms, connectionClosed, connectionBlocked, err := setupProducerChannel(ctx, config, channel, resource)
	if consumerOperationFailed(err) {
		return nil, ErrProducerUnavailable
	}
	eventsContext, stopEvents := context.WithCancel(context.Background())
	producer := &Producer{
		config:          config,
		session:         session,
		channel:         channel,
		resource:        resource,
		tracker:         newPublishTracker(config.MaxOutstanding),
		generationClose: &producerGenerationClose{},
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

func contextProvided(ctx context.Context) bool {
	return ctx != nil
}

func producerInputsPresent(session string, channel producerChannel, resource io.Closer) bool {
	return !invalidIdentity(session, maxProducerSessionBytes) && channel != nil && resource != nil
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
	if !contextProvided(ctx) {
		return PublishResult{State: PublishNotSent}, ErrContextRequired
	}
	if err := publication.Validate(producer.config.Limits); consumerOperationFailed(err) {
		return PublishResult{State: PublishNotSent}, err
	}
	if err := producer.admit(); consumerOperationFailed(err) {
		return PublishResult{State: PublishNotSent}, err
	}
	defer producer.release()
	return producer.publishAdmitted(ctx, publication)
}

// PublishAsync admits one bounded publication and returns a channel that emits
// exactly one terminal outcome. Admission failures do not create goroutines.
func (producer *Producer) PublishAsync(ctx context.Context, publication Publication) (<-chan PublishOutcome, error) {
	if !contextProvided(ctx) {
		return nil, ErrContextRequired
	}
	if err := publication.Validate(producer.config.Limits); consumerOperationFailed(err) {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if err := producer.admit(); consumerOperationFailed(err) {
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
	if priorityPresent(publication.Message.Priority) {
		priority := *publication.Message.Priority
		publication.Message.Priority = &priority
	}
	if expirationPresent(publication.Message.Expiration) {
		expiration := *publication.Message.Expiration
		publication.Message.Expiration = &expiration
	}
	return publication
}

func priorityPresent(priority *uint16) bool {
	return priority != nil
}

func expirationPresent(expiration *time.Duration) bool {
	return expiration != nil
}

// PublishBatch validates the complete bounded batch before publishing each
// item. Outcomes preserve input order; the batch is not an atomic broker unit.
func (producer *Producer) PublishBatch(ctx context.Context, publications []Publication) ([]PublishOutcome, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if !validPublishBatchSize(len(publications)) {
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

func validPublishBatchSize(size int) bool {
	return size > 0 && size <= MaxPublishBatchSize
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
	preflightCancellation := isPreflightCancellation(err, contextErr)
	if publishFailureRequiresRecovery(err, preflightCancellation) {
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

func isPreflightCancellation(err, contextErr error) bool {
	return contextErr != nil && errors.Is(err, contextErr)
}

func publishFailureRequiresRecovery(err error, preflightCancellation bool) bool {
	return err != nil && !preflightCancellation
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
	if !producerCapacityAvailable(producer.active, producer.config.MaxOutstanding) {
		producer.observe(Observation{Kind: ObservationBacklogPressure, Outcome: ObservationBacklogFull})
		return ErrOutstandingConfirmLimit
	}
	producer.active = nextProducerActive(producer.active)
	return nil
}

func producerCapacityAvailable(active, maximum int) bool {
	return active < maximum
}

func nextProducerActive(active int) int {
	return active + 1
}

func (producer *Producer) markRecovering() {
	producer.stateMu.Lock()
	changed := producerRecoveryAllowed(producer.closed, producer.recovering)
	if changed {
		producer.unavailable = true
		producer.recovering = true
	}
	producer.stateMu.Unlock()
	if changed {
		producer.observe(Observation{Kind: ObservationConnectionState, Outcome: ObservationRecovering})
	}
}

func producerRecoveryAllowed(closed, recovering bool) bool {
	return !closed && !recovering
}

func (producer *Producer) finishRecovery(recovered bool) {
	producer.stateMu.Lock()
	producer.recovering = false
	terminal := producerRecoveryTerminal(recovered, producer.closed)
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

func producerRecoveryTerminal(recovered, closed bool) bool {
	return !recovered && !closed
}

// failGeneration is called while publishMu owns the generation snapshot.
func (producer *Producer) failGeneration(tracker *publishTracker, failure chan<- struct{}) {
	if producer.tracker != tracker {
		return
	}
	producer.markRecovering()
	_ = signalProducerFailure(failure)
}

func signalProducerFailure(failure chan<- struct{}) bool {
	select {
	case failure <- struct{}{}:
		return true
	default:
		return false
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
	producer.active = previousProducerActive(producer.active)
	if producerDrainComplete(producer.closed, producer.active) {
		producer.drainedOnce.Do(func() { close(producer.drained) })
	}
}

func producerDrainComplete(closed bool, active int) bool {
	return closed && active == 0
}

func previousProducerActive(active int) int {
	return active - 1
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
		return outcomeOrCancellation(tracker, attempt, ctx.Err())
	}
}

func outcomeOrCancellation(
	tracker *publishTracker,
	attempt *publishAttempt,
	contextErr error,
) (PublishResult, error) {
	select {
	case result := <-attempt.outcome:
		return result, publishResultError(result)
	default:
	}
	if tracker.abandon(attempt.sequence, PublishAmbiguous) {
		return PublishResult{State: PublishAmbiguous}, errors.Join(ErrPublishAmbiguous, contextErr)
	}
	result := <-attempt.outcome
	return result, publishResultError(result)
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
	generationClose *producerGenerationClose,
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
	return producer.runGenerationWith(
		returns, confirms, connectionClosed, connectionBlocked, tracker, failure, producer.drainReturns,
	)
}

func (producer *Producer) runGenerationWith(
	returns <-chan amqp.Return,
	confirms <-chan amqp.Confirmation,
	connectionClosed <-chan *amqp.Error,
	connectionBlocked <-chan amqp.Blocking,
	tracker *publishTracker,
	failure <-chan struct{},
	drainReturns func(*publishTracker, <-chan amqp.Return) bool,
) bool {
	for {
		select {
		case returned, open := <-returns:
			if !open {
				returns = nil
			} else if !producer.applyReturn(tracker, returned) {
				return true
			}
		case confirmation, open := <-confirms:
			if !open {
				return true
			}
			if !drainReturns(tracker, returns) {
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
			} else {
				producer.setBlocked(blocking.Active)
			}
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
	if !producerBlockedChanged(producer.blocked, active) {
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
	if offerBlockedState(producer.blockedEvents, state) {
		return
	}
	_, _ = takeBlockedState(producer.blockedEvents)
	_ = offerBlockedState(producer.blockedEvents, state)
}

func producerBlockedChanged(current, next bool) bool {
	return current != next
}

func offerBlockedState(channel chan<- ConnectionBlockedState, state ConnectionBlockedState) bool {
	select {
	case channel <- state:
		return true
	default:
		return false
	}
}

func takeBlockedState(channel <-chan ConnectionBlockedState) (ConnectionBlockedState, bool) {
	select {
	case state := <-channel:
		return state, true
	default:
		return ConnectionBlockedState{}, false
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
	reason := sanitizedReturnReason(returned.ReplyText)
	return tracker.returned(token, Return{
		Code:   returned.ReplyCode,
		Reason: reason,
	})
}

func sanitizedReturnReason(reason string) string {
	if len(reason) <= 255 && !containsControl(reason) {
		return reason
	}
	return ""
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
	if producerDrainComplete(true, producer.active) {
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
	if !shouldObserveConfirmationLatency(confirmationStarted, result.State) {
		return
	}
	producer.observe(Observation{
		Kind: ObservationConfirmationLatency, Outcome: outcome,
		Duration: time.Since(confirmationStarted),
	})
}

func shouldObserveConfirmationLatency(started time.Time, state PublishState) bool {
	if started.IsZero() {
		return false
	}
	switch state {
	case PublishConfirmed, PublishRejected, PublishReturned:
		return true
	default:
		return false
	}
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
	closeState *producerGenerationClose,
	deadline time.Time,
) error {
	if !producerCloseStatePresent(closeState) {
		return nil
	}
	closeState.once.Do(func() {
		if resource != nil {
			if err := closeWithDeadline(resource, deadline); err != nil {
				closeState.err = ErrProducerUnavailable
			}
		}
		if channel != nil {
			if err := channel.Close(); err != nil {
				closeState.err = ErrProducerUnavailable
			}
		}
	})
	return closeState.err
}

func producerCloseStatePresent(closeState *producerGenerationClose) bool {
	return closeState != nil
}

type producerGenerationClose struct {
	once sync.Once
	err  error
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
	headers := make(amqp.Table)
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
	if message.Expiration != nil {
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
