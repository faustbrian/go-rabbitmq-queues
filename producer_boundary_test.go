package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestProducerConfigRejectsUnboundedPolicy(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*ProducerConfig){
		"limits": func(config *ProducerConfig) { config.Limits = Limits{} },
		"limits above safety cap": func(config *ProducerConfig) {
			config.Limits.MaxPayloadBytes++
		},
		"zero outstanding": func(config *ProducerConfig) { config.MaxOutstanding = 0 },
		"too many":         func(config *ProducerConfig) { config.MaxOutstanding = MaxOutstandingConfirms + 1 },
		"zero timeout":     func(config *ProducerConfig) { config.PublishTimeout = 0 },
		"long timeout":     func(config *ProducerConfig) { config.PublishTimeout = maximumDialTimeout + time.Nanosecond },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := testProducerConfig()
			mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidBounds) {
				t.Fatalf("Validate() error = %v, want %v", err, ErrInvalidBounds)
			}
		})
	}
}

func TestProducerConstructorRejectsInvalidResources(t *testing.T) {
	t.Parallel()

	validChannel := newFakeProducerChannel()
	validResource := io.NopCloser(nilReader{})
	tests := map[string]struct {
		config   ProducerConfig
		session  string
		channel  producerChannel
		resource io.Closer
		want     error
	}{
		"config":        {ProducerConfig{}, "session", validChannel, validResource, ErrInvalidBounds},
		"session":       {testProducerConfig(), "", validChannel, validResource, ErrProducerUnavailable},
		"channel":       {testProducerConfig(), "session", nil, validResource, ErrProducerUnavailable},
		"resource":      {testProducerConfig(), "session", validChannel, nil, ErrProducerUnavailable},
		"confirm setup": {testProducerConfig(), "session", &fakeProducerChannel{confirmErr: errors.New("secret setup detail")}, validResource, ErrProducerUnavailable},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			producer, err := newProducerFromChannel(test.config, test.session, test.channel, test.resource)
			if producer != nil || !errors.Is(err, test.want) {
				t.Fatalf("newProducerFromChannel() = (%#v, %v), want nil and %v", producer, err, test.want)
			}
		})
	}
}

func TestProducerRejectsNilAndPreCancelledContexts(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	called := false
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		called = true
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-context", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	var missingContext context.Context
	if result, err := producer.Publish(missingContext, testPublication()); result.State != PublishNotSent || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("Publish(nil) = (%#v, %v), want not sent context error", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := producer.Publish(ctx, testPublication()); result.State != PublishNotSent || !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish(cancelled) = (%#v, %v), want not sent cancellation", result, err)
	}
	if called {
		t.Fatal("client publish ran for a context cancelled before transmission")
	}
}

func TestProducerBoundsOutstandingConfirmations(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	started := make(chan struct{})
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		channel.nextSequence()
		close(started)
		return nil
	}
	config := testProducerConfig()
	config.MaxOutstanding = 1
	producer, err := newProducerFromChannel(config, "session-bound", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	first := make(chan PublishResult, 1)
	go func() {
		result, _ := producer.Publish(context.Background(), testPublication())
		first <- result
	}()
	<-started
	result, err := producer.Publish(t.Context(), testPublication())
	if result.State != PublishNotSent || !errors.Is(err, ErrOutstandingConfirmLimit) {
		t.Fatalf("second Publish() = (%#v, %v), want bounded not sent", result, err)
	}
	channel.confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	if result := <-first; result.State != PublishConfirmed {
		t.Fatalf("first result = %#v, want confirmed", result)
	}
}

func TestProducerRejectsAlreadyAdmittedPublishAfterConcurrentSendFailure(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	publishCalls := 0
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		publishCalls++
		if publishCalls > 1 {
			return errors.New("reused delivery tag")
		}
		close(firstStarted)
		<-releaseFirst
		return errors.New("ambiguous send")
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-concurrent-failure", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	first := make(chan PublishResult, 1)
	second := make(chan PublishResult, 1)
	go func() {
		result, _ := producer.Publish(context.Background(), testPublication())
		first <- result
	}()
	<-firstStarted
	go func() {
		result, _ := producer.Publish(context.Background(), testPublication())
		second <- result
	}()
	deadline := time.Now().Add(time.Second)
	for {
		producer.stateMu.Lock()
		active := producer.active
		producer.stateMu.Unlock()
		if active == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second publish was not admitted")
		}
		time.Sleep(time.Microsecond)
	}
	close(releaseFirst)
	if result := <-first; result.State != PublishAmbiguous {
		t.Fatalf("first result = %#v, want ambiguous", result)
	}
	if result := <-second; result.State != PublishNotSent {
		t.Fatalf("second result = %#v, want unavailable not sent", result)
	}
	if publishCalls != 1 {
		t.Fatalf("client publish calls = %d, want no reused delivery tag", publishCalls)
	}
}

func TestProducerPublishTimeoutForcesBlockedTransmissionClosed(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		close(started)
		<-release
		return errors.New("connection closed")
	}
	resource := &deadlineTrackingCloser{onDeadline: func() { releaseOnce.Do(func() { close(release) }) }}
	config := testProducerConfig()
	config.PublishTimeout = time.Millisecond
	producer, err := newProducerFromChannel(config, "session-publish-timeout", channel, resource)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}

	fallback := time.AfterFunc(50*time.Millisecond, func() { releaseOnce.Do(func() { close(release) }) })
	defer fallback.Stop()
	result, publishErr := producer.Publish(context.Background(), testPublication())
	if resource.deadlineCalls == 0 {
		releaseOnce.Do(func() { close(release) })
	}
	if result.State != PublishAmbiguous || !errors.Is(publishErr, ErrPublishAmbiguous) || !errors.Is(publishErr, context.DeadlineExceeded) {
		t.Fatalf("Publish() = (%#v, %v), want bounded ambiguous deadline", result, publishErr)
	}
	if resource.deadlineCalls != 1 {
		t.Fatalf("deadline close calls = %d, want forced connection close", resource.deadlineCalls)
	}
	if result, err := producer.Publish(t.Context(), testPublication()); result.State != PublishNotSent || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("Publish() after forced timeout = (%#v, %v), want unavailable", result, err)
	}
	select {
	case <-started:
	default:
		t.Fatal("client transmission did not start")
	}
}

func TestProducerConstructorBoundsConfirmSetup(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	channel.confirm = func() error {
		<-release
		return errors.New("connection closed")
	}
	resource := &deadlineTrackingCloser{onDeadline: func() { releaseOnce.Do(func() { close(release) }) }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	producer, err := newProducerFromChannelWithContext(ctx, testProducerConfig(), "session-confirm-timeout", channel, resource)
	if resource.deadlineCalls == 0 {
		releaseOnce.Do(func() { close(release) })
	}
	if producer != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("newProducerFromChannelWithContext() = (%#v, %v), want unavailable", producer, err)
	}
	if resource.deadlineCalls != 1 || channel.closeCount() != 1 {
		t.Fatalf("forced setup cleanup = resource %d channel %d, want one each", resource.deadlineCalls, channel.closeCount())
	}
}

func TestProducerCloseDeadlineForcesActivePublishAndClosesOnce(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	started := make(chan struct{})
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		channel.nextSequence()
		close(started)
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-close", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}

	published := make(chan PublishResult, 1)
	go func() {
		result, _ := producer.Publish(context.Background(), testPublication())
		published <- result
	}()
	<-started
	short, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := producer.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() error = %v, want deadline", err)
	}
	if result := <-published; result.State != PublishAmbiguous {
		t.Fatalf("active publish state = %s, want ambiguous after forced close", result.State)
	}
	if err := producer.Close(t.Context()); err != nil {
		t.Fatalf("second Close(): %v", err)
	}
	if err := producer.Close(t.Context()); err != nil {
		t.Fatalf("third Close(): %v", err)
	}
	if calls := channel.closeCount(); calls != 1 {
		t.Fatalf("channel Close() calls = %d, want 1", calls)
	}
	result, err := producer.Publish(t.Context(), testPublication())
	if result.State != PublishNotSent || !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("publish after close = (%#v, %v), want closed not sent", result, err)
	}
	var missingContext context.Context
	if err := producer.Close(missingContext); !errors.Is(err, ErrContextRequired) {
		t.Fatalf("Close(nil) error = %v, want %v", err, ErrContextRequired)
	}
}

func TestProducerChannelClosureMakesOutstandingPublishAmbiguous(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	publishCalls := 0
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		publishCalls++
		channel.nextSequence()
		if publishCalls == 1 {
			close(channel.confirms)
			return nil
		}
		return errors.New("publish ran after terminal failure")
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-close", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	result, err := producer.Publish(t.Context(), testPublication())
	if result.State != PublishAmbiguous || !errors.Is(err, ErrPublishAmbiguous) {
		t.Fatalf("Publish() = (%#v, %v), want ambiguous", result, err)
	}
	result, err = producer.Publish(t.Context(), testPublication())
	if result.State != PublishNotSent || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("Publish() after terminal channel failure = (%#v, %v), want unavailable not sent", result, err)
	}
	if publishCalls != 1 {
		t.Fatalf("client publish calls = %d, want no call after terminal failure", publishCalls)
	}
}

func TestProducerMakesUncorrelatedReturnAmbiguous(t *testing.T) {
	t.Parallel()

	for name, headers := range map[string]amqp.Table{
		"missing token": {"other": "value"},
		"wrong type":    {publishTokenHeader: int64(1)},
		"unknown token": {publishTokenHeader: "other-session/1"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			channel := newFakeProducerChannel()
			channel.publish = func(_ context.Context, _ string, _ string, _ bool, _ bool, _ amqp.Publishing) error {
				sequence := channel.nextSequence()
				channel.returns <- amqp.Return{ReplyCode: 312, Headers: headers}
				channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
				return nil
			}
			producer, err := newProducerFromChannel(
				testProducerConfig(), "session-return", channel, io.NopCloser(nilReader{}),
			)
			if err != nil {
				t.Fatalf("construct producer: %v", err)
			}
			t.Cleanup(func() { closeProducerForTest(t, producer) })

			result, err := producer.Publish(t.Context(), testPublication())
			if !errors.Is(err, ErrPublishAmbiguous) || result.State != PublishAmbiguous {
				t.Fatal("uncorrelated mandatory return was not made ambiguous")
			}
			result, err = producer.Publish(t.Context(), testPublication())
			if !errors.Is(err, ErrProducerUnavailable) || result.State != PublishNotSent {
				t.Fatal("producer remained available after losing return correlation")
			}
		})
	}
}

func TestProducerSanitizesReturnedReason(t *testing.T) {
	t.Parallel()

	for name, reason := range map[string]string{
		"control":   "NO_ROUTE\nsecret",
		"oversized": strings.Repeat("x", 256),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			channel := newFakeProducerChannel()
			channel.publish = func(_ context.Context, exchange, key string, _ bool, _ bool, message amqp.Publishing) error {
				sequence := channel.nextSequence()
				channel.returns <- amqp.Return{ReplyCode: 312, ReplyText: reason, Exchange: exchange, RoutingKey: key, Headers: message.Headers}
				channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
				return nil
			}
			producer, err := newProducerFromChannel(testProducerConfig(), "session-return", channel, io.NopCloser(nilReader{}))
			if err != nil {
				t.Fatalf("construct producer: %v", err)
			}
			t.Cleanup(func() { closeProducerForTest(t, producer) })

			result, err := producer.Publish(t.Context(), testPublication())
			if !errors.Is(err, ErrPublishReturned) || result.State != PublishReturned || result.Return == nil || result.Return.Reason != "" {
				t.Fatalf("Publish() = (%#v, %v), want returned with sanitized reason", result, err)
			}
		})
	}
}

func TestProducerReturnsRegisteredRouteInsteadOfBrokerMetadata(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(_ context.Context, _ string, _ string, _ bool, _ bool, message amqp.Publishing) error {
		sequence := channel.nextSequence()
		channel.returns <- amqp.Return{
			ReplyCode: 312, ReplyText: "NO_ROUTE",
			Exchange: "broker\nmetadata", RoutingKey: strings.Repeat("r", DefaultLimits().MaxRoutingKeyBytes+1),
			Headers: message.Headers,
		}
		channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
		return nil
	}
	producer, err := newProducerFromChannel(
		testProducerConfig(), "session-return-route", channel, io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	publication := testPublication()
	result, err := producer.Publish(t.Context(), publication)
	if !errors.Is(err, ErrPublishReturned) || result.State != PublishReturned || result.Return == nil {
		t.Fatal("publication did not retain its mandatory-return outcome")
	}
	if result.Return.Exchange != publication.Exchange || result.Return.RoutingKey != publication.RoutingKey {
		t.Fatal("mandatory return exposed broker route metadata instead of the registered route")
	}
}

func TestProducerContinuesAfterReturnListenerCloses(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		sequence := channel.nextSequence()
		close(channel.returns)
		channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-returns-closed", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	result, err := producer.Publish(t.Context(), testPublication())
	if err != nil || result.State != PublishConfirmed {
		t.Fatalf("Publish() = (%#v, %v), want confirmed", result, err)
	}
}

func TestProducerDrainsReturnStreamsDeterministically(t *testing.T) {
	t.Parallel()

	producer := &Producer{observations: newObservationStream(ObservationProducer, observationBufferSize)}
	tracker := newPublishTracker(1)

	closed := make(chan amqp.Return)
	close(closed)
	if !producer.drainReturns(tracker, closed) {
		t.Fatal("closed return stream prevented confirmation processing")
	}
	if !producer.drainReturns(tracker, nil) {
		t.Fatal("absent return stream prevented confirmation processing")
	}

	uncorrelated := make(chan amqp.Return, 1)
	uncorrelated <- amqp.Return{Headers: amqp.Table{"other": "value"}}
	if producer.drainReturns(tracker, uncorrelated) {
		t.Fatal("uncorrelated mandatory return did not stop confirmation processing")
	}
}

func TestProducerGenerationStopsOnUncorrelatedMandatoryReturn(t *testing.T) {
	t.Parallel()

	producer := &Producer{
		eventsContext: context.Background(),
		observations:  newObservationStream(ObservationProducer, observationBufferSize),
	}
	returns := make(chan amqp.Return, 1)
	returns <- amqp.Return{Headers: amqp.Table{"other": "value"}}

	if !producer.runGenerationWith(returns, nil, nil, nil, newPublishTracker(1), nil, producer.drainReturns) {
		t.Fatal("uncorrelated mandatory return did not terminate the producer generation")
	}
	select {
	case observation := <-producer.Observations():
		if observation.Kind != ObservationReturn || observation.Outcome != ObservationReturned {
			t.Fatalf("return observation = %#v", observation)
		}
	default:
		t.Fatal("uncorrelated mandatory return was not observed")
	}
}

func TestProducerCloseSanitizesResourceFailures(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.closeErr = errors.New("channel secret")
	resource := &countingCloser{err: errors.New("connection secret")}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-close-error", channel, resource)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	if err := producer.Close(t.Context()); !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("Close() error = %v, want %v", err, ErrProducerUnavailable)
	}
	if resource.calls != 1 || channel.closeCount() != 1 {
		t.Fatalf("close calls = resource %d channel %d, want one each", resource.calls, channel.closeCount())
	}
}

func TestProducerCloseReportsEarlierGenerationCleanupFailure(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		channel.nextSequence()
		return errors.New("connection lost after transmission")
	}
	resource := &concurrentCountingCloser{err: errors.New("connection cleanup failed")}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-runtime-cleanup-error", channel, resource)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}

	result, publishErr := producer.Publish(t.Context(), testPublication())
	if result.State != PublishAmbiguous || !errors.Is(publishErr, ErrPublishAmbiguous) {
		t.Fatalf("Publish() = (%#v, %v), want ambiguous", result, publishErr)
	}
	waitForHealth(t, func() bool { return producer.Liveness() == LivenessFailed })

	if err := producer.Close(t.Context()); !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("Close() error = %v, want %v", err, ErrProducerUnavailable)
	}
}

func TestProducerCloseUsesCallerDeadlineForOwnedConnection(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	resource := &deadlineTrackingCloser{}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-close-deadline", channel, resource)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if resource.deadlineCalls != 1 || resource.closeCalls != 0 || resource.deadline.IsZero() {
		t.Fatalf("resource close = deadline calls %d plain calls %d deadline %s", resource.deadlineCalls, resource.closeCalls, resource.deadline)
	}
}

func TestProducerCloseForcesOwnedConnectionWhenDrainDeadlineExpires(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		close(started)
		<-release
		return errors.New("connection closed")
	}
	resource := &deadlineTrackingCloser{onDeadline: func() { releaseOnce.Do(func() { close(release) }) }}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-force-close", channel, resource)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	published := make(chan PublishResult, 1)
	go func() {
		result, _ := producer.Publish(context.Background(), testPublication())
		published <- result
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	startedClose := time.Now()
	closeErr := producer.Close(ctx)
	if elapsed := time.Since(startedClose); elapsed > 100*time.Millisecond {
		t.Fatalf("Close() elapsed = %s, want prompt forced close after deadline", elapsed)
	}
	if resource.deadlineCalls == 0 {
		releaseOnce.Do(func() { close(release) })
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want deadline", closeErr)
	}
	if resource.deadlineCalls != 1 {
		t.Fatalf("deadline close calls = %d, want forced connection close", resource.deadlineCalls)
	}
	if result := <-published; result.State != PublishAmbiguous {
		t.Fatalf("blocked publish result = %#v, want ambiguous", result)
	}
}

func TestPublishResultErrorRejectsInvalidState(t *testing.T) {
	t.Parallel()

	if err := publishResultError(PublishResult{State: PublishNotSent}); !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("publishResultError() = %v, want %v", err, ErrProducerUnavailable)
	}
}

func TestAMQPPublishingOwnsPayloadAndMapsStableHeaders(t *testing.T) {
	t.Parallel()

	priority := uint16(255)
	expiration := 1500 * time.Millisecond
	body := []byte("body")
	bytesHeader := []byte{1, 2}
	message := Message{
		Body: body, MessageID: "id", CorrelationID: "correlation",
		ReplyTo:     "rpc.responses",
		ContentType: "application/octet-stream", ContentEncoding: "identity",
		Type: "event", AppID: "orders", Timestamp: time.Unix(1, 0),
		Expiration: &expiration, Priority: &priority,
		Headers: []Header{
			StringHeader("string", "value"), BoolHeader("bool", true),
			Int64Header("integer", 42), BytesHeader("bytes", bytesHeader),
		},
	}
	publishing := amqpPublishing(message, DeliveryPersistent, "session/1")
	body[0] = 'X'
	bytesHeader[0] = 9
	message.Headers[3].Bytes[0] = 8

	if string(publishing.Body) != "body" || publishing.DeliveryMode != amqp.Persistent ||
		publishing.Priority != 255 || publishing.Expiration != "1500" ||
		publishing.MessageId != "id" || publishing.CorrelationId != "correlation" ||
		publishing.ReplyTo != "rpc.responses" ||
		publishing.ContentType != "application/octet-stream" || publishing.ContentEncoding != "identity" ||
		publishing.Type != "event" || publishing.AppId != "orders" || !publishing.Timestamp.Equal(time.Unix(1, 0)) {
		t.Fatal("AMQP property mapping did not preserve the bounded contract")
	}
	if publishing.Headers["string"] != "value" || publishing.Headers["bool"] != true ||
		publishing.Headers["integer"] != int64(42) || publishing.Headers[publishTokenHeader] != "session/1" {
		t.Fatal("AMQP header mapping did not preserve the bounded contract")
	}
	if got := publishing.Headers["bytes"].([]byte); got[0] != 1 {
		t.Fatal("byte header was aliased")
	}
}

func TestAMQPPublishingDistinguishesOmittedAndImmediateExpiration(t *testing.T) {
	t.Parallel()

	immediate := time.Duration(0)
	omitted := amqpPublishing(Message{}, DeliveryPersistent, "session/1")
	expiring := amqpPublishing(Message{Expiration: &immediate}, DeliveryPersistent, "session/2")

	if omitted.Expiration != "" || expiring.Expiration != "0" {
		t.Fatalf("AMQP expirations = (%q, %q), want omitted and immediate", omitted.Expiration, expiring.Expiration)
	}
}

func TestPublicationRejectsReservedDeliveryMetadata(t *testing.T) {
	t.Parallel()

	for _, header := range []Header{
		StringHeader(publishTokenHeader, "collision"),
		Int64Header(acquiredCountHeader, 1),
		Int64Header(deliveryCountHeader, 1),
		StringHeader(deathHeader, "spoofed"),
		StringHeader(firstDeathQueueHeader, "spoofed"),
		StringHeader(firstDeathReasonHeader, "spoofed"),
		StringHeader(firstDeathExchangeHeader, "spoofed"),
		StringHeader(lastDeathQueueHeader, "spoofed"),
		StringHeader(lastDeathReasonHeader, "spoofed"),
		StringHeader(lastDeathExchangeHeader, "spoofed"),
	} {
		header := header
		t.Run(header.Key, func(t *testing.T) {
			t.Parallel()

			publication := testPublication()
			publication.Message.Headers = append(publication.Message.Headers, header)
			if err := publication.Validate(DefaultLimits()); !errors.Is(err, ErrReservedHeader) {
				t.Fatalf("Validate() error = %v, want %v", err, ErrReservedHeader)
			}
		})
	}
}

type countingCloser struct {
	calls int
	err   error
}

func (closer *countingCloser) Close() error {
	closer.calls++
	return closer.err
}

type deadlineTrackingCloser struct {
	closeCalls    int
	deadlineCalls int
	deadline      time.Time
	onDeadline    func()
}

func (closer *deadlineTrackingCloser) Close() error {
	closer.closeCalls++
	return nil
}

func (closer *deadlineTrackingCloser) CloseDeadline(deadline time.Time) error {
	closer.deadlineCalls++
	closer.deadline = deadline
	if closer.onDeadline != nil {
		closer.onDeadline()
	}
	return nil
}
