package rabbitmqqueue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestOpenProducerRecoversRuntimeLossWithFreshEndpointCredentialsAndSession(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	var credentialCalls atomic.Int32
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		generation := credentialCalls.Add(1)
		return Credentials{Username: fmt.Sprintf("publisher-%d", generation), Password: []byte("rotated")}, nil
	})
	var sessionCalls atomic.Int32
	session := func() (string, error) {
		return fmt.Sprintf("session-%d", sessionCalls.Add(1)), nil
	}

	first := newFakeProducerChannel()
	firstTransmitted := make(chan struct{}, 1)
	first.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		first.nextSequence()
		firstTransmitted <- struct{}{}
		return nil
	}
	firstResource := newFakeProducerConnectionEvents()
	second := newFakeProducerChannel()
	second.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		sequence := second.nextSequence()
		second.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
		return nil
	}
	secondResource := &concurrentCountingCloser{}
	type dialObservation struct {
		attempt  int
		endpoint Endpoint
		username string
	}
	dialed := make(chan dialObservation, 4)
	var dialCalls atomic.Int32
	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(), session,
		func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, credentials Credentials) (producerChannel, io.Closer, error) {
			attempt := int(dialCalls.Add(1))
			dialed <- dialObservation{attempt: attempt, endpoint: endpoint, username: credentials.Username}
			if attempt == 1 {
				return first, firstResource, nil
			}
			return second, secondResource, nil
		},
	)
	if err != nil {
		t.Fatalf("openProducerWith(): %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })
	if observation := <-dialed; observation.attempt != 1 || observation.endpoint != connection.Endpoints[0] || observation.username != "publisher-1" {
		t.Fatalf("initial dial = %#v", observation)
	}

	firstOutcome := make(chan PublishOutcome, 1)
	go func() {
		result, publishErr := producer.Publish(t.Context(), testPublication())
		firstOutcome <- PublishOutcome{Result: result, Err: publishErr}
	}()
	<-firstTransmitted
	firstResource.closed <- &amqp.Error{Code: 320, Reason: "sensitive broker detail"}
	if outcome := <-firstOutcome; outcome.Result.State != PublishAmbiguous || !errors.Is(outcome.Err, ErrPublishAmbiguous) {
		t.Fatalf("in-flight loss outcome = %#v, want ambiguous", outcome)
	}
	select {
	case observation := <-dialed:
		if observation.attempt != 2 || observation.endpoint != connection.Endpoints[1] || observation.username != "publisher-2" {
			t.Fatalf("runtime recovery dial = %#v", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime loss did not dial a recovery generation")
	}

	deadline := time.Now().Add(time.Second)
	for {
		result, publishErr := producer.Publish(t.Context(), testPublication())
		if !errors.Is(publishErr, ErrProducerUnavailable) {
			if publishErr != nil || result.State != PublishConfirmed {
				t.Fatalf("Publish() after recovery = (%#v, %v), want confirmed", result, publishErr)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("recovered generation did not become publishable")
		}
		time.Sleep(time.Millisecond)
	}
	if credentialCalls.Load() != 2 || sessionCalls.Load() != 2 {
		t.Fatalf("recovery snapshots = credentials %d sessions %d, want two each", credentialCalls.Load(), sessionCalls.Load())
	}
	if firstResource.count() != 1 {
		t.Fatalf("lost generation closes = %d, want one", firstResource.count())
	}
}

func TestProducerReportsSanitizedConnectionBlockedTransitions(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	resource := newFakeProducerConnectionEvents()
	producer, err := newProducerFromChannel(testProducerConfig(), "session-blocked", channel, resource)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	resource.blocked <- amqp.Blocking{Active: true, Reason: "sensitive broker detail"}
	select {
	case state := <-producer.BlockedNotifications():
		if !state.Active || !producer.IsBlocked() {
			t.Fatalf("blocked state = %#v snapshot %t", state, producer.IsBlocked())
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not report blocked connection")
	}
	resource.blocked <- amqp.Blocking{Active: false, Reason: "sensitive broker detail"}
	select {
	case state := <-producer.BlockedNotifications():
		if state.Active || producer.IsBlocked() {
			t.Fatalf("unblocked state = %#v snapshot %t", state, producer.IsBlocked())
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not report unblocked connection")
	}
}

func TestProducerCloseClearsBlockedSnapshot(t *testing.T) {
	t.Parallel()

	resource := newFakeProducerConnectionEvents()
	producer, err := newProducerFromChannel(
		testProducerConfig(),
		"session-blocked-close",
		newFakeProducerChannel(),
		resource,
	)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	resource.blocked <- amqp.Blocking{Active: true}
	select {
	case <-producer.BlockedNotifications():
	case <-time.After(time.Second):
		t.Fatal("producer did not report blocked connection")
	}
	if err := producer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if producer.IsBlocked() {
		t.Fatal("closed producer retained a blocked connection snapshot")
	}
}

func TestProducerRuntimeRecoveryExhaustionBecomesTerminal(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	resource := newFakeProducerConnectionEvents()
	dialed := make(chan int, 3)
	var dialCalls atomic.Int32
	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(),
		func() (string, error) { return fmt.Sprintf("session-%d", dialCalls.Load()+1), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			attempt := int(dialCalls.Add(1))
			dialed <- attempt
			if attempt == 1 {
				return newFakeProducerChannel(), resource, nil
			}
			return nil, nil, errors.New("broker unavailable")
		},
	)
	if err != nil {
		t.Fatalf("openProducerWith(): %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })
	<-dialed
	resource.closed <- &amqp.Error{Code: 320}
	for want := 2; want <= 3; want++ {
		select {
		case got := <-dialed:
			if got != want {
				t.Fatalf("recovery attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("runtime recovery did not reach bounded attempt %d", want)
		}
	}
	select {
	case _, open := <-producer.BlockedNotifications():
		if open {
			t.Fatal("blocked notification stream remained open after terminal recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not become terminal after bounded recovery")
	}
	if result, publishErr := producer.Publish(t.Context(), testPublication()); result.State != PublishNotSent || !errors.Is(publishErr, ErrProducerUnavailable) {
		t.Fatalf("Publish() after recovery exhaustion = (%#v, %v)", result, publishErr)
	}
}

func TestProducerCloseCancelsRuntimeRecoveryBackoff(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 3, InitialDelay: time.Second, MaxDelay: time.Second}
	resource := newFakeProducerConnectionEvents()
	recoveryStarted := make(chan struct{}, 1)
	var dialCalls atomic.Int32
	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(),
		func() (string, error) { return fmt.Sprintf("session-%d", dialCalls.Load()+1), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			if dialCalls.Add(1) == 1 {
				return newFakeProducerChannel(), resource, nil
			}
			recoveryStarted <- struct{}{}
			return nil, nil, errors.New("broker unavailable")
		},
	)
	if err != nil {
		t.Fatalf("openProducerWith(): %v", err)
	}
	resource.closed <- &amqp.Error{Code: 320}
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("runtime recovery did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := producer.Close(ctx); err != nil {
		t.Fatalf("Close() during recovery: %v", err)
	}
	if dialCalls.Load() != 2 {
		t.Fatalf("dial calls after cancelled recovery = %d, want two", dialCalls.Load())
	}
}

func TestProducerRecoveryDiscardsGenerationAfterClose(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	resource := &concurrentCountingCloser{}
	eventsContext, stopEvents := context.WithCancel(context.Background())
	defer stopEvents()
	producer := &Producer{
		config:        testProducerConfig(),
		eventsContext: eventsContext,
		observations:  newObservationStream(ObservationProducer, observationBufferSize),
		closed:        true,
		recovery: &producerRecovery{
			connection: testConnectionConfig(),
			session:    func() (string, error) { return "closed-recovery", nil },
			dial: func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
				return channel, resource, nil
			},
		},
	}

	if producer.recoverRuntime() {
		t.Fatal("closed producer installed a recovered generation")
	}
	if resource.count() != 1 || channel.closeCount() != 1 {
		t.Fatalf("discarded generation cleanup = resource %d channel %d, want one each", resource.count(), channel.closeCount())
	}
}

func TestProducerIgnoresFailureFromSupersededGeneration(t *testing.T) {
	t.Parallel()

	producer, err := newProducerFromChannel(
		testProducerConfig(),
		"session-stale-failure",
		newFakeProducerChannel(),
		io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	producer.publishMu.Lock()
	superseded := producer.tracker
	producer.tracker = newPublishTracker(producer.config.MaxOutstanding)
	producer.failGeneration(superseded, producer.failure)
	producer.publishMu.Unlock()
	if producer.isUnavailable() {
		t.Fatal("failure from superseded generation made current generation unavailable")
	}
	select {
	case <-producer.failure:
		t.Fatal("failure from superseded generation signaled the current lifecycle")
	default:
	}
}

func TestProducerCurrentGenerationFailureIsIndependentFromStaleSignal(t *testing.T) {
	t.Parallel()

	producer, err := newProducerFromChannel(
		testProducerConfig(),
		"session-independent-failure",
		newFakeProducerChannel(),
		io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	producer.publishMu.Lock()
	staleFailure := producer.failure
	currentTracker := newPublishTracker(producer.config.MaxOutstanding)
	currentFailure := make(chan struct{}, 1)
	producer.tracker = currentTracker
	producer.failure = currentFailure
	producer.stateMu.Lock()
	producer.unavailable = false
	producer.stateMu.Unlock()
	producer.publishMu.Unlock()
	staleFailure <- struct{}{}

	producer.publishMu.Lock()
	producer.failGeneration(currentTracker, currentFailure)
	producer.publishMu.Unlock()
	select {
	case <-currentFailure:
	default:
		t.Fatal("current generation failure was suppressed by stale signal")
	}
}

func TestProducerRecoveryOwnsMutableConnectionConfiguration(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	connection.TLS.RootCAs = [][]byte{[]byte("root-material")}
	connection.TLS.ClientCertificate = []byte("certificate-material")
	connection.TLS.ClientPrivateKey = []byte("private-key-material")
	firstResource := newFakeProducerConnectionEvents()
	type recoveryObservation struct {
		endpoint    Endpoint
		root        string
		certificate string
		privateKey  string
	}
	recovered := make(chan recoveryObservation, 1)
	var dialCalls atomic.Int32
	producer, err := openProducerWith(
		t.Context(),
		connection,
		testProducerConfig(),
		func() (string, error) { return fmt.Sprintf("session-%d", dialCalls.Load()+1), nil },
		func(_ context.Context, endpoint Endpoint, config ConnectionConfig, _ Credentials) (producerChannel, io.Closer, error) {
			if dialCalls.Add(1) == 1 {
				return newFakeProducerChannel(), firstResource, nil
			}
			recovered <- recoveryObservation{
				endpoint:    endpoint,
				root:        string(config.TLS.RootCAs[0]),
				certificate: string(config.TLS.ClientCertificate),
				privateKey:  string(config.TLS.ClientPrivateKey),
			}
			return newFakeProducerChannel(), &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("openProducerWith(): %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	connection.Endpoints[1].Host = "redirected.invalid"
	connection.TLS.RootCAs[0][0] = 'X'
	connection.TLS.ClientCertificate[0] = 'X'
	connection.TLS.ClientPrivateKey[0] = 'X'
	firstResource.closed <- &amqp.Error{Code: 320}
	select {
	case observation := <-recovered:
		if observation.endpoint.Host != "rabbitmq-2.internal" || observation.root != "root-material" ||
			observation.certificate != "certificate-material" || observation.privateKey != "private-key-material" {
			t.Fatalf("recovery configuration was aliased: %#v", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime recovery did not start")
	}
}

type fakeProducerConnectionEvents struct {
	concurrentCountingCloser
	closed  chan *amqp.Error
	blocked chan amqp.Blocking
}

func newFakeProducerConnectionEvents() *fakeProducerConnectionEvents {
	return &fakeProducerConnectionEvents{
		closed:  make(chan *amqp.Error, 1),
		blocked: make(chan amqp.Blocking, 2),
	}
}

func (resource *fakeProducerConnectionEvents) NotifyClose(chan *amqp.Error) chan *amqp.Error {
	return resource.closed
}

func (resource *fakeProducerConnectionEvents) NotifyBlocked(chan amqp.Blocking) chan amqp.Blocking {
	return resource.blocked
}
