package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestProducerReconcilesReturnBeforePositiveConfirm(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(_ context.Context, exchange, key string, mandatory, _ bool, message amqp.Publishing) error {
		if !mandatory {
			t.Error("publish was not mandatory")
		}
		channel.returns <- amqp.Return{
			ReplyCode:  312,
			ReplyText:  "NO_ROUTE",
			Exchange:   exchange,
			RoutingKey: key,
			Headers:    message.Headers,
			MessageId:  message.MessageId,
			Body:       message.Body,
		}
		channel.confirms <- amqp.Confirmation{DeliveryTag: channel.nextSequence(), Ack: true}
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-a", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	result, err := producer.Publish(t.Context(), testPublication())
	if !errors.Is(err, ErrPublishReturned) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrPublishReturned)
	}
	if result.State != PublishReturned || result.Return == nil || result.Return.Code != 312 ||
		result.Return.Exchange != "events" || result.Return.RoutingKey != "orders.created" {
		t.Fatalf("Publish() result = %#v, want bounded mandatory return", result)
	}
}

func TestProducerCorrelatesPositiveAndNegativeConfirms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		ack   bool
		state PublishState
		want  error
	}{
		{name: "confirmed", ack: true, state: PublishConfirmed},
		{name: "rejected", ack: false, state: PublishRejected, want: ErrPublishRejected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			channel := newFakeProducerChannel()
			channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
				channel.confirms <- amqp.Confirmation{DeliveryTag: channel.nextSequence(), Ack: test.ack}
				return nil
			}
			producer, err := newProducerFromChannel(testProducerConfig(), "session-a", channel, io.NopCloser(nilReader{}))
			if err != nil {
				t.Fatalf("construct producer: %v", err)
			}
			t.Cleanup(func() { closeProducerForTest(t, producer) })

			result, publishErr := producer.Publish(t.Context(), testPublication())
			if !errors.Is(publishErr, test.want) {
				t.Fatalf("Publish() error = %v, want %v", publishErr, test.want)
			}
			if result.State != test.state {
				t.Fatalf("Publish() state = %s, want %s", result.State, test.state)
			}
		})
	}
}

func TestProducerPublishesNativeFanoutRoutingWithoutInventingAKey(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(_ context.Context, exchange, key string, _ bool, _ bool, _ amqp.Publishing) error {
		if exchange != "events" || key != "" {
			t.Fatalf("publish route = (%q, %q), want native fanout route", exchange, key)
		}
		channel.confirms <- amqp.Confirmation{DeliveryTag: channel.nextSequence(), Ack: true}
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-a", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	publication := testPublication()
	publication.ExchangeKind = ExchangeFanout
	publication.RoutingKey = ""
	result, err := producer.Publish(t.Context(), publication)
	if err != nil || result.State != PublishConfirmed {
		t.Fatalf("Publish() = (%#v, %v), want confirmed fanout publication", result, err)
	}
}

func TestProducerPublishesToTheExplicitDefaultExchange(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(_ context.Context, exchange, key string, _ bool, _ bool, _ amqp.Publishing) error {
		if exchange != "" || key != "orders" {
			t.Fatalf("publish route = (%q, %q), want default exchange queue route", exchange, key)
		}
		channel.confirms <- amqp.Confirmation{DeliveryTag: channel.nextSequence(), Ack: true}
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-a", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	publication := testPublication()
	publication.Exchange = ""
	publication.ExchangeKind = ExchangeDirect
	publication.RoutingKey = "orders"
	result, err := producer.Publish(t.Context(), publication)
	if err != nil || result.State != PublishConfirmed {
		t.Fatalf("Publish() = (%#v, %v), want confirmed default-exchange publication", result, err)
	}
}

func TestProducerKeepsPostTransmissionCancellationAmbiguous(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	transmitted := make(chan struct{})
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		close(transmitted)
		return nil
	}
	config := testProducerConfig()
	config.PublishTimeout = 10 * time.Millisecond
	producer, err := newProducerFromChannel(config, "session-a", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	result, err := producer.Publish(t.Context(), testPublication())
	<-transmitted
	if !errors.Is(err, ErrPublishAmbiguous) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish() error = %v, want ambiguous deadline", err)
	}
	if result.State != PublishAmbiguous {
		t.Fatalf("Publish() state = %s, want %s", result.State, PublishAmbiguous)
	}
	channel.confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	select {
	case <-time.After(time.Millisecond):
	case <-producer.eventsDone:
		t.Fatal("late confirmation stopped the event loop")
	}
}

func TestProducerClassifiesPreflightAndTransmissionErrors(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	transmissionErr := errors.New("socket failed")
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		return transmissionErr
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-a", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	invalid := testPublication()
	invalid.Message.MessageID = ""
	result, err := producer.Publish(t.Context(), invalid)
	if result.State != PublishNotSent || !errors.Is(err, ErrMessageIDRequired) {
		t.Fatalf("invalid publish = (%#v, %v), want not sent validation", result, err)
	}
	result, err = producer.Publish(t.Context(), testPublication())
	if result.State != PublishAmbiguous || !errors.Is(err, ErrPublishAmbiguous) || errors.Is(err, transmissionErr) {
		t.Fatalf("failed transmission = (%#v, %v), want sanitized ambiguity", result, err)
	}
	result, err = producer.Publish(t.Context(), testPublication())
	if result.State != PublishNotSent || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("publish after ambiguous send = (%#v, %v), want terminal unavailable", result, err)
	}
}

func TestProducerPreservesDefinitiveOutcomeObservedBeforePublishError(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	var producer *Producer
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		sequence := channel.nextSequence()
		channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
		deadline := time.Now().Add(time.Second)
		for {
			producer.tracker.mu.Lock()
			_, outstanding := producer.tracker.sequence[sequence]
			producer.tracker.mu.Unlock()
			if !outstanding {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("confirmation was not processed")
			}
			time.Sleep(time.Microsecond)
		}
		return errors.New("socket closed after broker confirmation")
	}
	var err error
	producer, err = newProducerFromChannel(testProducerConfig(), "session-definitive", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	result, err := producer.Publish(t.Context(), testPublication())
	if err != nil || result.State != PublishConfirmed {
		t.Fatalf("Publish() = (%#v, %v), want observed confirmation", result, err)
	}
}

func TestProducerReportsCancellationBeforeClientTransmissionAsNotSent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	channel := newFakeProducerChannel()
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		cancel()
		return context.Canceled
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-a", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	result, err := producer.Publish(ctx, testPublication())
	if result.State != PublishNotSent || !errors.Is(err, context.Canceled) || errors.Is(err, ErrPublishAmbiguous) {
		t.Fatalf("Publish() = (%#v, %v), want cancelled not sent", result, err)
	}
}

func TestOpenProducerRotatesEndpointsAndCredentials(t *testing.T) {
	t.Parallel()

	credentialCalls := 0
	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		credentialCalls++
		return Credentials{Username: "publisher", Password: []byte("rotated")}, nil
	})
	channel := newFakeProducerChannel()
	dialCalls := 0
	dial := producerDialFunc(func(
		_ context.Context,
		endpoint Endpoint,
		_ ConnectionConfig,
		credentials Credentials,
	) (producerChannel, io.Closer, error) {
		dialCalls++
		if credentials.Username != "publisher" || string(credentials.Password) != "rotated" {
			t.Error("dial did not receive the current credential snapshot")
		}
		if dialCalls == 1 {
			if endpoint.Host != "rabbitmq.internal" {
				t.Errorf("first endpoint = %q", endpoint.Host)
			}
			return nil, nil, errors.New("unavailable")
		}
		if endpoint.Host != "rabbitmq-2.internal" {
			t.Errorf("second endpoint = %q", endpoint.Host)
		}
		return channel, io.NopCloser(nilReader{}), nil
	})

	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(),
		func() (string, error) { return "session-open", nil }, dial,
	)
	if err != nil {
		t.Fatalf("open producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })
	if credentialCalls != 2 || dialCalls != 2 {
		t.Fatalf("calls = credentials %d, dial %d; want 2 each", credentialCalls, dialCalls)
	}
}

func TestOpenProducerSanitizesSetupFailures(t *testing.T) {
	t.Parallel()

	providerErr := errors.New("source included a secret")
	connection := testConnectionConfig()
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		return Credentials{}, providerErr
	})
	dialCalled := false
	dial := producerDialFunc(func(
		context.Context,
		Endpoint,
		ConnectionConfig,
		Credentials,
	) (producerChannel, io.Closer, error) {
		dialCalled = true
		return nil, nil, errors.New("unexpected")
	})

	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(),
		func() (string, error) { return "session-open", nil }, dial,
	)
	if producer != nil || !errors.Is(err, ErrProducerUnavailable) || errors.Is(err, providerErr) {
		t.Fatalf("open result = (%#v, %v), want sanitized unavailable", producer, err)
	}
	if dialCalled {
		t.Fatal("dial ran after credential resolution failed")
	}
}

func TestOpenProducerBoundsCredentialResolutionWithDialBudget(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.DialTimeout = 50 * time.Millisecond
	observedBudget := time.Duration(0)
	connection.Credentials = CredentialProviderFunc(func(ctx context.Context) (Credentials, error) {
		if deadline, ok := ctx.Deadline(); ok {
			observedBudget = time.Until(deadline)
		}
		<-ctx.Done()
		return Credentials{}, ctx.Err()
	})
	parent, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	producer, err := openProducerWith(
		parent, connection, testProducerConfig(),
		func() (string, error) { return "session-open", nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			t.Fatal("dial ran after credential timeout")
			return nil, nil, nil
		},
	)
	if producer != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("open result = (%#v, %v), want unavailable", producer, err)
	}
	if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
		t.Fatalf("credential resolution used parent budget: %s", elapsed)
	}
	if observedBudget <= 0 || observedBudget > 100*time.Millisecond {
		t.Fatalf("credential budget = %s, want dial-scoped budget", observedBudget)
	}
}

func testProducerConfig() ProducerConfig {
	return ProducerConfig{
		Limits:         DefaultLimits(),
		MaxOutstanding: 8,
		PublishTimeout: time.Second,
	}
}

func testConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
		VirtualHost: "/events",
		Credentials: CredentialProviderFunc(func(context.Context) (Credentials, error) {
			return Credentials{Username: "publisher", Password: []byte("secret")}, nil
		}),
		TLS:         TLSConfig{ServerName: "rabbitmq.internal"},
		DialTimeout: time.Second,
		Heartbeat:   30 * time.Second,
		Recovery: RecoveryPolicy{
			MaxAttempts:  2,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
		},
	}
}

func testPublication() Publication {
	return Publication{
		Exchange:     "events",
		RoutingKey:   "orders.created",
		Mandatory:    true,
		DeliveryMode: DeliveryPersistent,
		Message: Message{
			Body:          []byte("payload"),
			MessageID:     "event-1",
			CorrelationID: "request-1",
			ContentType:   "application/json",
			Headers:       []Header{StringHeader("schema-version", "1")},
		},
	}
}

func closeProducerForTest(t *testing.T, producer *Producer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := producer.Close(ctx); err != nil {
		t.Errorf("close producer: %v", err)
	}
}

type fakeProducerChannel struct {
	mu         sync.Mutex
	sequence   uint64
	closeCalls int
	returns    chan amqp.Return
	confirms   chan amqp.Confirmation
	publish    func(context.Context, string, string, bool, bool, amqp.Publishing) error
	confirmErr error
	confirm    func() error
	closeErr   error
}

func newFakeProducerChannel() *fakeProducerChannel {
	return &fakeProducerChannel{sequence: 1}
}

func (channel *fakeProducerChannel) Confirm(bool) error {
	if channel.confirm != nil {
		return channel.confirm()
	}
	return channel.confirmErr
}

func (channel *fakeProducerChannel) NotifyReturn(listener chan amqp.Return) chan amqp.Return {
	channel.returns = listener
	return listener
}

func (channel *fakeProducerChannel) NotifyPublish(listener chan amqp.Confirmation) chan amqp.Confirmation {
	channel.confirms = listener
	return listener
}

func (channel *fakeProducerChannel) GetNextPublishSeqNo() uint64 {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.sequence
}

func (channel *fakeProducerChannel) nextSequence() uint64 {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	sequence := channel.sequence
	channel.sequence++
	return sequence
}

func (channel *fakeProducerChannel) PublishWithContext(
	ctx context.Context,
	exchange string,
	key string,
	mandatory bool,
	immediate bool,
	message amqp.Publishing,
) error {
	return channel.publish(ctx, exchange, key, mandatory, immediate, message)
}

func (channel *fakeProducerChannel) Close() error {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	channel.closeCalls++
	return channel.closeErr
}

func (channel *fakeProducerChannel) closeCount() int {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.closeCalls
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
