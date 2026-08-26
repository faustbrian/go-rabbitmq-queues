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

func TestConsumerConfiguresManualQOSAndAcknowledgesAfterHandler(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	var orderMu sync.Mutex
	order := []string{}
	channel.onAck = func() {
		orderMu.Lock()
		order = append(order, "ack")
		orderMu.Unlock()
	}
	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) {
		orderMu.Lock()
		order = append(order, "handler")
		orderMu.Unlock()
		return Acknowledge(), nil
	})
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), handler, channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if channel.prefetch != testConsumerConfig().Prefetch || channel.globalQOS || channel.autoAck ||
		channel.queue != "orders" || channel.consumer != "orders-worker" {
		t.Fatalf("consumer setup = %#v", channel)
	}

	channel.deliveries <- testAMQPDelivery(1)
	settled := <-channel.settled
	if settled.method != SettlementAcknowledge || settled.tag != 1 || settled.multiple || settled.requeue {
		t.Fatalf("settlement = %#v, want single ACK", settled)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) != 2 || order[0] != "handler" || order[1] != "ack" {
		t.Fatalf("execution order = %#v, want handler before ACK", order)
	}
}

func TestConsumerAppliesFailurePolicyWithoutRepublishing(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	handlerErr := errors.New("sensitive handler detail")
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Settlement{}, handlerErr
	}, channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })

	channel.deliveries <- testAMQPDelivery(2)
	settled := <-channel.settled
	if settled.method != SettlementReject || settled.tag != 2 || settled.requeue {
		t.Fatalf("failure settlement = %#v, want reject without republish", settled)
	}
}

func TestConsumerBoundsRequeueAndRejectsInvalidDelivery(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Reject(true), nil
	}, channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })

	redelivered := testAMQPDelivery(3)
	redelivered.Redelivered = true
	channel.deliveries <- redelivered
	if settled := <-channel.settled; settled.method != SettlementReject || settled.requeue {
		t.Fatalf("redelivery settlement = %#v, want terminal reject", settled)
	}
	invalid := testAMQPDelivery(4)
	invalid.Body = make([]byte, DefaultLimits().MaxPayloadBytes+1)
	channel.deliveries <- invalid
	if settled := <-channel.settled; settled.method != SettlementReject || settled.requeue {
		t.Fatalf("invalid delivery settlement = %#v, want terminal reject", settled)
	}
}

func TestConsumerCloseDrainsHandlerBeforeClosingResources(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	resource := &countingCloser{}
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		close(started)
		<-release
		return Acknowledge(), nil
	}, channel, resource)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	channel.deliveries <- testAMQPDelivery(5)
	<-started
	closed := make(chan error, 1)
	go func() { closed <- consumer.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close() returned before handler drained: %v", err)
	case <-time.After(time.Millisecond):
	}
	close(release)
	if settled := <-channel.settled; settled.method != SettlementAcknowledge {
		t.Fatalf("settlement = %#v, want ACK before close", settled)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if resource.calls != 1 || channel.closeCount() != 1 || channel.cancelCount() != 1 {
		t.Fatalf("close calls = resource %d channel %d cancel %d", resource.calls, channel.closeCount(), channel.cancelCount())
	}
	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("second Close(): %v", err)
	}
}

func TestConsumerUnexpectedCancellationBecomesTerminal(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}, channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	channel.cancelOnce.Do(func() { close(channel.deliveries) })
	select {
	case <-consumer.Done():
	case <-time.After(time.Second):
		t.Fatal("consumer did not terminate after broker cancellation")
	}
	if !errors.Is(consumer.Err(), ErrConsumerUnavailable) {
		t.Fatalf("Err() = %v, want %v", consumer.Err(), ErrConsumerUnavailable)
	}
	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestConsumerDrainStopsIntakeWithoutClosingOwnedResources(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	resource := &countingCloser{}
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}, channel, resource)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	if err := consumer.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(): %v", err)
	}
	if resource.calls != 0 || channel.closeCount() != 0 || channel.cancelCount() != 1 {
		t.Fatalf("drain resources = resource %d channel %d cancel %d", resource.calls, channel.closeCount(), channel.cancelCount())
	}
	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if resource.calls != 1 || channel.closeCount() != 1 || channel.cancelCount() != 1 {
		t.Fatalf("close resources = resource %d channel %d cancel %d", resource.calls, channel.closeCount(), channel.cancelCount())
	}
}

func TestConsumerCloseCleansResourcesAfterCancelFailure(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.cancelErr = errors.New("cancel failed")
	resource := &countingCloser{}
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}, channel, resource)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	if err := consumer.Close(t.Context()); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("Close() error = %v, want %v", err, ErrConsumerUnavailable)
	}
	if resource.calls != 1 || channel.closeCount() != 1 {
		t.Fatalf("cleanup calls = resource %d channel %d, want one each", resource.calls, channel.closeCount())
	}
}

func TestOpenConsumerRotatesEndpointsCredentialsAndOwnsConsumerOnlyChannel(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	credentialCalls := 0
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		credentialCalls++
		return Credentials{Username: "consumer", Password: []byte("rotated")}, nil
	})
	dials := 0
	channel := newFakeConsumerChannel()
	consumer, err := openConsumerWith(
		t.Context(), connection, testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, credentials Credentials) (consumerChannel, io.Closer, error) {
			dials++
			if credentials.Username != "consumer" || string(credentials.Password) != "rotated" {
				t.Fatal("dial did not receive refreshed credentials")
			}
			if dials == 1 {
				if endpoint.Host != "rabbitmq.internal" {
					t.Fatalf("first endpoint = %q", endpoint.Host)
				}
				return nil, nil, errors.New("unavailable")
			}
			if endpoint.Host != "rabbitmq-2.internal" {
				t.Fatalf("second endpoint = %q", endpoint.Host)
			}
			return channel, io.NopCloser(nilReader{}), nil
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if dials != 2 || credentialCalls != 2 {
		t.Fatalf("attempts = dials %d credentials %d, want two each", dials, credentialCalls)
	}
}

func TestOpenConsumerValidatesInputsBeforeDial(t *testing.T) {
	t.Parallel()

	dialed := false
	dial := consumerDialFunc(func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
		dialed = true
		return nil, nil, errors.New("unexpected")
	})
	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	if consumer, err := openConsumerWith(nil, testConnectionConfig(), testConsumerConfig(), handler, dial); consumer != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("nil context = (%#v, %v)", consumer, err)
	}
	if consumer, err := openConsumerWith(t.Context(), ConnectionConfig{}, testConsumerConfig(), handler, dial); consumer != nil || !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("invalid connection = (%#v, %v)", consumer, err)
	}
	if consumer, err := openConsumerWith(t.Context(), testConnectionConfig(), ConsumerConfig{}, handler, dial); consumer != nil || !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("invalid consumer = (%#v, %v)", consumer, err)
	}
	if consumer, err := openConsumerWith(t.Context(), testConnectionConfig(), testConsumerConfig(), nil, dial); consumer != nil || !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("nil handler = (%#v, %v)", consumer, err)
	}
	if consumer, err := openConsumerWith(t.Context(), testConnectionConfig(), testConsumerConfig(), handler, nil); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("nil dial = (%#v, %v)", consumer, err)
	}
	if dialed {
		t.Fatal("dial ran for invalid input")
	}
}

func testAMQPDelivery(tag uint64) amqp.Delivery {
	return amqp.Delivery{
		ConsumerTag: "orders-worker", DeliveryTag: tag,
		Exchange: "events", RoutingKey: "orders.created",
		Body: []byte("payload"), MessageId: "event-1", DeliveryMode: amqp.Persistent,
	}
}

func closeConsumerForTest(t *testing.T, consumer *Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := consumer.Close(ctx); err != nil {
		t.Errorf("close consumer: %v", err)
	}
}

type fakeConsumerSettlement struct {
	method   SettlementMethod
	tag      uint64
	multiple bool
	requeue  bool
}

type fakeConsumerChannel struct {
	mu           sync.Mutex
	prefetch     int
	globalQOS    bool
	queue        string
	consumer     string
	autoAck      bool
	deliveries   chan amqp.Delivery
	settled      chan fakeConsumerSettlement
	onAck        func()
	cancelCalls  int
	cancelErr    error
	qosErr       error
	qosBlock     chan struct{}
	consumeErr   error
	consumeBlock chan struct{}
	ackErr       error
	ackBlock     chan struct{}
	cancelBlock  chan struct{}
	closeCalls   int
	closeErr     error
	cancelOnce   sync.Once
}

func newFakeConsumerChannel() *fakeConsumerChannel {
	return &fakeConsumerChannel{
		deliveries: make(chan amqp.Delivery, 16),
		settled:    make(chan fakeConsumerSettlement, 16),
	}
}

func (channel *fakeConsumerChannel) Qos(prefetchCount, _ int, global bool) error {
	channel.prefetch = prefetchCount
	channel.globalQOS = global
	if channel.qosBlock != nil {
		<-channel.qosBlock
	}
	return channel.qosErr
}

func (channel *fakeConsumerChannel) Consume(
	queue, consumer string,
	autoAck, _ bool,
	_ bool,
	_ bool,
	_ amqp.Table,
) (<-chan amqp.Delivery, error) {
	channel.queue = queue
	channel.consumer = consumer
	channel.autoAck = autoAck
	if channel.consumeBlock != nil {
		<-channel.consumeBlock
	}
	return channel.deliveries, channel.consumeErr
}

func (channel *fakeConsumerChannel) Cancel(string, bool) error {
	channel.mu.Lock()
	channel.cancelCalls++
	channel.mu.Unlock()
	if channel.cancelBlock != nil {
		<-channel.cancelBlock
	}
	if channel.cancelErr == nil {
		channel.cancelOnce.Do(func() { close(channel.deliveries) })
	}
	return channel.cancelErr
}

func (channel *fakeConsumerChannel) Ack(tag uint64, multiple bool) error {
	if channel.ackBlock != nil {
		<-channel.ackBlock
	}
	if channel.onAck != nil {
		channel.onAck()
	}
	channel.settled <- fakeConsumerSettlement{method: SettlementAcknowledge, tag: tag, multiple: multiple}
	return channel.ackErr
}

func (channel *fakeConsumerChannel) Nack(tag uint64, multiple, requeue bool) error {
	channel.settled <- fakeConsumerSettlement{method: SettlementNegativeAcknowledge, tag: tag, multiple: multiple, requeue: requeue}
	return nil
}

func (channel *fakeConsumerChannel) Reject(tag uint64, requeue bool) error {
	channel.settled <- fakeConsumerSettlement{method: SettlementReject, tag: tag, requeue: requeue}
	return nil
}

func (channel *fakeConsumerChannel) Close() error {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	channel.closeCalls++
	return channel.closeErr
}

func (channel *fakeConsumerChannel) closeCount() int {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.closeCalls
}

func (channel *fakeConsumerChannel) cancelCount() int {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.cancelCalls
}
