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
		channel.exclusive || channel.noLocal || channel.noWait || len(channel.arguments) != 0 ||
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

func TestConsumerAcknowledgesFanoutDeliveryWithEmptyRoutingKey(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	handled := make(chan Delivery, 1)
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(_ context.Context, delivery Delivery) (Settlement, error) {
			handled <- delivery
			return Acknowledge(), nil
		},
		channel, io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })

	delivery := testAMQPDelivery(2)
	delivery.RoutingKey = ""
	delivery.Headers = amqp.Table{
		deathHeader: []any{amqp.Table{
			"count": int64(1), "reason": "rejected", "queue": "events",
			"exchange": "events", "routing-keys": []any{""},
			"time": time.Unix(100, 0),
		}},
	}
	channel.deliveries <- delivery

	settled := <-channel.settled
	if settled.method != SettlementAcknowledge || settled.tag != 2 {
		t.Fatalf("settlement = %#v, want ACK for fanout delivery", settled)
	}
	if received := <-handled; received.RoutingKey != "" || len(received.Deaths) != 1 ||
		len(received.Deaths[0].RoutingKeys) != 1 || received.Deaths[0].RoutingKeys[0] != "" {
		t.Fatalf("handler delivery = %#v, want native empty current and dead-letter routing keys", received)
	}
}

func TestConsumerMapsSignedPriorityAndClassicExclusivity(t *testing.T) {
	t.Parallel()

	priority := int32(-7)
	config := testConsumerConfig()
	config.Priority = &priority
	config.Exclusive = true
	channel := newFakeConsumerChannel()
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		channel, io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if !channel.exclusive || channel.noLocal || channel.noWait || len(channel.arguments) != 1 ||
		channel.arguments["x-priority"] != priority {
		t.Fatalf("consumer policy = exclusive %t no-local %t no-wait %t arguments %#v",
			channel.exclusive, channel.noLocal, channel.noWait, channel.arguments)
	}
}

func TestConsumerPreservesExplicitZeroPriority(t *testing.T) {
	t.Parallel()

	priority := int32(0)
	config := testConsumerConfig()
	config.Priority = &priority
	channel := newFakeConsumerChannel()
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		channel, io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if len(channel.arguments) != 1 || channel.arguments["x-priority"] != int32(0) {
		t.Fatalf("consumer arguments = %#v, want explicit zero priority", channel.arguments)
	}
}

func TestConsumerDeclaresAndConsumesClientOwnedTransientQueueOnOwnedGeneration(t *testing.T) {
	t.Parallel()

	config := testConsumerConfig()
	config.Queue = QueueReference{
		Type: QueueClassic,
		Transient: &TransientQueue{
			Exchange: Exchange{Name: "events", Kind: ExchangeFanout, Durable: true},
		},
	}
	channel := newFakeConsumerChannel()
	channel.declaredQueueName = "generated-events"
	resource := &concurrentCountingCloser{}
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		channel, resource,
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if channel.exchangeName != "events" || channel.exchangeKind != "fanout" || !channel.exchangeDurable ||
		channel.exchangeAutoDelete || channel.exchangeInternal || channel.declareQueue != "" ||
		channel.declareDurable || !channel.declareAutoDelete || !channel.declareExclusive ||
		channel.declareArguments["x-queue-type"] != "classic" || channel.bindQueue != "generated-events" ||
		channel.bindRoutingKey != "" || channel.bindExchange != "events" || len(channel.bindArguments) != 0 ||
		channel.queue != "generated-events" || resource.count() != 0 {
		t.Fatalf("transient consumer setup = channel %#v resource closes %d", channel, resource.count())
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

func TestConsumerIgnoresCancellationForAnotherTag(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		channel, io.NopCloser(nilReader{}),
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	channel.cancelNotifications <- "another-consumer"
	channel.deliveries <- testAMQPDelivery(90)
	select {
	case settled := <-channel.settled:
		if settled.method != SettlementAcknowledge || settled.tag != 90 {
			t.Fatalf("settlement = %#v, want ACK", settled)
		}
	case <-consumer.Done():
		t.Fatalf("unrelated cancellation terminated consumer: %v", consumer.Err())
	case <-time.After(time.Second):
		t.Fatal("consumer did not process delivery after unrelated cancellation")
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

func TestConsumerDrainSettlesDeliveryBufferedDuringPause(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	started := make(chan struct{}, 1)
	resource := &countingCloser{}
	consumer, err := newConsumerFromChannel(
		t.Context(),
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			started <- struct{}{}
			return Acknowledge(), nil
		},
		channel,
		resource,
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(): %v", err)
	}
	channel.deliveries <- testAMQPDelivery(52)
	for {
		select {
		case observation := <-consumer.Observations():
			if observation.Kind == ObservationDelivery {
				goto deliveryBuffered
			}
		case <-time.After(time.Second):
			t.Fatal("paused consumer did not buffer the delivery")
		}
	}

deliveryBuffered:
	if err := consumer.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(): %v", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("Drain() discarded the buffered delivery without handler admission")
	}
	select {
	case settled := <-channel.settled:
		if settled.method != SettlementAcknowledge || settled.tag != 52 {
			t.Fatalf("settlement = %#v, want ACK for buffered delivery", settled)
		}
	default:
		t.Fatal("Drain() returned before the buffered delivery settled")
	}
	if resource.calls != 0 || channel.closeCount() != 0 || channel.cancelCount() != 1 {
		t.Fatalf("drain resources = resource %d channel %d cancel %d", resource.calls, channel.closeCount(), channel.cancelCount())
	}
}

func TestConsumerPauseAndResumeTemporarilyStopsHandlerAdmission(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	started := make(chan struct{}, 1)
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			started <- struct{}{}
			return Acknowledge(), nil
		},
		channel,
		&countingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}

	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(): %v", err)
	}
	if err := consumer.Pause(); err != nil {
		t.Fatalf("idempotent Pause(): %v", err)
	}
	channel.deliveries <- testAMQPDelivery(51)
	select {
	case <-started:
		t.Fatal("paused consumer admitted a new handler")
	case <-time.After(20 * time.Millisecond):
	}
	if channel.cancelCount() != 0 || channel.closeCount() != 0 {
		t.Fatalf("pause changed generation ownership: cancel %d close %d", channel.cancelCount(), channel.closeCount())
	}

	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume(): %v", err)
	}
	if err := consumer.Resume(); err != nil {
		t.Fatalf("idempotent Resume(): %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("resumed consumer did not admit the pending delivery")
	}
	if settled := <-channel.settled; settled.method != SettlementAcknowledge || settled.tag != 51 {
		t.Fatalf("resumed settlement = %#v, want acknowledgement", settled)
	}

	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause() before close: %v", err)
	}
	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("Close() while paused: %v", err)
	}
	if err := consumer.Pause(); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Pause() after close error = %v, want %v", err, ErrConsumerClosed)
	}
	if err := consumer.Resume(); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Resume() after close error = %v, want %v", err, ErrConsumerClosed)
	}
}

func TestConsumerPauseAllowsAlreadyAdmittedHandlerToSettle(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			close(started)
			<-release
			return Acknowledge(), nil
		},
		channel,
		&countingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })

	channel.deliveries <- testAMQPDelivery(52)
	<-started
	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(): %v", err)
	}
	close(release)
	select {
	case settled := <-channel.settled:
		if settled.method != SettlementAcknowledge || settled.tag != 52 {
			t.Fatalf("admitted settlement = %#v, want acknowledgement", settled)
		}
	case <-time.After(time.Second):
		t.Fatal("paused consumer did not finish an admitted handler")
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
	mu                  sync.Mutex
	prefetch            int
	globalQOS           bool
	queue               string
	consumer            string
	autoAck             bool
	exclusive           bool
	noLocal             bool
	noWait              bool
	arguments           amqp.Table
	exchangeName        string
	exchangeKind        string
	exchangeDurable     bool
	exchangeAutoDelete  bool
	exchangeInternal    bool
	exchangeErr         error
	declareQueue        string
	declareDurable      bool
	declareAutoDelete   bool
	declareExclusive    bool
	declareArguments    amqp.Table
	declaredQueueName   string
	declareErr          error
	bindQueue           string
	bindRoutingKey      string
	bindExchange        string
	bindArguments       amqp.Table
	bindErr             error
	deliveries          chan amqp.Delivery
	settled             chan fakeConsumerSettlement
	onAck               func()
	cancelCalls         int
	cancelErr           error
	qosErr              error
	qosBlock            chan struct{}
	consumeErr          error
	consumeBlock        chan struct{}
	consumeCalled       chan struct{}
	consumeOnce         sync.Once
	cancelNotifications chan string
	ackErr              error
	ackBlock            chan struct{}
	cancelBlock         chan struct{}
	closeCalls          int
	closeErr            error
	cancelOnce          sync.Once
}

func newFakeConsumerChannel() *fakeConsumerChannel {
	return &fakeConsumerChannel{
		deliveries:          make(chan amqp.Delivery, 16),
		settled:             make(chan fakeConsumerSettlement, 16),
		consumeCalled:       make(chan struct{}),
		cancelNotifications: make(chan string, 1),
	}
}

func (channel *fakeConsumerChannel) NotifyCancel(listener chan string) chan string {
	channel.cancelNotifications = listener
	return listener
}

func (channel *fakeConsumerChannel) Qos(prefetchCount, _ int, global bool) error {
	channel.prefetch = prefetchCount
	channel.globalQOS = global
	if channel.qosBlock != nil {
		<-channel.qosBlock
	}
	return channel.qosErr
}

func (channel *fakeConsumerChannel) ExchangeDeclarePassive(
	name, kind string,
	durable, autoDelete, internal, _ bool,
	_ amqp.Table,
) error {
	channel.exchangeName = name
	channel.exchangeKind = kind
	channel.exchangeDurable = durable
	channel.exchangeAutoDelete = autoDelete
	channel.exchangeInternal = internal
	return channel.exchangeErr
}

func (channel *fakeConsumerChannel) QueueDeclare(
	name string,
	durable, autoDelete, exclusive, _ bool,
	arguments amqp.Table,
) (amqp.Queue, error) {
	channel.declareQueue = name
	channel.declareDurable = durable
	channel.declareAutoDelete = autoDelete
	channel.declareExclusive = exclusive
	channel.declareArguments = arguments
	return amqp.Queue{Name: channel.declaredQueueName}, channel.declareErr
}

func (channel *fakeConsumerChannel) QueueBind(
	name, key, exchange string,
	_ bool,
	arguments amqp.Table,
) error {
	channel.bindQueue = name
	channel.bindRoutingKey = key
	channel.bindExchange = exchange
	channel.bindArguments = arguments
	return channel.bindErr
}

func (channel *fakeConsumerChannel) Consume(
	queue, consumer string,
	autoAck, exclusive bool,
	noLocal bool,
	noWait bool,
	arguments amqp.Table,
) (<-chan amqp.Delivery, error) {
	channel.queue = queue
	channel.consumer = consumer
	channel.autoAck = autoAck
	channel.exclusive = exclusive
	channel.noLocal = noLocal
	channel.noWait = noWait
	channel.arguments = arguments
	channel.consumeOnce.Do(func() { close(channel.consumeCalled) })
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
