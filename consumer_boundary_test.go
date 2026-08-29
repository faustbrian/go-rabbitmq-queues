package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConsumerConstructorRejectsInvalidAndFailedSetup(t *testing.T) {
	t.Parallel()

	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	channel := newFakeConsumerChannel()
	resource := &concurrentCountingCloser{}
	var missingContext context.Context
	if consumer, err := newConsumerFromChannel(missingContext, testConsumerConfig(), handler, channel, resource); consumer != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("nil context = (%#v, %v)", consumer, err)
	}
	if consumer, err := newConsumerFromChannel(t.Context(), ConsumerConfig{}, handler, channel, resource); consumer != nil || !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("invalid config = (%#v, %v)", consumer, err)
	}
	if consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), nil, channel, resource); consumer != nil || !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("nil handler = (%#v, %v)", consumer, err)
	}
	if consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), handler, nil, resource); consumer != nil || !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("nil channel = (%#v, %v)", consumer, err)
	}
	if consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), handler, channel, nil); consumer != nil || !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("nil resource = (%#v, %v)", consumer, err)
	}

	qosChannel := newFakeConsumerChannel()
	qosChannel.qosErr = errors.New("qos failed")
	qosResource := &countingCloser{}
	if consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), handler, qosChannel, qosResource); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("qos failure = (%#v, %v)", consumer, err)
	}
	if qosResource.calls != 1 || qosChannel.closeCount() != 1 {
		t.Fatalf("qos cleanup = resource %d channel %d", qosResource.calls, qosChannel.closeCount())
	}

	consumeChannel := newFakeConsumerChannel()
	consumeChannel.consumeErr = errors.New("consume failed")
	consumeResource := &countingCloser{}
	if consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), handler, consumeChannel, consumeResource); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("consume failure = (%#v, %v)", consumer, err)
	}
	if consumeResource.calls != 1 || consumeChannel.closeCount() != 1 {
		t.Fatalf("consume cleanup = resource %d channel %d", consumeResource.calls, consumeChannel.closeCount())
	}
}

func TestConsumerConstructorBoundsBlockedSetupAndRejectsNilDeliveryStream(t *testing.T) {
	t.Parallel()

	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	config := testConsumerConfig()
	config.HandlerTimeout = time.Millisecond

	blocked := newFakeConsumerChannel()
	blocked.qosBlock = make(chan struct{})
	resource := &concurrentCountingCloser{}
	if consumer, err := newConsumerFromChannel(t.Context(), config, handler, blocked, resource); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("blocked QOS = (%#v, %v), want unavailable", consumer, err)
	}
	close(blocked.qosBlock)
	waitForConsumerCondition(t, func() bool { return resource.count() == 1 && blocked.closeCount() == 1 })
	if resource.count() != 1 || blocked.closeCount() != 1 {
		t.Fatalf("blocked QOS cleanup = resource %d channel %d", resource.count(), blocked.closeCount())
	}

	blockedConsume := newFakeConsumerChannel()
	blockedConsume.consumeBlock = make(chan struct{})
	consumeResource := &concurrentCountingCloser{}
	if consumer, err := newConsumerFromChannel(t.Context(), config, handler, blockedConsume, consumeResource); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("blocked consume = (%#v, %v), want unavailable", consumer, err)
	}
	close(blockedConsume.consumeBlock)
	waitForConsumerCondition(t, func() bool { return consumeResource.count() == 1 && blockedConsume.closeCount() == 1 })
	if consumeResource.count() != 1 || blockedConsume.closeCount() != 1 {
		t.Fatalf("blocked consume cleanup = resource %d channel %d", consumeResource.count(), blockedConsume.closeCount())
	}

	nilStream := newFakeConsumerChannel()
	nilStream.deliveries = nil
	nilResource := &countingCloser{}
	if consumer, err := newConsumerFromChannel(t.Context(), config, handler, nilStream, nilResource); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("nil delivery stream = (%#v, %v), want unavailable", consumer, err)
	}
	if nilResource.calls != 1 || nilStream.closeCount() != 1 {
		t.Fatalf("nil delivery cleanup = resource %d channel %d", nilResource.calls, nilStream.closeCount())
	}
}

func TestTransientConsumerCleansEveryTopologySetupFailure(t *testing.T) {
	t.Parallel()

	config := testConsumerConfig()
	config.Queue = QueueReference{
		Type: QueueClassic,
		Transient: &TransientQueue{
			Exchange: Exchange{Name: "events", Kind: ExchangeFanout, Durable: true},
		},
	}
	for name, configure := range map[string]func(*fakeConsumerChannel){
		"exchange": func(channel *fakeConsumerChannel) { channel.exchangeErr = errors.New("exchange detail") },
		"declaration": func(channel *fakeConsumerChannel) {
			channel.declaredQueueName = "generated"
			channel.declareErr = errors.New("declaration detail")
		},
		"invalid generated name": func(channel *fakeConsumerChannel) { channel.declaredQueueName = "bad\nname" },
		"binding": func(channel *fakeConsumerChannel) {
			channel.declaredQueueName = "generated"
			channel.bindErr = errors.New("binding detail")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			channel := newFakeConsumerChannel()
			configure(channel)
			resource := &concurrentCountingCloser{}
			consumer, err := newConsumerFromChannel(
				t.Context(), config,
				func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
				channel, resource,
			)
			if consumer != nil || !errors.Is(err, ErrConsumerUnavailable) ||
				resource.count() != 1 || channel.closeCount() != 1 {
				t.Fatalf("setup failure = (%#v, %v), cleanup resource %d channel %d",
					consumer, err, resource.count(), channel.closeCount())
			}
		})
	}
}

func TestConsumerTreatsHandlerDeadlineAndInvalidSettlementAsFailure(t *testing.T) {
	t.Parallel()

	for name, handler := range map[string]DeliveryHandler{
		"deadline": func(ctx context.Context, _ Delivery) (Settlement, error) {
			<-ctx.Done()
			return Acknowledge(), nil
		},
		"invalid": func(context.Context, Delivery) (Settlement, error) {
			return Settlement{Method: SettlementMethod("unknown")}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			channel := newFakeConsumerChannel()
			config := testConsumerConfig()
			consumer, err := newConsumerFromChannel(t.Context(), config, handler, channel, io.NopCloser(nilReader{}))
			if err != nil {
				t.Fatalf("construct consumer: %v", err)
			}
			consumer.config.HandlerTimeout = time.Millisecond
			t.Cleanup(func() { closeConsumerForTest(t, consumer) })
			channel.deliveries <- testAMQPDelivery(10)
			if settled := <-channel.settled; settled.method != SettlementReject || settled.requeue {
				t.Fatalf("settlement = %#v, want failure reject", settled)
			}
		})
	}
}

func TestConsumerSupportsNackAndExplicitDelegation(t *testing.T) {
	t.Parallel()

	t.Run("nack", func(t *testing.T) {
		channel := newFakeConsumerChannel()
		consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
			return NegativeAcknowledge(false), nil
		}, channel, io.NopCloser(nilReader{}))
		if err != nil {
			t.Fatalf("construct consumer: %v", err)
		}
		t.Cleanup(func() { closeConsumerForTest(t, consumer) })
		channel.deliveries <- testAMQPDelivery(11)
		if settled := <-channel.settled; settled.method != SettlementNegativeAcknowledge || settled.requeue || settled.multiple {
			t.Fatalf("settlement = %#v, want single NACK", settled)
		}
	})

	t.Run("delegate", func(t *testing.T) {
		channel := newFakeConsumerChannel()
		consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
			return Delegate(), nil
		}, channel, io.NopCloser(nilReader{}))
		if err != nil {
			t.Fatalf("construct consumer: %v", err)
		}
		channel.deliveries <- testAMQPDelivery(12)
		select {
		case settled := <-channel.settled:
			t.Fatalf("delegated delivery was settled: %#v", settled)
		case <-time.After(time.Millisecond):
		}
		closeConsumerForTest(t, consumer)
	})
}

func TestConsumerSettlementFailureTerminatesWithoutLeakingError(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.ackErr = errors.New("sensitive settlement detail")
	resource := &countingCloser{}
	consumer, err := newConsumerFromChannel(t.Context(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}, channel, resource)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	channel.deliveries <- testAMQPDelivery(13)
	select {
	case <-consumer.Done():
	case <-time.After(time.Second):
		t.Fatal("consumer did not terminate after settlement failure")
	}
	if !errors.Is(consumer.Err(), ErrConsumerUnavailable) || errors.Is(consumer.Err(), channel.ackErr) {
		t.Fatalf("Err() = %v, want sanitized unavailable", consumer.Err())
	}
	if resource.calls != 1 || channel.closeCount() != 1 {
		t.Fatalf("terminal cleanup = resource %d channel %d, want one each before caller Close", resource.calls, channel.closeCount())
	}
	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestBoundedConsumerCleanupReturnsAfterExpiredDeadline(t *testing.T) {
	t.Parallel()

	resource := newBlockingCloser()
	channel := newBlockingCloser()
	if err := boundedCloseConsumerResources(resource, channel, time.Now()); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("boundedCloseConsumerResources() error = %v, want unavailable", err)
	}
	select {
	case <-resource.started:
	case <-time.After(time.Second):
		t.Fatal("resource close was not attempted")
	}
	select {
	case <-channel.started:
	case <-time.After(time.Second):
		t.Fatal("channel close was not attempted")
	}
	close(resource.release)
	close(channel.release)
}

type blockingCloser struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type concurrentCountingCloser struct {
	calls atomic.Int32
	err   error
}

func (closer *concurrentCountingCloser) Close() error {
	closer.calls.Add(1)
	return closer.err
}

func (closer *concurrentCountingCloser) count() int {
	return int(closer.calls.Load())
}

func newBlockingCloser() *blockingCloser {
	return &blockingCloser{started: make(chan struct{}), release: make(chan struct{})}
}

func (closer *blockingCloser) Close() error {
	closer.once.Do(func() { close(closer.started) })
	<-closer.release
	return nil
}

func waitForConsumerCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("consumer cleanup condition was not reached")
		}
		runtime.Gosched()
	}
}

func TestConsumerDrainDeadlineForcesBlockedCancellationClosed(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.cancelBlock = make(chan struct{})
	resource := &concurrentCountingCloser{}
	config := testConsumerConfig()
	consumer, err := newConsumerFromChannel(t.Context(), config, func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}, channel, resource)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	consumer.config.HandlerTimeout = 20 * time.Millisecond
	drained := make(chan error, 1)
	go func() { drained <- consumer.Drain(context.Background()) }()
	var drainErr error
	select {
	case drainErr = <-drained:
	case <-time.After(200 * time.Millisecond):
		close(channel.cancelBlock)
		<-drained
		t.Fatal("Drain() did not apply the configured shutdown bound")
	}
	if !errors.Is(drainErr, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", drainErr)
	}
	close(channel.cancelBlock)
	waitForConsumerCondition(t, func() bool { return resource.count() == 1 && channel.closeCount() == 1 })
	if resource.count() != 1 || channel.closeCount() != 1 || channel.cancelCount() != 1 {
		t.Fatalf("forced cleanup = resource %d channel %d cancel %d", resource.count(), channel.closeCount(), channel.cancelCount())
	}
}

func TestConsumerSettlementDeadlineTerminatesAndClosesResources(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.ackBlock = make(chan struct{})
	resource := &concurrentCountingCloser{}
	config := testConsumerConfig()
	consumer, err := newConsumerFromChannel(t.Context(), config, func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}, channel, resource)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	consumer.config.HandlerTimeout = 20 * time.Millisecond
	channel.deliveries <- testAMQPDelivery(14)
	select {
	case <-consumer.Done():
	case <-time.After(time.Second):
		t.Fatal("consumer did not terminate after settlement deadline")
	}
	close(channel.ackBlock)
	if !errors.Is(consumer.Err(), ErrConsumerUnavailable) {
		t.Fatalf("Err() = %v, want unavailable", consumer.Err())
	}
	waitForConsumerCondition(t, func() bool { return resource.count() == 1 && channel.closeCount() == 1 })
	if resource.count() != 1 || channel.closeCount() != 1 {
		t.Fatalf("settlement cleanup = resource %d channel %d", resource.count(), channel.closeCount())
	}
}

func TestOpenConsumerRetriesCredentialFailuresAndCleansPartialDial(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 2
	connection.Recovery.InitialDelay = time.Millisecond
	connection.Recovery.MaxDelay = time.Millisecond
	credentialCalls := 0
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		credentialCalls++
		if credentialCalls == 1 {
			return Credentials{}, errors.New("credential backend detail")
		}
		return Credentials{Username: "consumer", Password: []byte("secret")}, nil
	})
	partial := newFakeConsumerChannel()
	consumer, err := openConsumerWith(
		t.Context(), connection, testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			return partial, nil, errors.New("partial dial detail")
		},
	)
	if consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("openConsumerWith() = (%#v, %v), want unavailable", consumer, err)
	}
	if credentialCalls != 2 || partial.closeCount() != 1 {
		t.Fatalf("retry cleanup = credential calls %d channel closes %d", credentialCalls, partial.closeCount())
	}
}

func TestAMQPConsumerBoundaryOwnsChannelAndCleansIncompatibleChannel(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(time.Second)
	compatible := &fakeConsumerAMQPChannel{fakeConsumerChannel: newFakeConsumerChannel()}
	connection := &fakeAMQPConnection{channel: compatible}
	channel, resource, err := openAMQPConsumerConnectionWith(
		"amqps://rabbitmq.internal:5671", amqp.Config{}, deadline,
		func(string, amqp.Config) (amqpConnection, error) { return connection, nil },
	)
	if err != nil || channel != compatible || resource != connection {
		t.Fatalf("compatible channel = (%#v, %#v, %v)", channel, resource, err)
	}

	incompatible := newFakeProducerChannel()
	badConnection := &fakeAMQPConnection{channel: incompatible}
	channel, resource, err = openAMQPConsumerConnectionWith(
		"amqps://rabbitmq.internal:5671", amqp.Config{}, deadline,
		func(string, amqp.Config) (amqpConnection, error) { return badConnection, nil },
	)
	if channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) ||
		badConnection.closeCalls != 1 || incompatible.closeCount() != 1 {
		t.Fatalf("incompatible channel = (%#v, %#v, %v), connection closes %d channel closes %d", channel, resource, err, badConnection.closeCalls, incompatible.closeCount())
	}

	nilConnection := &fakeAMQPConnection{}
	channel, resource, err = openAMQPConsumerConnectionWith(
		"amqps://rabbitmq.internal:5671", amqp.Config{}, deadline,
		func(string, amqp.Config) (amqpConnection, error) { return nilConnection, nil },
	)
	if channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) || nilConnection.closeCalls != 1 {
		t.Fatalf("nil channel = (%#v, %#v, %v), connection closes %d", channel, resource, err, nilConnection.closeCalls)
	}
}

type fakeConsumerAMQPChannel struct {
	*fakeConsumerChannel
}

func (*fakeConsumerAMQPChannel) Confirm(bool) error { return nil }

func (*fakeConsumerAMQPChannel) NotifyReturn(listener chan amqp.Return) chan amqp.Return {
	return listener
}

func (*fakeConsumerAMQPChannel) NotifyPublish(listener chan amqp.Confirmation) chan amqp.Confirmation {
	return listener
}

func (*fakeConsumerAMQPChannel) GetNextPublishSeqNo() uint64 { return 1 }

func (*fakeConsumerAMQPChannel) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	return nil
}

func TestSettlementValidationRejectsInvalidFlagsAndMethods(t *testing.T) {
	t.Parallel()

	for _, settlement := range []Settlement{
		{Method: SettlementAcknowledge, Requeue: true},
		{Method: SettlementDelegate, Requeue: true},
		{Method: SettlementMethod("unknown")},
	} {
		if err := settlement.Validate(); !errors.Is(err, ErrInvalidSettlement) {
			t.Fatalf("Settlement%#v.Validate() = %v, want invalid", settlement, err)
		}
	}
	for _, settlement := range []Settlement{Acknowledge(), NegativeAcknowledge(true), Reject(true), Delegate()} {
		if err := settlement.Validate(); err != nil {
			t.Fatalf("Settlement%#v.Validate(): %v", settlement, err)
		}
	}
}

func TestDeliveryExpirationAndIntegerHeaderVariants(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		value any
		want  int64
	}{
		"int8":   {value: int8(-1), want: -1},
		"int16":  {value: int16(2), want: 2},
		"int32":  {value: int32(3), want: 3},
		"int64":  {value: int64(4), want: 4},
		"uint8":  {value: uint8(255), want: 255},
		"uint16": {value: uint16(65535), want: 65535},
		"uint32": {value: uint32(4294967295), want: 4294967295},
	} {
		t.Run(name, func(t *testing.T) {
			source := testAMQPDelivery(20)
			source.Expiration = "1500"
			source.Headers = amqp.Table{"integer": test.value}
			delivery, err := deliveryFromAMQP(source, testConsumerConfig())
			if err != nil {
				t.Fatalf("deliveryFromAMQP(): %v", err)
			}
			if delivery.Expiration == nil || *delivery.Expiration != 1500*time.Millisecond || len(delivery.Headers) != 1 ||
				delivery.Headers[0].Kind != HeaderInt64 || delivery.Headers[0].Int64 != test.want {
				t.Fatal("integer header was not normalized into the stable signed policy")
			}
		})
	}
	for _, expiration := range []string{"-1", "invalid", "9999999999999999999"} {
		source := testAMQPDelivery(21)
		source.Expiration = expiration
		if _, err := deliveryFromAMQP(source, testConsumerConfig()); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatalf("expiration %q error = %v, want invalid delivery", expiration, err)
		}
	}
}

func TestDeliveryExpirationDistinguishesOmittedAndImmediate(t *testing.T) {
	t.Parallel()

	omittedSource := testAMQPDelivery(22)
	omitted, err := deliveryFromAMQP(omittedSource, testConsumerConfig())
	if err != nil {
		t.Fatalf("convert omitted expiration: %v", err)
	}
	immediateSource := testAMQPDelivery(23)
	immediateSource.Expiration = "0"
	immediate, err := deliveryFromAMQP(immediateSource, testConsumerConfig())
	if err != nil {
		t.Fatalf("convert immediate expiration: %v", err)
	}

	if omitted.Expiration != nil || immediate.Expiration == nil || *immediate.Expiration != 0 {
		t.Fatalf("delivery expirations = (%v, %v), want omitted and immediate", omitted.Expiration, immediate.Expiration)
	}
}
