package rabbitmqqueue

import (
	"context"
	"errors"
	"fmt"
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
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if closeErr := consumer.Close(ctx); closeErr != nil &&
					!errors.Is(closeErr, ErrConsumerUnavailable) && !errors.Is(closeErr, context.DeadlineExceeded) {
					t.Errorf("close failed consumer: %v", closeErr)
				}
			})
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

type deadlineProbeCloser struct {
	deadline chan time.Time
}

func (closer *deadlineProbeCloser) Close() error { return nil }

func (closer *deadlineProbeCloser) CloseDeadline(deadline time.Time) error {
	closer.deadline <- deadline
	return nil
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

func TestConsumerShutdownCallersHaveIndependentDeadlines(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	resource := &blockingCloser{started: make(chan struct{}), release: make(chan struct{})}
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			return Settlement{Method: SettlementAcknowledge}, nil
		},
		channel, resource,
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}

	first := make(chan error, 1)
	go func() { first <- consumer.Shutdown(t.Context()) }()
	<-resource.started

	short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	second := make(chan error, 1)
	go func() { second <- consumer.Shutdown(short) }()
	var secondErr error
	select {
	case secondErr = <-second:
	case <-time.After(250 * time.Millisecond):
		close(resource.release)
		t.Fatal("second Shutdown() blocked behind cleanup")
	}
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("second Shutdown() error = %v, want deadline", secondErr)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("second Shutdown() blocked for %s behind cleanup", elapsed)
	}

	close(resource.release)
	if err := <-first; err != nil {
		t.Fatalf("first Shutdown(): %v", err)
	}
	if err := consumer.Shutdown(t.Context()); err != nil {
		t.Fatalf("repeated Shutdown(): %v", err)
	}
}

func TestConsumerShutdownContinuesAfterCallerDeadline(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	started := make(chan struct{})
	config := testConsumerConfig()
	config.HandlerTimeout = 20 * time.Millisecond
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(ctx context.Context, _ Delivery) (Settlement, error) {
			close(started)
			<-ctx.Done()
			return Settlement{}, ctx.Err()
		},
		channel, &concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	channel.deliveries <- testAMQPDelivery(71)
	<-started

	short, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := consumer.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown(short) error = %v, want caller deadline", err)
	}
	if err := consumer.Shutdown(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown(after cleanup) error = %v, want internal drain deadline", err)
	}
	select {
	case <-consumer.Done():
	case <-time.After(time.Second):
		t.Fatal("cleanup did not continue to consumer termination")
	}
}

func TestConsumerShutdownMarksStoppingBeforeExpiredCallerReturns(t *testing.T) {
	t.Parallel()

	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		newFakeConsumerChannel(), &concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownErr := consumer.Shutdown(ctx)
	if !errors.Is(shutdownErr, context.Canceled) {
		t.Fatalf("Shutdown(cancelled) error = %v, want cancellation", shutdownErr)
	}
	if err := consumer.Pause(); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Pause() after Shutdown = %v, want %v", err, ErrConsumerClosed)
	}
	if err := consumer.Shutdown(t.Context()); err != nil {
		t.Fatalf("finish Shutdown(): %v", err)
	}
}

func TestConsumerShutdownDoesNotAdmitDeliveriesAfterReturning(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.cancelBlock = make(chan struct{})
	var releaseCancel sync.Once
	t.Cleanup(func() { releaseCancel.Do(func() { close(channel.cancelBlock) }) })
	firstRelease := make(chan struct{})
	handled := make(chan string, 3)
	config := testConsumerConfig()
	config.Prefetch = 1
	config.Concurrency = 1
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(_ context.Context, delivery Delivery) (Settlement, error) {
			handled <- delivery.MessageID
			if delivery.MessageID == "event-71" {
				<-firstRelease
			}
			return Acknowledge(), nil
		},
		channel, &concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	first := testAMQPDelivery(71)
	first.MessageId = "event-71"
	second := testAMQPDelivery(72)
	second.MessageId = "event-72"
	third := testAMQPDelivery(73)
	third.MessageId = "event-73"
	channel.deliveries <- first
	if messageID := <-handled; messageID != "event-71" {
		t.Fatalf("first handled message = %q", messageID)
	}
	channel.deliveries <- second
	channel.deliveries <- third
	waitForConsumerCondition(t, func() bool {
		return len(channel.deliveries) == 0 && len(consumer.jobs) == 1
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown(cancelled) error = %v, want cancellation", err)
	}
	waitForConsumerCondition(t, func() bool { return channel.cancelCount() == 1 })
	close(firstRelease)
	if messageID := <-handled; messageID != "event-72" {
		t.Fatalf("already admitted message = %q, want event-72", messageID)
	}
	select {
	case messageID := <-handled:
		t.Fatalf("pending message %q was admitted after Shutdown returned", messageID)
	case <-time.After(100 * time.Millisecond):
	}
	releaseCancel.Do(func() { close(channel.cancelBlock) })
	if err := consumer.Shutdown(t.Context()); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("finish Shutdown(): %v, want unavailable after pending delivery was left for redelivery", err)
	}
}

func TestConsumerDrainReportsShutdownDiscardOfPendingDelivery(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.cancelBlock = make(chan struct{})
	firstRelease := make(chan struct{})
	handled := make(chan string, 3)
	config := testConsumerConfig()
	config.Prefetch = 1
	config.Concurrency = 1
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(_ context.Context, delivery Delivery) (Settlement, error) {
			handled <- delivery.MessageID
			if delivery.MessageID == "event-81" {
				<-firstRelease
			}
			return Acknowledge(), nil
		},
		channel, &concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	for tag := uint64(81); tag <= 83; tag++ {
		delivery := testAMQPDelivery(tag)
		delivery.MessageId = fmt.Sprintf("event-%d", tag)
		channel.deliveries <- delivery
		if tag == 81 {
			if messageID := <-handled; messageID != "event-81" {
				t.Fatalf("first handled message = %q", messageID)
			}
		}
	}
	waitForConsumerCondition(t, func() bool {
		return len(channel.deliveries) == 0 && len(consumer.jobs) == 1
	})

	drained := make(chan error, 1)
	go func() { drained <- consumer.Drain(context.Background()) }()
	waitForConsumerCondition(t, func() bool { return channel.cancelCount() == 1 })
	shutdownContext, cancelShutdown := context.WithCancel(context.Background())
	cancelShutdown()
	if err := consumer.Shutdown(shutdownContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown(cancelled) = %v, want cancellation", err)
	}
	close(firstRelease)
	close(channel.cancelBlock)
	if err := <-drained; !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("Drain() = %v, want unavailable after Shutdown discarded pending work", err)
	}
	if err := consumer.Shutdown(t.Context()); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("completed Shutdown() = %v, want unavailable after discarded pending work", err)
	}
}

func TestConsumerShutdownRejectsNewBrokerDelivery(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	channel.cancelBlock = make(chan struct{})
	var releaseCancel sync.Once
	t.Cleanup(func() { releaseCancel.Do(func() { close(channel.cancelBlock) }) })
	handled := make(chan struct{}, 1)
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			handled <- struct{}{}
			return Acknowledge(), nil
		},
		channel, &concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown(cancelled) error = %v, want cancellation", err)
	}
	waitForConsumerCondition(t, func() bool { return channel.cancelCount() == 1 })
	channel.deliveries <- testAMQPDelivery(74)
	select {
	case <-handled:
		t.Fatal("broker delivery was admitted after Shutdown returned")
	case <-time.After(100 * time.Millisecond):
	}
	releaseCancel.Do(func() { close(channel.cancelBlock) })
	if err := consumer.Shutdown(t.Context()); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("finish Shutdown(): %v, want unavailable after broker delivery was left for redelivery", err)
	}
}

func TestConsumerCompletedShutdownHonorsPreCancelledCaller(t *testing.T) {
	t.Parallel()

	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		newFakeConsumerChannel(), &concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	if err := consumer.Shutdown(t.Context()); err != nil {
		t.Fatalf("initial Shutdown(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for attempt := 0; attempt < 64; attempt++ {
		if err := consumer.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown(cancelled) attempt %d = %v, want cancellation", attempt, err)
		}
	}
}

func TestConsumerCompletedDrainHonorsConcurrentCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumerCompletedDrainResult(ctx, ErrConsumerUnavailable); !errors.Is(err, context.Canceled) {
		t.Fatalf("completed drain with cancelled caller = %v, want cancellation", err)
	}
	if err := consumerCompletedDrainResult(t.Context(), ErrConsumerUnavailable); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("completed failed drain = %v, want consumer unavailable", err)
	}
}

func TestConsumerShutdownIsIndependentFromEarlierDrainCaller(t *testing.T) {
	t.Parallel()

	t.Run("completed cancelled drain", func(t *testing.T) {
		channel := newFakeConsumerChannel()
		channel.cancelBlock = make(chan struct{})
		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		defer func() {
			close(releaseHandler)
			close(channel.cancelBlock)
		}()
		config := testConsumerConfig()
		config.HandlerTimeout = 20 * time.Millisecond
		consumer, err := newConsumerFromChannel(
			t.Context(), config,
			func(context.Context, Delivery) (Settlement, error) {
				close(handlerStarted)
				<-releaseHandler
				return Acknowledge(), nil
			},
			channel, &concurrentCountingCloser{},
		)
		if err != nil {
			t.Fatalf("construct consumer: %v", err)
		}
		channel.deliveries <- testAMQPDelivery(75)
		<-handlerStarted
		drainContext, cancelDrain := context.WithCancel(context.Background())
		cancelDrain()
		if err := consumer.Drain(drainContext); !errors.Is(err, context.Canceled) {
			t.Fatalf("Drain(cancelled) = %v, want cancellation", err)
		}
		if err := consumer.Shutdown(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown() = %v, want its own cleanup deadline", err)
		}
	})

	t.Run("concurrent blocked drain", func(t *testing.T) {
		channel := newFakeConsumerChannel()
		channel.cancelBlock = make(chan struct{})
		config := testConsumerConfig()
		config.HandlerTimeout = 20 * time.Millisecond
		consumer, err := newConsumerFromChannel(
			t.Context(), config,
			func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
			channel, &concurrentCountingCloser{},
		)
		if err != nil {
			t.Fatalf("construct consumer: %v", err)
		}
		drained := make(chan error, 1)
		go func() { drained <- consumer.Drain(context.Background()) }()
		waitForConsumerCondition(t, func() bool { return channel.cancelCount() == 1 })
		shutdown := make(chan error, 1)
		go func() { shutdown <- consumer.Shutdown(context.Background()) }()
		select {
		case err := <-shutdown:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown() = %v, want its own cleanup deadline", err)
			}
		case <-time.After(250 * time.Millisecond):
			close(channel.cancelBlock)
			<-drained
			t.Fatal("Shutdown blocked behind an earlier Drain caller")
		}
		close(channel.cancelBlock)
		<-drained
	})
}

func TestConsumerShutdownUsesOneTotalHandlerTimeout(t *testing.T) {
	channel := newFakeConsumerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	resource := &deadlineProbeCloser{deadline: make(chan time.Time, 1)}
	config := testConsumerConfig()
	config.HandlerTimeout = 100 * time.Millisecond
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(context.Context, Delivery) (Settlement, error) {
			close(started)
			<-release
			return Settlement{}, context.Canceled
		},
		channel, resource,
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	channel.deliveries <- testAMQPDelivery(72)
	<-started
	shutdownStarted := time.Now()
	shutdownErr := consumer.Shutdown(context.Background())
	close(release)
	<-consumer.Done()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline", shutdownErr)
	}
	var cleanupDeadline time.Time
	select {
	case cleanupDeadline = <-resource.deadline:
	case <-time.After(time.Second):
		t.Fatal("resource cleanup did not receive the shared deadline")
	}
	wantDeadline := shutdownStarted.Add(config.HandlerTimeout)
	if delta := cleanupDeadline.Sub(wantDeadline); delta < -25*time.Millisecond || delta > 25*time.Millisecond {
		t.Fatalf("cleanup deadline = %s, want shared deadline near %s", cleanupDeadline, wantDeadline)
	}
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
