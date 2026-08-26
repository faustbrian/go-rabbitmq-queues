package rabbitmqqueue

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublishTrackerConcurrentCorrelationStress(t *testing.T) {
	const attempts = 512
	tracker := newPublishTracker(attempts)
	registered := make([]*publishAttempt, attempts)
	for index := range attempts {
		attempt, err := tracker.register(uint64(index+1), fmt.Sprintf("token-%d", index+1))
		if err != nil {
			t.Fatalf("register attempt %d: %v", index, err)
		}
		registered[index] = attempt
	}

	var returns sync.WaitGroup
	for index := 0; index < attempts; index += 2 {
		returns.Add(1)
		go func(index int) {
			defer returns.Done()
			if !tracker.returned(fmt.Sprintf("token-%d", index+1), Return{Code: 312}) {
				t.Errorf("return %d was not correlated", index)
			}
		}(index)
	}
	returns.Wait()

	var confirms sync.WaitGroup
	for index := range attempts {
		confirms.Add(1)
		go func(index int) {
			defer confirms.Done()
			if !tracker.confirm(uint64(index+1), true) {
				t.Errorf("confirmation %d was not correlated", index)
			}
		}(index)
	}
	confirms.Wait()

	for index, attempt := range registered {
		result := <-attempt.outcome
		want := PublishConfirmed
		if index%2 == 0 {
			want = PublishReturned
		}
		if result.State != want {
			t.Fatalf("attempt %d state = %s, want %s", index, result.State, want)
		}
		if tracker.confirm(uint64(index+1), true) || tracker.returned(fmt.Sprintf("token-%d", index+1), Return{}) {
			t.Fatalf("attempt %d accepted a late event", index)
		}
	}
}

func TestProducerConcurrentPublicationsCorrelateOutOfOrderConfirms(t *testing.T) {
	const attempts = 128
	channel := newFakeProducerChannel()
	transmitted := make(chan uint64, attempts)
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		sequence := channel.nextSequence()
		transmitted <- sequence
		return nil
	}
	config := testProducerConfig()
	config.MaxOutstanding = attempts
	producer, err := newProducerFromChannel(config, "stress-session", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	outcomes := make(chan PublishOutcome, attempts)
	start := make(chan struct{})
	for range attempts {
		go func() {
			<-start
			result, publishErr := producer.Publish(context.Background(), testPublication())
			outcomes <- PublishOutcome{Result: result, Err: publishErr}
		}()
	}
	close(start)
	sequences := make([]uint64, attempts)
	for index := range attempts {
		sequences[index] = <-transmitted
	}
	for index := attempts - 1; index >= 0; index-- {
		channel.confirms <- amqp.Confirmation{DeliveryTag: sequences[index], Ack: true}
	}
	for range attempts {
		outcome := <-outcomes
		if outcome.Err != nil || outcome.Result.State != PublishConfirmed {
			t.Fatalf("concurrent outcome = %#v, want confirmed", outcome)
		}
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestConsumerConcurrencyStressDrainsAndClosesCleanly(t *testing.T) {
	const deliveries = 128
	config := testConsumerConfig()
	config.Prefetch = 16
	config.Concurrency = 8
	channel := newFakeConsumerChannel()
	channel.deliveries = make(chan amqp.Delivery, deliveries)
	channel.settled = make(chan fakeConsumerSettlement, deliveries)
	started := make(chan struct{}, deliveries)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	handler := func(context.Context, Delivery) (Settlement, error) {
		current := active.Add(1)
		for observed := maximum.Load(); current > observed && !maximum.CompareAndSwap(observed, current); observed = maximum.Load() {
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return Acknowledge(), nil
	}
	consumer, err := newConsumerFromChannel(t.Context(), config, handler, channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	for index := range deliveries {
		channel.deliveries <- testAMQPDelivery(uint64(index + 1))
	}
	for range config.Concurrency {
		<-started
	}
	if got := maximum.Load(); got != int32(config.Concurrency) {
		t.Fatalf("peak handler concurrency = %d, want %d", got, config.Concurrency)
	}

	close(release)
	for range deliveries {
		if settlement := <-channel.settled; settlement.method != SettlementAcknowledge {
			t.Fatalf("settlement = %#v, want acknowledge", settlement)
		}
	}
	if err := consumer.Close(context.Background()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if active.Load() != 0 || channel.cancelCount() != 1 || channel.closeCount() != 1 {
		t.Fatalf("clean close state = active %d cancel %d channel close %d", active.Load(), channel.cancelCount(), channel.closeCount())
	}
}
