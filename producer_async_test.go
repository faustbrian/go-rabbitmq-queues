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

func TestProducerPublishesAsynchronouslyWithExactOutcome(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	transmitted := make(chan uint64, 1)
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		sequence := channel.nextSequence()
		transmitted <- sequence
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-async", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	future, err := producer.PublishAsync(t.Context(), testPublication())
	if err != nil {
		t.Fatalf("PublishAsync(): %v", err)
	}
	sequence := <-transmitted
	select {
	case outcome := <-future:
		t.Fatalf("asynchronous publish completed before broker outcome: %#v", outcome)
	default:
	}
	channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
	outcome, open := <-future
	if !open || outcome.Result.State != PublishConfirmed || outcome.Err != nil {
		t.Fatalf("asynchronous outcome = (%#v, %t), want confirmed", outcome, open)
	}
	if _, open := <-future; open {
		t.Fatal("asynchronous outcome channel remained open after one result")
	}
}

func TestProducerBoundsAsynchronousAdmission(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	transmitted := make(chan uint64, 1)
	var once sync.Once
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		sequence := channel.nextSequence()
		once.Do(func() { transmitted <- sequence })
		return nil
	}
	config := testProducerConfig()
	config.MaxOutstanding = 1
	producer, err := newProducerFromChannel(config, "session-async-bound", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	first, err := producer.PublishAsync(t.Context(), testPublication())
	if err != nil {
		t.Fatalf("first PublishAsync(): %v", err)
	}
	sequence := <-transmitted
	second, err := producer.PublishAsync(t.Context(), testPublication())
	if second != nil || !errors.Is(err, ErrOutstandingConfirmLimit) {
		t.Fatalf("second PublishAsync() = (%#v, %v), want bounded rejection", second, err)
	}
	channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
	if outcome := <-first; outcome.Result.State != PublishConfirmed || outcome.Err != nil {
		t.Fatalf("first asynchronous outcome = %#v", outcome)
	}
}

func TestProducerAsyncOwnsPublicationBeforeReturning(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	published := make(chan amqp.Publishing, 1)
	channel.publish = func(_ context.Context, _ string, _ string, _ bool, _ bool, message amqp.Publishing) error {
		sequence := channel.nextSequence()
		published <- message
		channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-async-owned", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	body := []byte("original")
	headerBytes := []byte{1, 2}
	expiration := 5 * time.Second
	publication := testPublication()
	publication.Message.Body = body
	publication.Message.Headers = []Header{BytesHeader("binary", headerBytes)}
	publication.Message.Expiration = &expiration
	producer.publishMu.Lock()
	future, err := producer.PublishAsync(t.Context(), publication)
	if err != nil {
		producer.publishMu.Unlock()
		t.Fatalf("PublishAsync(): %v", err)
	}
	body[0] = 'X'
	headerBytes[0] = 9
	publication.Message.Headers[0].Bytes[0] = 8
	expiration = 0
	producer.publishMu.Unlock()

	message := <-published
	if string(message.Body) != "original" || message.Headers["binary"].([]byte)[0] != 1 || message.Expiration != "5000" {
		t.Fatalf(
			"asynchronous publication was aliased: body %q headers %#v expiration %q",
			message.Body, message.Headers, message.Expiration,
		)
	}
	if outcome := <-future; outcome.Result.State != PublishConfirmed || outcome.Err != nil {
		t.Fatalf("asynchronous outcome = %#v", outcome)
	}
}

func TestProducerBatchPreservesOrderedIndependentOutcomes(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(_ context.Context, _ string, _ string, _ bool, _ bool, message amqp.Publishing) error {
		sequence := channel.nextSequence()
		channel.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: message.MessageId != "event-2"}
		return nil
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-batch", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	first := testPublication()
	first.Message.MessageID = "event-1"
	second := testPublication()
	second.Message.MessageID = "event-2"
	third := testPublication()
	third.Message.MessageID = "event-3"
	outcomes, err := producer.PublishBatch(t.Context(), []Publication{first, second, third})
	if err != nil {
		t.Fatalf("PublishBatch(): %v", err)
	}
	if len(outcomes) != 3 || outcomes[0].Result.State != PublishConfirmed || outcomes[0].Err != nil ||
		outcomes[1].Result.State != PublishRejected || !errors.Is(outcomes[1].Err, ErrPublishRejected) ||
		outcomes[2].Result.State != PublishConfirmed || outcomes[2].Err != nil {
		t.Fatalf("batch outcomes = %#v", outcomes)
	}
}

func TestProducerBatchRejectsInvalidBoundsBeforePublishing(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	published := false
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		published = true
		return errors.New("unexpected publish")
	}
	producer, err := newProducerFromChannel(testProducerConfig(), "session-batch-bound", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	if outcomes, err := producer.PublishBatch(nil, []Publication{testPublication()}); outcomes != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("nil-context batch = (%#v, %v)", outcomes, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if outcomes, err := producer.PublishBatch(cancelled, []Publication{testPublication()}); outcomes != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled batch = (%#v, %v)", outcomes, err)
	}
	if outcomes, err := producer.PublishBatch(t.Context(), nil); outcomes != nil || !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("empty batch = (%#v, %v)", outcomes, err)
	}
	if outcomes, err := producer.PublishBatch(t.Context(), make([]Publication, MaxPublishBatchSize+1)); outcomes != nil || !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("oversized batch = (%#v, %v)", outcomes, err)
	}
	invalid := testPublication()
	invalid.Message.MessageID = ""
	if outcomes, err := producer.PublishBatch(t.Context(), []Publication{testPublication(), invalid}); outcomes != nil ||
		!errors.Is(err, ErrInvalidBatch) || !errors.Is(err, ErrMessageIDRequired) {
		t.Fatalf("invalid publication batch = (%#v, %v)", outcomes, err)
	}
	if published {
		t.Fatal("batch preflight published before validating every item")
	}
}

func TestProducerAsyncRejectsInvalidInputBeforeAdmission(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	producer, err := newProducerFromChannel(testProducerConfig(), "session-async-input", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	if future, err := producer.PublishAsync(nil, testPublication()); future != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("nil-context async = (%#v, %v)", future, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if future, err := producer.PublishAsync(cancelled, testPublication()); future != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled async = (%#v, %v)", future, err)
	}
	invalid := testPublication()
	invalid.Message.MessageID = ""
	if future, err := producer.PublishAsync(t.Context(), invalid); future != nil || !errors.Is(err, ErrMessageIDRequired) {
		t.Fatalf("invalid async = (%#v, %v)", future, err)
	}
}

func TestProducerBatchCancellationLeavesRemainingItemsNotSent(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	channel.publish = func(ctx context.Context, _ string, _ string, _ bool, _ bool, _ amqp.Publishing) error {
		<-ctx.Done()
		return ctx.Err()
	}
	config := testProducerConfig()
	config.PublishTimeout = time.Millisecond
	producer, err := newProducerFromChannel(config, "session-batch-cancel", channel, io.NopCloser(nilReader{}))
	if err != nil {
		t.Fatalf("construct producer: %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })

	outcomes, err := producer.PublishBatch(context.Background(), []Publication{testPublication(), testPublication()})
	if err != nil {
		t.Fatalf("PublishBatch(): %v", err)
	}
	if len(outcomes) != 2 || outcomes[0].Result.State != PublishNotSent ||
		!errors.Is(outcomes[0].Err, context.DeadlineExceeded) ||
		outcomes[1].Result.State != PublishNotSent ||
		(!errors.Is(outcomes[1].Err, context.DeadlineExceeded) && !errors.Is(outcomes[1].Err, ErrProducerUnavailable)) {
		t.Fatalf("cancelled batch outcomes = %#v", outcomes)
	}
}
