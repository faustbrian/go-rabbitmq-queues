package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestProducerObservesPublishReturnConfirmLatencyBlockingRecoveryAndShutdown(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	first := newFakeProducerChannel()
	first.publish = func(_ context.Context, exchange, routingKey string, mandatory, _ bool, publishing amqp.Publishing) error {
		sequence := first.nextSequence()
		first.returns <- amqp.Return{
			ReplyCode: 312, ReplyText: "sensitive broker detail", Exchange: exchange,
			RoutingKey: routingKey, Headers: publishing.Headers,
		}
		first.confirms <- amqp.Confirmation{DeliveryTag: sequence, Ack: true}
		return nil
	}
	firstResource := newFakeProducerConnectionEvents()
	second := newFakeProducerChannel()
	recoveryStarted := make(chan struct{})
	releaseRecovery := make(chan struct{})
	dials := 0
	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(), func() (string, error) { return "observation-session", nil },
		func(ctx context.Context, _ Endpoint, _ ConnectionConfig, _ Credentials) (producerChannel, io.Closer, error) {
			dials++
			if dials == 1 {
				return first, firstResource, nil
			}
			close(recoveryStarted)
			select {
			case <-releaseRecovery:
				return second, &concurrentCountingCloser{}, nil
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		},
	)
	if err != nil {
		t.Fatalf("openProducerWith(): %v", err)
	}

	if result, publishErr := producer.Publish(t.Context(), testPublication()); result.State != PublishReturned || !errors.Is(publishErr, ErrPublishReturned) {
		t.Fatalf("Publish() = (%#v, %v), want returned", result, publishErr)
	}
	firstResource.blocked <- amqp.Blocking{Active: true, Reason: "sensitive broker detail"}
	waitForHealth(t, func() bool { return producer.IsBlocked() })
	firstResource.blocked <- amqp.Blocking{Active: false}
	waitForHealth(t, func() bool { return !producer.IsBlocked() })
	firstResource.closed <- &amqp.Error{Code: 320, Reason: "sensitive broker detail"}
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("producer recovery did not start")
	}
	close(releaseRecovery)
	waitForHealth(t, func() bool { return producer.Readiness() == ReadinessReady })

	if err := producer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	observations := drainObservations(producer.Observations())
	assertTerminalObservationResource(t, observations, ObservationProducer)
	assertObservationKinds(t, observations, map[ObservationKind]bool{
		ObservationConnectionState:     true,
		ObservationConnectionBlocked:   true,
		ObservationReconnect:           true,
		ObservationPublish:             true,
		ObservationReturn:              true,
		ObservationConfirm:             true,
		ObservationConfirmationLatency: true,
		ObservationShutdown:            true,
	})
	for _, observation := range observations {
		if observation.Kind == ObservationConfirmationLatency && observation.Duration <= 0 {
			t.Fatalf("confirmation latency = %s, want positive", observation.Duration)
		}
	}
}

func TestProducerObservesAmbiguousOutcomeAndBacklogPressure(t *testing.T) {
	t.Parallel()

	channel := newFakeProducerChannel()
	transmitted := make(chan struct{})
	release := make(chan struct{})
	channel.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		channel.nextSequence()
		close(transmitted)
		<-release
		return nil
	}
	config := testProducerConfig()
	config.MaxOutstanding = 1
	config.PublishTimeout = 10 * time.Millisecond
	producer, err := newProducerFromChannel(config, "pressure-session", channel, &concurrentCountingCloser{})
	if err != nil {
		t.Fatalf("newProducerFromChannel(): %v", err)
	}
	first := make(chan PublishOutcome, 1)
	go func() {
		result, publishErr := producer.Publish(context.Background(), testPublication())
		first <- PublishOutcome{Result: result, Err: publishErr}
	}()
	<-transmitted
	if _, publishErr := producer.Publish(t.Context(), testPublication()); !errors.Is(publishErr, ErrOutstandingConfirmLimit) {
		t.Fatalf("second Publish() error = %v, want outstanding limit", publishErr)
	}
	close(release)
	if outcome := <-first; outcome.Result.State != PublishAmbiguous || !errors.Is(outcome.Err, ErrPublishAmbiguous) {
		t.Fatalf("first outcome = %#v, want ambiguous", outcome)
	}
	if err := producer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	observations := drainObservations(producer.Observations())
	assertObservationKinds(t, observations, map[ObservationKind]bool{
		ObservationAmbiguous:       true,
		ObservationBacklogPressure: true,
	})
}

func TestConsumerObservesDeliveriesFailuresSettlementsBacklogAndShutdown(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	config := testConsumerConfig()
	config.Prefetch = 1
	config.Concurrency = 1
	handlerCalls := 0
	consumer, err := newConsumerFromChannel(
		t.Context(), config,
		func(context.Context, Delivery) (Settlement, error) {
			handlerCalls++
			if handlerCalls == 1 {
				close(started)
				<-release
				return Settlement{}, errors.New("sensitive handler detail")
			}
			return Acknowledge(), nil
		},
		channel,
		&concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("newConsumerFromChannel(): %v", err)
	}
	first := testAMQPDelivery(71)
	first.Redelivered = true
	first.Headers = amqp.Table{"x-death": []any{amqp.Table{
		"count": int64(1), "reason": "rejected", "queue": "orders",
		"exchange": "events", "routing-keys": []any{"orders.created"},
		"time": time.Unix(50, 0),
	}}}
	channel.deliveries <- first
	<-started
	channel.deliveries <- testAMQPDelivery(72)
	channel.deliveries <- testAMQPDelivery(73)
	prefix := waitForObservationKind(t, consumer.Observations(), ObservationBacklogPressure)
	close(release)
	for range 3 {
		select {
		case <-channel.settled:
		case <-time.After(time.Second):
			t.Fatal("consumer did not settle admitted delivery")
		}
	}
	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	observations := append(prefix, drainObservations(consumer.Observations())...)
	assertTerminalObservationResource(t, observations, ObservationConsumer)
	assertObservationKinds(t, observations, map[ObservationKind]bool{
		ObservationConnectionState: true,
		ObservationDelivery:        true,
		ObservationRedelivery:      true,
		ObservationAcknowledgement: true,
		ObservationSettlement:      true,
		ObservationHandlerFailure:  true,
		ObservationDeadLetter:      true,
		ObservationShutdown:        true,
	})
	for _, observation := range observations {
		if observation.Kind == ObservationConsumerCancellation {
			t.Fatalf("client shutdown emitted broker cancellation: %#v", observation)
		}
	}
}

func TestConsumerObservesBrokerCancellationAndRecovers(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	first := newFakeConsumerChannel()
	second := newFakeConsumerChannel()
	dials := 0
	consumer, err := openConsumerWith(
		t.Context(), connection, testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			dials++
			if dials == 1 {
				return first, &concurrentCountingCloser{}, nil
			}
			return second, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	first.cancelNotifications <- testConsumerConfig().Name
	first.cancelOnce.Do(func() { close(first.deliveries) })
	observations := waitForObservationKind(
		t, consumer.Observations(), ObservationConsumerCancellation,
	)
	last := observations[len(observations)-1]
	if last.Outcome != ObservationCancelled || last.Resource != ObservationConsumer {
		t.Fatalf("cancellation observation = %#v", last)
	}
	select {
	case <-second.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("broker cancellation did not create a replacement consumer")
	}
	select {
	case <-consumer.Done():
		t.Fatalf("consumer became terminal after recoverable cancellation: %v", consumer.Err())
	default:
	}
}

func TestObservationStreamReportsDroppedEventsWithoutBlocking(t *testing.T) {
	t.Parallel()

	stream := newObservationStream(ObservationProducer, 1)
	stream.emit(Observation{Kind: ObservationPublish})
	stream.emit(Observation{Kind: ObservationConfirm})
	if got := <-stream.channel; got.Kind != ObservationPublish {
		t.Fatalf("first observation = %#v", got)
	}
	stream.emit(Observation{Kind: ObservationShutdown})
	got := <-stream.channel
	if got.Kind != ObservationShutdown || got.Dropped != 1 {
		t.Fatalf("observation after pressure = %#v, want one reported drop", got)
	}
	stream.close()
	if terminal, open := <-stream.channel; !open || terminal.Kind != ObservationStreamClosed {
		t.Fatalf("terminal observation = (%#v, %t)", terminal, open)
	}
	if _, open := <-stream.channel; open {
		t.Fatal("observation stream remained open")
	}
}

func TestObservationStreamReportsTailDropsWhenSaturatedAtClose(t *testing.T) {
	t.Parallel()

	stream := newObservationStream(ObservationProducer, 2)
	stream.emit(Observation{Kind: ObservationPublish})
	stream.emit(Observation{Kind: ObservationConfirm})
	stream.emit(Observation{Kind: ObservationReturn})
	stream.close()

	observations := drainObservations(stream.channel)
	if len(observations) != 2 {
		t.Fatalf("closed saturated stream length = %d, want two", len(observations))
	}
	last := observations[len(observations)-1]
	if last.Kind != ObservationStreamClosed || last.Outcome != ObservationClosed || last.Dropped != 2 {
		t.Fatalf("terminal observation = %#v, want stream-closed with dropped event and displaced slot", last)
	}
}

func TestConsumerObservesBrokerDeliveryBeforeRejectingInvalidSnapshot(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	handlerCalled := make(chan struct{}, 1)
	consumer, err := newConsumerFromChannel(
		t.Context(), testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			handlerCalled <- struct{}{}
			return Acknowledge(), nil
		},
		channel,
		&concurrentCountingCloser{},
	)
	if err != nil {
		t.Fatalf("newConsumerFromChannel(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	invalid := testAMQPDelivery(74)
	invalid.Body = make([]byte, DefaultLimits().MaxPayloadBytes+1)
	channel.deliveries <- invalid
	waitForObservationKind(t, consumer.Observations(), ObservationDelivery)
	select {
	case settlement := <-channel.settled:
		if settlement.method != SettlementReject {
			t.Fatalf("invalid delivery settlement = %#v", settlement)
		}
	case <-time.After(time.Second):
		t.Fatal("invalid delivery was not rejected")
	}
	select {
	case <-handlerCalled:
		t.Fatal("handler received invalid delivery")
	default:
	}
}

func waitForObservationKind(t *testing.T, observations <-chan Observation, kind ObservationKind) []Observation {
	t.Helper()
	deadline := time.After(time.Second)
	result := []Observation{}
	for {
		select {
		case observation, open := <-observations:
			if !open {
				t.Fatalf("observation stream closed before %q", kind)
			}
			result = append(result, observation)
			if observation.Kind == kind {
				return result
			}
		case <-deadline:
			t.Fatalf("observation %q was not emitted", kind)
		}
	}
}

func drainObservations(observations <-chan Observation) []Observation {
	result := []Observation{}
	for observation := range observations {
		result = append(result, observation)
	}
	return result
}

func assertObservationKinds(t *testing.T, observations []Observation, wanted map[ObservationKind]bool) {
	t.Helper()
	for _, observation := range observations {
		delete(wanted, observation.Kind)
	}
	if len(wanted) != 0 {
		t.Fatalf("missing observation kinds %#v from %#v", wanted, observations)
	}
}

func assertTerminalObservationResource(t *testing.T, observations []Observation, resource ObservationResource) {
	t.Helper()
	for _, observation := range observations {
		if observation.Kind == ObservationStreamClosed {
			if observation.Resource != resource {
				t.Fatalf("terminal observation resource = %q, want %q", observation.Resource, resource)
			}
			return
		}
	}
	t.Fatal("terminal stream observation was not emitted")
}
