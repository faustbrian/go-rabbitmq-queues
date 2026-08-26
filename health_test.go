package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestProducerHealthSeparatesTemporaryDependencyFailureFromLiveness(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	first := newFakeProducerChannel()
	firstResource := newFakeProducerConnectionEvents()
	second := newFakeProducerChannel()
	recoveryStarted := make(chan struct{})
	releaseRecovery := make(chan struct{})
	dials := 0
	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(), func() (string, error) { return "health-session", nil },
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

	assertHealth(t, producer.Liveness(), LivenessLive, producer.Readiness(), ReadinessReady, producer.DependencyHealth(), DependencyAvailable)
	firstResource.blocked <- amqp.Blocking{Active: true, Reason: "sensitive broker detail"}
	waitForHealth(t, func() bool { return producer.DependencyHealth() == DependencyBlocked })
	assertHealth(t, producer.Liveness(), LivenessLive, producer.Readiness(), ReadinessNotReady, producer.DependencyHealth(), DependencyBlocked)
	firstResource.blocked <- amqp.Blocking{Active: false}
	waitForHealth(t, func() bool { return producer.Readiness() == ReadinessReady })

	firstResource.closed <- &amqp.Error{Code: 320, Reason: "sensitive broker detail"}
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("producer recovery did not start")
	}
	assertHealth(t, producer.Liveness(), LivenessLive, producer.Readiness(), ReadinessNotReady, producer.DependencyHealth(), DependencyRecovering)
	close(releaseRecovery)
	waitForHealth(t, func() bool { return producer.Readiness() == ReadinessReady })
	assertHealth(t, producer.Liveness(), LivenessLive, producer.Readiness(), ReadinessReady, producer.DependencyHealth(), DependencyAvailable)

	if err := producer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	assertHealth(t, producer.Liveness(), LivenessStopped, producer.Readiness(), ReadinessNotReady, producer.DependencyHealth(), DependencyUnknown)
}

func TestProducerHealthReportsTerminalRecoveryExhaustion(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	first := newFakeProducerChannel()
	firstResource := newFakeProducerConnectionEvents()
	dials := 0
	producer, err := openProducerWith(
		t.Context(), connection, testProducerConfig(), func() (string, error) { return "terminal-session", nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			dials++
			if dials == 1 {
				return first, firstResource, nil
			}
			return nil, nil, errors.New("broker unavailable")
		},
	)
	if err != nil {
		t.Fatalf("openProducerWith(): %v", err)
	}
	t.Cleanup(func() { closeProducerForTest(t, producer) })
	firstResource.closed <- &amqp.Error{Code: 320}
	waitForHealth(t, func() bool { return producer.Liveness() == LivenessFailed })
	assertHealth(t, producer.Liveness(), LivenessFailed, producer.Readiness(), ReadinessNotReady, producer.DependencyHealth(), DependencyUnavailable)
}

func TestConsumerHealthSeparatesTemporaryDependencyFailureFromLiveness(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	first := newFakeConsumerChannel()
	second := newFakeConsumerChannel()
	recoveryStarted := make(chan struct{})
	releaseRecovery := make(chan struct{})
	dials := 0
	consumer, err := openConsumerWith(
		t.Context(), connection, testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(ctx context.Context, _ Endpoint, _ ConnectionConfig, _ Credentials) (consumerChannel, io.Closer, error) {
			dials++
			if dials == 1 {
				return first, &concurrentCountingCloser{}, nil
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
		t.Fatalf("openConsumerWith(): %v", err)
	}

	assertHealth(t, consumer.Liveness(), LivenessLive, consumer.Readiness(), ReadinessReady, consumer.DependencyHealth(), DependencyAvailable)
	first.cancelOnce.Do(func() { close(first.deliveries) })
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("consumer recovery did not start")
	}
	assertHealth(t, consumer.Liveness(), LivenessLive, consumer.Readiness(), ReadinessNotReady, consumer.DependencyHealth(), DependencyRecovering)
	close(releaseRecovery)
	waitForHealth(t, func() bool { return consumer.Readiness() == ReadinessReady })
	assertHealth(t, consumer.Liveness(), LivenessLive, consumer.Readiness(), ReadinessReady, consumer.DependencyHealth(), DependencyAvailable)

	if err := consumer.Close(t.Context()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	assertHealth(t, consumer.Liveness(), LivenessStopped, consumer.Readiness(), ReadinessNotReady, consumer.DependencyHealth(), DependencyUnknown)
}

func TestConsumerHealthReflectsPausedAdmission(t *testing.T) {
	t.Parallel()

	consumer, err := newConsumerFromChannel(
		t.Context(),
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		newFakeConsumerChannel(),
		&countingCloser{},
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })

	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(): %v", err)
	}
	assertHealth(
		t,
		consumer.Liveness(), LivenessLive,
		consumer.Readiness(), ReadinessNotReady,
		consumer.DependencyHealth(), DependencyAvailable,
	)

	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume(): %v", err)
	}
	assertHealth(
		t,
		consumer.Liveness(), LivenessLive,
		consumer.Readiness(), ReadinessReady,
		consumer.DependencyHealth(), DependencyAvailable,
	)
}

func TestConsumerHealthReportsTerminalRecoveryExhaustion(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	first := newFakeConsumerChannel()
	dials := 0
	consumer, err := openConsumerWith(
		t.Context(), connection, testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			dials++
			if dials == 1 {
				return first, &concurrentCountingCloser{}, nil
			}
			return nil, nil, errors.New("broker unavailable")
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	first.cancelOnce.Do(func() { close(first.deliveries) })
	waitForHealth(t, func() bool { return consumer.Liveness() == LivenessFailed })
	assertHealth(t, consumer.Liveness(), LivenessFailed, consumer.Readiness(), ReadinessNotReady, consumer.DependencyHealth(), DependencyUnavailable)
}

func assertHealth(
	t *testing.T,
	gotLiveness, wantLiveness Liveness,
	gotReadiness, wantReadiness Readiness,
	gotDependency, wantDependency DependencyHealth,
) {
	t.Helper()
	if gotLiveness != wantLiveness || gotReadiness != wantReadiness || gotDependency != wantDependency {
		t.Fatalf("health = (%q, %q, %q), want (%q, %q, %q)", gotLiveness, gotReadiness, gotDependency, wantLiveness, wantReadiness, wantDependency)
	}
}

func waitForHealth(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("health condition was not reached")
		}
		runtime.Gosched()
	}
}
