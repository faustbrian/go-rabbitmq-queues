package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestOpenConsumerRecoversRuntimeLossWithoutDuplicatingConsumer(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	var credentialCalls atomic.Int32
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		generation := credentialCalls.Add(1)
		return Credentials{Username: "consumer-" + strconv.Itoa(int(generation)), Password: []byte("rotated")}, nil
	})
	first := newFakeConsumerChannel()
	first.closeErr = amqp.ErrClosed
	firstResource := &concurrentCountingCloser{err: amqp.ErrClosed}
	second := newFakeConsumerChannel()
	secondResource := &concurrentCountingCloser{}
	priority := int32(9)
	config := testConsumerConfig()
	config.Priority = &priority
	config.Exclusive = true
	type dialObservation struct {
		attempt  int
		endpoint Endpoint
		username string
	}
	dialed := make(chan dialObservation, 2)
	var dialCalls atomic.Int32
	consumer, err := openConsumerWith(
		t.Context(),
		connection,
		config,
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, credentials Credentials) (consumerChannel, io.Closer, error) {
			attempt := int(dialCalls.Add(1))
			dialed <- dialObservation{attempt: attempt, endpoint: endpoint, username: credentials.Username}
			if attempt == 1 {
				return first, firstResource, nil
			}
			return second, secondResource, nil
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	if got := <-dialed; got.attempt != 1 || got.endpoint != connection.Endpoints[0] || got.username != "consumer-1" {
		t.Fatalf("initial dial = %#v", got)
	}
	priority = -9

	first.cancelOnce.Do(func() { close(first.deliveries) })
	select {
	case got := <-dialed:
		if got.attempt != 2 || got.endpoint != connection.Endpoints[1] || got.username != "consumer-2" {
			t.Fatalf("recovery dial = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime loss did not dial a replacement consumer")
	}
	select {
	case <-consumer.Done():
		t.Fatalf("consumer became terminal during successful recovery: %v", consumer.Err())
	default:
	}
	second.deliveries <- testAMQPDelivery(41)
	if settled := <-second.settled; settled.method != SettlementAcknowledge || settled.tag != 41 {
		t.Fatalf("recovered settlement = %#v", settled)
	}
	if firstResource.count() != 1 || first.cancelCount() != 0 || first.closeCount() != 1 {
		t.Fatalf("superseded generation cleanup = resource %d cancel %d channel %d", firstResource.count(), first.cancelCount(), first.closeCount())
	}
	if credentialCalls.Load() != 2 {
		t.Fatalf("credential calls = %d, want two", credentialCalls.Load())
	}
	if second.prefetch != config.Prefetch || second.queue != "orders" || second.consumer != "orders-worker" ||
		!second.exclusive || len(second.arguments) != 1 || second.arguments["x-priority"] != int32(9) {
		t.Fatalf("replacement consumer setup = %#v", second)
	}
}

func TestPausedConsumerRecoversClosedDeliveryStreamBeforeResume(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	first := newFakeConsumerChannel()
	second := newFakeConsumerChannel()
	dialed := make(chan int, 2)
	var dialCalls atomic.Int32
	consumer, err := openConsumerWith(
		t.Context(),
		connection,
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			attempt := int(dialCalls.Add(1))
			dialed <- attempt
			if attempt == 1 {
				return first, &concurrentCountingCloser{}, nil
			}
			return second, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	<-dialed

	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause(): %v", err)
	}
	pending := make(chan struct{})
	go func() {
		first.deliveries <- testAMQPDelivery(54)
		close(pending)
	}()
	select {
	case <-pending:
	case <-time.After(time.Second):
		t.Fatal("paused consumer did not retain a broker delivery")
	}
	first.cancelOnce.Do(func() { close(first.deliveries) })
	select {
	case attempt := <-dialed:
		if attempt != 2 {
			t.Fatalf("recovery dial = %d, want second attempt", attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("paused consumer did not recover a closed delivery stream")
	}
	if consumer.Readiness() != ReadinessNotReady {
		t.Fatalf("recovered paused readiness = %q, want not ready", consumer.Readiness())
	}
	select {
	case settled := <-first.settled:
		t.Fatalf("failed paused generation settled pending delivery: %#v", settled)
	default:
	}

	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume(): %v", err)
	}
	second.deliveries <- testAMQPDelivery(53)
	select {
	case settled := <-second.settled:
		if settled.method != SettlementAcknowledge || settled.tag != 53 {
			t.Fatalf("resumed recovered settlement = %#v, want acknowledgement", settled)
		}
	case <-time.After(time.Second):
		t.Fatal("resumed recovered consumer did not settle delivery")
	}
}

func TestConsumerRuntimeRecoveryExhaustionBecomesTerminal(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	first := newFakeConsumerChannel()
	firstResource := &concurrentCountingCloser{}
	dialed := make(chan int, 3)
	var dialCalls atomic.Int32
	consumer, err := openConsumerWith(
		t.Context(),
		connection,
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			attempt := int(dialCalls.Add(1))
			dialed <- attempt
			if attempt == 1 {
				return first, firstResource, nil
			}
			return nil, nil, errors.New("broker unavailable")
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	<-dialed
	first.cancelOnce.Do(func() { close(first.deliveries) })
	for want := 2; want <= 3; want++ {
		select {
		case got := <-dialed:
			if got != want {
				t.Fatalf("recovery attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("runtime recovery did not reach attempt %d", want)
		}
	}
	select {
	case <-consumer.Done():
	case <-time.After(time.Second):
		t.Fatal("consumer did not become terminal after recovery exhaustion")
	}
	if !errors.Is(consumer.Err(), ErrConsumerUnavailable) {
		t.Fatalf("Err() = %v, want unavailable", consumer.Err())
	}
	if firstResource.count() != 1 || first.closeCount() != 1 {
		t.Fatalf("failed generation cleanup = resource %d channel %d", firstResource.count(), first.closeCount())
	}
}

func TestConsumerCloseCancelsRuntimeRecoveryBackoff(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 3, InitialDelay: time.Second, MaxDelay: time.Second}
	first := newFakeConsumerChannel()
	recoveryStarted := make(chan struct{}, 1)
	var dialCalls atomic.Int32
	consumer, err := openConsumerWith(
		t.Context(),
		connection,
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			if dialCalls.Add(1) == 1 {
				return first, &concurrentCountingCloser{}, nil
			}
			recoveryStarted <- struct{}{}
			return nil, nil, errors.New("broker unavailable")
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	first.cancelOnce.Do(func() { close(first.deliveries) })
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("runtime recovery did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := consumer.Close(ctx); err != nil {
		t.Fatalf("Close() during recovery: %v", err)
	}
	if dialCalls.Load() != 2 {
		t.Fatalf("dial calls after close = %d, want two", dialCalls.Load())
	}
}

func TestConsumerRecoveryOwnsMutableConnectionConfiguration(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	connection.TLS.RootCAs = [][]byte{[]byte("root-material")}
	connection.TLS.ClientCertificate = []byte("certificate-material")
	connection.TLS.ClientPrivateKey = []byte("private-key-material")
	first := newFakeConsumerChannel()
	type observation struct {
		endpoint    Endpoint
		root        string
		certificate string
		privateKey  string
	}
	recovered := make(chan observation, 1)
	var dialCalls atomic.Int32
	consumer, err := openConsumerWith(
		t.Context(),
		connection,
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		func(_ context.Context, endpoint Endpoint, config ConnectionConfig, _ Credentials) (consumerChannel, io.Closer, error) {
			if dialCalls.Add(1) == 1 {
				return first, &concurrentCountingCloser{}, nil
			}
			recovered <- observation{
				endpoint: endpoint, root: string(config.TLS.RootCAs[0]),
				certificate: string(config.TLS.ClientCertificate), privateKey: string(config.TLS.ClientPrivateKey),
			}
			return newFakeConsumerChannel(), &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	connection.Endpoints[1].Host = "redirected.invalid"
	connection.TLS.RootCAs[0][0] = 'X'
	connection.TLS.ClientCertificate[0] = 'X'
	connection.TLS.ClientPrivateKey[0] = 'X'
	first.cancelOnce.Do(func() { close(first.deliveries) })
	select {
	case got := <-recovered:
		if got.endpoint.Host != "rabbitmq-2.internal" || got.root != "root-material" ||
			got.certificate != "certificate-material" || got.privateKey != "private-key-material" {
			t.Fatalf("recovery configuration was aliased: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime recovery did not start")
	}
}

func TestConsumerCloseKeepsCallerDeadlineDuringRuntimeCleanup(t *testing.T) {
	t.Parallel()

	channel := newFakeConsumerChannel()
	resource := newBlockingCloser()
	config := testConsumerConfig()
	config.HandlerTimeout = time.Second
	consumer, err := newConsumerFromChannel(
		t.Context(),
		config,
		func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil },
		channel,
		resource,
	)
	if err != nil {
		t.Fatalf("construct consumer: %v", err)
	}
	channel.cancelOnce.Do(func() { close(channel.deliveries) })
	select {
	case <-resource.started:
	case <-time.After(time.Second):
		t.Fatal("runtime cleanup did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	closeErr := consumer.Close(ctx)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		close(resource.release)
		t.Fatalf("Close() elapsed = %s, want caller-bounded return", elapsed)
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		close(resource.release)
		t.Fatalf("Close() error = %v, want deadline exceeded", closeErr)
	}
	close(resource.release)
}

func TestConsumerDoesNotSettleSupersededDeliveryOnRecoveredChannel(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	first := newFakeConsumerChannel()
	first.ackErr = errors.New("lost connection")
	firstResource := &concurrentCountingCloser{}
	second := newFakeConsumerChannel()
	started := make(chan struct{})
	release := make(chan struct{})
	dialed := make(chan int, 2)
	var dialCalls atomic.Int32
	consumer, err := openConsumerWith(
		t.Context(),
		connection,
		testConsumerConfig(),
		func(context.Context, Delivery) (Settlement, error) {
			select {
			case <-started:
			default:
				close(started)
				<-release
			}
			return Acknowledge(), nil
		},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
			attempt := int(dialCalls.Add(1))
			dialed <- attempt
			if attempt == 1 {
				return first, firstResource, nil
			}
			return second, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("openConsumerWith(): %v", err)
	}
	t.Cleanup(func() { closeConsumerForTest(t, consumer) })
	<-dialed
	first.deliveries <- testAMQPDelivery(42)
	<-started
	first.cancelOnce.Do(func() { close(first.deliveries) })
	<-dialed
	close(release)
	second.deliveries <- testAMQPDelivery(43)
	select {
	case settled := <-second.settled:
		if settled.tag != 43 {
			t.Fatalf("recovered channel settled superseded tag: %#v", settled)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered consumer did not remain available after stale settlement failure")
	}
	if consumer.Readiness() != ReadinessReady || consumer.DependencyHealth() != DependencyAvailable {
		t.Fatalf("stale settlement changed recovered health = (%q, %q)", consumer.Readiness(), consumer.DependencyHealth())
	}
}
