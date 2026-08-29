package rabbitmqqueue

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublicOpenBoundariesRejectNilContextsBeforeDial(t *testing.T) {
	t.Parallel()

	var missingContext context.Context
	if producer, err := OpenProducer(missingContext, testConnectionConfig(), testProducerConfig()); producer != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("OpenProducer(nil) = (%#v, %v), want context required", producer, err)
	}
	if consumer, err := OpenConsumer(missingContext, testConnectionConfig(), testConsumerConfig(), func(context.Context, Delivery) (Settlement, error) {
		return Acknowledge(), nil
	}); consumer != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("OpenConsumer(nil) = (%#v, %v), want context required", consumer, err)
	}
}

func TestAMQPClientConfigurationRejectsUnboundedAndInvalidTLS(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	credentials := Credentials{Username: "worker", Password: []byte("secret")}
	if _, _, _, err := buildAMQPClientConfig(context.Background(), connection.Endpoints[0], connection, credentials); !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("buildAMQPClientConfig() error = %v, want unavailable", err)
	}

	connection.TLS.RootCAs = [][]byte{[]byte("not a certificate")}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, _, err := buildAMQPClientConfig(ctx, connection.Endpoints[0], connection, credentials); !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("invalid TLS error = %v, want unavailable", err)
	}
}

func TestAMQPClientDialUsesTheBoundedContext(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, config, _, err := buildAMQPClientConfig(ctx, connection.Endpoints[0], connection, Credentials{Username: "worker", Password: []byte("secret")})
	if err != nil {
		t.Fatalf("buildAMQPClientConfig(): %v", err)
	}
	if _, err := config.Dial("unsupported-network", "ignored"); err == nil {
		t.Fatal("Dial() accepted an unsupported network")
	}
}

func TestBoundedNetworkDialRejectsUnboundedAndDeadlineFailures(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	if connection, err := boundedNetworkDial(context.Background(), func(context.Context, string, string) (net.Conn, error) {
		return client, nil
	}, "tcp", "ignored"); connection != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("unbounded dial = (%#v, %v), want unavailable", connection, err)
	}

	client, server = net.Pipe()
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	failing := &deadlineErrorConnection{Conn: client}
	if connection, err := boundedNetworkDial(ctx, func(context.Context, string, string) (net.Conn, error) {
		return failing, nil
	}, "tcp", "ignored"); connection != nil || !errors.Is(err, ErrProducerUnavailable) || !failing.closed {
		t.Fatalf("deadline failure = (%#v, %v), closed %t", connection, err, failing.closed)
	}
}

func TestAMQPDialWrappersSanitizeConfigurationFailures(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	credentials := Credentials{Username: "worker", Password: []byte("secret")}
	if channel, resource, err := dialAMQPProducer(context.Background(), connection.Endpoints[0], connection, credentials); channel != nil || resource != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("dialAMQPProducer() = (%#v, %#v, %v)", channel, resource, err)
	}
	if channel, resource, err := dialAMQPConsumer(context.Background(), connection.Endpoints[0], connection, credentials); channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("dialAMQPConsumer() = (%#v, %#v, %v)", channel, resource, err)
	}
	if channel, resource, err := dialAMQPTopology(context.Background(), connection.Endpoints[0], connection, credentials); channel != nil || resource != nil || !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("dialAMQPTopology() = (%#v, %#v, %v)", channel, resource, err)
	}
}

func TestAMQPConnectionWrappersSanitizeNativeDialFailures(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(time.Second)
	if channel, resource, err := openAMQPConnection("://", amqp.Config{}, deadline); channel != nil || resource != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("openAMQPConnection() = (%#v, %#v, %v)", channel, resource, err)
	}
	if channel, resource, err := openAMQPConsumerConnection("://", amqp.Config{}, deadline); channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("openAMQPConsumerConnection() = (%#v, %#v, %v)", channel, resource, err)
	}
	if channel, resource, err := openAMQPTopologyConnection("://", amqp.Config{}, deadline); channel != nil || resource != nil || !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("openAMQPTopologyConnection() = (%#v, %#v, %v)", channel, resource, err)
	}
}

func TestQueueReferenceAndBindingPublicValidation(t *testing.T) {
	t.Parallel()

	if err := (QueueReference{Name: "orders", Type: QueueQuorum}).Validate(); err != nil {
		t.Fatalf("QueueReference.Validate(): %v", err)
	}
	if !validBindingArguments([]Header{StringHeader("tenant", "north")}) {
		t.Fatal("validBindingArguments() rejected a stable header")
	}
}

func TestUnsignedAMQPIntegerCoversEveryBrokerIntegerRepresentation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value any
		want  uint64
		ok    bool
	}{
		{int8(-1), ^uint64(0), false},
		{int16(-2), ^uint64(1), false},
		{int32(-3), ^uint64(2), false},
		{int64(-4), ^uint64(3), false},
		{uint8(5), 5, true},
		{uint16(6), 6, true},
		{uint32(7), 7, true},
		{uint64(8), 8, true},
		{"9", 0, false},
	}
	for _, test := range tests {
		got, ok := unsignedAMQPInteger(test.value)
		if got != test.want || ok != test.ok {
			t.Fatalf("unsignedAMQPInteger(%T(%v)) = (%d, %t), want (%d, %t)", test.value, test.value, got, ok, test.want, test.ok)
		}
	}
}

func TestNilSettlementCompletionAndUnavailableProducerHealth(t *testing.T) {
	t.Parallel()

	(Delivery{}).completeSettlement(nil)
	producer := &Producer{unavailable: true}
	if got := producer.DependencyHealth(); got != DependencyUnavailable {
		t.Fatalf("DependencyHealth() = %q, want unavailable", got)
	}
}

func TestInvalidSettlementAndNilTopologyErrorsRemainExplicit(t *testing.T) {
	t.Parallel()

	if err := applySettlement(newFakeConsumerChannel(), 1, Settlement{}); !errors.Is(err, ErrInvalidSettlement) {
		t.Fatalf("applySettlement() error = %v, want invalid settlement", err)
	}
	if err := topologyOperationError(nil); err != nil {
		t.Fatalf("topologyOperationError(nil) = %v", err)
	}
}

func TestProducerSessionAndTLSHelpersExposeFailureBoundaries(t *testing.T) {
	t.Parallel()

	if session, err := producerSessionFrom(nilReader{}); session != "" || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("producerSessionFrom() = (%q, %v), want unavailable", session, err)
	}
	if _, err := buildTLSConfigWithSystemRoots(TLSConfig{}, func() (*x509.CertPool, error) {
		return nil, errors.New("root store unavailable")
	}); !errors.Is(err, ErrInvalidTLS) {
		t.Fatalf("system root failure = %v, want invalid TLS", err)
	}
	config, err := buildTLSConfigWithSystemRoots(TLSConfig{ServerName: "rabbitmq.internal"}, func() (*x509.CertPool, error) {
		return nil, nil
	})
	if err != nil || config.RootCAs == nil {
		t.Fatalf("nil system roots = (%#v, %v), want private root pool", config, err)
	}
}

func TestNativeAMQPDialOwnershipIsSanitized(t *testing.T) {
	t.Parallel()

	owned := &fakeAMQPConnection{}
	wrap := func(*amqp.Connection) amqpConnection { return owned }
	connection, err := dialAMQPConnectionWithNative("amqps://ignored", amqp.Config{}, func(string, amqp.Config) (*amqp.Connection, error) {
		return &amqp.Connection{}, nil
	}, wrap)
	if err != nil || connection != owned {
		t.Fatalf("successful native dial = (%#v, %v)", connection, err)
	}
	connection, err = dialAMQPConnectionWithNative("amqps://ignored", amqp.Config{}, func(string, amqp.Config) (*amqp.Connection, error) {
		return &amqp.Connection{}, errors.New("handshake failed")
	}, wrap)
	if connection != nil || !errors.Is(err, ErrProducerUnavailable) || owned.closeCalls != 1 {
		t.Fatalf("partial native dial = (%#v, %v), close calls %d", connection, err, owned.closeCalls)
	}
	connection, err = dialAMQPConnectionWithNative("amqps://ignored", amqp.Config{}, func(string, amqp.Config) (*amqp.Connection, error) {
		return nil, nil
	}, wrap)
	if connection != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("nil native dial = (%#v, %v)", connection, err)
	}
	if wrapped := wrapNativeAMQPConnection(&amqp.Connection{}); wrapped == nil {
		t.Fatal("wrapNativeAMQPConnection() returned nil")
	}
	native := &nativeAMQPConnection{opener: fakeNativeChannelOpener{err: amqp.ErrClosed}}
	if channel, err := native.Channel(); err == nil {
		t.Fatalf("native Channel() = (%#v, %v), want closed", channel, err)
	}
}

func TestSettlementResultPrefersCompletedBrokerOutcome(t *testing.T) {
	t.Parallel()

	settlement := newDeliverySettlement()
	resultErr := errors.New("settlement failed")
	Delivery{settlement: settlement}.completeSettlement(resultErr)
	if err := settlementResultOrContext(settlement, context.Canceled); !errors.Is(err, resultErr) {
		t.Fatalf("settlementResultOrContext() = %v, want broker result", err)
	}
	pending := newDeliverySettlement()
	if err := settlementResultOrContext(pending, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("pending settlement = %v, want context cancellation", err)
	}
}

func TestDeliveryRejectsMalformedBrokerMetadataBoundaries(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*amqp.Delivery){
		"identity control":    func(delivery *amqp.Delivery) { delivery.MessageId = "bad\nidentity" },
		"pre epoch timestamp": func(delivery *amqp.Delivery) { delivery.Timestamp = time.Unix(-1, 0) },
		"delivery mode":       func(delivery *amqp.Delivery) { delivery.DeliveryMode = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			source := testAMQPDelivery(41)
			mutate(&source)
			if _, err := deliveryFromAMQP(source, testConsumerConfig()); !errors.Is(err, ErrInvalidDelivery) {
				t.Fatalf("deliveryFromAMQP() error = %v, want invalid delivery", err)
			}
		})
	}

	limits := DefaultLimits()
	table := make(amqp.Table, limits.MaxHeaderEntries+1)
	for index := 0; index <= limits.MaxHeaderEntries; index++ {
		table[fmt.Sprintf("header-%d", index)] = "value"
	}
	if _, _, err := deliveryHeaders(table, limits); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("too many headers error = %v", err)
	}
	if _, _, err := deliveryHeaders(amqp.Table{"bad\nkey": "value"}, limits); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("invalid header identity error = %v", err)
	}
	limits.MaxHeaderBytes = 4
	if _, _, err := deliveryHeaders(amqp.Table{"key": "value"}, limits); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("header byte budget error = %v", err)
	}
	if _, err := deliveryDeathSummaryBytes(amqp.Table{firstDeathQueueHeader: "orders"}, limits); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("death summary budget error = %v", err)
	}
	if _, err := deliveryDeaths(amqp.Table{deathHeader: []any{"not a table"}}, DefaultLimits(), 0); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("non-table death error = %v", err)
	}
	if _, _, err := deliveryDeath(amqp.Table{}, DefaultLimits()); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("missing death count error = %v", err)
	}
	death := validDeathTable()
	death["routing-keys"] = "orders"
	if _, _, err := deliveryDeath(death, DefaultLimits()); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("invalid death routing list error = %v", err)
	}
	death = validDeathTable()
	death["routing-keys"] = []any{"bad\nrouting"}
	if _, _, err := deliveryDeath(death, DefaultLimits()); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("invalid death routing key error = %v", err)
	}
}

func TestTopologyValidationReportsEachOwningBoundary(t *testing.T) {
	t.Parallel()

	passive := TopologyPolicy{Mode: TopologyPassive}
	tests := []struct {
		topology Topology
		policy   TopologyPolicy
		want     error
	}{
		{Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}}}, TopologyPolicy{Mode: TopologyDeclare}, ErrTopologyMutationDenied},
		{Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeKind("invalid")}}}, passive, ErrUnsupportedExchangeKind},
		{Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic}, {Name: "events", Kind: ExchangeTopic}}}, passive, ErrInvalidTopology},
		{Topology{Queues: []Queue{{Name: "orders", Type: QueueType("invalid")}}}, passive, ErrUnsupportedQueuePolicy},
	}
	for _, test := range tests {
		if err := test.topology.Validate(test.policy); !errors.Is(err, test.want) {
			t.Fatalf("Topology.Validate() error = %v, want %v", err, test.want)
		}
	}
	if validExchangeBindingWithLimits(ExchangeKind("invalid"), "", nil, DefaultLimits()) {
		t.Fatal("unknown exchange kind accepted a binding")
	}
	limits := DefaultLimits()
	arguments := make([]Header, limits.MaxHeaderEntries+1)
	if validBindingArgumentsWithLimits(arguments, limits) {
		t.Fatal("oversized binding argument list was accepted")
	}
}

func TestTopologyApplyRejectsInvalidInputsWithoutDial(t *testing.T) {
	t.Parallel()

	valid := Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}}}
	policy := TopologyPolicy{Mode: TopologyPassive}
	if _, err := applyTopologyWith(t.Context(), ConnectionConfig{}, policy, valid, nil); !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("invalid connection error = %v", err)
	}
	if _, err := applyTopologyWith(t.Context(), testConnectionConfig(), policy, Topology{}, nil); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("invalid topology error = %v", err)
	}
	if _, err := applyTopologyWith(t.Context(), testConnectionConfig(), policy, valid, nil); !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("nil dial error = %v", err)
	}
}

func TestConsumerCleanupAndSetupBoundariesAreBounded(t *testing.T) {
	t.Parallel()

	channel := &nilCancellationConsumerChannel{fakeConsumerChannel: newFakeConsumerChannel()}
	resource := &countingCloser{}
	if generation, err := setupConsumerGeneration(t.Context(), testConsumerConfig(), channel, resource); generation != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("nil cancellation stream = (%#v, %v)", generation, err)
	}
	consumer := &Consumer{}
	var missingContext context.Context
	if err := consumer.Drain(missingContext); !errors.Is(err, ErrContextRequired) {
		t.Fatalf("Drain(nil) error = %v", err)
	}
	if err := consumer.Close(missingContext); !errors.Is(err, ErrContextRequired) {
		t.Fatalf("Close(nil) error = %v", err)
	}
	if err := consumer.closeGeneration(nil, time.Now()); err != nil {
		t.Fatalf("closeGeneration(nil) = %v", err)
	}

	result := make(chan error)
	if err, completed := waitForConsumerClose(result, time.Now().Add(5*time.Millisecond)); err != nil || completed {
		t.Fatalf("waitForConsumerClose() = (%v, %t), want timeout", err, completed)
	}
	blocking := newBlockingCloser()
	deadline := time.Now().Add(5 * time.Millisecond)
	if err := boundedCloseConsumerResources(nil, blocking, deadline); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("boundedCloseConsumerResources() = %v, want unavailable", err)
	}
	close(blocking.release)
}

func TestAMQPConsumerAndProducerConnectionFailureOwnership(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(time.Second)
	partial := &fakeAMQPConnection{}
	if channel, resource, err := openAMQPConsumerConnectionWith("ignored", amqp.Config{}, deadline, func(string, amqp.Config) (amqpConnection, error) {
		return partial, errors.New("handshake failed")
	}); channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) || partial.closeCalls != 1 {
		t.Fatalf("partial consumer dial = (%#v, %#v, %v), closes %d", channel, resource, err, partial.closeCalls)
	}
	channelFailure := &fakeAMQPConnection{channelErr: errors.New("channel failed")}
	if channel, resource, err := openAMQPConsumerConnectionWith("ignored", amqp.Config{}, deadline, func(string, amqp.Config) (amqpConnection, error) {
		return channelFailure, nil
	}); channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) || channelFailure.closeCalls != 1 {
		t.Fatalf("consumer channel failure = (%#v, %#v, %v), closes %d", channel, resource, err, channelFailure.closeCalls)
	}
	if channel, resource, err := openAMQPConnectionWith("ignored", amqp.Config{}, deadline, func(string, amqp.Config) (amqpConnection, error) {
		return nil, nil
	}); channel != nil || resource != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("nil producer client = (%#v, %#v, %v)", channel, resource, err)
	}

	connection := testConnectionConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	endpoint := Endpoint{Host: "127.0.0.1", Port: 1}
	if channel, resource, err := dialAMQPConsumer(ctx, endpoint, connection, Credentials{Username: "worker", Password: []byte("secret")}); channel != nil || resource != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("native consumer refusal = (%#v, %#v, %v)", channel, resource, err)
	}
	if channel, resource, err := dialAMQPProducerWith(ctx, endpoint, connection, Credentials{Username: "worker", Password: []byte("secret")}, func(string, amqp.Config, time.Time) (producerChannel, io.Closer, error) {
		return nil, nil, errors.New("open failed")
	}); channel != nil || resource != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("producer open failure = (%#v, %#v, %v)", channel, resource, err)
	}
}

func TestConsumerOpeningAndRecoveryCancellationBranches(t *testing.T) {
	t.Parallel()

	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if consumer, err := openConsumerWith(ctx, testConnectionConfig(), testConsumerConfig(), handler, unavailableConsumerDial); consumer != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled open = (%#v, %v)", consumer, err)
	}

	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 3
	connection.Recovery.InitialDelay = time.Millisecond
	connection.Recovery.MaxDelay = time.Millisecond
	openContext, stopOpen := context.WithCancel(context.Background())
	dials := 0
	_, err := openConsumerWith(openContext, connection, testConsumerConfig(), handler, func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
		dials++
		if dials == 1 {
			stopOpen()
		}
		return nil, nil, errors.New("unavailable")
	})
	if !errors.Is(err, context.Canceled) || dials != 1 {
		t.Fatalf("cancelled backoff = (%v, %d dials)", err, dials)
	}

	recoveryContext, stopRecovery := context.WithCancel(context.Background())
	consumer := &Consumer{
		config: testConsumerConfig(), recoveryContext: recoveryContext, stopRecovery: stopRecovery,
		observations: newObservationStream(ObservationConsumer, observationBufferSize),
		recovery:     &consumerRecovery{connection: testConnectionConfig(), dial: unavailableConsumerDial},
	}
	stopRecovery()
	if generation, ok := consumer.recoverRuntime(); generation != nil || ok {
		t.Fatalf("cancelled recovery = (%#v, %t)", generation, ok)
	}
}

func TestConsumerRuntimeRecoveryRejectsInvalidSetupAndStoppingReplacement(t *testing.T) {
	t.Parallel()

	newConsumer := func(connection ConnectionConfig, dial consumerDialFunc) *Consumer {
		recoveryContext, stopRecovery := context.WithCancel(context.Background())
		return &Consumer{
			config: testConsumerConfig(), recoveryContext: recoveryContext, stopRecovery: stopRecovery,
			observations: newObservationStream(ObservationConsumer, observationBufferSize),
			recovery:     &consumerRecovery{connection: connection, dial: dial},
		}
	}
	invalidCredentials := testConnectionConfig()
	invalidCredentials.Recovery.MaxAttempts = 1
	invalidCredentials.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) { return Credentials{}, nil })
	consumer := newConsumer(invalidCredentials, unavailableConsumerDial)
	if generation, ok := consumer.recoverRuntime(); generation != nil || ok {
		t.Fatalf("invalid credential recovery = (%#v, %t)", generation, ok)
	}
	consumer.stopRecovery()

	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 2
	connection.Recovery.InitialDelay = time.Millisecond
	connection.Recovery.MaxDelay = 4 * time.Millisecond
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) { return Credentials{}, nil })
	consumer = newConsumer(connection, unavailableConsumerDial)
	if generation, ok := consumer.recoverRuntime(); generation != nil || ok {
		t.Fatalf("exhausted invalid credential recovery = (%#v, %t)", generation, ok)
	}
	consumer.stopRecovery()

	connection = testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	consumer = newConsumer(connection, func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
		channel := newFakeConsumerChannel()
		channel.qosErr = errors.New("setup failed")
		return channel, &countingCloser{}, nil
	})
	if generation, ok := consumer.recoverRuntime(); generation != nil || ok {
		t.Fatalf("setup failure recovery = (%#v, %t)", generation, ok)
	}
	consumer.stopRecovery()

	consumer = newConsumer(connection, func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
		return newFakeConsumerChannel(), &countingCloser{}, nil
	})
	consumer.stopping = true
	if generation, ok := consumer.recoverRuntime(); generation != nil || ok {
		t.Fatalf("stopping recovery = (%#v, %t)", generation, ok)
	}
	consumer.stopRecovery()
}

func TestConsumerGenerationSelectsCancellationAndFailureBoundaries(t *testing.T) {
	t.Parallel()

	newConsumer := func() (*Consumer, context.CancelFunc, context.CancelFunc) {
		lifetime, stopLifetime := context.WithCancel(context.Background())
		recovery, stopRecovery := context.WithCancel(context.Background())
		return &Consumer{
			config: testConsumerConfig(), lifetimeContext: lifetime, stopLifetime: stopLifetime,
			recoveryContext: recovery, stopRecovery: stopRecovery,
			jobs: make(chan consumerEnvelope, 8), admissionChanged: make(chan struct{}, 1),
			observations: newObservationStream(ObservationConsumer, observationBufferSize),
		}, stopLifetime, stopRecovery
	}
	consumer, stopLifetime, stopRecovery := newConsumer()
	channel := &failingRejectConsumerChannel{fakeConsumerChannel: newFakeConsumerChannel()}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{DeliveryTag: 1}
	generation := &consumerGeneration{channel: channel, deliveries: deliveries, cancellations: make(chan string), failure: make(chan struct{}, 1)}
	if !consumer.consumeGeneration(generation) {
		t.Fatal("invalid delivery settlement failure did not request recovery")
	}
	stopLifetime()
	stopRecovery()

	consumer, stopLifetime, stopRecovery = newConsumer()
	cancellations := make(chan string, 1)
	cancellations <- consumer.config.Name
	generation = &consumerGeneration{channel: newFakeConsumerChannel(), deliveries: make(chan amqp.Delivery), cancellations: cancellations, failure: make(chan struct{}, 1)}
	if !consumer.consumeGeneration(generation) {
		t.Fatal("broker cancellation did not request recovery")
	}
	stopLifetime()
	stopRecovery()

	consumer, stopLifetime, stopRecovery = newConsumer()
	stopRecovery()
	generation = &consumerGeneration{channel: newFakeConsumerChannel(), deliveries: make(chan amqp.Delivery), cancellations: make(chan string), failure: make(chan struct{}, 1)}
	if consumer.consumeGeneration(generation) {
		t.Fatal("recovery shutdown requested another recovery")
	}
	stopLifetime()

	consumer, stopLifetime, stopRecovery = newConsumer()
	consumer.stopping = true
	cancellations = make(chan string, 1)
	cancellations <- consumer.config.Name
	deliveries = make(chan amqp.Delivery)
	generation = &consumerGeneration{channel: newFakeConsumerChannel(), deliveries: deliveries, cancellations: cancellations, failure: make(chan struct{}, 1)}
	go func() {
		time.Sleep(time.Millisecond)
		close(deliveries)
	}()
	if consumer.consumeGeneration(generation) {
		t.Fatal("stopping cancellation requested recovery")
	}
	stopLifetime()
	stopRecovery()

	consumer, stopLifetime, stopRecovery = newConsumer()
	pending := make(chan string, 1)
	pending <- consumer.config.Name
	consumer.observePendingCancellation(pending)
	stopLifetime()
	stopRecovery()

	consumer, stopLifetime, stopRecovery = newConsumer()
	closedCancellations := make(chan string)
	close(closedCancellations)
	generation = &consumerGeneration{channel: newFakeConsumerChannel(), deliveries: make(chan amqp.Delivery), cancellations: closedCancellations, failure: make(chan struct{}, 1)}
	go func() {
		time.Sleep(time.Millisecond)
		stopLifetime()
	}()
	if consumer.consumeGeneration(generation) {
		t.Fatal("closed cancellation stream requested recovery")
	}
	stopRecovery()
}

func TestConsumerGenerationCloseHonorsItsOwnDeadline(t *testing.T) {
	t.Parallel()

	generation := &consumerGeneration{closeDone: make(chan struct{})}
	generation.closeOnce.Do(func() {})
	consumer := &Consumer{}
	if err := consumer.closeGeneration(generation, time.Now().Add(5*time.Millisecond)); !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("closeGeneration() = %v, want unavailable", err)
	}
}

func TestProducerCoverageBoundariesRemainDeterministic(t *testing.T) {
	t.Parallel()

	var nilContext context.Context
	if producer, err := newProducerFromChannelWithRecovery(nilContext, testProducerConfig(), "session", newFakeProducerChannel(), &countingCloser{}, nil); producer != nil || !errors.Is(err, ErrContextRequired) {
		t.Fatalf("nil producer context = (%#v, %v)", producer, err)
	}
	priority := uint16(7)
	expiration := time.Second
	original := Publication{Message: Message{Priority: &priority, Expiration: &expiration}}
	owned := ownPublication(original)
	priority = 1
	expiration = 2 * time.Second
	if *owned.Message.Priority != 7 || *owned.Message.Expiration != time.Second {
		t.Fatal("owned publication retained pointer aliases")
	}

	tracker := newPublishTracker(2)
	attempt, err := tracker.register(1, "session/1", "events", "orders")
	if err != nil {
		t.Fatalf("register attempt: %v", err)
	}
	attempt.outcome <- PublishResult{State: PublishConfirmed}
	producer := &Producer{}
	if result, err := producer.waitForOutcome(t.Context(), tracker, attempt); err != nil || result.State != PublishConfirmed {
		t.Fatalf("immediate outcome = (%#v, %v)", result, err)
	}

	tracker = newPublishTracker(2)
	attempt, err = tracker.register(2, "session/2", "events", "orders")
	if err != nil {
		t.Fatalf("register cancellation attempt: %v", err)
	}
	if result, err := outcomeOrCancellation(tracker, attempt, context.Canceled); result.State != PublishAmbiguous || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation outcome = (%#v, %v)", result, err)
	}
	attempt = &publishAttempt{sequence: 3, outcome: make(chan PublishResult, 1)}
	attempt.outcome <- PublishResult{State: PublishRejected}
	if result, err := outcomeOrCancellation(newPublishTracker(1), attempt, context.Canceled); result.State != PublishRejected || !errors.Is(err, ErrPublishRejected) {
		t.Fatalf("completed cancellation outcome = (%#v, %v)", result, err)
	}
	attempt = &publishAttempt{sequence: 4, outcome: make(chan PublishResult)}
	go func() { attempt.outcome <- PublishResult{State: PublishReturned} }()
	if result, err := outcomeOrCancellation(newPublishTracker(1), attempt, context.Canceled); result.State != PublishReturned || !errors.Is(err, ErrPublishReturned) {
		t.Fatalf("removed cancellation outcome = (%#v, %v)", result, err)
	}
	if err := closeProducerGeneration(nil, nil, nil, time.Now()); err != nil {
		t.Fatalf("closeProducerGeneration(nil) = %v", err)
	}
}

func TestProducerPublishCoversFullTrackerAndPostTransmissionTimeout(t *testing.T) {
	t.Parallel()

	producer := &Producer{
		config: testProducerConfig(), session: "session", channel: newFakeProducerChannel(),
		resource: &countingCloser{}, tracker: newPublishTracker(1),
		generationClose: &producerGenerationClose{}, failure: make(chan struct{}, 1),
		observations: newObservationStream(ObservationProducer, observationBufferSize),
	}
	if _, err := producer.tracker.register(99, "session/99", "events", "orders"); err != nil {
		t.Fatalf("fill tracker: %v", err)
	}
	if result, err := producer.publishAdmitted(t.Context(), testPublication()); result.State != PublishNotSent || !errors.Is(err, ErrOutstandingConfirmLimit) {
		t.Fatalf("full tracker publish = (%#v, %v)", result, err)
	}

	release := make(chan struct{})
	channel := newFakeProducerChannel()
	channel.publish = func(ctx context.Context, _, _ string, _, _ bool, _ amqp.Publishing) error {
		<-ctx.Done()
		<-release
		return nil
	}
	resource := &deadlineTrackingCloser{onDeadline: func() { close(release) }}
	config := testProducerConfig()
	config.PublishTimeout = 5 * time.Millisecond
	producer = &Producer{
		config: config, session: "session", channel: channel, resource: resource,
		tracker: newPublishTracker(1), generationClose: &producerGenerationClose{},
		failure: make(chan struct{}, 1), observations: newObservationStream(ObservationProducer, observationBufferSize),
	}
	if result, err := producer.publishAdmitted(context.Background(), testPublication()); result.State != PublishAmbiguous || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("post-transmission timeout = (%#v, %v)", result, err)
	}
}

func TestProducerGenerationIgnoresClosedBlockingStream(t *testing.T) {
	t.Parallel()

	events, stopEvents := context.WithCancel(context.Background())
	defer stopEvents()
	producer := &Producer{
		eventsContext: events, blockedEvents: make(chan ConnectionBlockedState, 1),
		observations: newObservationStream(ObservationProducer, observationBufferSize),
	}
	blocked := make(chan amqp.Blocking)
	close(blocked)
	failure := make(chan struct{}, 1)
	go func() {
		time.Sleep(time.Millisecond)
		failure <- struct{}{}
	}()
	if !producer.runGeneration(nil, nil, nil, blocked, newPublishTracker(1), failure) {
		t.Fatal("generation failure did not request recovery")
	}
}

func TestCompletePublishErrorPreservesEarlierBrokerOutcome(t *testing.T) {
	t.Parallel()

	tracker := newPublishTracker(1)
	attempt, err := tracker.register(1, "session/1", "events", "orders")
	if err != nil {
		t.Fatalf("register attempt: %v", err)
	}
	tracker.confirm(1, true)
	producer := &Producer{}
	result, resultErr := producer.completePublishError(tracker, attempt, PublishConfirmed, ErrPublishAmbiguous)
	if result.State != PublishConfirmed || !errors.Is(resultErr, ErrPublishAmbiguous) {
		t.Fatalf("completePublishError() = (%#v, %v)", result, resultErr)
	}
}

func TestProducerRecoveryCoversCredentialSessionSetupAndClosedBoundaries(t *testing.T) {
	t.Parallel()

	newProducer := func(connection ConnectionConfig, session func() (string, error), dial producerDialFunc) (*Producer, context.CancelFunc) {
		events, stopEvents := context.WithCancel(context.Background())
		return &Producer{
			config: testProducerConfig(), eventsContext: events, stopEvents: stopEvents,
			observations: newObservationStream(ObservationProducer, observationBufferSize),
			recovery:     &producerRecovery{connection: connection, session: session, dial: dial},
		}, stopEvents
	}
	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) { return Credentials{}, nil })
	producer, stop := newProducer(connection, func() (string, error) { return "session", nil }, unavailableDial)
	if producer.recoverRuntime() {
		t.Fatal("invalid credentials recovered producer")
	}
	stop()

	connection = testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	producer, stop = newProducer(connection, func() (string, error) { return "", errors.New("session failed") }, unavailableDial)
	if producer.recoverRuntime() {
		t.Fatal("invalid session recovered producer")
	}
	stop()

	producer, stop = newProducer(connection, func() (string, error) { return "session", nil }, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
		channel := newFakeProducerChannel()
		channel.confirmErr = errors.New("confirm failed")
		return channel, &countingCloser{}, nil
	})
	if producer.recoverRuntime() {
		t.Fatal("failed setup recovered producer")
	}
	stop()

	producer, stop = newProducer(connection, func() (string, error) { return "session", nil }, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
		return newFakeProducerChannel(), &countingCloser{}, nil
	})
	producer.closed = true
	if producer.recoverRuntime() {
		t.Fatal("closed producer accepted recovered generation")
	}
	stop()

	producer, stop = newProducer(connection, func() (string, error) { return "session", nil }, unavailableDial)
	stop()
	if producer.recoverRuntime() {
		t.Fatal("cancelled producer recovery succeeded")
	}

	connection.Recovery.MaxAttempts = 2
	connection.Recovery.InitialDelay = time.Millisecond
	connection.Recovery.MaxDelay = 4 * time.Millisecond
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) { return Credentials{}, nil })
	producer, stop = newProducer(connection, func() (string, error) { return "session", nil }, unavailableDial)
	if producer.recoverRuntime() {
		t.Fatal("exhausted producer recovery succeeded")
	}
	stop()
}

func TestOpeningBackoffGrowthAndSuccessfulAMQPBoundary(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 3
	connection.Recovery.InitialDelay = time.Millisecond
	connection.Recovery.MaxDelay = 4 * time.Millisecond
	if producer, err := openProducerWith(t.Context(), connection, testProducerConfig(), func() (string, error) { return "session", nil }, unavailableDial); producer != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("exhausted producer open = (%#v, %v)", producer, err)
	}
	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	if consumer, err := openConsumerWith(t.Context(), connection, testConsumerConfig(), handler, unavailableConsumerDial); consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("exhausted consumer open = (%#v, %v)", consumer, err)
	}
	client := &fakeAMQPConnection{channel: newFakeProducerChannel()}
	channel, resource, err := openAMQPConnectionWith("ignored", amqp.Config{}, time.Now().Add(time.Second), func(string, amqp.Config) (amqpConnection, error) {
		return client, nil
	})
	if err != nil || channel == nil || resource != client {
		t.Fatalf("successful AMQP boundary = (%#v, %#v, %v)", channel, resource, err)
	}
}

func TestTopologyOperationAndResourceFailuresAreExplicit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	generation := &topologyGeneration{}
	if err := runTopologyOperation(ctx, generation, func() error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled topology operation = %v", err)
	}
	resource := &countingCloser{err: errors.New("close failed")}
	if err := closeTopologyResources(nil, resource, time.Now().Add(time.Second)); !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("topology resource close = %v", err)
	}

	channel := &fakeTopologyChannel{queuePassive: func(string, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
		return amqp.Queue{}, nil
	}}
	_, err := applyTopologyChannel(t.Context(), &topologyGeneration{channel: channel}, TopologyPolicy{Mode: TopologyPassive}, Topology{Queues: []Queue{{Name: "orders", Type: QueueQuorum, Durable: true}}})
	if !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("invalid declared queue name error = %v", err)
	}
	channel = &fakeTopologyChannel{bind: func(string, string, string, amqp.Table) error { return errors.New("bind failed") }}
	_, err = applyTopologyChannel(t.Context(), &topologyGeneration{channel: channel}, TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}, Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}},
		Queues:    []Queue{{Name: "orders", Type: QueueQuorum, Durable: true}},
		Bindings:  []Binding{{Exchange: "events", Queue: "orders", RoutingKey: "orders.created"}},
	})
	if !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("binding failure error = %v", err)
	}
}

func TestTopologyRetriesAndExhaustionRemainBounded(t *testing.T) {
	t.Parallel()

	topology := Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}}}
	policy := TopologyPolicy{Mode: TopologyPassive}
	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) { return Credentials{}, nil })
	if _, err := applyTopologyWith(t.Context(), connection, policy, topology, func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
		return nil, nil, errors.New("unexpected")
	}); !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("invalid credential exhaustion = %v", err)
	}

	connection = testConnectionConfig()
	connection.Recovery.MaxAttempts = 3
	connection.Recovery.InitialDelay = time.Millisecond
	connection.Recovery.MaxDelay = 4 * time.Millisecond
	if _, err := applyTopologyWith(t.Context(), connection, policy, topology, func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
		return nil, nil, errors.New("dial failed")
	}); !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("dial exhaustion = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	dials := 0
	_, err := applyTopologyWith(ctx, connection, policy, topology, func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
		dials++
		cancel()
		return nil, nil, errors.New("dial failed")
	})
	if !errors.Is(err, context.Canceled) || dials != 1 {
		t.Fatalf("cancelled topology retry = (%v, %d dials)", err, dials)
	}

	ctx, cancel = context.WithCancel(context.Background())
	connection.Recovery.InitialDelay = 100 * time.Millisecond
	connection.Recovery.MaxDelay = 100 * time.Millisecond
	dials = 0
	_, err = applyTopologyWith(ctx, connection, policy, topology, func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
		dials++
		go func() {
			time.Sleep(time.Millisecond)
			cancel()
		}()
		return nil, nil, errors.New("dial failed")
	})
	if !errors.Is(err, context.Canceled) || dials != 1 {
		t.Fatalf("cancelled topology backoff = (%v, %d dials)", err, dials)
	}
}

func unavailableConsumerDial(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
	return nil, nil, errors.New("unavailable")
}

type nilCancellationConsumerChannel struct {
	*fakeConsumerChannel
}

type failingRejectConsumerChannel struct {
	*fakeConsumerChannel
}

type fakeNativeChannelOpener struct {
	channel *amqp.Channel
	err     error
}

func (opener fakeNativeChannelOpener) Channel() (*amqp.Channel, error) {
	return opener.channel, opener.err
}

func (*failingRejectConsumerChannel) Reject(uint64, bool) error {
	return errors.New("reject failed")
}

func (*nilCancellationConsumerChannel) NotifyCancel(chan string) chan string {
	return nil
}

func validDeathTable() amqp.Table {
	return amqp.Table{
		"count": int64(1), "reason": "rejected", "queue": "orders",
		"exchange": "events", "routing-keys": []any{"orders.created"},
		"time": time.Unix(100, 0),
	}
}

type deadlineErrorConnection struct {
	net.Conn
	closed bool
}

func (*deadlineErrorConnection) SetDeadline(time.Time) error {
	return errors.New("deadline rejected")
}

func (connection *deadlineErrorConnection) Close() error {
	connection.closed = true
	return connection.Conn.Close()
}

var _ io.Closer = (*deadlineErrorConnection)(nil)
