package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestApplyTopologyPassivelyVerifiesExchangeAndQueueWithoutMutation(t *testing.T) {
	t.Parallel()

	deliveryLimit := QueueDeliveryLimit(20)
	channel := &fakeTopologyChannel{}
	channel.exchangePassive = func(name, kind string, durable, autoDelete, internal bool) error {
		if name != "events" || kind != "topic" || !durable || autoDelete || internal {
			t.Fatalf("passive exchange = %q %q %t %t %t", name, kind, durable, autoDelete, internal)
		}
		return nil
	}
	channel.queuePassive = func(name string, durable, autoDelete, exclusive bool, arguments amqp.Table) (amqp.Queue, error) {
		if name != "orders" || !durable || autoDelete || exclusive || arguments["x-queue-type"] != "quorum" ||
			arguments["x-delivery-limit"] != int64(20) || arguments["x-single-active-consumer"] != true {
			t.Fatalf("passive queue = %q %t %t %t %#v", name, durable, autoDelete, exclusive, arguments)
		}
		return amqp.Queue{Name: name}, nil
	}
	resource := &concurrentCountingCloser{}
	topology := Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}},
		Queues: []Queue{{
			Name: "orders", Type: QueueQuorum, Durable: true,
			SingleActiveConsumer: true, DeliveryLimit: &deliveryLimit,
		}},
	}
	result, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive}, topology,
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, resource, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith(): %v", err)
	}
	if len(result.QueueNames) != 1 || result.QueueNames[0] != "orders" {
		t.Fatalf("queue names = %#v, want orders", result.QueueNames)
	}
	if channel.exchangeDeclareCalls != 0 || channel.queueDeclareCalls != 0 || channel.bindCalls != 0 {
		t.Fatalf("passive verification mutated topology: %#v", channel)
	}
	if channel.closeCount() != 1 || resource.count() != 1 {
		t.Fatalf("close calls = channel %d resource %d", channel.closeCount(), resource.count())
	}
}

func TestQueueArgumentsDistinguishesOmittedAndExplicitZeroDeliveryLimit(t *testing.T) {
	t.Parallel()

	omitted := queueArguments(Queue{Name: "orders", Type: QueueQuorum, Durable: true})
	if _, exists := omitted["x-delivery-limit"]; exists {
		t.Fatal("omitted delivery limit was declared")
	}

	zero := QueueDeliveryLimit(0)
	explicit := queueArguments(Queue{
		Name: "orders", Type: QueueQuorum, Durable: true, DeliveryLimit: &zero,
	})
	value, exists := explicit["x-delivery-limit"]
	if !exists || value != int64(0) {
		t.Fatalf("explicit zero delivery limit = %#v", explicit)
	}
}

func TestApplyTopologyMapsBoundedQuorumQueuePolicyArguments(t *testing.T) {
	t.Parallel()

	messageTTL := 30 * time.Second
	queueExpires := 10 * time.Minute
	maxLength := uint64(10_000)
	maxLengthBytes := uint64(64 << 20)
	routingKey := "orders.failed"
	channel := &fakeTopologyChannel{queuePassive: func(
		_ string, _, _, _ bool, arguments amqp.Table,
	) (amqp.Queue, error) {
		if len(arguments) != 9 || arguments["x-queue-type"] != "quorum" ||
			arguments["x-message-ttl"] != int64(30_000) ||
			arguments["x-expires"] != int64(600_000) ||
			arguments["x-max-length"] != int64(10_000) ||
			arguments["x-max-length-bytes"] != int64(64<<20) ||
			arguments["x-overflow"] != "reject-publish" ||
			arguments["x-dead-letter-exchange"] != "orders.dead" ||
			arguments["x-dead-letter-routing-key"] != "orders.failed" ||
			arguments["x-dead-letter-strategy"] != "at-least-once" {
			t.Fatal("passive queue did not receive the complete bounded queue policy")
		}
		return amqp.Queue{Name: "orders"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{
			Name: "orders", Type: QueueQuorum, Durable: true,
			MessageTTL: &messageTTL, Expires: &queueExpires,
			MaxLength: &maxLength, MaxLengthBytes: &maxLengthBytes,
			Overflow: QueueOverflowRejectPublish,
			DeadLetter: &QueueDeadLetter{
				Exchange: "orders.dead", RoutingKey: &routingKey, Strategy: DeadLetterAtLeastOnce,
			},
		}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith() error = %v", err)
	}
}

func TestApplyTopologyMapsQuorumConsumerTimeout(t *testing.T) {
	t.Parallel()

	consumerTimeout := time.Duration(0)
	var captured amqp.Table
	channel := &fakeTopologyChannel{queuePassive: func(
		_ string, _, _, _ bool, arguments amqp.Table,
	) (amqp.Queue, error) {
		captured = arguments
		return amqp.Queue{Name: "orders"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{
			Name: "orders", Type: QueueQuorum, Durable: true,
			ConsumerTimeout: &consumerTimeout,
		}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith() error = %v", err)
	}
	if len(captured) != 2 || captured["x-queue-type"] != "quorum" ||
		captured["x-consumer-timeout"] != int64(0) {
		t.Fatalf("passive queue arguments = %#v", captured)
	}
}

func TestApplyTopologyMapsQuorumDisconnectedConsumerTimeout(t *testing.T) {
	t.Parallel()

	disconnectedTimeout := time.Minute
	var captured amqp.Table
	channel := &fakeTopologyChannel{queuePassive: func(
		_ string, _, _, _ bool, arguments amqp.Table,
	) (amqp.Queue, error) {
		captured = arguments
		return amqp.Queue{Name: "orders"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{
			Name: "orders", Type: QueueQuorum, Durable: true,
			DisconnectedConsumerTimeout: &disconnectedTimeout,
		}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith() error = %v", err)
	}
	if len(captured) != 2 || captured["x-queue-type"] != "quorum" ||
		captured["x-consumer-disconnected-timeout"] != int64(60_000) {
		t.Fatalf("passive queue arguments = %#v", captured)
	}
}

func TestApplyTopologyMapsQuorumDelayedRetry(t *testing.T) {
	t.Parallel()

	maximum := 30 * time.Second
	var captured amqp.Table
	channel := &fakeTopologyChannel{queuePassive: func(
		_ string, _, _, _ bool, arguments amqp.Table,
	) (amqp.Queue, error) {
		captured = arguments
		return amqp.Queue{Name: "orders"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{
			Name: "orders", Type: QueueQuorum, Durable: true,
			DelayedRetry: &QueueDelayedRetry{
				Type: DelayedRetryFailed, Minimum: time.Second, Maximum: &maximum,
			},
		}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith() error = %v", err)
	}
	if len(captured) != 4 || captured["x-queue-type"] != "quorum" ||
		captured["x-delayed-retry-type"] != "failed" ||
		captured["x-delayed-retry-min"] != int64(1_000) ||
		captured["x-delayed-retry-max"] != int64(30_000) {
		t.Fatalf("passive queue arguments = %#v", captured)
	}

	disabled := queueArguments(Queue{
		Name: "orders", Type: QueueQuorum, Durable: true,
		DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryDisabled},
	})
	if len(disabled) != 2 || disabled["x-queue-type"] != "quorum" ||
		disabled["x-delayed-retry-type"] != "disabled" {
		t.Fatalf("disabled delayed retry arguments = %#v", disabled)
	}
}

func TestApplyTopologyMapsLegacyClassicDeadLetterArgumentsWithoutQuorumStrategy(t *testing.T) {
	t.Parallel()

	routingKey := "jobs.failed"
	channel := &fakeTopologyChannel{queuePassive: func(
		_ string, _, _, _ bool, arguments amqp.Table,
	) (amqp.Queue, error) {
		if len(arguments) != 4 || arguments["x-queue-type"] != "classic" ||
			arguments["x-overflow"] != "reject-publish-dlx" ||
			arguments["x-dead-letter-exchange"] != "jobs.dead" ||
			arguments["x-dead-letter-routing-key"] != "jobs.failed" {
			t.Fatal("passive queue did not receive the classic dead-letter policy")
		}
		if _, exists := arguments["x-dead-letter-strategy"]; exists {
			t.Fatal("classic queue received unsupported dead-letter strategy")
		}
		return amqp.Queue{Name: "jobs"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{
			Name: "jobs", Type: QueueClassic, Durable: true,
			Overflow:   QueueOverflowRejectPublishDeadLetter,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", RoutingKey: &routingKey},
		}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith() error = %v", err)
	}
}

func TestQueueArgumentsDistinguishesOmittedAndExplicitEmptyDeadLetterRoutingKey(t *testing.T) {
	t.Parallel()

	omitted := queueArguments(Queue{
		Name: "jobs", Type: QueueClassic, Durable: true,
		DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead"},
	})
	if _, exists := omitted["x-dead-letter-routing-key"]; exists {
		t.Fatal("omitted dead-letter routing key was declared")
	}

	empty := ""
	explicit := queueArguments(Queue{
		Name: "jobs", Type: QueueClassic, Durable: true,
		DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", RoutingKey: &empty},
	})
	value, exists := explicit["x-dead-letter-routing-key"]
	if !exists || value != "" {
		t.Fatal("explicit empty dead-letter routing key was not declared")
	}
}

func TestApplyTopologyOwnsOptionalQueuePolicyBeforeDial(t *testing.T) {
	t.Parallel()

	messageTTL := 30 * time.Second
	queueExpires := 10 * time.Minute
	consumerTimeout := 5 * time.Minute
	disconnectedConsumerTimeout := time.Minute
	deliveryLimit := QueueDeliveryLimit(20)
	delayedRetryMaximum := 30 * time.Second
	delayedRetry := &QueueDelayedRetry{
		Type: DelayedRetryAll, Minimum: time.Second, Maximum: &delayedRetryMaximum,
	}
	maxLength := uint64(10_000)
	maxLengthBytes := uint64(64 << 20)
	routingKey := "orders.failed"
	deadLetter := &QueueDeadLetter{
		Exchange: "orders.dead", RoutingKey: &routingKey, Strategy: DeadLetterAtLeastOnce,
	}
	var captured amqp.Table
	channel := &fakeTopologyChannel{queuePassive: func(
		_ string, _, _, _ bool, arguments amqp.Table,
	) (amqp.Queue, error) {
		captured = arguments
		return amqp.Queue{Name: "orders"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{
			Name: "orders", Type: QueueQuorum, Durable: true,
			DeliveryLimit: &deliveryLimit,
			MessageTTL:    &messageTTL, Expires: &queueExpires, ConsumerTimeout: &consumerTimeout,
			DisconnectedConsumerTimeout: &disconnectedConsumerTimeout,
			DelayedRetry:                delayedRetry,
			MaxLength:                   &maxLength, MaxLengthBytes: &maxLengthBytes,
			Overflow: QueueOverflowRejectPublish, DeadLetter: deadLetter,
		}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			messageTTL = -time.Millisecond
			queueExpires = 0
			consumerTimeout = time.Minute - time.Millisecond
			disconnectedConsumerTimeout = -time.Millisecond
			deliveryLimit = 0
			delayedRetryMaximum = time.Millisecond
			delayedRetry.Type = DelayedRetryType("mutated")
			delayedRetry.Minimum = time.Microsecond
			maxLength = uint64(math.MaxInt64) + 1
			maxLengthBytes = uint64(math.MaxInt64) + 1
			routingKey = "mutated\nroute"
			deadLetter.Exchange = "mutated\nexchange"
			deadLetter.Strategy = DeadLetterStrategy("mutated")
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith() error = %v", err)
	}
	if captured["x-message-ttl"] != int64(30_000) ||
		captured["x-delivery-limit"] != int64(20) ||
		captured["x-expires"] != int64(600_000) ||
		captured["x-consumer-timeout"] != int64(300_000) ||
		captured["x-consumer-disconnected-timeout"] != int64(60_000) ||
		captured["x-delayed-retry-type"] != "all" ||
		captured["x-delayed-retry-min"] != int64(1_000) ||
		captured["x-delayed-retry-max"] != int64(30_000) ||
		captured["x-max-length"] != int64(10_000) ||
		captured["x-max-length-bytes"] != int64(64<<20) ||
		captured["x-dead-letter-exchange"] != "orders.dead" ||
		captured["x-dead-letter-routing-key"] != "orders.failed" ||
		captured["x-dead-letter-strategy"] != "at-least-once" {
		t.Fatal("topology retained caller-owned optional queue policy")
	}
}

func TestApplyTopologyPublicBoundaryRejectsNilContext(t *testing.T) {
	t.Parallel()

	var missingContext context.Context
	result, err := ApplyTopology(
		missingContext, testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
	)
	if !errors.Is(err, ErrContextRequired) || len(result.QueueNames) != 0 {
		t.Fatalf("ApplyTopology(nil) = (%#v, %v), want context required", result, err)
	}
}

func TestApplyTopologyDeclaresDevelopmentTopologyWithExplicitArgumentsAndBinding(t *testing.T) {
	t.Parallel()

	channel := &fakeTopologyChannel{}
	var boundBytes []byte
	channel.exchangeDeclare = func(name, kind string, durable, autoDelete, internal bool) error {
		if name != "events" || kind != "headers" || !durable || autoDelete || internal {
			t.Fatalf("exchange declaration = %q %q %t %t %t", name, kind, durable, autoDelete, internal)
		}
		return nil
	}
	channel.queueDeclare = func(name string, durable, autoDelete, exclusive bool, arguments amqp.Table) (amqp.Queue, error) {
		if name != "orders" || !durable || autoDelete || exclusive || arguments["x-queue-type"] != "classic" ||
			arguments["x-max-priority"] != int32(5) {
			t.Fatalf("queue declaration = %q %t %t %t %#v", name, durable, autoDelete, exclusive, arguments)
		}
		return amqp.Queue{Name: name}, nil
	}
	channel.bind = func(queue, routingKey, exchange string, arguments amqp.Table) error {
		if queue != "orders" || routingKey != "" || exchange != "events" ||
			arguments["x-match"] != "all" || arguments["format"] != "json" ||
			arguments["active"] != true || arguments["attempt"] != int64(2) {
			t.Fatalf("binding = %q %q %q %#v", queue, routingKey, exchange, arguments)
		}
		var ok bool
		boundBytes, ok = arguments["signature"].([]byte)
		if !ok || len(boundBytes) != 2 || boundBytes[0] != 1 {
			t.Fatalf("binary binding argument = %#v", arguments["signature"])
		}
		return nil
	}
	bytesArgument := []byte{1, 2}
	topology := Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeHeaders, Durable: true}},
		Queues:    []Queue{{Name: "orders", Type: QueueClassic, Durable: true, MaxPriority: 5}},
		Bindings: []Binding{{
			Exchange: "events", Queue: "orders",
			Arguments: []Header{
				StringHeader("x-match", "all"), StringHeader("format", "json"),
				BoolHeader("active", true), Int64Header("attempt", 2),
				{Key: "signature", Kind: HeaderBytes, Bytes: bytesArgument},
			},
		}},
	}
	policy := TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}
	result, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), policy, topology,
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("applyTopologyWith(): %v", err)
	}
	if len(result.QueueNames) != 1 || result.QueueNames[0] != "orders" || channel.bindCalls != 1 {
		t.Fatalf("declaration result = %#v binds %d", result, channel.bindCalls)
	}
	topology.Bindings[0].Arguments[4].Bytes[0] = 9
	if boundBytes[0] != 1 {
		t.Fatal("binding declaration retained a caller-owned byte argument")
	}
}

func TestApplyTopologyPreservesNativeEmptyDirectAndTopicBindingKeys(t *testing.T) {
	t.Parallel()

	for _, kind := range []ExchangeKind{ExchangeDirect, ExchangeTopic} {
		kind := kind
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()

			channel := &fakeTopologyChannel{}
			channel.bind = func(queue, routingKey, exchange string, arguments amqp.Table) error {
				if queue != "orders" || routingKey != "" || exchange != "events" || len(arguments) != 0 {
					t.Fatalf("binding = %q %q %q %#v, want native empty %s key", queue, routingKey, exchange, arguments, kind)
				}
				return nil
			}
			_, err := applyTopologyWith(
				t.Context(), testConnectionConfig(),
				TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()},
				Topology{
					Exchanges: []Exchange{{Name: "events", Kind: kind, Durable: true}},
					Queues:    []Queue{{Name: "orders", Type: QueueClassic, Durable: true}},
					Bindings:  []Binding{{Exchange: "events", Queue: "orders"}},
				},
				func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
					return channel, &concurrentCountingCloser{}, nil
				},
			)
			if err != nil {
				t.Fatalf("applyTopologyWith() error = %v", err)
			}
			if channel.bindCalls != 1 {
				t.Fatalf("binding calls = %d, want 1", channel.bindCalls)
			}
		})
	}
}

func TestApplyTopologyRejectsExclusiveQueuesItCannotKeepAlive(t *testing.T) {
	t.Parallel()

	for name, queue := range map[string]Queue{
		"server named": {Type: QueueClassic, Exclusive: true, AutoDelete: true},
		"client named": {Name: "reply", Type: QueueClassic, Exclusive: true, AutoDelete: true},
	} {
		queue := queue
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dialed := false
			result, err := applyTopologyWith(
				t.Context(), testConnectionConfig(),
				TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()},
				Topology{Queues: []Queue{queue}},
				func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
					dialed = true
					return &fakeTopologyChannel{}, &concurrentCountingCloser{}, nil
				},
			)
			if !errors.Is(err, ErrInvalidTopology) || len(result.QueueNames) != 0 || dialed {
				t.Fatalf("applyTopologyWith() = (%#v, %v), dialed %t", result, err, dialed)
			}
		})
	}
}

func TestApplyTopologyRejectsPassiveBindingsAndInvalidGraphsBeforeDial(t *testing.T) {
	t.Parallel()

	valid := Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeDirect, Durable: true}},
		Queues:    []Queue{{Name: "orders", Type: QueueClassic, Durable: true}},
	}
	tests := map[string]struct {
		policy   TopologyPolicy
		topology Topology
		want     error
	}{
		"passive binding": {
			policy:   TopologyPolicy{Mode: TopologyPassive},
			topology: topologyWithBinding(valid, Binding{Exchange: "events", Queue: "orders", RoutingKey: "orders.created"}),
			want:     ErrPassiveBindingVerificationUnsupported,
		},
		"unknown exchange": {
			policy: TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()},
			topology: topologyWithBinding(valid, Binding{
				Exchange: "missing", Queue: "orders", RoutingKey: "orders.created",
			}),
			want: ErrInvalidTopology,
		},
		"duplicate queue": {
			policy:   TopologyPolicy{Mode: TopologyPassive},
			topology: Topology{Queues: append(valid.Queues, valid.Queues[0])},
			want:     ErrInvalidTopology,
		},
		"empty topology": {
			policy: TopologyPolicy{Mode: TopologyPassive}, want: ErrInvalidTopology,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dialed := false
			_, err := applyTopologyWith(
				t.Context(), testConnectionConfig(), test.policy, test.topology,
				func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
					dialed = true
					return nil, nil, errors.New("unexpected")
				},
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("applyTopologyWith() error = %v, want %v", err, test.want)
			}
			if dialed {
				t.Fatal("invalid topology reached the broker")
			}
		})
	}
}

func TestTopologyRejectsUnsafeOrAmbiguousBindingArguments(t *testing.T) {
	t.Parallel()

	base := Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeHeaders, Durable: true}},
		Queues:    []Queue{{Name: "orders", Type: QueueClassic, Durable: true}},
	}
	policy := TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}
	tests := map[string][]Header{
		"missing criteria": nil,
		"x-match without criteria": {
			StringHeader("x-match", "all"),
		},
		"invalid x-match value": {
			StringHeader("x-match", "some"), StringHeader("format", "json"),
		},
		"invalid x-match type": {
			BoolHeader("x-match", true), StringHeader("format", "json"),
		},
		"default ignores extension-only criteria": {
			StringHeader("x-tenant", "orders"),
		},
		"duplicate": {
			StringHeader("format", "json"), StringHeader("format", "xml"),
		},
		"reserved": {
			StringHeader(publishTokenHeader, "collision"),
		},
		"control string": {
			StringHeader("format", "json\n"),
		},
		"conflicting string": {
			{Key: "format", Kind: HeaderString, String: "json", Bool: true},
		},
		"conflicting bool": {
			{Key: "active", Kind: HeaderBool, String: "true"},
		},
		"conflicting integer": {
			{Key: "attempt", Kind: HeaderInt64, Bytes: []byte{1}},
		},
		"conflicting bytes": {
			{Key: "signature", Kind: HeaderBytes, Int64: 1},
		},
		"unsupported kind": {
			{Key: "format", Kind: HeaderKind(255)},
		},
	}
	for name, arguments := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			topology := topologyWithBinding(base, Binding{
				Exchange: "events", Queue: "orders", Arguments: arguments,
			})
			if err := topology.Validate(policy); !errors.Is(err, ErrInvalidTopology) {
				t.Fatalf("Topology.Validate() error = %v, want invalid topology", err)
			}
		})
	}
}

func TestTopologyAcceptsRabbitMQHeadersMatchModes(t *testing.T) {
	t.Parallel()

	base := Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeHeaders, Durable: true}},
		Queues:    []Queue{{Name: "orders", Type: QueueClassic, Durable: true}},
	}
	policy := TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}
	tests := map[string][]Header{
		"default all": {StringHeader("format", "json")},
		"explicit all": {
			StringHeader("x-match", "all"), StringHeader("format", "json"),
		},
		"explicit any": {
			StringHeader("x-match", "any"), StringHeader("format", "json"),
		},
		"all with extensions": {
			StringHeader("x-match", "all-with-x"), StringHeader("x-tenant", "orders"),
		},
		"any with extensions": {
			StringHeader("x-match", "any-with-x"), StringHeader("x-tenant", "orders"),
		},
	}
	for name, arguments := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			topology := topologyWithBinding(base, Binding{
				Exchange: "events", Queue: "orders", Arguments: arguments,
			})
			if err := topology.Validate(policy); err != nil {
				t.Fatalf("Topology.Validate() error = %v", err)
			}
		})
	}
}

func TestApplyTopologyMapsBrokerDriftAndAuthorizationWithoutLeakingDetails(t *testing.T) {
	t.Parallel()

	topology := Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeDirect, Durable: true}}}
	for name, test := range map[string]struct {
		code uint16
		want error
	}{
		"missing":              {code: 404, want: ErrTopologyUnavailable},
		"exclusive name drift": {code: 405, want: ErrTopologyInequivalent},
		"property drift":       {code: 406, want: ErrTopologyInequivalent},
		"unauthorized":         {code: 403, want: ErrTopologyUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			brokerErr := &amqp.Error{Code: int(test.code), Reason: "sensitive broker detail"}
			var calls atomic.Int32
			channel := &fakeTopologyChannel{exchangePassive: func(string, string, bool, bool, bool) error {
				return brokerErr
			}}
			resource := &concurrentCountingCloser{}
			_, err := applyTopologyWith(
				t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive}, topology,
				func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
					calls.Add(1)
					return channel, resource, nil
				},
			)
			if !errors.Is(err, test.want) || errors.Is(err, brokerErr) ||
				containsSensitiveText(err, "sensitive broker detail") {
				t.Fatalf("applyTopologyWith() error = %v, want sanitized %v", err, test.want)
			}
			if calls.Load() != 1 || channel.closeCount() != 1 || resource.count() != 1 {
				t.Fatalf(
					"failure cleanup = calls %d channel %d resource %d",
					calls.Load(), channel.closeCount(), resource.count(),
				)
			}
		})
	}
}

func TestApplyTopologyRejectsUnexpectedNamedQueueIdentity(t *testing.T) {
	t.Parallel()

	channel := &fakeTopologyChannel{queuePassive: func(
		string, bool, bool, bool, amqp.Table,
	) (amqp.Queue, error) {
		return amqp.Queue{Name: "different"}, nil
	}}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if !errors.Is(err, ErrTopologyInequivalent) {
		t.Fatalf("applyTopologyWith() error = %v, want %v", err, ErrTopologyInequivalent)
	}
}

func TestApplyTopologyRotatesEndpointsAndCredentials(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	var credentials atomic.Int32
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		generation := credentials.Add(1)
		return Credentials{Username: "topology", Password: []byte{byte(generation)}}, nil
	})
	var calls atomic.Int32
	result, err := applyTopologyWith(
		t.Context(), connection, TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, credential Credentials) (topologyChannel, io.Closer, error) {
			call := calls.Add(1)
			if call == 1 {
				if endpoint.Host != "rabbitmq.internal" || credential.Password[0] != 1 {
					t.Fatal("first attempt did not use first endpoint and credential snapshot")
				}
				return nil, nil, errors.New("unavailable")
			}
			if endpoint.Host != "rabbitmq-2.internal" || credential.Password[0] != 2 {
				t.Fatal("second attempt did not rotate endpoint and credentials")
			}
			return &fakeTopologyChannel{}, &concurrentCountingCloser{}, nil
		},
	)
	if err != nil || len(result.QueueNames) != 1 || result.QueueNames[0] != "orders" {
		t.Fatalf("applyTopologyWith() = (%#v, %v)", result, err)
	}
	if calls.Load() != 2 || credentials.Load() != 2 {
		t.Fatalf("attempts = calls %d credentials %d", calls.Load(), credentials.Load())
	}
}

func TestApplyTopologyRetriesPassiveTransportFailureOnNextEndpoint(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	first := &fakeTopologyChannel{queuePassive: func(
		string, bool, bool, bool, amqp.Table,
	) (amqp.Queue, error) {
		return amqp.Queue{}, errors.New("sensitive transport detail")
	}}
	second := &fakeTopologyChannel{}
	firstResource := &concurrentCountingCloser{}
	secondResource := &concurrentCountingCloser{}
	var calls atomic.Int32
	result, err := applyTopologyWith(
		t.Context(), connection, TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, _ Credentials) (topologyChannel, io.Closer, error) {
			if calls.Add(1) == 1 {
				if endpoint.Host != "rabbitmq.internal" {
					t.Fatalf("first endpoint = %q", endpoint.Host)
				}
				return first, firstResource, nil
			}
			if endpoint.Host != "rabbitmq-2.internal" {
				t.Fatalf("second endpoint = %q", endpoint.Host)
			}
			return second, secondResource, nil
		},
	)
	if err != nil || len(result.QueueNames) != 1 || result.QueueNames[0] != "orders" {
		t.Fatalf("applyTopologyWith() = (%#v, %v), want recovered passive verification", result, err)
	}
	if calls.Load() != 2 || first.closeCount() != 1 || firstResource.count() != 1 ||
		second.closeCount() != 1 || secondResource.count() != 1 {
		t.Fatalf(
			"attempt cleanup = calls %d first %d/%d second %d/%d",
			calls.Load(), first.closeCount(), firstResource.count(), second.closeCount(), secondResource.count(),
		)
	}
}

func TestApplyTopologyDoesNotRetryActiveDeclarationFailure(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Endpoints = append(connection.Endpoints, Endpoint{Host: "rabbitmq-2.internal", Port: 5671})
	first := &fakeTopologyChannel{exchangeDeclare: func(string, string, bool, bool, bool) error {
		return errors.New("sensitive declaration detail")
	}}
	firstResource := &concurrentCountingCloser{}
	var calls atomic.Int32
	_, err := applyTopologyWith(
		t.Context(), connection,
		TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()},
		Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeDirect, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			calls.Add(1)
			return first, firstResource, nil
		},
	)
	if !errors.Is(err, ErrTopologyUnavailable) || containsSensitiveText(err, "sensitive declaration detail") {
		t.Fatalf("applyTopologyWith() error = %v, want sanitized terminal failure", err)
	}
	if calls.Load() != 1 || first.closeCount() != 1 || firstResource.count() != 1 {
		t.Fatalf(
			"failure cleanup = calls %d channel %d resource %d",
			calls.Load(), first.closeCount(), firstResource.count(),
		)
	}
}

func TestApplyTopologyPreservesCancellationObservedDuringPassiveFailureCleanup(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	channel := &fakeTopologyChannel{queuePassive: func(
		string, bool, bool, bool, amqp.Table,
	) (amqp.Queue, error) {
		return amqp.Queue{}, errors.New("sensitive transport detail")
	}}
	resource := &deadlineTrackingCloser{onDeadline: cancel}
	var calls atomic.Int32
	_, err := applyTopologyWith(
		ctx, testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			calls.Add(1)
			return channel, resource, nil
		},
	)
	if !errors.Is(err, ErrTopologyUnavailable) || !errors.Is(err, context.Canceled) ||
		containsSensitiveText(err, "sensitive transport detail") {
		t.Fatalf("applyTopologyWith() error = %v, want sanitized cancellation", err)
	}
	if calls.Load() != 1 || resource.deadlineCalls != 1 || channel.closeCount() != 1 {
		t.Fatalf(
			"failure cleanup = calls %d resource %d channel %d",
			calls.Load(), resource.deadlineCalls, channel.closeCount(),
		)
	}
}

func TestApplyTopologyPreservesFinalCredentialProviderCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		cancel()
		return Credentials{}, errors.New("sensitive credential detail")
	})
	_, err := applyTopologyWith(
		ctx, connection, TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			t.Fatal("credential failure reached dial")
			return nil, nil, nil
		},
	)
	if !errors.Is(err, ErrTopologyUnavailable) || !errors.Is(err, context.Canceled) ||
		containsSensitiveText(err, "sensitive credential detail") {
		t.Fatalf("applyTopologyWith() error = %v, want sanitized cancellation", err)
	}
}

func TestApplyTopologyPreservesFinalDialDeadline(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.DialTimeout = time.Millisecond
	connection.Recovery.MaxAttempts = 1
	_, err := applyTopologyWith(
		context.Background(), connection, TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(ctx context.Context, _ Endpoint, _ ConnectionConfig, _ Credentials) (topologyChannel, io.Closer, error) {
			<-ctx.Done()
			return nil, nil, errors.New("sensitive dial detail")
		},
	)
	if !errors.Is(err, ErrTopologyUnavailable) || !errors.Is(err, context.DeadlineExceeded) ||
		containsSensitiveText(err, "sensitive dial detail") {
		t.Fatalf("applyTopologyWith() error = %v, want sanitized deadline", err)
	}
}

func TestApplyTopologyDeadlineClosesConnectionAndUnblocksPassiveOperation(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	var releaseOnce sync.Once
	channel := &fakeTopologyChannel{exchangePassive: func(string, string, bool, bool, bool) error {
		<-release
		return amqp.ErrClosed
	}}
	resource := &deadlineTrackingCloser{onDeadline: func() {
		releaseOnce.Do(func() { close(release) })
	}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := applyTopologyWith(
		ctx, testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeDirect, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, resource, nil
		},
	)
	releaseOnce.Do(func() { close(release) })
	if !errors.Is(err, ErrTopologyUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("applyTopologyWith() error = %v, want bounded cancellation", err)
	}
	if resource.deadlineCalls != 0 || channel.closeCount() != 0 {
		t.Fatalf("pre-dial cancellation touched resources: deadline %d channel %d", resource.deadlineCalls, channel.closeCount())
	}
}

func TestApplyTopologyOperationDeadlineForcesOwnedConnectionClosed(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	var releaseOnce sync.Once
	channel := &fakeTopologyChannel{exchangePassive: func(string, string, bool, bool, bool) error {
		<-release
		return amqp.ErrClosed
	}}
	resource := &deadlineTrackingCloser{onDeadline: func() {
		releaseOnce.Do(func() { close(release) })
	}}
	connection := testConnectionConfig()
	connection.DialTimeout = time.Millisecond
	connection.Recovery.MaxAttempts = 1
	_, err := applyTopologyWith(
		context.Background(), connection, TopologyPolicy{Mode: TopologyPassive},
		Topology{Exchanges: []Exchange{{Name: "events", Kind: ExchangeDirect, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, resource, nil
		},
	)
	if resource.deadlineCalls == 0 {
		releaseOnce.Do(func() { close(release) })
	}
	if !errors.Is(err, ErrTopologyUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("applyTopologyWith() error = %v, want bounded deadline", err)
	}
	if resource.deadlineCalls != 1 || channel.closeCount() != 1 {
		t.Fatalf("forced cleanup = resource %d channel %d", resource.deadlineCalls, channel.closeCount())
	}
}

func TestDialAMQPTopologyBuildsBoundedCredentialFreeAddressAndVerifiedTLS(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	opened := false
	channel := &fakeTopologyChannel{}
	resource := &concurrentCountingCloser{}
	gotChannel, gotResource, err := dialAMQPTopologyWith(
		ctx,
		Endpoint{Host: "rabbitmq.internal", Port: 5671},
		testConnectionConfig(),
		Credentials{Username: "topology", Password: []byte("credential")},
		func(address string, config amqp.Config, deadline time.Time) (topologyChannel, io.Closer, error) {
			opened = true
			if address != "amqps://rabbitmq.internal:5671" || config.Vhost != "/events" ||
				config.TLSClientConfig == nil || config.TLSClientConfig.ServerName != "rabbitmq.internal" ||
				config.TLSClientConfig.InsecureSkipVerify || deadline.IsZero() {
				t.Fatal("topology dial did not preserve bounded verified connection policy")
			}
			return channel, resource, nil
		},
	)
	if err != nil || !opened || gotChannel != channel || gotResource != resource {
		t.Fatalf("dialAMQPTopologyWith() = (%#v, %#v, %v), opened %t", gotChannel, gotResource, err, opened)
	}
	if _, _, err := dialAMQPTopologyWith(
		ctx, Endpoint{Host: "rabbitmq.internal", Port: 5671}, testConnectionConfig(),
		Credentials{Username: "topology", Password: []byte("credential")}, nil,
	); !errors.Is(err, ErrTopologyUnavailable) {
		t.Fatalf("nil topology open error = %v, want unavailable", err)
	}
}

func TestOpenAMQPTopologyConnectionOwnsSuccessAndCleansEveryFailureBoundary(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(time.Second)
	t.Run("success", func(t *testing.T) {
		channel := &fakeTopologyChannel{}
		connection := &fakeAMQPConnection{channel: channel}
		gotChannel, resource, err := openAMQPTopologyConnectionWith(
			"amqps://rabbitmq.internal:5671", amqp.Config{}, deadline,
			func(string, amqp.Config) (amqpConnection, error) { return connection, nil },
		)
		if err != nil || gotChannel != channel || resource != connection || connection.closeCalls != 0 {
			t.Fatalf("open success = (%#v, %#v, %v), close calls %d", gotChannel, resource, err, connection.closeCalls)
		}
		_ = closeTopologyResources(gotChannel, resource, deadline)
	})

	for name, setup := range map[string]func() (*fakeAMQPConnection, amqpConnectionDialFunc, *fakeProducerChannel){
		"dial error": func() (*fakeAMQPConnection, amqpConnectionDialFunc, *fakeProducerChannel) {
			connection := &fakeAMQPConnection{}
			return connection, func(string, amqp.Config) (amqpConnection, error) {
				return connection, errors.New("sensitive dial detail")
			}, nil
		},
		"nil connection": func() (*fakeAMQPConnection, amqpConnectionDialFunc, *fakeProducerChannel) {
			return nil, func(string, amqp.Config) (amqpConnection, error) { return nil, nil }, nil
		},
		"channel error": func() (*fakeAMQPConnection, amqpConnectionDialFunc, *fakeProducerChannel) {
			connection := &fakeAMQPConnection{channelErr: errors.New("sensitive channel detail")}
			return connection, func(string, amqp.Config) (amqpConnection, error) { return connection, nil }, nil
		},
		"unsupported channel": func() (*fakeAMQPConnection, amqpConnectionDialFunc, *fakeProducerChannel) {
			channel := newFakeProducerChannel()
			connection := &fakeAMQPConnection{channel: channel}
			return connection, func(string, amqp.Config) (amqpConnection, error) { return connection, nil }, channel
		},
	} {
		t.Run(name, func(t *testing.T) {
			connection, dial, unsupported := setup()
			channel, resource, err := openAMQPTopologyConnectionWith(
				"amqps://rabbitmq.internal:5671", amqp.Config{}, deadline, dial,
			)
			if channel != nil || resource != nil || !errors.Is(err, ErrTopologyUnavailable) {
				t.Fatalf("open failure = (%#v, %#v, %v)", channel, resource, err)
			}
			if connection != nil && connection.closeCalls != 1 {
				t.Fatalf("connection close calls = %d, want 1", connection.closeCalls)
			}
			if unsupported != nil && unsupported.closeCount() != 1 {
				t.Fatalf("unsupported channel close calls = %d, want 1", unsupported.closeCount())
			}
		})
	}
}

func TestApplyTopologyReportsCleanupFailureAfterSuccessfulVerification(t *testing.T) {
	t.Parallel()

	channel := &fakeTopologyChannel{closeErr: errors.New("sensitive channel close detail")}
	_, err := applyTopologyWith(
		t.Context(), testConnectionConfig(), TopologyPolicy{Mode: TopologyPassive},
		Topology{Queues: []Queue{{Name: "orders", Type: QueueClassic, Durable: true}}},
		func(context.Context, Endpoint, ConnectionConfig, Credentials) (topologyChannel, io.Closer, error) {
			return channel, &concurrentCountingCloser{}, nil
		},
	)
	if !errors.Is(err, ErrTopologyUnavailable) || containsSensitiveText(err, "sensitive channel close detail") {
		t.Fatalf("cleanup error = %v, want sanitized unavailable", err)
	}
}

func topologyWithBinding(topology Topology, binding Binding) Topology {
	topology.Bindings = []Binding{binding}
	return topology
}

func containsSensitiveText(err error, text string) bool {
	return err != nil && len(text) > 0 && strings.Contains(err.Error(), text)
}

type fakeTopologyChannel struct {
	mu                   sync.Mutex
	exchangePassive      func(string, string, bool, bool, bool) error
	queuePassive         func(string, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	exchangeDeclare      func(string, string, bool, bool, bool) error
	queueDeclare         func(string, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	bind                 func(string, string, string, amqp.Table) error
	exchangeDeclareCalls int
	queueDeclareCalls    int
	bindCalls            int
	closeCalls           int
	closeErr             error
}

func (channel *fakeTopologyChannel) ExchangeDeclarePassive(
	name, kind string, durable, autoDelete, internal, _ bool, _ amqp.Table,
) error {
	if channel.exchangePassive != nil {
		return channel.exchangePassive(name, kind, durable, autoDelete, internal)
	}
	return nil
}

func (channel *fakeTopologyChannel) QueueDeclarePassive(
	name string, durable, autoDelete, exclusive, _ bool, arguments amqp.Table,
) (amqp.Queue, error) {
	if channel.queuePassive != nil {
		return channel.queuePassive(name, durable, autoDelete, exclusive, arguments)
	}
	return amqp.Queue{Name: name}, nil
}

func (channel *fakeTopologyChannel) ExchangeDeclare(
	name, kind string, durable, autoDelete, internal, _ bool, _ amqp.Table,
) error {
	channel.mu.Lock()
	channel.exchangeDeclareCalls++
	channel.mu.Unlock()
	if channel.exchangeDeclare != nil {
		return channel.exchangeDeclare(name, kind, durable, autoDelete, internal)
	}
	return nil
}

func (channel *fakeTopologyChannel) QueueDeclare(
	name string, durable, autoDelete, exclusive, _ bool, arguments amqp.Table,
) (amqp.Queue, error) {
	channel.mu.Lock()
	channel.queueDeclareCalls++
	channel.mu.Unlock()
	if channel.queueDeclare != nil {
		return channel.queueDeclare(name, durable, autoDelete, exclusive, arguments)
	}
	return amqp.Queue{Name: name}, nil
}

func (channel *fakeTopologyChannel) QueueBind(
	queue, routingKey, exchange string, _ bool, arguments amqp.Table,
) error {
	channel.mu.Lock()
	channel.bindCalls++
	channel.mu.Unlock()
	if channel.bind != nil {
		return channel.bind(queue, routingKey, exchange, arguments)
	}
	return nil
}

func (channel *fakeTopologyChannel) Close() error {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	channel.closeCalls++
	return channel.closeErr
}

func (channel *fakeTopologyChannel) closeCount() int {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.closeCalls
}

func (*fakeTopologyChannel) Confirm(bool) error { return nil }

func (*fakeTopologyChannel) NotifyReturn(listener chan amqp.Return) chan amqp.Return {
	return listener
}

func (*fakeTopologyChannel) NotifyPublish(listener chan amqp.Confirmation) chan amqp.Confirmation {
	return listener
}

func (*fakeTopologyChannel) GetNextPublishSeqNo() uint64 { return 1 }

func (*fakeTopologyChannel) PublishWithContext(
	context.Context, string, string, bool, bool, amqp.Publishing,
) error {
	return nil
}
