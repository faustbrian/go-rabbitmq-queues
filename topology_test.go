package rabbitmqqueue

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestQueuePolicyModelsTTLLengthOverflowAndDeadLetterCapabilities(t *testing.T) {
	t.Parallel()

	messageTTL := 30 * time.Second
	queueExpires := 10 * time.Minute
	maxLength := uint64(10_000)
	maxLengthBytes := uint64(64 << 20)
	routingKey := "jobs.failed"
	emptyRoutingKey := ""
	tooLongRoutingKey := strings.Repeat("r", 256)
	zeroTTL := time.Duration(0)
	zeroLength := uint64(0)
	tooLarge := uint64(math.MaxInt64) + 1
	negativeTTL := -time.Millisecond
	subMillisecond := time.Microsecond
	zeroExpires := time.Duration(0)

	valid := map[string]Queue{
		"classic declaration arguments": {
			Name: "jobs", Type: QueueClassic, Durable: true,
			MessageTTL: &messageTTL, Expires: &queueExpires,
			MaxLength: &maxLength, MaxLengthBytes: &maxLengthBytes,
			Overflow:   QueueOverflowRejectPublishDeadLetter,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", RoutingKey: &routingKey},
		},
		"quorum at least once dead lettering": {
			Name: "orders", Type: QueueQuorum, Durable: true,
			MaxLength: &maxLength, Overflow: QueueOverflowRejectPublish,
			DeadLetter: &QueueDeadLetter{
				Exchange: "orders.dead", Strategy: DeadLetterAtLeastOnce,
			},
		},
		"zero message ttl and queue length": {
			Name: "immediate", Type: QueueClassic, Durable: true,
			MessageTTL: &zeroTTL, MaxLength: &zeroLength,
		},
		"default dead letter exchange": {
			Name: "default-dlx", Type: QueueClassic, Durable: true,
			DeadLetter: &QueueDeadLetter{RoutingKey: &emptyRoutingKey},
		},
		"explicit quorum at most once strategy": {
			Name: "audit", Type: QueueQuorum, Durable: true,
			DeadLetter: &QueueDeadLetter{Exchange: "audit.dead", Strategy: DeadLetterAtMostOnce},
		},
	}
	for name, queue := range valid {
		queue := queue
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := queue.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	invalid := map[string]Queue{
		"non durable non exclusive classic": {
			Name: "transient", Type: QueueClassic,
		},
		"sub millisecond message ttl": {
			Name: "jobs", Type: QueueClassic, Durable: true, MessageTTL: &subMillisecond,
		},
		"negative message ttl": {
			Name: "jobs", Type: QueueClassic, Durable: true, MessageTTL: &negativeTTL,
		},
		"zero queue expiry": {
			Name: "jobs", Type: QueueClassic, Durable: true, Expires: &zeroExpires,
		},
		"length exceeds AMQP integer": {
			Name: "jobs", Type: QueueClassic, Durable: true, MaxLength: &tooLarge,
		},
		"byte length exceeds AMQP integer": {
			Name: "jobs", Type: QueueClassic, Durable: true, MaxLengthBytes: &tooLarge,
		},
		"unknown overflow": {
			Name: "jobs", Type: QueueClassic, Durable: true, Overflow: QueueOverflow("unknown"),
		},
		"quorum reject publish dead letter overflow": {
			Name: "jobs", Type: QueueQuorum, Durable: true,
			Overflow: QueueOverflowRejectPublishDeadLetter,
		},
		"classic explicit dead letter strategy": {
			Name: "jobs", Type: QueueClassic, Durable: true,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", Strategy: DeadLetterAtMostOnce},
		},
		"quorum at least once requires reject publish": {
			Name: "jobs", Type: QueueQuorum, Durable: true, Overflow: QueueOverflowDropHead,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", Strategy: DeadLetterAtLeastOnce},
		},
		"reject publish dead letter requires exchange policy": {
			Name: "jobs", Type: QueueClassic, Durable: true,
			Overflow: QueueOverflowRejectPublishDeadLetter,
		},
		"dead letter exchange contains control": {
			Name: "jobs", Type: QueueClassic, Durable: true,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs\ndead"},
		},
		"dead letter routing key exceeds AMQP short string": {
			Name: "jobs", Type: QueueClassic, Durable: true,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", RoutingKey: &tooLongRoutingKey},
		},
		"unknown dead letter strategy": {
			Name: "jobs", Type: QueueQuorum, Durable: true,
			DeadLetter: &QueueDeadLetter{Exchange: "jobs.dead", Strategy: DeadLetterStrategy("unknown")},
		},
	}
	for name, queue := range invalid {
		queue := queue
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := queue.Validate(); !errors.Is(err, ErrUnsupportedQueuePolicy) {
				t.Fatalf("Validate() error = %v, want %v", err, ErrUnsupportedQueuePolicy)
			}
		})
	}
}

func TestQueuePolicyModelsQueueTypeCapabilities(t *testing.T) {
	t.Parallel()

	deliveryLimit := QueueDeliveryLimit(20)
	tests := map[string]struct {
		queue Queue
		want  error
	}{
		"durable classic": {
			queue: Queue{Name: "orders", Type: QueueClassic, Durable: true},
		},
		"exclusive server named classic": {
			queue: Queue{Type: QueueClassic, Exclusive: true, AutoDelete: true},
		},
		"exclusive classic is transient": {
			queue: Queue{Name: "reply", Type: QueueClassic, Durable: true, Exclusive: true},
			want:  ErrUnsupportedQueuePolicy,
		},
		"durable quorum with delivery limit": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DeliveryLimit: &deliveryLimit,
			},
		},
		"quorum must be durable": {
			queue: Queue{Name: "orders", Type: QueueQuorum},
			want:  ErrUnsupportedQueuePolicy,
		},
		"quorum cannot be exclusive": {
			queue: Queue{Name: "orders", Type: QueueQuorum, Durable: true, Exclusive: true},
			want:  ErrUnsupportedQueuePolicy,
		},
		"classic has no broker delivery limit": {
			queue: Queue{
				Name: "orders", Type: QueueClassic, Durable: true,
				DeliveryLimit: &deliveryLimit,
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"classic priority is explicitly bounded": {
			queue: Queue{Name: "orders", Type: QueueClassic, Durable: true, MaxPriority: 5},
		},
		"quorum priority is intrinsic in RabbitMQ 4.3": {
			queue: Queue{Name: "orders", Type: QueueQuorum, Durable: true, MaxPriority: 5},
			want:  ErrUnsupportedQueuePolicy,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := test.queue.Validate()
			if !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestQueuePolicyDistinguishesOmittedAndExplicitQuorumDeliveryLimit(t *testing.T) {
	t.Parallel()

	zero := QueueDeliveryLimit(0)
	configured := QueueDeliveryLimit(50)

	for name, queue := range map[string]Queue{
		"broker default": {
			Name: "orders", Type: QueueQuorum, Durable: true,
		},
		"first failed redelivery exceeds zero": {
			Name: "orders", Type: QueueQuorum, Durable: true, DeliveryLimit: &zero,
		},
		"configured bounded redeliveries": {
			Name: "orders", Type: QueueQuorum, Durable: true, DeliveryLimit: &configured,
		},
	} {
		queue := queue
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := queue.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestQueuePolicyModelsQuorumConsumerTimeout(t *testing.T) {
	t.Parallel()

	immediate := time.Duration(0)
	minimum := time.Millisecond
	recommended := 5 * time.Minute
	subMillisecond := time.Minute + time.Microsecond
	negative := -time.Millisecond

	tests := map[string]struct {
		queue Queue
		want  error
	}{
		"omitted quorum timeout": {
			queue: Queue{Name: "orders", Type: QueueQuorum, Durable: true},
		},
		"immediate quorum timeout": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				ConsumerTimeout: &immediate,
			},
		},
		"minimum quorum timeout": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				ConsumerTimeout: &minimum,
			},
		},
		"recommended quorum timeout": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				ConsumerTimeout: &recommended,
			},
		},
		"classic timeout is unsupported in RabbitMQ 4.3": {
			queue: Queue{
				Name: "orders", Type: QueueClassic, Durable: true,
				ConsumerTimeout: &recommended,
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"negative timeout": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				ConsumerTimeout: &negative,
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"timeout outside millisecond precision": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				ConsumerTimeout: &subMillisecond,
			},
			want: ErrUnsupportedQueuePolicy,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := test.queue.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestQueuePolicyModelsQuorumDisconnectedConsumerTimeout(t *testing.T) {
	t.Parallel()

	immediate := time.Duration(0)
	brokerDefault := time.Minute
	negative := -time.Millisecond
	subMillisecond := time.Microsecond

	tests := map[string]struct {
		queue Queue
		want  error
	}{
		"omitted quorum timeout": {
			queue: Queue{Name: "orders", Type: QueueQuorum, Durable: true},
		},
		"explicit zero timeout": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DisconnectedConsumerTimeout: &immediate,
			},
		},
		"broker default timeout": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DisconnectedConsumerTimeout: &brokerDefault,
			},
		},
		"classic timeout is unsupported": {
			queue: Queue{
				Name: "orders", Type: QueueClassic, Durable: true,
				DisconnectedConsumerTimeout: &brokerDefault,
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"negative timeout is unsupported": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DisconnectedConsumerTimeout: &negative,
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"timeout requires millisecond precision": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DisconnectedConsumerTimeout: &subMillisecond,
			},
			want: ErrUnsupportedQueuePolicy,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := test.queue.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestQueuePolicyModelsQuorumDelayedRetry(t *testing.T) {
	t.Parallel()

	minimum := time.Second
	maximum := 30 * time.Second
	belowMinimum := minimum - time.Millisecond
	subMillisecond := minimum + time.Microsecond
	subMillisecondMaximum := maximum + time.Microsecond

	tests := map[string]struct {
		queue Queue
		want  error
	}{
		"omitted quorum delayed retry": {
			queue: Queue{Name: "orders", Type: QueueQuorum, Durable: true},
		},
		"explicitly disabled quorum delayed retry": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryDisabled},
			},
		},
		"all returned deliveries with fixed delay": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryAll, Minimum: minimum},
			},
		},
		"failed deliveries with bounded delay": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{
					Type: DelayedRetryFailed, Minimum: minimum, Maximum: &maximum,
				},
			},
		},
		"returned deliveries with bounded delay": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{
					Type: DelayedRetryReturned, Minimum: minimum, Maximum: &maximum,
				},
			},
		},
		"classic delayed retry is unsupported": {
			queue: Queue{
				Name: "orders", Type: QueueClassic, Durable: true,
				DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryAll, Minimum: minimum},
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"enabled retry requires positive minimum": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryAll},
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"minimum requires millisecond precision": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryAll, Minimum: subMillisecond},
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"maximum cannot precede minimum": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{
					Type: DelayedRetryAll, Minimum: minimum, Maximum: &belowMinimum,
				},
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"maximum requires millisecond precision": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{
					Type: DelayedRetryAll, Minimum: minimum, Maximum: &subMillisecondMaximum,
				},
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"disabled retry rejects delay values": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryDisabled, Minimum: minimum},
			},
			want: ErrUnsupportedQueuePolicy,
		},
		"unknown retry type": {
			queue: Queue{
				Name: "orders", Type: QueueQuorum, Durable: true,
				DelayedRetry: &QueueDelayedRetry{
					Type: DelayedRetryType("unknown"), Minimum: minimum,
				},
			},
			want: ErrUnsupportedQueuePolicy,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := test.queue.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestTopologyMutationRequiresExplicitDevelopmentPermit(t *testing.T) {
	t.Parallel()

	passive := TopologyPolicy{Mode: TopologyPassive}
	if err := passive.Validate(); err != nil {
		t.Fatalf("passive topology rejected: %v", err)
	}

	declaration := TopologyPolicy{Mode: TopologyDeclare}
	if err := declaration.Validate(); !errors.Is(err, ErrTopologyMutationDenied) {
		t.Fatalf("declaration without permit error = %v, want %v", err, ErrTopologyMutationDenied)
	}

	declaration.Development = PermitDevelopmentTopology()
	if err := declaration.Validate(); err != nil {
		t.Fatalf("explicit development declaration rejected: %v", err)
	}
}

func TestExchangeValidation(t *testing.T) {
	t.Parallel()

	for _, kind := range []ExchangeKind{ExchangeDirect, ExchangeTopic, ExchangeFanout, ExchangeHeaders} {
		exchange := Exchange{Name: "events", Kind: kind, Durable: true}
		if err := exchange.Validate(); err != nil {
			t.Fatalf("exchange kind %q rejected: %v", kind, err)
		}
	}

	if err := (Exchange{Name: "events", Kind: ExchangeKind("plugin")}).Validate(); !errors.Is(err, ErrUnsupportedExchangeKind) {
		t.Fatalf("unsupported exchange error = %v, want %v", err, ErrUnsupportedExchangeKind)
	}
}
