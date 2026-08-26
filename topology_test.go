package rabbitmqqueue

import (
	"errors"
	"testing"
)

func TestQueuePolicyModelsQueueTypeCapabilities(t *testing.T) {
	t.Parallel()

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
			queue: Queue{Name: "orders", Type: QueueQuorum, Durable: true, DeliveryLimit: 20},
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
			queue: Queue{Name: "orders", Type: QueueClassic, Durable: true, DeliveryLimit: 20},
			want:  ErrUnsupportedQueuePolicy,
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
