package rabbitmqqueue

import (
	"context"
	"errors"
	"strconv"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRejectedDeliveryHeadersDoNotAllocateSnapshots(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxHeaderEntries = 1
	limits.MaxHeaderBytes = 8
	countTable := make(amqp.Table, 64)
	for index := range 64 {
		countTable[strconv.Itoa(index)] = true
	}
	for name, table := range map[string]amqp.Table{
		"entry count": countTable,
		"byte value":  {"a": make([]byte, 32)},
	} {
		t.Run(name, func(t *testing.T) {
			var headers []Header
			var err error
			allocations := testing.AllocsPerRun(1, func() {
				headers, _, err = deliveryHeaders(table, limits)
			})
			if len(headers) != 0 || !errors.Is(err, ErrInvalidDelivery) {
				t.Fatalf("rejected headers = %v, error = %v", headers, err)
			}
			if allocations != 0 {
				t.Errorf("rejected headers allocated %.0f snapshots before admission", allocations)
			}
		})
	}
}

func TestInvalidConsumerConfigurationIsNotCopied(t *testing.T) {
	config := testConsumerConfig()
	config.Limits.MaxHeaderBytes = 8
	config.Queue = QueueReference{
		Type: QueueClassic,
		Transient: &TransientQueue{
			Exchange:  Exchange{Name: "events", Kind: ExchangeHeaders, Durable: true},
			Arguments: []Header{{Key: "kind", Kind: HeaderBytes, Bytes: make([]byte, 32)}},
		},
	}
	if !errors.Is(config.Validate(), ErrInvalidConsumer) {
		t.Fatal("fixture must fail consumer validation")
	}
	for _, owner := range []string{"public opener", "channel constructor"} {
		t.Run(owner, func(t *testing.T) {
			ctx := t.Context()
			connection := testConnectionConfig()
			called := false
			connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
				called = true
				return Credentials{}, nil
			})
			var consumer *Consumer
			var err error
			validationAllocations := testing.AllocsPerRun(1, func() {
				if owner == "public opener" {
					_ = connection.Validate()
				}
				_ = config.Validate()
			})
			allocations := testing.AllocsPerRun(1, func() {
				if owner == "public opener" {
					consumer, err = OpenConsumer(ctx, connection, config, nil)
				} else {
					consumer, err = newConsumerFromChannel(ctx, config, nil, nil, nil)
				}
			})
			if consumer != nil || !errors.Is(err, ErrInvalidConsumer) || called {
				t.Fatalf("invalid configuration result = %v, error = %v, credential callback = %t", consumer, err, called)
			}
			if allocations > validationAllocations {
				t.Errorf("invalid configuration allocated %.0f snapshots beyond validation", allocations-validationAllocations)
			}
		})
	}
}
