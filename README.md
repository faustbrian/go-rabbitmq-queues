# go-rabbitmq-queues

`rabbitmqqueue` is the RabbitMQ-native AMQP 0-9-1 queue policy package for Go.
It keeps exchanges, routing, classic and quorum queue capabilities, publisher
outcomes, manual settlement, bounded recovery, and topology ownership visible.
It is intentionally separate from retained RabbitMQ Streams and from the
backend-neutral `go-queue` job API.

## Status

The package provides independent producer and consumer resources with explicit
topology, recovery, health, settlement, and observation policies. See the
[documentation index](docs/README.md) for operational detail and current
evidence boundaries.

For shared package families, selection guidance, ownership, and lifecycle
vocabulary, see the versioned [v1.3.0 Go library ecosystem
index](https://github.com/faustbrian/go-library-tools/blob/v1.3.0/docs/ecosystem/README.md).

## Policy example

```go
package main

import (
	"context"
	"time"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

func main() {
	config := rabbitmqqueue.ConnectionConfig{
		Endpoints:   []rabbitmqqueue.Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
		VirtualHost: "/orders",
		Credentials: rabbitmqqueue.CredentialProviderFunc(func(context.Context) (rabbitmqqueue.Credentials, error) {
			return rabbitmqqueue.Credentials{Username: "orders", Password: []byte("resolved secret")}, nil
		}),
		TLS:         rabbitmqqueue.TLSConfig{ServerName: "rabbitmq.internal"},
		DialTimeout: 5 * time.Second,
		Heartbeat:   30 * time.Second,
		Recovery: rabbitmqqueue.RecoveryPolicy{
			MaxAttempts: 8, InitialDelay: 100 * time.Millisecond, MaxDelay: 30 * time.Second,
		},
	}
	_, err := rabbitmqqueue.ApplyTopology(context.Background(), config,
		rabbitmqqueue.TopologyPolicy{Mode: rabbitmqqueue.TopologyPassive},
		rabbitmqqueue.Topology{
			Exchanges: []rabbitmqqueue.Exchange{{
				Name: "events", Kind: rabbitmqqueue.ExchangeTopic, Durable: true,
			}},
			Queues: []rabbitmqqueue.Queue{{
				Name: "orders", Type: rabbitmqqueue.QueueQuorum, Durable: true,
			}},
		},
	)
	if err != nil {
		panic(err)
	}

	producer, err := rabbitmqqueue.OpenProducer(context.Background(), config, rabbitmqqueue.ProducerConfig{
		Limits:         rabbitmqqueue.DefaultLimits(),
		MaxOutstanding: 256,
		PublishTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close(context.Background())

	consumer, err := rabbitmqqueue.OpenConsumer(context.Background(), config, rabbitmqqueue.ConsumerConfig{
		Limits:         rabbitmqqueue.DefaultLimits(),
		Queue:          rabbitmqqueue.QueueReference{Name: "orders", Type: rabbitmqqueue.QueueQuorum},
		Name:           "orders-worker",
		Prefetch:       32,
		Concurrency:    8,
		HandlerTimeout: 30 * time.Second,
		MaxRequeues:    2,
		Failure:        rabbitmqqueue.Reject(false),
	}, func(ctx context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
		// Persist the application effect before acknowledging the delivery.
		return rabbitmqqueue.Acknowledge(), nil
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close(context.Background())
}
```

## Guarantees and boundaries

- Publisher confirmation and consumer acknowledgement are separate effects.
- Cancellation or connection loss after transmission can be ambiguous.
- Mandatory returns must be reconciled with confirms before acceptance.
- Connection loss can redeliver a message while its earlier handler invocation
  is still completing. Applications must tolerate concurrent duplicates.
- Manual settlement provides at-least-once processing; applications remain
  responsible for idempotency.
- The package does not implement RabbitMQ Streams, application schemas,
  exactly-once processing, an outbox, or a generic messaging interface.

Read the [complete guarantees](docs/guarantees.md), [capability
matrix](docs/capability-matrix.md), [performance evidence](docs/performance.md),
the [specification decision register](docs/specification-decisions.md), and
the [compatibility policy](COMPATIBILITY.md) before production use.
