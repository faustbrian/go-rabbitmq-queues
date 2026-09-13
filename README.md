# go-rabbitmq-queues

`rabbitmqqueue` is the RabbitMQ-native AMQP 0-9-1 queue policy package for Go.
It keeps exchanges, routing, classic and quorum queue capabilities, publisher
outcomes, manual settlement, bounded recovery, and topology ownership visible.
It is intentionally separate from retained RabbitMQ Streams and from the
backend-neutral `go-queue` job API.

## Status

The module is stable at v1 and requires Go 1.27.0. It provides independent
producer and consumer resources with explicit topology, recovery, health,
settlement, and observation policies. See the [documentation index](docs/README.md)
for operational detail and current evidence boundaries.

```bash
go get github.com/faustbrian/go-rabbitmq-queues@v1
```

For shared package families, selection guidance, ownership, and lifecycle
vocabulary, see the versioned [v1.4.0 Go library ecosystem
index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Integration and data movement family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).

## Five-minute producer and worker

The example assumes an operator has provisioned the durable quorum queue
`orders`; passive verification does not mutate production topology. It opens a
consumer, publishes through RabbitMQ's default direct exchange, observes one
handler invocation, and shuts both owned resources down. It uses the deprecated
`Close(ctx)` compatibility spelling so it compiles across every published v1
minor; prefer `Shutdown(ctx)` when the selected release provides it.

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

	handled := make(chan struct{}, 1)
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
		handled <- struct{}{}
		return rabbitmqqueue.Acknowledge(), nil
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close(context.Background())

	result, err := producer.Publish(context.Background(), rabbitmqqueue.Publication{
		ExchangeKind: rabbitmqqueue.ExchangeDirect,
		RoutingKey:   "orders",
		Mandatory:    true,
		DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{
			Body: []byte(`{"order_id":"order-1"}`), MessageID: "order-1",
			ContentType: "application/json",
		},
	})
	if err != nil || result.State != rabbitmqqueue.PublishConfirmed {
		panic("publication was not confirmed")
	}
	select {
	case <-handled:
	case <-time.After(30 * time.Second):
		panic("delivery was not handled")
	}
}
```

## Package map

- `github.com/faustbrian/go-rabbitmq-queues` owns native AMQP 0-9-1 topology,
  publishing, consumption, settlement, recovery, health, and observations.
- `github.com/faustbrian/go-rabbitmq-queues/adapters/otel` adds optional
  RabbitMQ producer/process spans and W3C Trace Context propagation without
  adding OpenTelemetry dependencies to the root module.
- `github.com/faustbrian/go-queue/adapters/rabbitmq` adapts these native
  contracts to the backend-neutral `go-queue` worker API; adopt it only through
  a published, cleanly resolvable module version.
- `github.com/faustbrian/go-queue/rabbitmq` is the deprecated compatibility
  facade for the historical adapter path.

## Guarantees and boundaries

- Publisher confirmation and consumer acknowledgement are separate effects.
- Cancellation or connection loss after transmission can be ambiguous.
- Mandatory returns must be reconciled with confirms before acceptance.
- Connection loss can redeliver a message while its earlier handler invocation
  is still completing. Applications must tolerate concurrent duplicates.
- Manual settlement provides at-least-once processing; applications remain
  responsible for idempotency.
- `Shutdown(ctx)` is the preferred producer and consumer lifecycle method. It
  is repeatable and safe for concurrent use; each caller is bounded by its own
  context while the one package-owned cleanup continues. The deprecated
  `Close(ctx)` methods delegate to the same lifecycle for source compatibility.
  A producer caller whose context is cancelled or expires accelerates the
  shared cleanup and can make active publications ambiguous. A consumer caller
  context bounds only that caller's wait; the consumer's configured handler
  timeout bounds its shared drain and cleanup.
- Package-owned connection attempts call credential providers synchronously
  with a non-nil, bounded context. A shared provider must be concurrency-safe,
  return on cancellation, avoid re-entering the producer or consumer being
  opened, and must not panic. The package zeroes its returned password snapshot
  after each attempt but cannot zero aliases retained by the provider. Direct
  provider callers own their snapshots and receive callback errors unchanged.
- The package does not implement RabbitMQ Streams, application schemas,
  exactly-once processing, an outbox, or a generic messaging interface.

Read the [complete guarantees](docs/guarantees.md), [capability
matrix](docs/capability-matrix.md), [performance evidence](docs/performance.md),
the [specification decision register](docs/specification-decisions.md), and
the [compatibility policy](COMPATIBILITY.md) before production use. Report
vulnerabilities through the private process in [SECURITY.md](SECURITY.md); use
[SUPPORT.md](SUPPORT.md) for reproducible defects and adoption questions, and
consult the [FAQ](docs/faq.md) for common ownership and delivery questions.
Run `go test ./...` for the package test suite and `make ci` for the complete
repository gate. This module is distributed under the [MIT license](LICENSE).
