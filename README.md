# go-rabbitmq-queues

`rabbitmqqueue` is the RabbitMQ-native AMQP 0-9-1 queue policy package for Go.
It keeps exchanges, routing, classic and quorum queue capabilities, publisher
outcomes, manual settlement, bounded recovery, and topology ownership visible.
It is intentionally separate from retained RabbitMQ Streams and from the
backend-neutral `go-queue` job API.

## Status

The repository contains connection, topology, message, publisher-outcome,
delivery, and settlement policy foundations plus independent synchronous
producer and consumer resources. The producer uses mandatory routing, exact
confirm/return correlation, bounded startup and runtime recovery, credential
refresh, verified TLS, connection-blocked notifications, bounded asynchronous
admission, and ordered non-atomic batches.
The consumer uses manual settlement, bounded QoS and concurrency, bounded
delivery snapshots, explicit failure policy, and graceful drain/close. Consumer
runtime recovery, health, broader observability, broker and failover evidence,
PHP interoperability, and optional OpenTelemetry remain in progress.

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

Production clients use passive topology verification. Active declarations are
available only behind the explicit development permit and are not a production
topology-management mechanism.

## Guarantees and boundaries

- Publisher confirmation and consumer acknowledgement are separate effects.
- Cancellation or connection loss after transmission can be ambiguous.
- Mandatory returns must be reconciled with confirms before acceptance.
- Asynchronous publishing owns the admitted publication and emits exactly one
  terminal outcome; a full admission window rejects new work without spawning
  another worker.
- Batches validate every item before publishing, preserve input order, and
  report independent per-item outcomes; they are not atomic broker operations.
- The producer makes in-flight work ambiguous on connection loss, rejects new
  work while recovering, then rebuilds a fresh confirm generation with bounded
  endpoint rotation and refreshed credentials. Exhausted recovery is terminal.
- `BlockedNotifications` reports coalesced blocked/unblocked transitions without
  exposing broker reason text; a blocked connection does not by itself retry a
  publication.
- The current consumer retries bounded startup attempts but reaches a terminal
  unavailable state after runtime delivery, settlement, channel, or connection
  loss.
- Manual settlement provides at-least-once processing; applications remain
  responsible for idempotency.
- Handler, settlement, and shutdown work is bounded by the configured handler
  timeout; handlers must observe cancellation for graceful draining.
- Requeue is bounded by delivery state and configured policy. The package does
  not automatically publish replacement messages.
- The package does not implement RabbitMQ Streams, application schemas,
  exactly-once processing, an outbox, or a generic messaging interface.

See [the capability matrix](docs/capability-matrix.md) and
[pinned compatibility evidence](COMPATIBILITY.md).
