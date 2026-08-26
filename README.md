# go-rabbitmq-queues

`rabbitmqqueue` is the RabbitMQ-native AMQP 0-9-1 queue policy package for Go.
It keeps exchanges, routing, classic and quorum queue capabilities, publisher
outcomes, manual settlement, bounded recovery, and topology ownership visible.
It is intentionally separate from retained RabbitMQ Streams and from the
backend-neutral `go-queue` job API.

## Status

The repository currently contains the initial connection, topology, message,
and publisher-outcome policy foundations. The AMQP transport, producer,
consumer, settlement, recovery, health, observability, integration, failover,
PHP interoperability, and optional OpenTelemetry work remains in progress.

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

	if err := config.Validate(); err != nil {
		panic(err)
	}
}
```

Production clients use passive topology verification. Active declarations are
available only behind the explicit development permit and are not a production
topology-management mechanism.

## Guarantees and boundaries

- Publisher confirmation and consumer acknowledgement are separate effects.
- Cancellation or connection loss after transmission can be ambiguous.
- Mandatory returns must be reconciled with confirms before acceptance.
- Manual settlement provides at-least-once processing; applications remain
  responsible for idempotency.
- The package does not implement RabbitMQ Streams, application schemas,
  exactly-once processing, an outbox, or a generic messaging interface.

See [the capability matrix](docs/capability-matrix.md) and
[pinned compatibility evidence](COMPATIBILITY.md).
