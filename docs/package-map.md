# Package map

[Documentation index](README.md) · [Repository entry](../README.md)

This map follows the current source modules, not a synchronized release.
Use the repository README for published installation versions; an unpublished
source major is not installable merely because its package appears here.
Internal packages and non-releasable fixtures are intentionally excluded.

## github.com/faustbrian/go-rabbitmq-queues

RabbitMQ-native AMQP 0-9-1 classic and quorum queue policy for bounded publishing, consumption, settlement, recovery, topology verification, health, and observations.

| Import path | Responsibility |
| --- | --- |
| [`github.com/faustbrian/go-rabbitmq-queues`](../doc.go) | Use for direct RabbitMQ AMQP 0-9-1 classic and quorum queue semantics; use go-queue for a backend-neutral worker abstraction and go-rabbitmq-streams for append-log, offset, and replay semantics. |

Choose the next guide in the [documentation index](README.md)
or return to the [repository entry](../README.md) for installation and ownership.
