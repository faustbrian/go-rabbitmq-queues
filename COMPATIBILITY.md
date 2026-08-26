# Compatibility pins

Research and initial local policy tests use these immutable inputs as of
2026-08-26:

| Input | Pin |
|---|---|
| RabbitMQ server | `v4.3.5`, Git `0dde27bfdd1984ff7e157226fd97656854a7f359` |
| RabbitMQ container | `rabbitmq:4.3.5-management-alpine`, Linux arm64 digest `sha256:aa626c7c8b7d41c708796b336ff721897b176ab29c94d944a26eb2b1b2e3a455` |
| `amqp091-go` | `v1.14.0`, Git `387d77a50ea8b8c38705bb18cc80f5d6599a8477` |
| RabbitMQ Cluster Operator | `v2.22.5`, Git `17dd297f71de40a722baf69167b8af511072175e` |
| Messaging Topology Operator | `v1.20.2`, Git `58cdfa3610a8bbac51a0fc8a7fd90f2fa448b960` |
| RabbitMQ documentation | Git `1c99c9687f012ad700385c1e9a6990f6520720d3` |
| Go | `go1.27.0` |
| Initial local OS/architecture | Darwin 27.0.0 arm64 |

The container pin is the intended Linux arm64 broker fixture. Local unit tests
exercise TLS configuration, producer/consumer AMQP client boundaries, manual
settlement policy, bounded delivery conversion, and lifecycle behavior without
contacting a broker. No container, live TLS handshake, broker settlement,
cluster, operator, failure, or PHP interoperability claim is established by
those tests.

## Authoritative sources

- [Queues](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/queues.md)
- [Classic queues](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/classic-queues.md)
- [Quorum queues](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/quorum-queues/index.md)
- [Acknowledgements and confirms](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/confirms.md)
- [Publishers](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/publishers/index.md)
- [Dead-letter exchanges](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/dlx.md)
- [Reliability](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/reliability.md)
- [Network partitions](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/partitions.md)
- [TLS](https://github.com/rabbitmq/rabbitmq-website/blob/1c99c9687f012ad700385c1e9a6990f6520720d3/docs/ssl/index.md)
- [Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview)
- [Messaging Topology Operator](https://www.rabbitmq.com/kubernetes/operator/using-topology-operator)
