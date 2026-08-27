# Compatibility pins

Research and initial local policy tests use these immutable inputs as of
2026-08-26:

| Input | Pin |
|---|---|
| RabbitMQ server | `v4.3.5`, Git `0dde27bfdd1984ff7e157226fd97656854a7f359` |
| RabbitMQ container | `rabbitmq:4.3.5-management-alpine`, Linux arm64 digest `sha256:aa626c7c8b7d41c708796b336ff721897b176ab29c94d944a26eb2b1b2e3a455`; Linux amd64 digest `sha256:7224161872a48060e980a611f4778ad18168f00cfa974cab30604dbd855511dc` |
| `amqp091-go` | `v1.14.0`, Git `387d77a50ea8b8c38705bb18cc80f5d6599a8477` |
| PHP interoperability runtime | PHP `8.5.8`, Composer `2.10.1` |
| `php-amqplib/php-amqplib` | `v3.7.4`, Git `381b6f7c600e0e0c7463cdd7f7a1a3bc6268e5fd` |
| RabbitMQ Cluster Operator | `v2.22.5`, Git `17dd297f71de40a722baf69167b8af511072175e` |
| Messaging Topology Operator | `v1.20.2`, Git `58cdfa3610a8bbac51a0fc8a7fd90f2fa448b960` |
| Operator schema validator | kubeconform `v0.8.0`, Git `02374e583d700721f57300fae78e11acd27ee539`; PyYAML `6.0.3` |
| RabbitMQ documentation | Git `1c99c9687f012ad700385c1e9a6990f6520720d3` |
| Go | `go1.27.0` |
| Initial local OS/architecture | Darwin 27.0.0 arm64 |
| Retained broker-evidence runner | GitHub Actions `ubuntu-24.04`, Linux amd64 |

The arm64 container pin is the intended Kubernetes Operator fixture. The amd64
pin is the CI-hosted single-node broker fixture. The opt-in
[`livebroker` evidence harness](docs/live-broker-testing.md) exercises an
externally provisioned TLS broker through the public API without controlling
broker resources. The retained GitHub Actions broker gate provisions the pinned
amd64 container and proves verified TLS connections, classic and quorum queue
round trips, mandatory-return reconciliation, and bounded quorum requeue
behavior through that public harness. Local unit, fuzz,
race, leak, stress, and wrapper benchmark harnesses exercise TLS configuration,
producer/consumer AMQP client boundaries, manual settlement policy,
synchronous/asynchronous/batch outcome handling, bounded producer and consumer
recovery seams, sanitized connection-blocked transitions, bounded health and
observation seams, broker consumer-cancellation dispatch, passive topology
equivalence mapping, connection-scoped transient-consumer setup, bounded
delivery conversion, lifecycle behavior, and the pinned PHP runner's local
corpus construction and validation without contacting a broker. No three-node
cluster, node or leader failover, rolling-upgrade, installed-operator
reconciliation, or PHP broker-interoperability claim is currently established.
The operator manifests pass strict validation against the exact
pinned Cluster and Messaging Topology Operator CRD schemas. That structural
result is not installed-controller reconciliation, effective broker topology,
Kubernetes scheduling, TLS, failover, or rolling-upgrade evidence.

See [performance evidence](docs/performance.md) for workload targets, the local
wrapper benchmark boundary, and the required live three-node profile.

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
