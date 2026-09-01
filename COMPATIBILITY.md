# Compatibility pins

Observable AMQP and RabbitMQ interpretations are versioned in the
[specification decision register](docs/specification-decisions.md). Changes to
those decisions require compatibility review even when the earlier behavior
was undocumented.

Research and initial local policy tests use these immutable inputs as of
2026-08-27:

| Input | Pin |
|---|---|
| RabbitMQ server | `v4.3.5`, Git `0dde27bfdd1984ff7e157226fd97656854a7f359` |
| RabbitMQ container | `rabbitmq:4.3.5-management-alpine`, Linux arm64 digest `sha256:aa626c7c8b7d41c708796b336ff721897b176ab29c94d944a26eb2b1b2e3a455`; Linux amd64 digest `sha256:7224161872a48060e980a611f4778ad18168f00cfa974cab30604dbd855511dc` |
| RabbitMQ rolling-upgrade source | Server `v4.3.4`, Git `d5186e66e056960f58e2d0fbee2fcc66e1ed6fb9`; Linux amd64 container `rabbitmq:4.3.4-management-alpine`, digest `sha256:39f934e10a7b95179171a70f15f02636201a153a2c689e961fc0f445bac275f2` |
| `amqp091-go` | `v1.14.0`, Git `387d77a50ea8b8c38705bb18cc80f5d6599a8477` |
| PHP interoperability runtime | PHP `8.5.10`, Composer `2.10.1` |
| `php-amqplib/php-amqplib` | `v3.7.4`, Git `381b6f7c600e0e0c7463cdd7f7a1a3bc6268e5fd` |
| RabbitMQ Cluster Operator | `v2.22.5`, Git `17dd297f71de40a722baf69167b8af511072175e` |
| Messaging Topology Operator | `v1.20.2`, Git `58cdfa3610a8bbac51a0fc8a7fd90f2fa448b960` |
| Operator schema validator | kubeconform `v0.8.0`, Git `02374e583d700721f57300fae78e11acd27ee539`; PyYAML `6.0.3` |
| RabbitMQ documentation | Git `1c99c9687f012ad700385c1e9a6990f6520720d3` |
| Go | `go1.27.0` |
| Initial local OS/architecture | Darwin 27.0.0 arm64 |
| Retained broker-evidence runner | GitHub Actions `ubuntu-24.04`, Linux amd64 |

The arm64 container pin is the intended Kubernetes Operator fixture. The amd64
pin is used by the CI-hosted single-node and three-node broker fixtures. The
opt-in
[`livebroker` evidence harness](docs/live-broker-testing.md) exercises an
externally provisioned TLS broker through the public API without controlling
broker resources. The retained GitHub Actions broker gate provisions the pinned
amd64 container and proves verified TLS connections, classic and quorum queue
round trips, mandatory-return reconciliation, and bounded quorum requeue
behavior through that public harness. The retained PHP interoperability gate
uses the same TLS fixture to prove exact body, property, and typed-header
mapping in both directions with the pinned PHP runtime and `php-amqplib`, plus
publisher confirms, mandatory returns, and manual acknowledgement. Local unit,
fuzz,
race, leak, stress, and wrapper benchmark harnesses exercise TLS configuration,
producer/consumer AMQP client boundaries, manual settlement policy,
synchronous/asynchronous/batch outcome handling, bounded producer and consumer
recovery seams, sanitized connection-blocked transitions, bounded health and
observation seams, broker consumer-cancellation dispatch, passive topology
equivalence mapping, connection-scoped transient-consumer setup, bounded
delivery conversion, lifecycle behavior, and the pinned PHP runner's local
corpus construction and validation without contacting a broker. The retained
single-node performance matrix proves CI-runner throughput for classic and
single-member quorum queues at the 1M, 10M, and 100M daily-volume profiles. The
retained three-node matrix proves bounded client behavior while the classic
queue's hosting node is stopped and restarted, and while a quorum leader is
stopped and replaced. It also proves delivery reconciliation while one quorum
node is partitioned and healed, and recovery after a complete three-node
cluster restart with a 20-second outage. A separate retained scenario proves
three complete five-second cluster outages across four producer and consumer
pairs, fresh recovery after every cycle, exactly four active consumers after
each recovery, and duplicate-free delivery reconciliation. It does not
establish installed-operator reconciliation, Laravel-framework integration,
application rollout, or production performance capacity. The rolling-upgrade
harness starts a three-node quorum cluster on RabbitMQ 4.3.4, checks the
quorum-safety precondition, replaces one node at a time with RabbitMQ 4.3.5,
verifies each node rejoins before the next replacement, and accounts for
publications and deliveries across every replacement and mixed-version
checkpoint. Retained run `33055826884` upgraded all three nodes with 203 of 203
publications confirmed and delivered, zero rejected, ambiguous, not-sent, or
duplicate outcomes, and exactly one active consumer after every replacement.
The prolonged-outage harness holds all three RabbitMQ 4.3.5 nodes down for 90
seconds, samples producer and consumer liveness, readiness, and dependency
health every 10 seconds, and requires complete message reconciliation after
recovery. Retained run `33058061295` held the complete cluster down for 91
seconds across 10 samples. Producer and consumer liveness remained live while
readiness stayed not ready and dependency health stayed recovering. After
recovery, the ledger accounted for all 34 attempts as 24 confirmed and
delivered plus 10 not-sent, with zero rejected, ambiguous, or duplicate
outcomes.
The retained replicated-performance matrix runs four three-member quorum queues
over verified TLS at the 1M, 10M, and 100M messages-per-day targets while
stopping the current leader. Retained run `33063180768` met the steady-rate gate
in 3/3, 3/3, and 2/3 samples respectively, confirmed and delivered every
leader-loss publication, recovered queue leaders in 47 to 64 milliseconds,
drained the application backlog in 502 milliseconds to 8.561 seconds, and
reported zero management backlog after drain. This is CI-hosted replicated
failure evidence, not application or production capacity evidence.
The operator manifests pass strict validation against the exact
pinned Cluster and Messaging Topology Operator CRD schemas. That structural
result is not installed-controller reconciliation, effective broker topology,
Kubernetes scheduling, TLS, failover, or rolling-upgrade evidence.

See [performance evidence](docs/performance.md) for workload targets, the local
wrapper benchmark boundary, and the required live three-node profile.

Retained CI evidence:

- protocol, TLS, and PHP interoperability:
  [run 33041075968](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33041075968)
  at commit `cb48c8cd7e5cda6feebdeea76e7ab92b8f0e5d76`;
- single-node classic and quorum performance matrix:
  [run 33043064904](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33043064904)
  at commit `a233bb315a72f878c8412d9c6d3ea3529b1507a2`;
- three-node classic host loss and quorum leader loss:
  [run 33044777633](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33044777633)
  at commit `d28812cb7d510319d5d0ac57ce145c5566dd8849`;
- three-node quorum partition and complete cluster restart:
  [run 33047281659](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33047281659)
  at commit `57467ae61533d21c16dae019a45ddea2342d9a5e`;
- three-cycle, four-resource-pair reconnect storm:
  [run 33049501240](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33049501240)
  at commit `a9b0be10a291c92bc179e8073530e3f53c4a131c`;
- three-node RabbitMQ 4.3.4 to 4.3.5 rolling upgrade:
  [run 33055826884](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33055826884)
  at commit `6a8e5b4728e34dbd1c61e28dca2d2e756b47bcc9`;
- three-node replicated performance under quorum leader loss:
  [run 33063180768](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33063180768)
  at commit `22ef3cda679874abe978d878b57d5ec54c9a32b9`.

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
