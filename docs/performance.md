# Performance evidence

RabbitMQ capacity and package wrapper cost are separate measurements. Local Go
benchmarks measure only validation, AMQP conversion, owned-copy allocation, and
confirmation tracking. They do not contact a broker and cannot establish queue,
TLS, replication, failover, handler, or end-to-end throughput.

## Workload targets

| Daily volume | Sustained average | Required live profile |
|---:|---:|---|
| 1,000,000 messages | 11.57 messages/second | steady state and burst |
| 10,000,000 messages | 115.74 messages/second | steady state and burst |
| 100,000,000 messages | 1,157.41 messages/second | steady state and burst |

Each live profile must use representative payload and header distributions,
four independent queues, verified TLS, mandatory publishing, publisher
confirmations, consumer backlogs, and bounded handlers. Classic and quorum
queues must be reported separately. The environment record must pin RabbitMQ,
the container or operating system, the Go client, Go, CPU architecture, CPU and
memory limits, storage class, queue policy, replication, and topology.

The three-node quorum profile must also measure leader loss, recovery time,
publisher ambiguity, redelivery, duplicates, backlog drain time, and error rate.
A retained run requires every publication and delivery to have an accounted-for
terminal outcome. Aggregate throughput must not conceal return, confirmation,
settlement, transport, or handler failures.

## Local wrapper benchmarks

Run the benchmarks with `go test -run '^$' -bench 'Benchmark(PublicationValidate|DeliveryFromAMQP|AMQPPublishing|PublishTrackerRegisterConfirm)$' -benchmem -count 10 ./...`.

Record the raw ten-sample output, Go and OS versions, CPU model, power profile,
and repository commit. Compare distributions against a named baseline; do not
turn a single wall-clock sample into a normal unit-test threshold. Report local
operations/second and allocations separately from broker and handler
throughput.

## Evidence status

The repository's performance evidence currently provides the local wrapper
benchmark harness only. Live single-node and three-node results, TLS and
authorization cost, classic/quorum broker throughput, backlog recovery, and
node-failure evidence remain required before any production-capacity claim.
