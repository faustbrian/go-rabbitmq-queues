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

## Live profile harness

The `livebroker` build tag includes `TestLiveBrokerPerformanceProfiles`. It uses
four dedicated, pre-existing queues of one selected type, mandatory persistent
publishing, exact confirmations, concurrent bounded producers and consumers,
representative payload and header bytes, optional bounded handler work, a warmup,
at least three steady samples, and at least three burst samples. Run separate
retained configurations for classic and quorum queues and for the three daily
volume targets.

Add a `performance` object to the connection configuration described by the
[live broker harness](live-broker-testing.md):

```json
{
  "performance": {
    "queue_type": "quorum",
    "queues": [
      {"name": "performance.one", "routing_key": "performance.one"},
      {"name": "performance.two", "routing_key": "performance.two"},
      {"name": "performance.three", "routing_key": "performance.three"},
      {"name": "performance.four", "routing_key": "performance.four"}
    ],
    "daily_messages": 100000000,
    "warmup_seconds": 10,
    "sample_seconds": 60,
    "samples": 5,
    "burst_multiplier": 4,
    "burst_seconds": 15,
    "publisher_concurrency": 64,
    "consumer_concurrency": 16,
    "payload_bytes": [512, 4096, 16384],
    "header_bytes": [0, 128, 1024],
    "handler_delay_ms": 1
  }
}
```

The full configuration needs the endpoint, virtual host, credentials, TLS, and
durable direct exchange fields, but does not need the single-node harness's
`classic`, `quorum`, or unroutable-route fields. Supply either one endpoint for
single-node evidence or exactly three endpoints for cluster evidence. All four
queues must be empty, dedicated, durably bound to their unique configured keys,
and free of delayed retry or competing publishers and consumers. Payload shapes
are bounded to 16 MiB in aggregate, header shapes to the package's 64 KiB cap,
and each sample to 250,000 accounted messages. Warmup is at least 5 seconds,
steady samples at least 30 seconds, bursts at least 5 seconds, and each mode has
at least three measured samples.

```sh
task_cache_root=$(mktemp -d /tmp/go-rabbitmq-queues-performance.XXXXXX) && trap 'chmod -R u+w "$task_cache_root"; find "$task_cache_root" -depth -delete' EXIT HUP INT TERM && mkdir "$task_cache_root/build" "$task_cache_root/modules" && RABBITMQ_QUEUE_PERFORMANCE_CONFIG=/secure/path/live-performance.json GOCACHE="$task_cache_root/build" GOMODCACHE="$task_cache_root/modules" go test -v -tags=livebroker -run '^TestLiveBrokerPerformanceProfiles$' -count=1 .
```

Each retained `PERFORMANCE_SAMPLE` records queue type, scheduled and achieved
rate, publish-confirm elapsed time, backlog-drain time, confirmations,
ambiguities, not-sent outcomes, deliveries, duplicates, and invalid outcome
pairings. The harness fails a sample that misses its configured daily-volume
rate or has any unaccounted, ambiguous, not-sent, duplicate, or invalid result.
It measures client-to-broker publish-confirm throughput and consumer drain; it
does not measure storage-device saturation or prove a cluster fault unless the
run is paired with separately retained broker and fault evidence.

## Evidence status

The repository provides local wrapper benchmarks and an opt-in live profile
harness. No retained live results exist yet. Single-node and three-node runs,
TLS and authorization cost, classic/quorum throughput, backlog recovery, and
node-failure evidence remain required before any production-capacity claim.
