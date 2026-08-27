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

Each retained `PERFORMANCE_SAMPLE` records queue type, target, offered, and
achieved rate, publish-confirm elapsed time, backlog-drain time, confirmations,
ambiguities, not-sent outcomes, deliveries, duplicates, and invalid outcome
pairings. Every sample fails on an unaccounted, ambiguous, not-sent, duplicate,
or invalid result. The throughput gate requires a strict majority of the three
or more retained steady samples and burst samples to meet the configured rate,
and retains the minimum, median, and maximum. This prevents one shared-runner
scheduling pause from hiding an otherwise sustained profile while persistent
rate misses still fail. The harness offers 1% load headroom above the named
target so final confirmation overhead cannot turn an exact target into a false
failure.
It measures client-to-broker publish-confirm throughput and consumer drain; it
does not measure storage-device saturation or prove a cluster fault unless the
run is paired with separately retained broker and fault evidence.

## Evidence status

GitHub Actions [run 33043064904](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33043064904)
at commit `a233bb315a72f878c8412d9c6d3ea3529b1507a2` retained the
single-node matrix below. Every steady and burst profile met its target in all
three samples. Every sample confirmed and delivered every message and recorded
zero ambiguous, not-sent, duplicate, or invalid outcomes.

| Queue | Daily volume | Steady samples/second (min/median/max) | Burst samples/second (min/median/max) |
|---|---:|---:|---:|
| Classic | 1M | 11.72 / 11.72 / 11.72 | 46.94 / 46.95 / 46.95 |
| Classic | 10M | 116.93 / 116.93 / 116.93 | 467.63 / 467.64 / 467.65 |
| Classic | 100M | 1,168.98 / 1,168.98 / 1,168.98 | 4,671.57 / 4,672.92 / 4,673.40 |
| Quorum | 1M | 11.72 / 11.72 / 11.72 | 46.94 / 46.94 / 46.94 |
| Quorum | 10M | 116.92 / 116.92 / 116.93 | 467.64 / 467.67 / 467.67 |
| Quorum | 100M | 1,168.93 / 1,168.96 / 1,168.96 | 4,672.29 / 4,672.91 / 4,673.01 |

The retained environment was RabbitMQ 4.3.5 using the pinned Linux amd64
container digest, `amqp091-go` 1.14.0, Go 1.27.0, Linux amd64, and the GitHub
Actions `ubuntu-24.04` image. Each isolated job used one TLS endpoint, four
queues, 64 publisher workers, 16 consumer workers, payload shapes of 256,
1,024, and 4,096 bytes, header shapes of 0, 64, and 512 bytes, a 5-second
warmup, three 30-second steady samples, and three 5-second 4x burst samples.

This is CI-runner capacity evidence, not production capacity evidence. The
hosted runner does not pin broker CPU, memory, or storage class, the quorum
queues had one member, and no node fault occurred. Three-node replication,
leader loss, backlog recovery under failure, storage saturation, application
handlers, and deployment-specific capacity remain unverified.
