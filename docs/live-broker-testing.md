# Live broker evidence harness

The `livebroker` build tag enables an external-package integration suite through
the stable public API. The suite never starts, stops, provisions, purges, or
deletes RabbitMQ resources. It requires a dedicated, externally provisioned TLS
fixture and passively verifies its exchange and queues before publishing.

The current suite proves, for the supplied broker endpoint:

- durable classic and quorum direct routing through independent producer and
  consumer connections;
- exact persistent body, property, timestamp, and application-header mapping;
- publisher-confirm acceptance reconciled with mandatory returns;
- exact unroutable exchange and routing-key classification; and
- RabbitMQ quorum `x-acquired-count` behavior across the package's bounded
  requeue policy.

It does not trigger restart, partition, node loss, leader failover, rolling
upgrade, credential rotation, authorization failure, or reconnect storms. Those
remain separate externally orchestrated evidence gates.

## Fixture contract

Provision a dedicated durable direct exchange, a durable classic queue, and a
durable quorum queue. Bind each queue to its configured unique routing key.
Configure exactly one broker endpoint. The queues must be empty, must not have
other publishers or consumers, and the quorum queue must not apply delayed
retry. The configured unroutable key must have no binding. The test identity
needs passive topology access, consume, publish, and settlement permissions but
does not need topology mutation or resource deletion.

Store the connection configuration in a permission-restricted JSON file:

```json
{
  "endpoints": [{"host": "rabbitmq.test.internal", "port": 5671}],
  "virtual_host": "/go-rabbitmq-queues-test",
  "username": "integration-client",
  "password": "replace-outside-source-control",
  "tls": {
    "server_name": "rabbitmq.test.internal",
    "root_ca_file": "/secure/path/ca.pem",
    "client_certificate_file": "",
    "client_private_key_file": ""
  },
  "exchange": "go-rabbitmq-queues.integration",
  "classic": {"name": "go-rabbitmq-queues.classic", "routing_key": "classic"},
  "quorum": {"name": "go-rabbitmq-queues.quorum", "routing_key": "quorum"},
  "unroutable_routing_key": "intentionally-unbound"
}
```

Set both client-certificate paths for mTLS. Leave `root_ca_file` empty only
when the broker certificate chains to the host's trusted system roots. The
suite bounds configuration and TLS material reads and never prints credential,
certificate, key, header, or payload values.

Compile the opt-in suite without contacting a broker:

```sh
task_cache_root=$(mktemp -d /tmp/go-rabbitmq-queues-live.XXXXXX) && trap 'chmod -R u+w "$task_cache_root"; find "$task_cache_root" -depth -delete' EXIT HUP INT TERM && mkdir "$task_cache_root/build" "$task_cache_root/modules" && GOCACHE="$task_cache_root/build" GOMODCACHE="$task_cache_root/modules" go test -tags=livebroker -run '^$' ./...
```

Run the evidence suite against the supplied fixture:

```sh
task_cache_root=$(mktemp -d /tmp/go-rabbitmq-queues-live.XXXXXX) && trap 'chmod -R u+w "$task_cache_root"; find "$task_cache_root" -depth -delete' EXIT HUP INT TERM && mkdir "$task_cache_root/build" "$task_cache_root/modules" && RABBITMQ_QUEUE_LIVE_CONFIG=/secure/path/live-broker.json GOCACHE="$task_cache_root/build" GOMODCACHE="$task_cache_root/modules" go test -tags=livebroker -run '^TestLiveBrokerSingleNode$' -count=1 .
```

The commands automatically remove their task-owned disposable caches. The
caller owns the externally provisioned fixture. Retain the raw test output with
the exact repository, RabbitMQ, container or operating-system, Go, client, TLS,
CPU architecture, and topology pins. A passing single-node run is not cluster,
failover, operator, performance, PHP, Laravel, or production evidence.

## Three-node interruption harness

`TestLiveBrokerThreeNodeInterruption` reuses the same dedicated topology with
exactly three TLS endpoints. Add these fields to a separate configuration:

```json
{
  "fault_start_gate_file": "/secure/operator-owned/fault-started",
  "fault_complete_gate_file": "/secure/operator-owned/failover-complete",
  "fault_queue_type": "quorum",
  "fault_window_messages": 1000
}
```

The complete JSON object still needs every connection and topology field from
the single-node example. Run separate retained executions with
`fault_queue_type` set to `classic` and `quorum`. Neither gate file may exist
when the test starts. Run the test with verbose output and wait for
`FAULT_WINDOW_READY`. An external operator may then perform the recorded node
loss, leader failover, or bounded network-interruption scenario. Create the
start gate once the fault is active; the test then publishes throughout the
bounded fault window. Create the complete gate only after the intended recovery
condition is observable.

```sh
task_cache_root=$(mktemp -d /tmp/go-rabbitmq-queues-cluster.XXXXXX) && trap 'chmod -R u+w "$task_cache_root"; find "$task_cache_root" -depth -delete' EXIT HUP INT TERM && mkdir "$task_cache_root/build" "$task_cache_root/modules" && RABBITMQ_QUEUE_CLUSTER_CONFIG=/secure/path/live-cluster.json GOCACHE="$task_cache_root/build" GOMODCACHE="$task_cache_root/modules" go test -v -tags=livebroker -run '^TestLiveBrokerThreeNodeInterruption$' -count=1 .
```

The test requires every confirmed publication to arrive, permits deliveries
only for confirmed or explicitly ambiguous attempts, rejects returned or
broker-rejected outcomes on the bound route, and emits countable confirmed,
ambiguous, not-sent, delivered, and duplicate totals. A passing client run must
be retained with the external fault timeline and broker evidence; the gate file
alone does not prove that any fault occurred. The external operator owns gate
removal and broker restoration.
