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

The pinned [Kubernetes Operator fixture](operator-compatibility.md) provides a
schema-validated reference for the exchange, base queues, PHP queues, classic
and quorum performance queues, bindings, TLS-only cluster, and generated test
identity. Applying that fixture is not itself live evidence; the environment
must still supply its secrets and retain reconciled controller and broker
state.

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

The repository CI runs this same public single-node contract against the pinned
Linux amd64 RabbitMQ image through [`ci-live-broker.sh`](../scripts/ci-live-broker.sh).
That script refuses to run outside GitHub Actions, creates a task-owned
TLS-only container and temporary certificate authority, provisions a
non-administrative client without configure permission, verifies TLS 1.2 and
TLS 1.3, and removes the container and generated material on every exit. A
passing CI job is retained single-node Linux amd64 evidence only; it does not
replace the externally orchestrated three-node, Operator, PHP, performance, or
application-rollout gates.

## PHP interoperability harness

`TestLiveBrokerPHPInteroperability` uses the same single TLS endpoint and
direct exchange with two additional dedicated, durable queues. Bind the
Go-to-PHP and PHP-to-Go queues to their distinct configured routing keys. The
queue names and all three PHP routing keys must also be distinct from the base
classic, quorum, and unroutable fixture identities. The PHP-only unroutable key
must have no binding. Both queues must be empty and must not have other
publishers or consumers. Add this object to the live configuration:

```json
{
  "php_interoperability": {
    "binary": "/absolute/path/to/php-8.5.9",
    "queue_type": "classic",
    "go_to_php_queue": "go-rabbitmq-queues.go-to-php",
    "go_to_php_routing_key": "interop.go-to-php",
    "php_to_go_queue": "go-rabbitmq-queues.php-to-go",
    "php_to_go_routing_key": "interop.php-to-go",
    "unroutable_routing_key": "interop.intentionally-unbound"
  }
}
```

The binary path must be absolute. `queue_type` may be `classic` or `quorum`;
the externally owned topology must match it. Install the locked dependencies
into a task-owned vendor path, pass its absolute autoload path through
`PHP_AMQPLIB_AUTOLOAD`, and run the evidence test with task-owned Composer and
Go caches:

```sh
task_interop_root=$(mktemp -d /tmp/go-rabbitmq-queues-php-interop.XXXXXX) && trap 'chmod -R u+w "$task_interop_root"; find "$task_interop_root" -depth -delete' EXIT HUP INT TERM && mkdir "$task_interop_root/composer-home" "$task_interop_root/php-vendor" "$task_interop_root/go-build" "$task_interop_root/go-modules" && composer --version --no-ansi | rg '^Composer version 2\.10\.1 ' && COMPOSER_HOME="$task_interop_root/composer-home" COMPOSER_VENDOR_DIR="$task_interop_root/php-vendor" composer install --working-dir=testdata/interoperability/php --no-dev --no-interaction --no-ansi --no-progress --classmap-authoritative && PHP_AMQPLIB_AUTOLOAD="$task_interop_root/php-vendor/autoload.php" RABBITMQ_QUEUE_LIVE_CONFIG=/secure/path/live-broker.json GOCACHE="$task_interop_root/go-build" GOMODCACHE="$task_interop_root/go-modules" go test -v -tags=livebroker -run '^TestLiveBrokerPHPInteroperability$' -count=1 .
```

The harness first checks the exact PHP runtime, required extensions, package
version, and source reference. It then proves Go-to-PHP consumption,
PHP-to-Go publication,
publisher confirms, mandatory returns, exact body and property values, manual
acknowledgement, and the directional header-type guarantees documented under
[AMQP interoperability](interoperability.md). It never provisions, purges, or
deletes the queues. The CI-hosted fixture now retains this evidence for the
pinned PHP client. Laravel framework integration remains outside this harness
and is not established by the protocol-level result.

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
only for confirmed or explicitly ambiguous attempts, accepts an explicit
broker rejection only during the fault window, rejects returned outcomes on
the bound route, and emits countable confirmed, rejected, ambiguous, not-sent,
delivered, and duplicate totals. A passing client run must be retained with the
external fault timeline and broker evidence; the gate file alone does not prove
that any fault occurred. The external operator owns gate removal and broker
restoration.

The GitHub Actions matrix runs the same test through
[`ci-live-cluster.sh`](../scripts/ci-live-cluster.sh) against three pinned
RabbitMQ 4.3.5 Linux amd64 containers. It provisions the topology before the
client starts, verifies three quorum members, and targets the actual classic
queue host or quorum leader reported by RabbitMQ. Retained
[run 33044777633](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33044777633)
at commit `d28812cb7d510319d5d0ac57ce145c5566dd8849` recorded:

- classic host loss and restart: 88 attempts, 24 confirmed, 62 rejected, two
  ambiguous, zero not-sent, 24 delivered, and zero duplicates; and
- quorum leader loss from `rabbit@rabbit1` with `rabbit@rabbit3` elected: 88
  attempts, confirmations, and deliveries, with zero rejected, ambiguous,
  not-sent, or duplicate outcomes.

Retained
[run 33047281659](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33047281659)
at commit `57467ae61533d21c16dae019a45ddea2342d9a5e` recorded:

- quorum partition of `rabbit@rabbit1`, healed with `rabbit@rabbit2` as leader:
  88 attempts, 59 confirmed, 29 ambiguous, zero rejected or not-sent, 88
  delivered, and zero duplicates; and
- complete quorum-cluster restart after a 20-second stopped interval, with
  `rabbit@rabbit3` as leader after recovery: 88 attempts, 24 confirmed, 64
  not-sent, zero rejected or ambiguous, 24 delivered, and zero duplicates.

Retained
[run 33049501240](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33049501240)
at commit `a9b0be10a291c92bc179e8073530e3f53c4a131c` recorded three
complete quorum-cluster outages across four producer and consumer pairs. Each
outage held all three nodes down for five seconds. All eight resources became
unavailable and recovered after every cycle, RabbitMQ reported exactly four
active consumers after each recovery, and the public observation streams
recorded 108 producer and 108 consumer reconnect attempts. The message ledger
recorded 14 attempts, 11 confirmed, three not-sent, zero rejected or ambiguous,
11 delivered, and zero duplicates.

The CI matrix also defines a three-node `rolling-upgrade` gate. It starts the
cluster on the pinned RabbitMQ 4.3.4 Linux amd64 image, enables stable feature
flags, and then replaces nodes from highest to lowest index with the pinned
RabbitMQ 4.3.5 image. Before every stop it requires
`rabbitmq-upgrade await_online_quorum_plus_one` to pass. During each
node-replacement interval the public client publishes 64 uniquely identified
messages through the three configured endpoints; after each replacement the
gate requires all three nodes and quorum members, a running leader, no local
alarms on the upgraded node, client readiness, exactly one active consumer, and
a confirmed post-upgrade round trip before continuing. The final ledger reports
confirmed, rejected, ambiguous, not-sent, delivered, and duplicate totals.

Retained
[run 33055826884](https://github.com/faustbrian/go-rabbitmq-queues/actions/runs/33055826884)
at commit `6a8e5b4728e34dbd1c61e28dca2d2e756b47bcc9` upgraded `rabbit3`,
`rabbit2`, then `rabbit1` from RabbitMQ 4.3.4 to 4.3.5. Every replacement
retained three running quorum members and exactly one active consumer. The
ledger recorded 203 attempts, confirmations, and deliveries, with zero
rejected, ambiguous, not-sent, or duplicate outcomes.

This is CI-hosted node-loss, quorum-partition, complete-cluster-restart,
bounded reconnect-storm, and patch rolling-upgrade evidence. It is not
installed-Operator reconciliation, application rollout, or production-capacity
evidence.

The four-queue steady and burst runner is documented under
[performance evidence](performance.md).
