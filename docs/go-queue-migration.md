# `go-queue/rabbitmq` migration and rollback gate

This document records the compatibility boundary between
`github.com/faustbrian/go-queue/rabbitmq` and `rabbitmqqueue`. It is a migration
plan, not evidence that an adapter or an application cutover is complete.
Cross-library ownership and application requirements are recorded in the
[adoption audit](adoption-audit.md).

The comparison was refreshed on 2026-08-26 against:

- the `go-rabbitmq-queues` source at this document's revision;
- `go-queue` commit `569b6af`;
- `go-queue/rabbitmq` using `amqp091-go` `v1.11.0` and a RabbitMQ `3.13.7`
  integration fixture; and
- this package targeting the pins in [`COMPATIBILITY.md`](../COMPATIBILITY.md),
  including `amqp091-go` `v1.14.0` and RabbitMQ `4.3.5`.

The native module has no published version tag. `go-queue` must not add a local
filesystem `replace` or an unretrievable pseudo-version to ship an adapter.

## Contract comparison

| Legacy `go-queue/rabbitmq` contract | Native package contract | Adapter requirement |
|---|---|---|
| One `Worker` implements `Run`, `Queue`, `Request`, and `Shutdown` | Independent `Producer` and `Consumer` resources | The adapter must own two resources without coupling their failure or shutdown state |
| `Request` pulls one delivery and attaches acknowledgement callbacks to `job.Message` | `Consumer` pushes an owned `Delivery` into a handler and requires the handler to return `Settlement` | A bridge must preserve handler-before-ACK and classified failure settlement without holding unbounded work |
| `Queue` starts the consumer lazily before publishing | A producer never creates a consumer | Adapter publishing must remain producer-only; it must not retain this legacy side effect |
| Constructor and consumer startup actively declare exchanges, queues, bindings, and dead-letter topology; the main queue includes `x-dead-letter-exchange` and `x-dead-letter-routing-key` | `QueueDeadLetter` can represent those declaration arguments, while production policies and binding equivalence remain operator-owned | Passive verification may supply the exact legacy arguments with no explicit classic dead-letter strategy; policy-managed settings and bindings require separate operator evidence |
| One credential-bearing AMQP URI configures one endpoint | Structured endpoints, rotating credentials, verified TLS, heartbeat, and bounded recovery | Deprecated URI input needs an explicit, sanitized conversion or a migration error; credentials must never enter diagnostics |
| Durable classic queue assumptions are implicit | Classic and quorum queue types are explicit | Initial parity must name classic queues; quorum adoption is a separate topology and failure-semantics migration |
| `autoAck` can disable manual settlement | Native consumers always use manual settlement | `autoAck=true` cannot be represented as equivalent reliable behavior and must be rejected or migrated explicitly |
| Retryable failures are confirmed to a replacement publication before the source is acknowledged; terminal failures use a package-owned dead-letter publication | Native settlement supports bounded requeue, reject, dead-letter, or application-delegated behavior and does not automatically republish | The adapter needs a reviewed retry/dead-letter strategy with explicit duplicate and loss windows; generic native settlement is not parity by itself |
| Publish is persistent and synchronously confirmed but non-mandatory | Native publish distinguishes confirmed, returned, rejected, not-sent, and ambiguous outcomes and supports mandatory routing | Adapter error mapping must not collapse a return or ambiguous outcome into legacy success |
| Legacy publications omit AMQP `MessageId` | Native publication validation requires `MessageID` | The adapter must obtain one stable ID at admission, preserve it across every retry, and expose ambiguity for reconciliation without creating a replacement ID |
| Runtime connection loss is terminal and requires a replacement `Worker` | Runtime recovery is bounded and resource-local | Supervision and error mapping must be characterized so recovery does not duplicate workers or hide terminal exhaustion |
| `WithRequestTimeout` bounds an idle pull | Native handler and settlement timing use `ConsumerConfig.HandlerTimeout` | Idle polling and per-delivery execution are different clocks and must not be mapped to one timeout silently |

The legacy public surface that must remain source-compatible until a deliberate
major migration includes `Worker`, `NewWorker`, `NewWorkerE`, `Option`,
`ReconnectConfig`, `DeadLetterConfig`, `BackendName`, `QueueName`, `Run`,
`Shutdown`, `Queue`, and `Request`. Its constants and options require these
explicit dispositions:

| Legacy surface | Current default | Disposition and native boundary |
|---|---|---|
| `ExchangeDirect`, `ExchangeTopic` | Not applicable; `WithExchangeType` defaults to `ExchangeDirect` | Conditional direct mapping to the corresponding native `ExchangeKind` when the binding has no arguments; bounded routing keys, including empty keys, are preserved |
| `ExchangeFanout` | Not applicable | Requires characterization and normalization: the legacy worker binds with the configured routing key, while native fanout topology requires an empty routing key; reject configurations whose behavior cannot be proven equivalent |
| `ExchangeHeaders` | Not applicable | Incompatible without an explicit migration: the legacy worker supplies a routing key and no binding arguments, while native headers topology requires an empty routing key and non-empty bounded match arguments |
| `WithAddr` | `amqp://guest:guest@localhost:5672/` | Deprecated and rejected for production adoption; bridge only into explicitly supplied structured endpoint, virtual-host, credential-provider, and verified-TLS configuration without retaining or logging the URI |
| `WithReconnectConfig` | `MaxRetries: 5`, producing five total attempts under the current loop; 500 ms initial delay; 5 s maximum delay | Conditional mapping to `RecoveryPolicy` only after attempt counting and startup-versus-runtime recovery semantics are characterized; otherwise bridge-owned and rejected as ambiguous |
| `WithExchangeName` | `test-exchange` | Direct identity mapping into operator-owned exchange topology and each publication reference; the adapter does not declare it in production |
| `WithExchangeType` | `direct` | Direct mapping is limited to direct/topic; fanout requires characterized empty-key normalization, and headers requires explicit binding arguments absent from the legacy API |
| `WithRoutingKey` | `test-key` | Direct mapping for direct/topic publication and binding; fanout must normalize the binding key to empty after equivalence proof, while headers is incompatible until an explicit match-argument policy exists |
| `WithTag` | `golang-queue` | Direct mapping to `ConsumerConfig.Name` |
| `WithAutoAck` | `false` | `false` maps to manual settlement; `true` is incompatible and must be rejected or deliberately migrated without a parity claim |
| `WithQueue` | `golang-queue` | Maps to `QueueReference.Name` only after configuration explicitly selects `QueueClassic`; queue type must not be inferred after the initial migration |
| `WithRunFunc` | no-op handler | Bridge-owned handler used by the push consumer; source acknowledgement remains downstream of successful root-queue handling |
| `WithLogger` | `queue.NewLogger()` | Bridge-owned compatibility input; the native package has no logger dependency, and applications consume its bounded observations externally |
| `WithRequestTimeout` | 6 s | Bridge-owned idle pull timeout; it must not map to `ConsumerConfig.HandlerTimeout` |
| `WithPublishTimeout` | 5 s | Direct mapping to `ProducerConfig.PublishTimeout` after validating the native bound |
| `WithDeadLetter` | `<exchange>-dead`, `<queue>-dead`, `<routing-key>.dead`, 5 attempts | Exchange and routing key map to `QueueDeadLetter`, with the classic broker default left implicit; dead-letter queue/binding evidence remains operator-owned, while replacement publications, attempt metadata, and duplicate/loss windows remain a blocking adapter retry-strategy gap |

`TaskMessage` does not guarantee an identifier, and `job.Metadata.OriginalID` is
optional. Production adapter configuration must therefore provide a stable ID
source. It should use an application-owned original ID when available and must
reject a publication when no stable source exists; generating a new random ID
inside a publish or recovery attempt is not acceptable. The adapter must capture
the ID once for a `Queue` admission, reuse it for replacement publications and
native retries, and return an ambiguity error with a non-logging reconciliation
accessor when transmission may have occurred. Automatic retry after ambiguity
requires application reconciliation against that same ID.

## Adapter prerequisites

The adapter may be implemented only after all of these gates are satisfied:

1. Publish an immutable `go-rabbitmq-queues` version and consume it from
   `go-queue` without a local `replace` directive.
2. Characterize every legacy constructor, option default, queue operation,
   acknowledgement callback, retry classification, dead-letter header, error,
   and repeated-shutdown result before replacing its implementation.
3. Decide the pull-to-push bridge at the `go-queue` core boundary. The bridge
   must be bounded, cancellation-aware, and unable to acknowledge before the
   root queue finishes the handler.
4. Define production configuration that supplies structured credentials, TLS,
   endpoints, queue type, topology references, prefetch, concurrency, and
   handler bounds. Do not infer these from the legacy URI or defaults when the
   mapping is ambiguous. Supply `QueueDeadLetter` only when passively verifying
   the same declaration-time arguments; policy-managed settings and bindings
   require separate operator evidence for the complete effective topology.
5. Define and characterize the stable message-ID source, preservation across
   publish and recovery retries, ambiguity error, and reconciliation procedure.
   Preserve the existing job envelope bytes and stable failure metadata, or
   version them with bidirectional old/new decode evidence.
6. Resolve the Go toolchain boundary before adding the dependency: `go-queue`
   declares Go `1.26.6`, while this module declares Go `1.27.0`. Record whether
   the adapter raises the consuming module's minimum version or waits for a
   compatible native release, then verify the chosen toolchain against the
   complete `go-queue` gate.
7. Prove classic-queue parity through a live RabbitMQ `4.3.5` broker for
   routing, mandatory returns, confirms, settlement, retry, dead letters,
   malformed deliveries, shutdown, and runtime loss. Mocks are insufficient.
8. Run the complete `go-queue` contract, race, leak, fuzz, and integration
   gates after minimum version selection upgrades `amqp091-go` to `v1.14.0`.
9. Treat quorum queues as a separate adoption gate with delivery-count,
   delivery-limit, dead-letter strategy, leader-failover, and redelivery
   evidence.

A bounded application inventory must search for both the current module path
`github.com/faustbrian/go-queue/rabbitmq` and the historical path
`github.com/golang-queue/rabbitmq`. A search of primary local checkouts,
excluding archived and duplicate worktrees, found no source import outside
`go-queue` documentation. That local search is not an application inventory and
must not be used to claim that production has no consumers.

## Application adoption

For each application:

1. Inventory its exact options, queue/exchange/binding arguments, envelope
   version, retry and dead-letter behavior, handler deadline, idempotency key,
   dashboards, alerts, and shutdown budget.
2. Provision and verify operator-owned topology before starting the adapter.
   Keep the old queue declaration arguments unchanged while rollback remains
   possible.
3. Exercise a separate canary queue and binding with synthetic messages. Old
   and new consumers on the same production queue are competing consumers and
   do not provide deterministic shadow comparison.
4. Compare publish outcomes, handler outcomes, acknowledgements, rejections,
   retries, dead letters, redeliveries, duplicates, backlog, and recovery using
   countable message identifiers. A green process health check is insufficient.
5. Stop old consumers, wait for their admitted handlers to settle, and verify
   broker unacknowledged count reaches zero before enabling adapter consumers.
6. Enable bounded adapter consumers first, then producers if the wire envelope
   or publish policy changes. Record the exact commit, module versions,
   topology generation, and rollback checkpoint.
7. Remove the legacy implementation only after the observation window passes
   and no rollback prerequisite has been discarded.

## Rollback

The current old worker cannot verify topology passively: startup always actively
redeclares its exchanges, queues, bindings, and dead-letter topology. It is a
valid rollback only while those declarations remain equivalent, client-side
declaration remains permitted, and the old worker can decode every queued
message. Otherwise rollback requires a changed legacy worker that can use the
new topology safely or a separate topology recovery plan.

1. Stop adapter producers if their wire format or routing differs.
2. Pause and drain adapter consumers; verify admitted handlers completed and
   the broker reports zero unacknowledged deliveries for those consumers.
3. Preserve ready messages and dead letters in place. Do not copy or delete
   them as part of an ordinary rollback.
4. Restore the old application artifact and its pinned dependency set.
5. Start one old consumer against verified compatible topology, process
   synthetic probes, then expand gradually while monitoring duplicate and
   redelivery counts.
6. If topology, envelope, or dead-letter metadata is no longer backward
   compatible, stop. A separate data/topology migration and recovery plan is
   required; redeploying old code is not a valid rollback.

Publishing a replacement message and acknowledging its source remain separate
effects in both designs. A crash after the replacement confirm and before the
source acknowledgement can duplicate work. A timeout or connection loss after
transmission can also be ambiguous. Application idempotency and message-level
reconciliation are therefore prerequisites for both migration and rollback.
