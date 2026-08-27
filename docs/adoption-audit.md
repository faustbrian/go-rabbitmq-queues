# Adoption audit

This audit records the source contracts that must remain intact when
`rabbitmqqueue` is connected to adjacent Go libraries or adopted by Shipit
applications. It is design input and a migration gate, not evidence that an
adapter, broker deployment, application cutover, or production inventory is
complete.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as shown
here.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174

## Snapshot and evidence boundary

The audit was refreshed on 2026-08-27 against these local source snapshots:

| Repository | Commit | Branch |
|---|---|---|
| `go-queue` | `0c78f7f23c0f` | `main` |
| `go-rabbitmq-streams` | `0b11f5b3449b` | `main` |
| `go-transactional-outbox` | `51507d3af020` | `main` |
| `go-event-sourcing` | `3b0ffaf50fa9` | `main` |
| API | `8c6f7414c7b7` | `bugfix/develop-2717-receipt-amount-consistency` |
| Bill | `289c75f0d6e5` | `bugfix/develop-2753-prevent-wallet-refetch-double-charge` |
| Track | `62e2f5ab07b0` | `bugfix/tracking-webhook-load` |
| Location | `efcf46836eb7` | `main` |
| Postal | `1b2a7b3d82fc` | `bugfix/disambiguate-swedish-address-validation` |
| Shopify Laravel | `7a71417564ce` | `main` |

The adjacent Go repositories were clean at those commits. The application
review inspected tracked source and dependency manifests only. Untracked local
operator files in some application checkouts were outside this audit.

A bounded search of the six application snapshots found no direct RabbitMQ or
AMQP implementation and no installed AMQP client package. Composer lock files
contain optional AMQP suggestions from transitive packages; those suggestions
are not installed-client evidence. All six applications depend on Laravel
Horizon. These findings do not prove that other checkouts, deployed artifacts,
or production services have no RabbitMQ consumers.

The application requirements below cover API-to-Bill billing imports and
Track webhook fan-out. Location, Postal, Shopify Laravel, other application
flows, deployment manifests, live topology, and production usage remain
deferred inventory.

## Adjacent-library boundaries

| Surface | Verified contract | Adoption consequence |
|---|---|---|
| `go-queue/rabbitmq` | One `Worker` couples production and consumption, actively declares topology, accepts a credential-bearing URI, confirms non-mandatory publications, and republishes failures. Its fixture uses RabbitMQ 3.13.7 and `amqp091-go` 1.11.0. | Compatibility requires the separate characterization, migration, and rollback gates in [`go-queue` migration](go-queue-migration.md). Native queue policy MUST keep producer and consumer lifecycles separate. It MUST NOT mutate production topology. |
| `go-rabbitmq-streams` | Provides retained histories, offsets, replay, and Super Streams through the RabbitMQ Streams protocol with AMQP 1.0 messages. | It MUST remain separate from process-and-remove classic and quorum queues. Queue adoption MUST NOT replace stream retention or replay contracts. |
| `go-transactional-outbox` | The relay owns leases and durable state transitions behind an error-only `Publisher`. It has a generic `go-queue` adapter and a confirmed RabbitMQ Streams adapter, but no `rabbitmqqueue` adapter. | A future adapter MUST map native outcomes into the relay error/classification contract without claiming exactly-once delivery or taking ownership of the outbox store. |
| `go-event-sourcing/adapters/outbox` | Stages event rows followed by outbox rows inside a caller-owned PostgreSQL transaction. The caller commits or rolls back the outer transaction. | A queue publisher MUST remain downstream of the relay. It MUST NOT publish inside event staging or absorb transaction ownership. |
| `go-event-sourcing/adapters/queue` | Directly encodes and enqueues live or explicitly permitted replay deliveries. A queue error leaves backend acceptance unknown. | This is a separate live dispatcher, not a transactional outbox. A native adapter MUST preserve acceptance ambiguity and MUST NOT imply that enqueue success proves durable processing. |

### Transactional outbox adapter requirements

A future `go-transactional-outbox` adapter for `rabbitmqqueue` MUST satisfy all
of these mappings before the relay can use it:

| Outbox value or outcome | Required native mapping |
|---|---|
| `Envelope.ID` | Stable `Publication.MessageID`, unchanged across relay attempts and ambiguity reconciliation |
| `Envelope.Topic` | Explicit configured exchange and routing policy; topic text MUST NOT silently create topology |
| `Envelope.Payload` | Opaque body bytes with an explicitly configured or metadata-derived content type |
| `Envelope.PayloadVersion` | A language-neutral schema-version metadata value |
| `Envelope.CreatedAt` | AMQP timestamp without changing payload bytes |
| Idempotency, ordering, correlation, and trace metadata | Bounded reviewed headers or routing fields with adapter-owned names and collision checks |
| Native confirmed outcome | `nil` only after exact confirmation and mandatory-return reconciliation |
| Native rejected, returned, not-sent, or ambiguous outcome | A typed error whose relay classification and acceptance state are explicit; ambiguity MUST retain the stable message ID for reconciliation |

The adapter MUST validate the complete mapped publication before broker
admission. It MUST NOT own outbox leases, retries, database transitions,
application schemas, topology creation, or consumer idempotency. Live broker
evidence MUST demonstrate that a relay crash after confirmation and before the
durable delivered transition produces a reconcilable duplicate rather than a
false exactly-once claim.

## API to Bill billing commands

### Current source contract

API creates a ULID-backed `BillingExport`, persists its type, context, and
built payload, and then dispatches `ProcessBillingExport`. The job is queued
after the surrounding database commit, uses type-specific queues, and applies
type-specific overlap keys. Its handlers currently send the persisted payload
to Bill over HTTP RPC and mark the export passed only after an accepted
response.

Bill's RPC boundary creates or refreshes an `ImportCall` by import type and
external reference, persists the payload, and dispatches `ProcessImportCall`.
The model serializes concurrent admissions with a transaction-scoped advisory
lock and applies state-aware deduplication. Import types select separate
processing queues. Successful billing exports and processed import calls become
prunable after approximately 30 days.

### Migration requirements

An API-to-Bill broker migration:

1. MUST publish from the persisted `BillingExport` payload rather than rebuild
   mutable application state during a retry.
2. MUST use the billing export ULID as the stable AMQP message identifier for
   publish reconciliation while preserving Bill's distinct import-type and
   external-reference deduplication key.
3. MUST route each billing type explicitly and preserve the current
   type-specific concurrency and overlap invariants.
4. MUST make Bill's inbound `ImportCall` durable before acknowledging the
   broker delivery. Processing dispatch after that point MUST be recoverable
   from the durable inbound record.
5. MUST treat an existing equivalent import attempt as an idempotent admission,
   not as evidence that its business processing has completed.
6. MUST preserve the distinction between broker confirmation, Bill admission,
   Bill processing completion, and API export completion.
7. MUST provide reconciliation by billing export ID for ambiguous publication,
   duplicate delivery, failed inbound admission, and processing backlog.
8. MUST define a reconciliation and audit-retention horizon before either side
   prunes its durable records.

No source change, adapter, wire envelope, live broker proof, or cutover plan for
this flow exists in the audited snapshots.

## Track delivery fan-out

### Current source contract

PostgreSQL `tracking_events` is the canonical event history. `StoreEvent`
transactionally inserts a new event and creates one
`tracking_event_deliveries` row for every configured destination. The default
destinations are Shipit and Shopify, and the event/destination pair is unique.

Each delivery row owns destination, attempt count, retry availability, queued
time, delivered time, terminal failure time, and the last error. Pending rows
are claimed with row locks and stale-claim recovery. `BroadcastEvent` currently
sends the event over HTTP, marks success on the delivery row, and applies a
bounded retry schedule with a terminal attempt limit.

### Migration requirements

A Track broker migration:

1. MUST keep PostgreSQL events and delivery rows canonical unless a separate
   persistence migration proves an equivalent replay and repair contract.
2. SHOULD publish the stable delivery-row ID, or a bounded versioned envelope
   containing it, to independent destination queues. It MUST NOT publish one
   competing-consumer queue when every destination must receive the event.
3. MUST make broker admission recoverable from the delivery row and MUST leave
   stale claims eligible for bounded repair after publisher interruption.
4. MUST make each consumer atomically claim or lock the delivery row, skip
   already delivered rows, and persist the destination outcome before
   acknowledging the broker delivery.
5. MUST preserve independent Shipit and Shopify retry, terminal-failure,
   backlog, and observability states.
6. MUST reconcile broker redelivery and publication ambiguity against the
   delivery-row ID. A broker message MUST NOT become a second canonical event
   record.
7. MUST retain a bounded database-driven repair path for missing broker
   publications and stranded delivery rows throughout migration and rollback.

No source change, adapter, live broker proof, or cutover plan for this flow
exists in the audited snapshots.

## Adoption gates

An adapter or application MUST NOT claim production readiness until every
applicable gate has retained, versioned evidence.

### PHP interoperability

- A supported PHP AMQP client and runtime MUST be pinned.
- The language-neutral corpus MUST pass in both directions through a live
  broker with exact body bytes, properties, header types, routing, mandatory
  returns, publisher confirmations, and manual settlement.
- Laravel process lifecycle, cancellation, reconnect, and graceful shutdown
  behavior MUST be exercised separately from the Go client.

### Live RabbitMQ behavior

- The broker, container, Go client, Go toolchain, OS, and architecture MUST
  match recorded compatibility pins.
- Classic and quorum queues MUST pass single-node and three-node routing,
  settlement, dead-letter, recovery, and ambiguity scenarios.
- Broker restart, node loss, queue-leader failover, network interruption,
  rolling upgrade, prolonged outage, reconnect storm, and application rolling
  deployment MUST have countable message-level outcomes.

### Operator-owned topology

- RabbitMQ Cluster Operator and Messaging Topology Operator versions and
  manifests MUST be pinned.
- Production exchanges, queues, bindings, policies, permissions, and TLS
  identities MUST be operator-owned and reviewed before applications start.
- Applications MUST use passive verification. Effective policy and binding
  equivalence require operator evidence because AMQP passive declarations do
  not expose the complete effective topology.

### Idempotency and reconciliation

- Every publication MUST have a stable persisted message identifier before its
  first send.
- Every consumer MUST persist or detect its application effect before ACK and
  tolerate concurrent duplicate delivery.
- Returned, rejected, ambiguous, dead-lettered, and exhausted outcomes MUST be
  queryable without logging payloads or credentials.
- Operators MUST have bounded repair procedures for unpublished records,
  ambiguous sends, stranded claims, duplicate deliveries, and dead letters.

### Canary, cutover, and rollback

- Canary evidence MUST use isolated queues and bindings; old and new consumers
  on one queue are competitors, not a deterministic shadow comparison.
- Cutover MUST record artifact versions, topology generation, message counts,
  backlog, unacknowledged deliveries, and a rollback checkpoint.
- Old consumers MUST drain admitted work before new consumers take ownership.
- Rollback MUST preserve topology and envelope compatibility or provide a
  separate migration plan. It MUST NOT delete ready messages or dead letters
  as an ordinary rollback step.
- Legacy removal MUST wait until the observation window and reconciliation
  checks pass without discarding rollback prerequisites.
