# Guarantees and boundaries

Production clients use passive topology verification. Active declarations are
available only behind the explicit development permit and are not a production
topology-management mechanism. AMQP 0-9-1 has no passive binding inspection,
so passive application checks verify exchange and queue equivalence while the
infrastructure control plane verifies bindings. Mandatory publishing still
reports routing drift as a returned publication. Development declarations are
ordered, non-transactional AMQP operations and can leave a partial test topology
when a later declaration fails.

Queue TTL, expiry, length, overflow, consumer timeout, and dead-letter fields
describe AMQP declaration arguments, not mutable operator policies. RabbitMQ
policies remain the production default. Passive verification includes an
optional argument only when the caller supplies it, so a policy-managed queue
requires separate operator evidence for the effective policy. Quorum
at-least-once dead-lettering is accepted only with `reject-publish` overflow
and can still duplicate at the target while RabbitMQ retries an unconfirmed
internal transfer.

- Publisher confirmation and consumer acknowledgement are separate effects.
- Cancellation or connection loss after transmission can be ambiguous.
- Mandatory returns must be reconciled with confirms before acceptance.
- Asynchronous publishing owns the admitted publication and emits exactly one
  terminal outcome; a full admission window rejects new work without spawning
  another worker.
- Batches validate every item before publishing, preserve input order, and
  report independent per-item outcomes; they are not atomic broker operations.
- The producer makes in-flight work ambiguous on connection loss, rejects new
  work while recovering, then rebuilds a fresh confirm generation with bounded
  endpoint rotation and refreshed credentials. Exhausted recovery is terminal.
- `BlockedNotifications` reports coalesced blocked/unblocked transitions without
  exposing broker reason text; a blocked connection does not by itself retry a
  publication.
- `Liveness`, `Readiness`, and `DependencyHealth` are separate local snapshots.
  Bounded recovery and broker blocking remove readiness without declaring the
  process dead; exhausted recovery is a failed liveness state suitable for
  supervision.
- `Observations` exposes bounded best-effort producer and consumer events with
  fixed resource, kind, and outcome values. Events never contain credentials,
  certificates, payloads, headers, routes, broker reason text, or identifiers.
  Slow readers do not block delivery correctness; the next delivered event
  reports how many observations were dropped while its stream was full.
- The consumer closes a failed generation before bounded replacement, reapplies
  QoS, consumer identity, signed priority, and classic-only exclusivity, and
  refreshes endpoints and credentials. Work from the failed generation is never
  settled on its replacement; exhausted recovery is terminal.
- A matching broker `basic.cancel` emits a payload-free consumer-cancellation
  observation and starts generation replacement. Cancellation notifications
  for other tags are ignored. Notification-channel closure during connection
  loss still follows the connection-recovery path, while client-initiated
  cancellation during drain is reported only as shutdown.
- Consumer priority is an explicit AMQP `x-priority` policy: nil omits the
  argument and uses RabbitMQ's zero default, while a pointer to zero preserves
  explicit-zero intent. Exclusive consumption is rejected for quorum queues and
  for queue references marked single-active-consumer. The reference records
  expected topology; use passive topology verification for broker equivalence.
- A consumer with `QueueReference.Transient` passively verifies the referenced
  exchange and declares, binds, and consumes one non-durable, auto-delete,
  exclusive, server-named classic queue on its owned connection. Recovery
  creates a new queue and cannot recover messages deleted with the failed
  connection. `ApplyTopology` rejects this lifecycle because it closes its
  connection before returning.
- `Pause` temporarily stops new handler admission without cancelling the broker
  consumer. Already admitted work settles normally, up to the configured
  prefetch may remain unsettled, runtime recovery continues, and `Resume`
  restores admission. A paused consumer remains live with an available
  dependency but is not ready.
- Connection loss can redeliver a message while its earlier handler invocation
  is still completing. Applications must tolerate concurrent duplicates.
- Manual settlement provides at-least-once processing; applications remain
  responsible for idempotency.
- Publications and deliveries preserve bounded `reply-to` and correlation
  metadata for application-owned request/reply flows. The package does not own
  reply queues or provide an RPC lifecycle abstraction.
- Handler, settlement, and shutdown work is bounded by the configured handler
  timeout; handlers must observe cancellation for graceful draining.
- `Queue.ConsumerTimeout` models RabbitMQ 4.3's quorum-only broker deadline for
  delivery acknowledgement. It is omitted by default, must be at least one
  minute, and is independent of `ConsumerConfig.HandlerTimeout`. When the
  broker deadline expires, RabbitMQ closes the channel and requeues all of its
  outstanding deliveries; application handler deadlines should finish before
  the effective broker timeout.
- Requeue is bounded by delivery state and configured policy. The package does
  not automatically publish replacement messages.
- The package does not implement RabbitMQ Streams, application schemas,
  exactly-once processing, an outbox, or a generic messaging interface.
