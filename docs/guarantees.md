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
- Mandatory-return route details come from the exact bounded publication
  registered for the correlated token. Broker-supplied route fields never
  cross the public result boundary; reply text is sanitized before exposure.
  A return without an active exact token makes outstanding work ambiguous and
  forces generation recovery instead of allowing a positive confirm to report
  acceptance.
- Publications can name the expected direct, topic, fanout, or headers exchange
  kind so routing-key validation matches the exchange semantic. The kind is a
  local policy assertion; passive topology verification proves the broker's
  actual exchange declaration.
- An empty publication exchange is accepted only with an explicit direct kind,
  representing RabbitMQ's predeclared default exchange. The non-empty routing
  key remains the target queue identity; an omitted exchange is not inferred.
- Deliveries and their bounded `x-death` history preserve empty routing keys
  because fanout and headers exchanges route natively without one; the broker
  exchange and binding remain the authoritative routing evidence.
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
- Deliveries preserve RabbitMQ 4.3's bounded `x-acquired-count` and
  `x-delivery-count` as separate optional unsigned counters. Assignment to a
  consumer does not prove that the handler observed the message, and only the
  failed-delivery counter participates in the quorum delivery limit. The
  package applies `MaxRequeues` to the acquired count when available, so returns
  caused by NACK, reject, or connection loss all consume the local retry bound.
  Without that quorum counter, the fallback permits at most one redelivery.
- Publications and deliveries preserve bounded `reply-to` and correlation
  metadata for application-owned request/reply flows. The package does not own
  reply queues or provide an RPC lifecycle abstraction.
- Handler, settlement, and shutdown work is bounded by the configured handler
  timeout; handlers must observe cancellation for graceful draining.
- `Queue.DeliveryLimit` models RabbitMQ 4.3's quorum-only failed-redelivery
  bound. Omission leaves the broker policy or default of 20 effective, while an
  explicit zero makes the first failed redelivery exceed the limit. The package
  does not expose RabbitMQ's `-1` unlimited compatibility mode. RabbitMQ 4.3
  `basic.nack` returns do not increment `x-delivery-count`, so applications
  still need the package's bounded requeue policy.
- `Queue.ConsumerTimeout` models RabbitMQ 4.3's quorum-only broker deadline for
  delivery acknowledgement. It is omitted by default, must be at least one
  minute, and is independent of `ConsumerConfig.HandlerTimeout`. When the
  broker deadline expires, RabbitMQ closes the channel and requeues all of its
  outstanding deliveries; application handler deadlines should finish before
  the effective broker timeout.
- `Queue.DisconnectedConsumerTimeout` models how long a RabbitMQ 4.3 quorum
  queue waits before returning deliveries held by a consumer node that becomes
  unreachable. It is omitted by default, preserving the broker's 60-second
  default, and accepts explicit non-negative millisecond values. This bounds
  broker-side partition recovery; it does not prove that the original handler
  stopped or prevent concurrent duplicate effects after redelivery.
- `Queue.DelayedRetry` models RabbitMQ 4.3's quorum-only broker-managed linear
  backoff for returned deliveries. It is omitted by default. Enabled policy is
  bounded by a positive minimum and an optional maximum delay; explicit
  disabling cannot carry stale delay values. This changes when the broker
  makes a returned delivery available again. It does not publish a replacement
  message, make handler effects atomic, or remove the application's idempotency
  responsibility.
- Requeue is bounded by delivery state and configured policy. The package does
  not automatically publish replacement messages.
- The package does not implement RabbitMQ Streams, application schemas,
  exactly-once processing, an outbox, or a generic messaging interface.
