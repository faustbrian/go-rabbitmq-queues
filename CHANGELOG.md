# Changelog

## Unreleased

### Documentation

- Organize detailed guarantees behind a concise package overview and
  documentation index.
- Document the verified `go-queue/rabbitmq` parity gaps, adapter prerequisites,
  staged adoption, and rollback gate.
- Record adjacent-library ownership, API-to-Bill and Track migration
  requirements, evidence limitations, and application adoption gates.

### Added

- Add bounded connection, credential, verified TLS, and recovery policy types.
- Add explicit classic/quorum queue and passive/development topology policy.
- Add bounded declaration-equivalent queue TTL, expiry, length, overflow, and
  dead-letter policy with explicit RabbitMQ 4.3 queue-type restrictions.
- Distinguish an omitted quorum delivery limit from explicit bounded values,
  including zero, without exposing RabbitMQ's unlimited compatibility mode.
- Add quorum-only delivery-acknowledgement timeout policy with RabbitMQ 4.3
  minimum and declaration-equivalence validation.
- Add quorum-only disconnected-consumer timeout policy with explicit RabbitMQ
  4.3 partition-recovery bounds and declaration-equivalence validation.
- Add quorum-only delayed-retry policy with bounded RabbitMQ 4.3 linear-backoff
  modes and declaration-equivalence validation.
- Add bounded passive exchange and queue equivalence checks plus explicit
  development-only binding declaration without mutating production bindings.
- Add a language-neutral AMQP message corpus for byte-preserving interoperability
  checks without claiming broker or PHP evidence.
- Add bounded AMQP publication metadata and distinct publisher outcome states.
- Add exchange-kind-aware publication routing so fanout and headers exchanges
  preserve their native empty routing key without weakening direct/topic policy.
- Add bounded AMQP reply-to publication metadata for application-owned
  request/reply flows.
- Add bounded exact correlation for confirmations, mandatory returns, late
  events, and ambiguous channel-generation failure.
- Add an independent synchronous producer with mandatory routing, publisher
  confirms, endpoint rotation, credential refresh, verified TLS, bounded
  startup retry, graceful close, and explicit ambiguous outcomes.
- Add bounded asynchronous publishing and prevalidated, ordered non-atomic
  batches with independent per-item outcomes.
- Add bounded producer runtime recovery with fresh confirm generations,
  endpoint and credential rotation, and sanitized connection-blocked state.
- Add an independent bounded consumer with manual ACK, NACK, reject and
  delegated settlement, owned delivery snapshots, dead-letter history,
  per-consumer QoS, explicit failure policy, and graceful drain/close.
- Preserve RabbitMQ 4.3 acquired and failed-delivery counters as separate
  bounded delivery metadata.
- Bound quorum requeue requests with RabbitMQ 4.3's acquired count, including
  returns that do not increment the failed-delivery counter.
- Add signed consumer priority and classic-only exclusive-consumer policy with
  single-active-consumer conflict validation and recovery preservation.
- Add explicit client-owned transient consumers that keep server-named classic
  queue declaration, binding, consumption, and recovery on one connection.
- Add bounded consumer runtime recovery with complete generation replacement,
  endpoint and credential rotation, and generation-owned settlement.
- Add broker consumer-cancellation notifications that distinguish matching
  `basic.cancel` events from connection loss and client-initiated shutdown,
  then recover the affected consumer generation.
- Add idempotent consumer pause/resume admission that preserves in-flight
  settlement, bounded buffering, runtime recovery, and readiness semantics.
- Add separate producer and consumer liveness, readiness, and dependency-health
  contracts that distinguish temporary recovery from terminal failure.
- Add bounded low-cardinality producer and consumer observation streams with
  explicit loss reporting and no message, route, credential, or broker text.
- Add leak, fuzz, deterministic concurrency stress, clean-consumer, and local
  wrapper benchmark harnesses with explicit live-broker evidence boundaries.
- Pin RabbitMQ 4.3 compatibility research and queue capability distinctions.
