# Changelog

## Unreleased

### Documentation

- Organize detailed guarantees behind a concise package overview and
  documentation index.
- Document the verified `go-queue/rabbitmq` parity gaps, adapter prerequisites,
  staged adoption, and rollback gate.
- Record adjacent-library ownership, API-to-Bill and Track migration
  requirements, evidence limitations, and application adoption gates.
- Record retained CI evidence for a 91-second complete three-node RabbitMQ
  outage, sustained client health, recovery, and message reconciliation.
- Record retained CI evidence for the 1M, 10M, and 100M messages-per-day
  three-node quorum profiles under leader loss.

### Added

- Expose bounded delivery settlement results so compatibility adapters can
  report broker acknowledgement failures synchronously.
- Add a CI-only RabbitMQ 4.3.5 TLS fixture that provisions least-scope
  credentials and exercises the public single-node live-broker contract.
- Add broker-backed authorization-denial and classic/quorum dead-letter
  evidence to the CI-only single-node fixture.
- Add three-node application rolling-deployment evidence with an explicit
  drained old-to-new consumer handoff and countable message outcomes.
- Add an opt-in, externally provisioned live-broker evidence harness for TLS,
  classic/quorum routing, confirms, mandatory returns, and bounded settlement.
- Add an externally coordinated three-node interruption harness with countable
  confirmation, ambiguity, delivery, and duplicate outcomes.
- Add CI-hosted three-node RabbitMQ patch rolling-upgrade evidence with
  quorum-safety, client recovery, and message-accounting gates.
- Add a CI-hosted 90-second complete-cluster outage gate for sustained health,
  recovery, and message-accounting evidence.
- Add an externally provisioned four-queue performance harness for the 1M,
  10M, and 100M messages-per-day steady and burst profiles.
- Add a pinned PHP AMQP runner and opt-in bidirectional live-broker harness for
  corpus mapping, confirms, mandatory returns, and manual settlement.
- Add pinned RabbitMQ Operator manifests and strict CRD-schema validation for
  the operator-owned cluster, topology, identity, and permission boundary.
- Reject sub-second publication timestamps that AMQP would silently truncate.
- Add bounded connection, credential, verified TLS, and recovery policy types.
- Make configurable message limits reducible only, preserving package-wide
  allocation caps and AMQP short-string bounds.
- Apply reduced name limits to consumer, queue, and transient-exchange
  identities before broker intake begins.
- Add explicit classic/quorum queue and passive/development topology policy.
- Add bounded declaration-equivalent queue TTL, expiry, length, overflow, and
  dead-letter policy with explicit RabbitMQ 4.3 queue-type restrictions.
- Distinguish an omitted quorum delivery limit from explicit bounded values,
  including zero, without exposing RabbitMQ's unlimited compatibility mode.
- Add quorum-only delivery-acknowledgement timeout policy with RabbitMQ 4.3
  non-negative millisecond values and declaration-equivalence validation.
- Add quorum-only disconnected-consumer timeout policy with explicit RabbitMQ
  4.3 partition-recovery bounds and declaration-equivalence validation.
- Add quorum-only delayed-retry policy with bounded RabbitMQ 4.3 linear-backoff
  modes and declaration-equivalence validation.
- Add bounded passive exchange and queue equivalence checks plus explicit
  development-only binding declaration without mutating production bindings.
- Reject exclusive queues from detached topology operations that cannot retain
  the declaring connection required for their lifetime.
- Classify a broker `RESOURCE_LOCKED` response during detached inspection as
  exclusive-name topology drift instead of retrying it as an outage.
- Validate RabbitMQ 4.3 headers-exchange match modes and reject bindings whose
  criteria RabbitMQ would ignore, producing an unintended match-all route.
- Add a language-neutral AMQP message corpus for byte-preserving interoperability
  checks without claiming broker or PHP evidence.
- Preserve bounded string application-header contents across publication and
  delivery without treating control characters as identity data.
- Normalize lossless AMQP unsigned byte, uint16, and uint32 delivery headers
  into the stable signed int64 application-header policy.
- Reject malformed or oversized reserved publish-correlation metadata on
  delivery instead of silently bypassing header bounds.
- Add bounded AMQP publication metadata and distinct publisher outcome states.
- Add exchange-kind-aware publication routing so every built-in exchange
  preserves its native bounded routing-key semantics, including empty keys.
- Preserve native empty routing keys, including bounded `x-death` history, when
  consuming from any built-in exchange.
- Preserve bounded `x-death.original-expiration` metadata, including explicit
  zero, after RabbitMQ removes a per-message TTL during dead lettering.
- Distinguish omitted per-message expiration from RabbitMQ's explicit zero TTL
  across publication, delivery snapshots, and interoperability fixtures.
- Reject package- and RabbitMQ-owned delivery metadata on publication, and
  bound and hide redundant broker death summaries from application headers.
- Represent RabbitMQ's predeclared default direct exchange through an explicit
  direct kind and empty exchange identity for queue and reply routing.
- Add bounded AMQP reply-to publication metadata for application-owned
  request/reply flows.
- Add bounded exact correlation for confirmations, mandatory returns, late
  events, and ambiguous channel-generation failure.
- Bind mandatory-return route details to the exact validated publication rather
  than exposing broker-supplied exchange or routing-key metadata.
- Treat mandatory returns without an active exact token as generation failures
  so a following positive confirm cannot report an unroutable publish accepted.
- Add an independent synchronous producer with mandatory routing, publisher
  confirms, endpoint rotation, credential refresh, verified TLS, bounded
  startup retry, graceful close, and explicit ambiguous outcomes.
- Add bounded asynchronous publishing and prevalidated, ordered non-atomic
  batches with independent per-item outcomes.
- Add bounded producer runtime recovery with fresh confirm generations,
  endpoint and credential rotation, and sanitized connection-blocked state.
- Preserve terminal producer-generation cleanup failures across idempotent
  close calls without carrying them into a recovered generation.
- Add an independent bounded consumer with manual ACK, NACK, reject and
  delegated settlement, owned delivery snapshots, dead-letter history,
  per-consumer QoS, explicit failure policy, and graceful drain/close.
- Ensure graceful drain admits and settles deliveries already buffered by the
  consumer, including while paused, before leaving healthy resources open, and
  close the generation when settlement fails or remains delegated during
  shutdown.
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
