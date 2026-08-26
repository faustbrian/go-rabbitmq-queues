# Changelog

## Unreleased

### Added

- Add bounded connection, credential, verified TLS, and recovery policy types.
- Add explicit classic/quorum queue and passive/development topology policy.
- Add bounded AMQP publication metadata and distinct publisher outcome states.
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
- Add bounded consumer runtime recovery with complete generation replacement,
  endpoint and credential rotation, and generation-owned settlement.
- Add separate producer and consumer liveness, readiness, and dependency-health
  contracts that distinguish temporary recovery from terminal failure.
- Add bounded low-cardinality producer and consumer observation streams with
  explicit loss reporting and no message, route, credential, or broker text.
- Pin RabbitMQ 4.3 compatibility research and queue capability distinctions.
