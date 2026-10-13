# RabbitMQ queue specification decisions

[Documentation index](README.md) · [Repository entry](../README.md)

This register makes the module's observable AMQP 0-9-1 and RabbitMQ 4.3
interpretations explicit. Normative protocol text and pinned RabbitMQ 4.3
documentation outrank examples and client behavior. Maintained-peer evidence
is informative and cannot weaken a protocol requirement.

## RABBITMQ-QUEUE-DEC-001: Bounded lossless AMQP message mapping

- **Status and owner:** `resolved`; `go-rabbitmq-queues maintainers`.
- **Classification and scope:** `interoperability policy`; `defensive`.
- **Specification and authority:** `AMQP 0-9-1`, version `AMQP 0-9-1`, authority `amqp091-protocol`, [source](https://raw.githubusercontent.com/rabbitmq/amqp-0.9.1-spec/b8e975a762b8677263ebbbba1e70654b5263af81/docs/amqp-0-9-1-protocol.md), `Sections 4.2.5, 4.2.6, and basic content properties`; requirement strength `not specified`.
- **Issue:** AMQP field tables and basic properties expose more wire representations than the public language-neutral message model can preserve safely.
- **Interpretations:** `Expose every client-library value`; `Coerce every value into strings`; `Preserve one bounded language-neutral subset and reject unsupported representations`.
- **Peer behavior:** The pinned php-amqplib v3.7.4 live-broker harness exchanges the same fixture in both directions; unsupported nested field-table values remain outside the shared corpus.
- **Selected behavior:** Preserve opaque body bytes and a bounded public string, boolean, signed int64 and byte-string header subset; inbound int8/int16/int32/int64 and uint8/uint16/uint32 values map to int64. Publication timestamps require non-negative whole seconds; omitted expiration remains distinct from explicit zero. Unsupported or over-limit metadata is rejected.
- **Rationale:** A small explicit mapping prevents silent precision, presence, type, and allocation changes across Go and PHP clients.
- **Security consequences:** Wrapper application-header mapping checks supported types, count and cumulative bytes before allocating owned snapshots. Native AMQP decoding precedes wrapper admission; reserved broker metadata has separate bounded validators. These checks do not establish hostile-broker decoder protection.
- **Resource consequences:** The package applies fixed maximums and caller-reducible limits to every mapped field.
- **Compatibility consequences:** Adding a field-table type or changing integer, timestamp, or expiration representation requires a compatibility review.
- **Wire consequences:** Supported public values and opaque body bytes are preserved; accepted inbound integer widths normalize to int64 rather than preserving their wire tags. Publication rejects sub-second timestamps and unsupported field-table shapes.
- **Executable evidence:** `TestLanguageNeutralInteroperabilityFixturePreservesAMQPMetadataAndBytes`; `TestDeliveryConversionOwnsBoundedMetadataAndDeadLetterHistory`.
- **Fixture, fuzz, and interoperability evidence:** `testdata/interoperability/message-v1.json`; `FuzzPublicationValidation`; `FuzzDeliveryConversion`; `testdata/interoperability/php/composer.lock`; `live_interoperability_test.go`; `scripts/ci-live-broker.sh`.
- **Public APIs and documentation:** `Message`; `Header`; `Publication`; `Delivery`; `docs/specification-decisions.md`.
- **Upstream status:** AMQP 0-9-1 defines the wire types; the narrower public mapping is package policy.
- **Reconsider when:** A supported peer or application requires another AMQP field-table type without weakening deterministic bounds.

## RABBITMQ-QUEUE-DEC-002: Publisher confirms and mandatory returns form one outcome

- **Status and owner:** `resolved`; `go-rabbitmq-queues maintainers`.
- **Classification and scope:** `optional behavior`; `transport-specific`.
- **Specification and authority:** `RabbitMQ 4.3`, version `RabbitMQ 4.3`, authority `rabbitmq43-confirms`, [source](https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/confirms.md), `Publisher Confirms and Publishing Messages`; requirement strength `not specified`.
- **Issue:** A mandatory unroutable publication can receive both a basic.return and a positive publisher confirmation, and callback order does not by itself define one application outcome.
- **Interpretations:** `Treat the positive confirmation as acceptance`; `Treat the first callback as final`; `Correlate both callbacks to the exact publication before reporting one result`.
- **Peer behavior:** The pinned php-amqplib v3.7.4 harness confirms routed publications through RabbitMQ 4.3, while the Go live-broker suite separately proves mandatory-return reconciliation.
- **Selected behavior:** Enable publisher confirms, attach an exact private correlation token, reconcile mandatory returns before a positive confirmation, and report an unroutable publication as returned rather than confirmed.
- **Rationale:** A broker acknowledgement of processing is not evidence that a mandatory message reached a queue.
- **Security consequences:** Package correlation metadata is stripped from public deliveries, and return routes come from the registered publication. Return.Reason retains broker text only when at most 255 bytes and without ASCII controls; this is bounded text, not secret redaction or authenticated diagnostic content.
- **Resource consequences:** Outstanding correlations are bounded by MaxOutstanding and released on every terminal generation path.
- **Compatibility consequences:** Applications can distinguish confirmed, returned, rejected, not-sent, and ambiguous outcomes without callback-order races.
- **Wire consequences:** Mandatory publication uses RabbitMQ confirms plus a package-owned header that is stripped from deliveries.
- **Executable evidence:** `TestPublishTrackerReconcilesExactConfirmAndMandatoryReturn`; `TestProducerReconcilesReturnBeforePositiveConfirm`.
- **Fixture, fuzz, and interoperability evidence:** `testdata/interoperability/message-v1.json`; `FuzzPublishTrackerCorrelation`; `testdata/interoperability/php/composer.lock`; `live_interoperability_test.go`; `scripts/ci-live-broker.sh`.
- **Public APIs and documentation:** `Producer.Publish`; `Producer.PublishAsync`; `PublishResult`; `Return`; `docs/specification-decisions.md`.
- **Upstream status:** RabbitMQ documents confirms and mandatory returns as related but distinct protocol effects.
- **Reconsider when:** RabbitMQ introduces a single authoritative routed-publication outcome or changes confirm and return ordering guarantees.

## RABBITMQ-QUEUE-DEC-003: Post-transmission failure remains ambiguous

- **Status and owner:** `resolved`; `go-rabbitmq-queues maintainers`.
- **Classification and scope:** `omission`; `defensive`.
- **Specification and authority:** `RabbitMQ 4.3`, version `RabbitMQ 4.3`, authority `rabbitmq43-confirms`, [source](https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/confirms.md), `Publisher Confirms and When Will Published Messages Be Confirmed`; requirement strength `not specified`.
- **Issue:** Connection loss or context cancellation after client transmission can hide whether RabbitMQ accepted the publication.
- **Interpretations:** `Report every local error as not sent`; `Assume every transmitted message was accepted`; `Preserve an explicit ambiguous outcome until application reconciliation`.
- **Peer behavior:** RabbitMQ clients generally cannot recover a missing publisher confirmation after their channel or connection is lost.
- **Selected behavior:** Report not-sent only before client transmission; after transmission, cancellation, channel loss, and missing terminal confirmation produce PublishAmbiguous unless a definitive return or confirmation was already observed.
- **Rationale:** Retrying an uncertain publication as definitely absent can duplicate an accepted message.
- **Security consequences:** Sanitized ambiguity reporting does not expose broker, endpoint, credential, or payload details.
- **Resource consequences:** Generation failure completes every outstanding attempt once and bounds retained state.
- **Compatibility consequences:** Callers must reconcile or idempotently retry ambiguous outcomes instead of treating them as definite failures.
- **Wire consequences:** The package does not emit a compensating AMQP frame or automatic duplicate publication after an uncertain send.
- **Executable evidence:** `TestProducerKeepsPostTransmissionCancellationAmbiguous`; `TestProducerPreservesDefinitiveOutcomeObservedBeforePublishError`; `TestPublishTrackerBoundsOutstandingAndFailsGeneration`.
- **Fuzz evidence:** `FuzzPublishTrackerCorrelation`.
- **Public APIs and documentation:** `Producer.Publish`; `Producer.PublishAsync`; `PublishAmbiguous`; `docs/specification-decisions.md`.
- **Upstream status:** No protocol method reconstructs an unobserved confirmation after channel loss.
- **Reconsider when:** A supported RabbitMQ protocol extension provides durable publication identity and authoritative outcome recovery.

## RABBITMQ-QUEUE-DEC-004: Manual settlement preserves bounded at-least-once delivery

- **Status and owner:** `resolved`; `go-rabbitmq-queues maintainers`.
- **Classification and scope:** `optional behavior`; `application-policy`.
- **Specification and authority:** `AMQP 0-9-1`, version `AMQP 0-9-1`, authority `amqp091-protocol`, [source](https://raw.githubusercontent.com/rabbitmq/amqp-0.9.1-spec/b8e975a762b8677263ebbbba1e70654b5263af81/docs/amqp-0-9-1-protocol.md), `Basic.Ack, Basic.Reject, Basic.Recover, and consumer acknowledgements`; requirement strength `not specified`.
- **Issue:** AMQP permits automatic or manual acknowledgement and repeated requeue, but neither choice defines a bounded application failure policy.
- **Interpretations:** `Acknowledge on receipt`; `Requeue every failure forever`; `Settle after the handler with an explicit bounded failure policy`.
- **Peer behavior:** Maintained AMQP clients expose manual acknowledgement primitives but leave handler timing and retry limits to applications.
- **Selected behavior:** Use per-consumer manual acknowledgement and settle after handler completion. Handler error, expired context or invalid settlement selects ConsumerConfig.Failure; otherwise retain the explicit ack, nack, reject or delegate choice. Reduce requeue using quorum acquired count first, quorum Reject delivery count when available next, and an initial non-redelivered fallback; reaching the cap clears Requeue without replacing the method or republishing.
- **Rationale:** Explicit post-handler settlement lets applications choose failure/requeue outcomes while retaining responsibility for successful processing and duplicate effects.
- **Security consequences:** Malformed broker counters and settlement metadata fail closed without exposing broker text.
- **Resource consequences:** Prefetch, concurrency, observed-state requeue requests and settlement waits are bounded. Handler contexts require callback cooperation and do not forcibly terminate application code.
- **Compatibility consequences:** Handlers must be idempotent and must not assume that connection loss prevents concurrent redelivery.
- **Wire consequences:** The consumer invokes one single-delivery Basic.Ack, Basic.Nack or Basic.Reject on the originating generation, or delegates to channel closure. Successful local send is not broker confirmation of settlement.
- **Executable evidence:** `TestConsumerConfiguresManualQOSAndAcknowledgesAfterHandler`; `TestConsumerBoundsRequeueAndRejectsInvalidDelivery`; `TestSettlementPolicyPreventsUnboundedRequeue`.
- **Fuzz evidence:** `FuzzDeliveryConversion`.
- **Public APIs and documentation:** `Consumer`; `Delivery`; `Settlement`; `ConsumerConfig`; `docs/specification-decisions.md`.
- **Upstream status:** AMQP defines settlement methods; retry budgets and handler ownership remain application policy.
- **Reconsider when:** RabbitMQ changes delivery counter semantics or a supported settlement mode can prove a stronger bounded guarantee.

## RABBITMQ-QUEUE-DEC-005: Queue capabilities and topology mutation are explicit

- **Status and owner:** `resolved`; `go-rabbitmq-queues maintainers`.
- **Classification and scope:** `interoperability policy`; `defensive`.
- **Specification and authority:** `RabbitMQ 4.3`, version `RabbitMQ 4.3`, authority `rabbitmq43-queues`, [source](https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/queues.md), `Queue Types, Declaration and Property Equivalence, and Temporary Queues`; requirement strength `not specified`.
- **Additional authority:** `rabbitmq43-quorum`, version `RabbitMQ 4.3`, [source](https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/quorum-queues/index.md), covers `RabbitMQ 4.3`.
- **Additional authority record:** `{"id":"rabbitmq43-quorum","version":"RabbitMQ 4.3","url":"https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/quorum-queues/index.md","specifications":["RabbitMQ 4.3"]}`.
- **Issue:** RabbitMQ classic and quorum queues support different declaration arguments, defaults, and lifecycle constraints, while active declaration can mutate broker topology.
- **Interpretations:** `Pass every option to every queue type`; `Rely on RabbitMQ defaults and active declaration`; `Validate queue-specific capabilities and require explicit authority before topology mutation`.
- **Peer behavior:** RabbitMQ 4.3 rejects or ignores incompatible queue arguments depending on the feature; ordinary AMQP clients expose declarations without package-level environment policy.
- **Selected behavior:** Require explicit classic/quorum queue type and topology mode, validate the package-supported arguments and omitted versus explicit delivery limits, request passive broker checks for production, and permit declarations/bindings only under an explicit development topology capability. Passive responses do not independently prove every declaration property, effective policy or binding.
- **Rationale:** Preflight capability checks and explicit mutation authority prevent silent broker defaults, ignored policy, and production topology drift.
- **Security consequences:** Application configuration cannot silently gain topology mutation authority or create unsupported queue policies.
- **Resource consequences:** Queue names, TTLs, lengths, delivery limits, timeouts, retry delays, and topology collections are bounded before broker calls.
- **Compatibility consequences:** New RabbitMQ queue types or arguments require an explicit capability and migration decision rather than generic pass-through.
- **Wire consequences:** Passive checks preserve broker state; authorized development mode emits declarations with deterministic type-specific argument presence.
- **Executable evidence:** `TestQueuePolicyModelsQueueTypeCapabilities`; `TestQueuePolicyDistinguishesOmittedAndExplicitQuorumDeliveryLimit`; `TestTopologyMutationRequiresExplicitDevelopmentPermit`.
- **Fixture and fuzz evidence:** `testdata/operator/topology.yaml`; `FuzzTopologyValidation`.
- **Public APIs and documentation:** `Queue`; `Topology`; `TopologyPolicy`; `ApplyTopology`; `docs/specification-decisions.md`.
- **Upstream status:** RabbitMQ 4.3 documents queue-type property equivalence and feature differences; mutation authority remains package policy.
- **Reconsider when:** RabbitMQ changes classic or quorum declaration equivalence, adds a supported queue type, or standardizes a safe topology ownership protocol.

Choose the next guide in the [documentation index](README.md)
or return to the [repository entry](../README.md) for installation and ownership.
