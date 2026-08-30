# RabbitMQ queue specification decisions

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
- **Selected behavior:** Preserve opaque body bytes, bounded string, boolean, byte, uint16, and uint32 field-table values, normalize unsigned integers to non-negative int64, preserve whole-second timestamps, and distinguish omitted expiration from explicit zero while rejecting unsupported or over-limit metadata.
- **Rationale:** A small explicit mapping prevents silent precision, presence, type, and allocation changes across Go and PHP clients.
- **Security consequences:** Header counts, names, values, body bytes, and broker-owned metadata are bounded before allocation or exposure.
- **Resource consequences:** The package applies fixed maximums and caller-reducible limits to every mapped field.
- **Compatibility consequences:** Adding a field-table type or changing integer, timestamp, or expiration representation requires a compatibility review.
- **Wire consequences:** Supported metadata and body bytes round-trip without coercion; sub-second timestamps and unsupported field-table shapes are rejected before publication.
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
- **Security consequences:** Broker return text and package correlation metadata are not exposed as application-controlled diagnostics.
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
- **Selected behavior:** Use per-consumer manual acknowledgement, settle only after handler completion, preserve explicit ack, nack, reject, and delegate choices, and cap requeue using RabbitMQ delivery counters before applying the configured terminal failure policy.
- **Rationale:** Post-handler settlement provides at-least-once processing without an unbounded poison-message loop.
- **Security consequences:** Malformed broker counters and settlement metadata fail closed without exposing broker text.
- **Resource consequences:** Prefetch, concurrency, handler duration, requeue attempts, and settlement waits are bounded.
- **Compatibility consequences:** Handlers must be idempotent and must not assume that connection loss prevents concurrent redelivery.
- **Wire consequences:** The consumer emits one Basic.Ack, Basic.Nack, or Basic.Reject for a tracked generation delivery, or leaves it to channel closure only when explicitly delegated.
- **Executable evidence:** `TestConsumerConfiguresManualQOSAndAcknowledgesAfterHandler`; `TestConsumerBoundsRequeueAndRejectsInvalidDelivery`; `TestSettlementPolicyPreventsUnboundedRequeue`.
- **Fuzz evidence:** `FuzzDeliveryConversion`.
- **Public APIs and documentation:** `Consumer`; `Delivery`; `Settlement`; `FailurePolicy`; `docs/specification-decisions.md`.
- **Upstream status:** AMQP defines settlement methods; retry budgets and handler ownership remain application policy.
- **Reconsider when:** RabbitMQ changes delivery counter semantics or a supported settlement mode can prove a stronger bounded guarantee.

## RABBITMQ-QUEUE-DEC-005: Queue capabilities and topology mutation are explicit

- **Status and owner:** `resolved`; `go-rabbitmq-queues maintainers`.
- **Classification and scope:** `interoperability policy`; `defensive`.
- **Specification and authority:** `RabbitMQ 4.3`, version `RabbitMQ 4.3`, authority `rabbitmq43-queues`, [source](https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/queues.md), `Queue Types, Declaration and Property Equivalence, and Temporary Queues`; requirement strength `not specified`.
- **Additional authority:** `rabbitmq43-quorum`, version `RabbitMQ 4.3`, [source](https://raw.githubusercontent.com/rabbitmq/rabbitmq-website/1c99c9687f012ad700385c1e9a6990f6520720d3/versioned_docs/version-4.3/quorum-queues/index.md), covers `RabbitMQ 4.3`.
- **Issue:** RabbitMQ classic and quorum queues support different declaration arguments, defaults, and lifecycle constraints, while active declaration can mutate broker topology.
- **Interpretations:** `Pass every option to every queue type`; `Rely on RabbitMQ defaults and active declaration`; `Validate queue-specific capabilities and require explicit authority before topology mutation`.
- **Peer behavior:** RabbitMQ 4.3 rejects or ignores incompatible queue arguments depending on the feature; ordinary AMQP clients expose declarations without package-level environment policy.
- **Selected behavior:** Require an explicit classic or quorum queue type, validate only type-supported arguments, preserve omitted versus explicit quorum delivery limits, use passive topology by default, and permit declarations and bindings only under an explicit development topology capability.
- **Rationale:** Preflight capability checks and explicit mutation authority prevent silent broker defaults, ignored policy, and production topology drift.
- **Security consequences:** Application configuration cannot silently gain topology mutation authority or create unsupported queue policies.
- **Resource consequences:** Queue names, TTLs, lengths, delivery limits, timeouts, retry delays, and topology collections are bounded before broker calls.
- **Compatibility consequences:** New RabbitMQ queue types or arguments require an explicit capability and migration decision rather than generic pass-through.
- **Wire consequences:** Passive checks preserve broker state; authorized development mode emits declarations with deterministic type-specific argument presence.
- **Executable evidence:** `TestQueuePolicyModelsQueueTypeCapabilities`; `TestQueuePolicyDistinguishesOmittedAndExplicitQuorumDeliveryLimit`; `TestTopologyMutationRequiresExplicitDevelopmentPermit`.
- **Fixture and fuzz evidence:** `testdata/operator/topology.yaml`; `FuzzTopologyValidation`.
- **Public APIs and documentation:** `Queue`; `QueuePolicy`; `Topology`; `TopologyPolicy`; `ApplyTopology`; `docs/specification-decisions.md`.
- **Upstream status:** RabbitMQ 4.3 documents queue-type property equivalence and feature differences; mutation authority remains package policy.
- **Reconsider when:** RabbitMQ changes classic or quorum declaration equivalence, adds a supported queue type, or standardizes a safe topology ownership protocol.
