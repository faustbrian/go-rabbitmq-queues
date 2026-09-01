# Specification conformance matrix

The module owns a bounded AMQP 0-9-1 and RabbitMQ 4.3 policy surface. It does
not claim to implement the complete AMQP protocol or every RabbitMQ feature.
Exact source and change-authority digests are recorded in `monitoring.json` and
`sources.lock.json`.

The canonical
[specification decision register](../docs/specification-decisions.md) records
the interpretation, consequences, evidence, and reconsideration condition for
each observable decision.

| Decision | Authority | Executable evidence | Peer or fixture evidence |
| --- | --- | --- | --- |
| RABBITMQ-QUEUE-DEC-001 | AMQP 0-9-1 message and field-table types | `TestLanguageNeutralInteroperabilityFixturePreservesAMQPMetadataAndBytes`, `TestDeliveryConversionOwnsBoundedMetadataAndDeadLetterHistory` | Language-neutral fixture and pinned php-amqplib v3.7.4 |
| RABBITMQ-QUEUE-DEC-002 | RabbitMQ 4.3 publisher confirms | `TestPublishTrackerReconcilesExactConfirmAndMandatoryReturn`, `TestProducerReconcilesReturnBeforePositiveConfirm` | Language-neutral fixture and pinned php-amqplib v3.7.4 |
| RABBITMQ-QUEUE-DEC-003 | RabbitMQ 4.3 publisher confirms | `TestProducerKeepsPostTransmissionCancellationAmbiguous`, `TestProducerPreservesDefinitiveOutcomeObservedBeforePublishError`, `TestPublishTrackerBoundsOutstandingAndFailsGeneration` | Not assessed |
| RABBITMQ-QUEUE-DEC-004 | AMQP 0-9-1 consumer settlement | `TestConsumerConfiguresManualQOSAndAcknowledgesAfterHandler`, `TestConsumerBoundsRequeueAndRejectsInvalidDelivery`, `TestSettlementPolicyPreventsUnboundedRequeue` | Not assessed |
| RABBITMQ-QUEUE-DEC-005 | RabbitMQ 4.3 queue and quorum guides | `TestQueuePolicyModelsQueueTypeCapabilities`, `TestQueuePolicyDistinguishesOmittedAndExplicitQuorumDeliveryLimit`, `TestTopologyMutationRequiresExplicitDevelopmentPermit` | Pinned Operator topology fixture |

Use the archived `go-library-tools` source revision recorded by CI to run the
offline specification check. Review monitored source changes before updating
their digests; a digest change alone does not authorize a behavior change.
