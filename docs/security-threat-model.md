# Security threat model: go-rabbitmq-queues

**Model version:** 1.0, 2026-10-08.

**Runtime source baseline:** `b3dc3f587aaa9c408039ec1f3d0de6352d48e463`.
**Scope:** root module `github.com/faustbrian/go-rabbitmq-queues`, using
`github.com/rabbitmq/amqp091-go v1.14.0`. This is a source-level model, not a
deployment certification or approval of a new public release.

## Assets, attacker inputs and authority

Protect credentials, TLS material, message confidentiality, publication and
settlement integrity, bounded memory/work and connection lifetimes. Publishers
may supply malformed or oversized payloads, headers and routing properties;
connections can fail or delay protocol replies; broker redelivery can duplicate
effects. The authenticated broker, application credential providers/handlers,
explicit topology configuration and deployment admission policies have separate
trust and ownership obligations.

Network access occurs only through explicit open/apply operations and supplied
endpoints. TLS verifies certificates and forbids insecure verification. Endpoint
selection, broker permissions and deployment network restrictions remain caller
controlled; TLS authentication is not authorization or protection against a
compromised broker. The package performs no custom cryptography.

## Boundary and control matrix

| Boundary | Owned control | Evidence and remaining obligation |
| --- | --- | --- |
| Endpoint and TLS configuration | Validated structured endpoints, verified TLS and explicit connection/credential bounds. | `config.go`, `producer_open.go`; callers own permitted destinations, broker permissions, roots and credential rotation. |
| AMQP channel startup and recovery | Attempt context and dial deadline close the owned connection to release the native channel RPC; cancellation cleanup is stopped or joined before transfer. | `channel_open.go`, `channel_open_deadline_test.go`; finite in-memory regressions do not establish live broker behavior. |
| Incoming application headers | Check supported types, count and cumulative key/value bytes before key storage or byte copying; valid output remains sorted and owned. | `delivery.go`, `allocation_admission_test.go`; reserved metadata has separate validators and can reject after bounded application-header copying. |
| Consumer configuration | Validate before copying transient binding arguments; retain owned snapshots only after admission. | `consumer_open.go`, `consumer.go`, `allocation_admission_test.go`; callers must not concurrently mutate borrowed inputs during construction. |
| Message ingress | Explicit payload, header, name and routing limits reject unsupported representations. | `message.go`, `delivery.go`; native AMQP decoding precedes these wrapper limits. Broker message-size/admission controls remain necessary. |
| Publication and retry | Bounded outstanding work, explicit confirms/returns, generation failure and ambiguous outcomes; no automatic replay of uncertain sends. | `producer.go`, `publish_tracker.go`, producer tests and specification decisions; applications reconcile uncertainty and provide idempotency. |
| Delivery and settlement | Bounded prefetch/concurrency/requeue policy, generation-specific settlement and explicit shutdown ownership. | `consumer.go`, `settlement.go` and consumer tests; handlers must honor cancellation and tolerate at-least-once redelivery. |
| Topology | Production uses passive checks; active declarations require an explicit development permit and are not transactional. | `topology.go`, `topology_apply.go`; bindings and effective operator policies require infrastructure evidence. |
| Diagnostics | Public errors are sanitized; observation categories exclude credentials, payloads, headers, routes and identifiers, with bounded best-effort delivery. | Error, producer, consumer and observation tests; caller logging and callbacks remain application-owned. |
| Dependencies and publication | Pinned Go dependencies and immutable CI/tool inputs, with package-owned release boundaries. | `go.mod`, workflow and release configuration; executed security gates, signed artifacts and actual public consumers must be checked for the candidate. |

## Residual obligations and review conditions

These are tracked trust-boundary obligations, not waivers of a known material
finding or substitutes for the original security qualification gates.

| Obligation | Owner and rationale | Mitigation and review condition |
| --- | --- | --- |
| Native decoding before wrapper limits | Broker/deployment owner and transport maintainers; the wrapper receives already decoded AMQP values. | Enforce broker ingress/message limits and review pinned decoder/frame behavior before security qualification; revisit on native dependency or admission changes. |
| Cooperative application callbacks | Application owner; credential providers and handlers are trusted caller code, not forcibly terminable routines. | Honor bounded contexts, constrain external work and redact diagnostics; revisit when callbacks or their dependencies change. |
| Duplicate or ambiguous effects | Application and broker operator; confirmation, handler effects and settlement are separate operations. | Use stable message identity, durable idempotency/reconciliation and an explicit poison-message policy; revisit on retry, dead-letter or persistence changes. |
| Secret retention | Credential/application owner; copied passwords are wiped best-effort, but Go strings and provider aliases cannot be guaranteed to be fully erased. | Limit credential lifetime and access, rotate secrets and protect dumps/logs; revisit on provider, telemetry or incident tooling changes. |
| Effective topology and authorization | Infrastructure owner; passive AMQP cannot prove bindings or application permissions. | Verify broker policies, least privilege and bindings independently; revisit on operator/configuration changes. |
| Supply-chain compromise | Repository and dependency maintainers; source review does not prove published artifact identity. | Verify immutable source, required gates, signatures/checksums and clean public resolution; revisit on tool, dependency, action or release changes. |

### Pinned AMQP decoder assessment

The assessed native dependency is `amqp091-go v1.14.0`, upstream commit
`387d77a50ea8b8c38705bb18cc80f5d6599a8477`. Its
[frame reader](https://github.com/rabbitmq/amqp091-go/blob/387d77a50ea8b8c38705bb18cc80f5d6599a8477/read.go)
checks the outer frame size, but long strings and byte arrays allocate from
declared lengths before checking available bytes. Table and array decoding has
no explicit nesting or entry budget. Its
[content receiver](https://github.com/rabbitmq/amqp091-go/blob/387d77a50ea8b8c38705bb18cc80f5d6599a8477/channel.go)
assembles the complete message before wrapper payload admission; a frame limit
does not impose a total-body limit. The wrapper leaves client `FrameSize` zero,
so framing depends on the authenticated broker's negotiated limit, which can
also be unlimited. Cancellation cannot undo an allocation already attempted.

Broker and deployment owners must ensure well-formed, resource-bounded protocol
output, finite framing, and individual-message and metadata admission before
delivery to this client. Admission must match the configured consumer payload,
header count and byte budgets and separately bound nested protocol metadata.
Queue length and prefetch alone do not supply these per-message controls.
Process memory limits are containment, not proof of safe decoder admission.

This is an explicit trusted-broker boundary, not protection against malicious
or compromised broker output. Hostile-broker protection would require native
remaining-byte checks before allocation, total-body admission and nesting/entry
limits; wrapper checks and startup cancellation cannot provide that guarantee.
Transport maintainers own reassessment on native decoder changes; deployment
owners own admission verification before using this package. No deployment
compliance or hostile-decoder resistance is certified here.

### Affected versions and remediation disposition

Published root versions `v1.0.0` (source
`59b21563c1f36f3f5cd201c79903913a04fd10e3`) and `v1.1.0` (source
`5dc01b54e9a1a3b2e70d8647de1cfd1676f34241`) contain the corrected availability
and resource-admission defects: channel startup lacks the new cancellation
ownership boundary, delivery headers allocate before complete admission, and
consumer configuration is copied before validation. These are defects in owned
wrapper behavior, separate from the trusted native decoder obligations above.

Maintainers plan the compatible `v1.1.1` remediation release from main. Upgrade
guidance applies only after that release is published and verified; until then,
neither published version is represented as containing these corrections.
Applications must still apply broker admission and cooperative callback controls
after upgrading. The repository maintainers own the remediation notice and
release qualification; no hosted security advisory identifier or deployment
remediation is asserted by this document.

## Findings and release verdict

The stated source includes corrections for channel-open lifetime enforcement,
incoming-header preallocation admission and configuration-copy ordering. Focused
regressions and independent source review are recorded in the coordination
ledger. They do not certify the released `v1.1.0` source or older versions.

**Root module: security qualification pending; no new release approved by this
document.** Required candidate CI/security and native integration evidence,
the source assessments above, final release qualification,
planned `v1.1.1` publication and actual clean public consumption remain
release boundaries. The two declared `go-queue` RabbitMQ consumers must select
and verify the corrected public module before their adoption is credited.

Report vulnerabilities through the [security policy](../SECURITY.md). Revisit
this model when protocol, lifetime, input limits, retry/settlement, dependency,
callback, consumer or release contracts change.
