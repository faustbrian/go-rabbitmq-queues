# Kubernetes Operator compatibility

The operator fixture maps the package's operator-owned topology boundary onto
the pinned RabbitMQ Cluster Operator v2.22.5 and Messaging Topology Operator
v1.20.2 CRDs. It defines a three-node TLS-only RabbitMQ 4.3.5 arm64 cluster,
an isolated vhost, durable direct exchange, explicit classic and quorum
queues, bindings, a bounded quorum delivery-limit policy, a generated client
identity, and least-scope permissions for passive verification, publishing,
consumption, and settlement. The base classic queue dead-letters rejected and
overflowed messages. The base quorum queue uses the RabbitMQ 4.3 at-least-once
dead-letter policy prerequisites. Both route to a dedicated retained quorum
dead-letter queue rather than dropping bounded failures. Dedicated classic
PHP-interoperability queues and four-queue classic and quorum performance
profiles use unique bindings so the opt-in live suites can target the same
operator-owned exchange without sharing consumer queues.

RabbitMQ 4.3 permits passive declarations when the identity has any applicable
resource permission, so the generated client has no configure permission:
write access covers the exchange check and publishing, while read access covers
source and retained dead-letter queue checks, consumption, and settlement. The
identity has no write permission for the dead-letter exchange. This prevents
the evidence client from actively declaring or mutating operator-owned
topology while allowing the live harness to acknowledge retained dead letters.

Fresh RabbitMQ 4.3 nodes enable stable feature flags, including the
`stream_queue` prerequisite for at-least-once dead lettering. The fixture does
not automatically enable future feature flags because that can make downgrade
rollback impossible; a live gate must record the effective flag state and an
upgrade must enable new flags only after its rollback checkpoint permits it.

The fixture is split between
[`cluster.yaml`](../testdata/operator/cluster.yaml) and
the topology files under [`testdata/operator`](../testdata/operator). It
contains no credentials, certificate material, application payloads, or
environment-owned storage class. The referenced TLS secrets, namespace,
storage class, capacity,
availability zones, ingress, network policies, backup policy, and monitoring
must be supplied and reviewed by the deploying environment. The arm64 image
digest and node selector deliberately match the recorded compatibility pin;
another architecture requires a separately pinned image digest and evidence.
The server secret must contain `tls.crt` and `tls.key`, the CA secret must
contain `ca.crt`, and the Messaging Topology Operator deployment must be
configured to trust that CA before reconciling these topology resources.

The User controller generates credentials and records their Secret reference
in `User.status.credentials`. An evidence runner may copy those values into its
permission-restricted live configuration without printing or committing them;
the manifest does not contain a static username or password.

Queues and the vhost use `deletionPolicy: retain`. The Exchange CRD has no
equivalent retention field, so deleting these custom resources is not an
ordinary rollback procedure. Retain the topology manifests and verify broker
objects and bindings before any operator-resource deletion.

## Structural gate

Run the repository-owned validator from the repository root:

```sh
./scripts/validate-operator-fixtures.sh
```

The script downloads only the three pinned source revisions into a task-owned
temporary directory, verifies each Git revision, builds kubeconform v0.8.0 with
task-owned Go caches, converts the exact pinned CRDs to local JSON schemas, and
validates every fixture document in strict mode. It removes all downloaded
sources, Python environment, generated schemas, binaries, and Go caches after
success or failure. Python 3 with venv support, Git, Go 1.27, and network
access are prerequisites; PyYAML 6.0.3 is installed only inside the task-owned
environment.

A passing structural gate proves that the fixture is accepted by the pinned
CRD schemas. It does not prove controller reconciliation, generated
StatefulSet behavior, Kubernetes scheduling, TLS issuance, effective RabbitMQ
policy, binding existence, broker health, rolling upgrades, failover, or live
client connectivity. Those claims require a retained run on a versioned
Kubernetes cluster with both operators installed, every CR reporting `Ready`,
the effective broker topology recorded, and the live broker suites passing.
