// Package rabbitmqqueue provides RabbitMQ-native AMQP 0-9-1 classic/quorum
// queue policy. OpenProducer and OpenConsumer own independent verified-TLS
// connections and runtime goroutines; their open context bounds startup only.
// Call Shutdown to finish the resource lifetime. ApplyTopology uses a separate
// connection for explicit passive verification or permitted development mutation.
// No cgo or live broker is needed to validate policy values.
//
// The package deliberately keeps queue semantics such as exchanges, routing,
// publisher outcomes, manual settlement, queue types, and recovery visible. It
// does not provide a backend-neutral queue abstraction and does not claim
// exactly-once processing. Application effects, idempotency, broker authorization,
// infrastructure policies and trusted callback cooperation remain caller-owned.
// Received messages are decoded by amqp091-go before wrapper limits apply;
// these limits do not protect against malicious broker framing or allocation.
//
// Publisher confirms do not acknowledge consumer processing, and local manual
// settlement-send success is not broker confirmation. Connection loss can cause
// ambiguous publication and concurrent duplicate handling. Sources of retained
// secrets and printable broker return text require caller diagnostic discipline.
// See docs/guarantees.md, docs/security-threat-model.md and the AMQP 0-9-1 /
// RabbitMQ 4.3 interpretations DEC-001 through DEC-005 in
// docs/specification-decisions.md; these are policy, not complete compliance.
package rabbitmqqueue
