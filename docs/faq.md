# Frequently asked questions

## Does shutdown guarantee that every published message was accepted?

No. A producer shutdown forced by cancellation or deadline expiry makes any
publication already transmitted without a terminal broker confirmation
ambiguous. Reconcile those outcomes with application-owned idempotency and
message identifiers before retrying.

## Does consumer shutdown provide exactly-once processing?

No. Shutdown stops new handler admission, drains handlers already admitted
within the configured bound, and closes package-owned AMQP resources. Messages
that were delivered but not admitted to a handler remain unsettled for broker
redelivery. Applications remain responsible for idempotent processing.

## Who owns topology and credentials?

Applications own configuration, broker authorization, credential-provider
state, and the decision to apply topology. The package owns only the bounded
connection-attempt snapshots and AMQP resources it successfully opens.

## Should an application use this module or `go-queue`?

Use this module for RabbitMQ-native publishing, consumption, recovery,
settlement, and topology behavior. A published version of
`github.com/faustbrian/go-queue/adapters/rabbitmq` is the target-oriented choice
when an application must preserve the backend-neutral `go-queue` worker
contract. If no successor version is yet cleanly resolvable, use the deprecated
`github.com/faustbrian/go-queue/rabbitmq` compatibility facade instead of a
local replacement.
