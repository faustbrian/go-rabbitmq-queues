# Contributing

Changes should remain inside the package's explicit AMQP and RabbitMQ policy
boundaries and include focused evidence for every observable contract they
alter.

Before changing message mapping, publication outcomes, settlement, queue
policy, topology, or another protocol-facing behavior, review the
[specification decision register](docs/specification-decisions.md). Update the
register, conformance manifests, decision history, compatibility notes, and
changelog together when a decision changes. Superseded decisions remain in
the register and ledger with a replacement link.

Run `make check` for the complete local contract. Broker- and PHP-backed suites
remain CI-owned as documented in [live broker testing](docs/live-broker-testing.md).
