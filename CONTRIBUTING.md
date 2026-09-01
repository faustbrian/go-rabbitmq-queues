# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected module's documentation.
2. Run `make inventory` and the narrow baseline gate for the module.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and generated or corpus provenance.

Before changing message mapping, publication outcomes, settlement, queue
policy, topology, or another protocol-facing behavior, review the
[specification decision register](docs/specification-decisions.md). Update the
register, conformance manifests, decision history, compatibility notes, and
changelog together when a decision changes. Superseded decisions remain in
the register and ledger with a replacement link.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Specification behavior requires a decision record,
fixture coverage, and interoperability evidence.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain).
Package-local update bots are forbidden; repository policy owns every module
and action update.

Required mutation gates must finish with zero surviving viable mutants.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Run during development:

```bash
make inventory
make check
```

Before submitting a repository-wide change:

```bash
make ci
```

The full scheduled and release gate is `make ci`. Broker- and PHP-backed suites
remain CI-owned as documented in [live broker testing](docs/live-broker-testing.md).
Report every unavailable or failing command; do not describe partial results as
release-ready.

## Adding A Module

Follow the [repository structure policy](AGENTS.md#repository-structure). New
modules require an explicit purpose, ownership boundary, dependency review,
package catalog entry, full quality gates, documentation, changelog, license,
security policy, compatibility plan, and release dry-run.
