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

Required release mutation gates must finish with zero surviving viable mutants.

Do not fork shared verification workflows, add permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Use Go 1.27.2; the public module minimum remains Go 1.27.0. An isolated
checkout needs no sibling repositories or coordination goal files. Begin
with deterministic tests and one active target when publication validation
changes, using a disposable cache per bounded session:

```bash
(
  set -eu
  GOCACHE="$(mktemp -d)"
  export GOCACHE
  trap 'rm -rf "${GOCACHE}"' EXIT
  GOWORK=off go test ./...
  GOWORK=off go test . -run '^$' -fuzz '^FuzzPublicationValidation$' -fuzztime=2s -timeout=2m
)
```

Deterministic tests execute fuzz seeds/corpus, not active fuzzing, and do not
qualify live brokers. Install the public v2.4.0 binary archive for your platform
from [go-library-tools releases](https://github.com/faustbrian/go-library-tools/releases/tag/v2.4.0),
checking `checksums.txt` against `.golib.yaml` and the archive against that
checksum set. Put the extracted `golib` binary on `PATH`, or select it through
`GOLIB`; do not build Tools from source. CI uses this public release and checks
out its immutable reusable workflow source.

Run adjacent commands in one disposable-cache session as shown above:

```bash
make inventory
make check
make ci
make specification-check
golib mutation --module . --package .
```

`make check` and `make ci` select the bounded contract. The root race runs
once with a two-minute timeout: 100 concurrent completions keep mandatory
returns correlated with the right confirm, and concurrent shutdown callers
retain independent deadlines. Fake channels do not prove TLS, live AMQP
behavior or PHP interoperability. Base-free checks run seeds/corpus;
changed-source CI selects causal active fuzz smoke. Optional mutation selects
one production package, one worker and a twenty-minute deadline. Offline
specification checking is ordinary; the module has no default service or tag.

PR/push changes outside the explicitly named non-executable policy documents
jointly select the existing single-node TLS and PHP interoperability jobs
(two jobs, each capped at twenty minutes). Executable verifier/configuration,
workflow, source, tests, protocol fixtures and unknown paths remain selected;
missing or invalid Git evidence fails the selector. Policy-only changes keep
mandatory local/workflow/security checks without live fixtures. The selector
has a two-minute bound. Required demands success for selected jobs and exactly
skipped for unselected jobs; missing, failed or cancelled results never pass.

Select full qualification and release rehearsal explicitly:

```bash
make check-full
make specification-check-online
make -f verification/package.mk operator
golib api check
golib release dry-run --module .
```

Full checks retain all four fuzz targets at 10000x/20m, the 100ms/20m benchmark,
operator conformance and language-neutral interoperability operations, plus
all original API/module gates. Workflow dispatch with `full_assurance: true`
or `release_dry_run: true` also selects all unchanged broker/PHP/performance/
cluster campaigns: 19 matrix executions, each capped at twenty minutes.
Scheduled CI is bounded and does not automatically run those campaigns.
Broker- and PHP-backed suites remain CI-owned as documented in
[live broker testing](docs/live-broker-testing.md); operator checking validates
fixtures, not a live Kubernetes deployment. Report unavailable or failing
commands; bounded checks alone do not establish release qualification.

## Selected Monthly Assurance

The owner-level [selected-assurance caller](.github/workflows/assurance.yml) is
configured for main only, on day 5 of each month at 06:13 UTC. It selects one
of the 4 current declared root-module fuzz targets using the shared UTC-week
rotation, with 30 seconds of active exploration. The selected risks are:
deterministic publication/topology validation, bounded delivery conversion
with copied body ownership, and exact publish-return/confirm correlation.
The root targets use in-process AMQP values and tracker state, not a broker.
They do not qualify TLS, live delivery/settlement, PHP interoperability or cluster
behavior. Existing selected PR broker/PHP jobs and explicit full/manual broker,
operator and performance campaigns retain their separate contracts.

Base-free ordinary checks execute deterministic seeds, not active exploration;
committed-range CI selects affected active smoke. Manual assurance requires an
exact anchored selector from the root fuzz operation in `.golib.yaml`, for
example `^FuzzPublicationValidation$`.
Empty, unknown and malformed selections refuse. The shared job bounds setup
and execution to ten minutes, active workers and CPU to two, owned memory to
2 GiB, native output to 1 MiB and private-cache lifetime. Failure, cancellation,
exhausted budgets and cleanup refusal remain attributable failures. This lane
does not run mutation or replace Required, original full fuzz budgets,
explicit `make check-full`, or relevant release qualification.

## Adding A Module

Follow the [repository structure policy](AGENTS.md#repository-structure). New
modules require an explicit purpose, ownership boundary, dependency review,
package catalog entry, full quality gates, documentation, changelog, license,
security policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
