# Source documentation audit

## Scope and acceptance

Base: `7cd9e85cf024ff6d4363f0fe86a96d936006f229` (remote main).
Tier A, owned by this standalone repository: clarify real admission, publish
outcomes, manual settlement, resource ownership, callbacks and cleanup without
changing runtime, dependencies, protocol fixtures or broker qualification.
Direct consumers: native callers and declared go-queue RabbitMQ adapters.
Risks: not-sent versus ambiguous outcomes, ACK write versus broker confirmation,
handler/credential cooperation, stale-generation settlement, return-text privacy,
secret copies, topology partial mutation and native decoding before wrapper limits.
Smallest checks: formatting, all-file token/AST and directive equality, public
contracts/header and parsed/rendered docs, historical API/fixture identity,
native source/local-doc/offline specification validation, changed secrets and
one ordinary complete final-diff review. No broker, Docker or runtime campaigns.

## Reproducible inventory

Use `git ls-files '*.go' 'go.mod' 'go.sum'`, module/package catalogs, and
`rg -n '^func (Test|TestMain|Fuzz|Benchmark|Example)|go:|Code generated|TODO|FIXME|HACK|NOTE|WARNING|Deprecated:|nolint|nosec' --glob '*.go'`.
One module and public package: github.com/faustbrian/go-rabbitmq-queues,
package rabbitmqqueue. All 52 tracked Go files, every exported declaration,
field/interface method and non-obvious test/provider helper are included.
No nested/provider module, vendor source or generated Go file exists.

| File | Investigated role |
| --- | --- |
| `aaa_mutation_contract_test.go` | assertions and test-only transport/provider/helpers |
| `allocation_admission_test.go` | assertions and test-only transport/provider/helpers |
| `amqp_client.go` | implementation, public/private contracts |
| `benchmark_test.go` | four local workloads, not broker performance |
| `boundary_test.go` | assertions and test-only transport/provider/helpers |
| `channel_open.go` | implementation, public/private contracts |
| `channel_open_deadline_test.go` | assertions and test-only transport/provider/helpers |
| `config.go` | implementation, public/private contracts |
| `config_test.go` | assertions and test-only transport/provider/helpers |
| `connection_state.go` | implementation, public/private contracts |
| `consumer.go` | implementation, public/private contracts |
| `consumer_boundary_test.go` | assertions and test-only transport/provider/helpers |
| `consumer_open.go` | implementation, public/private contracts |
| `consumer_recovery_test.go` | assertions and test-only transport/provider/helpers |
| `consumer_test.go` | assertions and test-only transport/provider/helpers |
| `coverage_edges_test.go` | assertions and test-only transport/provider/helpers |
| `delivery.go` | implementation, public/private contracts |
| `delivery_settlement.go` | implementation, public/private contracts |
| `delivery_test.go` | assertions and test-only transport/provider/helpers |
| `doc.go` | single canonical package responsibility/entry/lifetime/trust comment |
| `errors.go` | implementation, public/private contracts |
| `example_test.go` | direct credential callback example; no executable change planned |
| `fuzz_test.go` | four bounded in-process input/oracle targets |
| `health.go` | implementation, public/private contracts |
| `health_test.go` | assertions and test-only transport/provider/helpers |
| `interoperability_test.go` | assertions and test-only transport/provider/helpers |
| `live_broker_test.go` | livebroker-tagged broker/PHP/cluster/performance fixtures; not executed |
| `live_cluster_test.go` | livebroker-tagged broker/PHP/cluster/performance fixtures; not executed |
| `live_interoperability_test.go` | livebroker-tagged broker/PHP/cluster/performance fixtures; not executed |
| `live_performance_test.go` | livebroker-tagged broker/PHP/cluster/performance fixtures; not executed |
| `main_test.go` | goleak TestMain; must inspect before selected example execution |
| `message.go` | implementation, public/private contracts |
| `message_test.go` | assertions and test-only transport/provider/helpers |
| `mixed_delivery_admission_test.go` | assertions and test-only transport/provider/helpers |
| `observation.go` | implementation, public/private contracts |
| `observation_test.go` | assertions and test-only transport/provider/helpers |
| `producer.go` | implementation, public/private contracts |
| `producer_async_test.go` | assertions and test-only transport/provider/helpers |
| `producer_boundary_test.go` | assertions and test-only transport/provider/helpers |
| `producer_open.go` | implementation, public/private contracts |
| `producer_open_test.go` | assertions and test-only transport/provider/helpers |
| `producer_recovery_test.go` | assertions and test-only transport/provider/helpers |
| `producer_test.go` | assertions and test-only transport/provider/helpers |
| `publish_tracker.go` | implementation, public/private contracts |
| `publish_tracker_test.go` | assertions and test-only transport/provider/helpers |
| `result.go` | implementation, public/private contracts |
| `settlement.go` | implementation, public/private contracts |
| `stress_test.go` | assertions and test-only transport/provider/helpers |
| `topology.go` | implementation, public/private contracts |
| `topology_apply.go` | implementation, public/private contracts |
| `topology_apply_test.go` | assertions and test-only transport/provider/helpers |
| `topology_test.go` | assertions and test-only transport/provider/helpers |

## Missing documentation and investigated questions

Missing: configuration/message/delivery/topology/outcome/observation fields and
public enum/bound constants; credential method obligations; construction and
zero-value ownership; callback/panic/cooperation boundaries; local ACK-send
results; partial topology and batch outcomes. Add useful adjacent rationale for
exact confirm/return correlation, generation ownership, requeue counters,
channel-open cancellation, secret copies and observation tail loss.

Investigated against code and named tests: configuration is copied after
validation (provider/handler functions remain borrowed); publication buffers
are copied after admission, but PublishBatch reads each item at its turn and
is not atomic. Opens use caller context only during startup; runtime goroutines
require Shutdown. Handler cancellation cannot forcibly stop application code.
AwaitSettlement observes the client settlement call, not a broker ACK-of-ACK.
A prior definitive publish result can win over a later cancellation/error.
Return.Reason retains broker text only if at most 255 bytes and free of ASCII
controls; that is not secret redaction. Header strings/body are opaque, not
UTF-8/semantic normalization or source authentication. Received values are
already decoded by pinned amqp091-go before wrapper admission; no hostile-broker
allocation resistance is claimed (dated security model is preserved).

Requeue policy uses quorum acquired count first, then the special quorum Reject
failed-delivery counter when present; the remaining fallback permits only an
initial non-redelivered request. Reaching the cap clears Requeue, not automatic
replacement publication or substitution of another settlement method. Delivery
copies share slices/pointers and one settlement result; independent deliveries
own their copied buffers. Handler counter mutations also affect the envelope
used by boundedSettlement after return, so counters must stay read-only to rely
on the observed-state bound. Old-generation handlers can overlap new deliveries;
settlement must keep the old channel, never retag onto a replacement.

Current-record contradictions: DEC-001 omits accepted signed integer input
variants; DEC-002 implies broker return text is not exposed; DEC-004 references
nonexistent FailurePolicy and misstates failure/cap timing and processing
guarantees; DEC-005 references
nonexistent QueuePolicy and could imply a usable zero topology mode. Reconcile
only affected current prose and typed records, append native typed decision
digests, preserve IDs and historical digest prefixes/source pins. No protocol
interpretation, authority edition or behavioral decision is being changed.

Unresolved/retained obligations: trusted callback termination and native decoder
admission are deployment/transport-owner boundaries, not fixable by comments.
Printability cannot establish broker text confidentiality. Topology passive RPCs
provide only the broker's passive response semantics, not effective policies,
bindings or authorization qualification. No new broker/security/release verdict
or current-publication claim follows from these source checks.

## Variants, generation and evidence

The only build variant is livebroker on four live_ files. Preserve their tags,
all seven G115 and one G101 rationale annotations, and both Deprecated: Close
headers. No debt marker requires invented resolution or generated header.
Four fuzz targets cover publication/topology validation, delivery copy/bounds
and exact tracker correlation; four benchmarks cover local validation,
conversion, AMQP property copying and tracker completion. Inspect their setup,
oracles and sinks without fresh campaign/performance claims. TestMain runs
package goleak; no unchanged test/example selector is executed.

AMQP 0-9-1 authority is pinned in specification/sources.lock.json; RabbitMQ 4.3
queue/quorum/confirm interpretations retain DEC-001 through DEC-005 and named
executable/fixture evidence. The PHP peer and Operator fixtures are separately
pinned; do not refresh upstream URLs or execute live harnesses for this pass.

Inspect .golib.yaml, verification/package.mk and scripts/: api/baseline.txt
is apidiff and is preserved, with no prose-bearing snapshot regeneration. Root
package is the sole catalog/package boundary. Native parent CLI supports offline
specification, source-docs and local-doc checks; use Go 1.27.2 and all three
owned caches in a serialized bounded session. Scripts are broker/PHP/operator
campaigns, not source generators. No new dependency/tool/workflow pins, fixture
bytes, historical changelog entries or dated security threat review are edited.

## Bounded local validation

Pinned Go 1.27.2 formatting and all-file parsing passed. All 52 executable
scanner token/literal/implicit-semicolon streams and comment/position-free ASTs
match the base exactly, including tagged tests. One canonical package header,
387 public declaration/field/interface-method contracts and 430 comment groups
parsed/rendered; root go-doc rendered 65,186 bytes. Existing Close annotations
now use the required separate deprecation paragraph without changing markers.
The selected apidiff baseline and module/catalog/policy/fixture bytes are intact.

Native typed decision marshaling reproduced every historical baseline digest;
changed current DEC-001/002/004/005 digests were appended without replacing
history. Native offline specification check reports all five records valid;
source-docs check and local docs check passed. No online monitoring/authority
repin or runtime interpretation changed. A temporary digest-helper syntax error
was corrected before any history write; all failure/success resources were
removed. The consumed v1.14.0 module source explicitly checks cancellation before
its native Publish call; later possible transmission still requires ambiguity.

No executable example changed. No unchanged example or TestMain selector,
runtime suite, livebroker/tagged compilation, Docker/broker/PHP/operator,
race/leak, active fuzz, benchmark, mutation, consumer or release campaign ran.
Every exact owned GOCACHE/GOMODCACHE/GOTMPDIR, helper and render output was
removed; parent tools and historical evidence remain retained. Complete final
ordinary diff/changed-content secret inspection and Intent commit are local
handoff evidence, not broker/interoperability, hosted-CI or release qualification.
