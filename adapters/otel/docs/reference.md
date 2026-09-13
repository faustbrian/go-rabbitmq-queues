# OpenTelemetry adapter reference

## Ownership and lifecycle

The caller owns the `trace.TracerProvider`, exporter, batching, flush, and
shutdown lifecycle. `Instrumentation` owns no goroutines, connections, timers,
buffers, or credentials and is safe for concurrent use when the provider and
wrapped publisher are safe for concurrent use.

Stop RabbitMQ producers and consumers before flushing and shutting down the
provider. The adapter has no `Close` or `Shutdown` method.

## Producer spans

`Instrumentation.Publisher` wraps a synchronous publisher. Each accepted
publication creates a `PRODUCER` span named `publish {destination}`. The span
covers context injection, transport, mandatory-return reconciliation, and
publisher confirmation. The exact root `PublishResult` and error are returned
unchanged.

Destination names follow the current RabbitMQ messaging convention:

```text
exchange:routing-key
exchange
routing-key
amq.default
```

The adapter records only bounded message ID, routing key, payload size,
operation, destination, and a closed publish outcome. It does not record the
payload, headers, return reason, error text, broker endpoint, or credentials.

Input validation occurs before instrumentation. Injection uses an owned deep
copy and validates the result against the same root-module `Limits`. If adding
Trace Context would exceed those limits, publication is rejected before
transport with the root validation error and `PublishNotSent`.

## Consumer spans

`Instrumentation.Handler` wraps one push-based delivery handler. It extracts
the message creation context and creates a `CONSUMER` span named
`process {destination}`. Because this API processes exactly one message and is
normally invoked without another ambient operation, the message creation
context is the process span's parent. Ambiguous duplicate or malformed Trace
Context is ignored and the supplied ambient context remains the parent.

RabbitMQ process destinations contain the available exchange, routing key,
and queue. When routing key and queue are equal, the queue is not repeated.
Untrusted identifiers outside the configured limits are omitted rather than
recorded. A server-named transient queue uses the stable `(temporary)` label.

The span covers application handler execution and records the returned
settlement choice. RabbitMQ applies that settlement only after the handler
returns. This module therefore does not claim to observe broker ACK/NACK
completion. Handler settlement and external database or HTTP effects are not
atomic and consumers remain responsible for idempotency.

## Propagation

The module uses the W3C Trace Context propagator directly and does not consult
or mutate OpenTelemetry globals. Header matching is ASCII case-insensitive.
Outbound injection replaces stale `traceparent` and `tracestate` fields and
removes `baggage`; inbound duplicate propagation fields fail closed by leaving
the supplied context unchanged.

Caller publication bytes and headers are not mutated. The wrapped publisher
receives an owned body and owned byte-header values. Delivery values are
borrowed synchronously for the handler invocation.

## Failure behavior

Tracer construction and span-operation panics are contained. A tracer failure
falls back to the supplied context and does not block the wrapped RabbitMQ
operation. Application publisher and handler panics are not recovered.

The adapter never replaces RabbitMQ results or handler errors with telemetry
errors. It records only closed low-cardinality failure categories:
`not_sent`, `rejected`, `returned`, `ambiguous`, `invalid`, `propagation`,
`handler`, and `invalid_settlement`.

## Security and cardinality

Do not put secrets or customer data in RabbitMQ routing keys, queue names, or
message IDs. Although these values are bounded, they are exported as span
attributes. Use operator-owned low-cardinality destinations and opaque message
identifiers.

The adapter does not propagate baggage because baggage frequently carries
tenant or customer data. Applications needing additional cross-service data
must define explicit bounded RabbitMQ headers and assess their sensitivity
independently.

## Metrics

This adapter creates spans and propagates context. Operational connection,
confirmation, redelivery, settlement, and backlog metrics remain available
through the root module's bounded `Observations()` streams. Applications may
translate those observations into their existing metric policy without
coupling RabbitMQ delivery to telemetry exporter behavior.

## Semantic-convention status

The RabbitMQ messaging semantic conventions are currently development status.
This v1 module pins the emitted names documented above; a future incompatible
convention change requires explicit compatibility handling rather than a
silent rename.
