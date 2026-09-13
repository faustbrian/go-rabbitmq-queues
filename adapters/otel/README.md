# RabbitMQ queue OpenTelemetry adapter

`rabbitmqqueueotel` adds RabbitMQ producer and process spans plus W3C Trace
Context propagation to the root `rabbitmqqueue` module. It keeps the root
module telemetry-vendor neutral and leaves exporter/provider lifecycle with
the application.

## Install

```sh
go get github.com/faustbrian/go-rabbitmq-queues/adapters/otel@v1
```

## Quick start

```go
instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
    TracerProvider: tracerProvider,
    Limits:         rabbitmqqueue.DefaultLimits(),
})
if err != nil {
    return err
}

publisher, err := instrumentation.Publisher(producer)
if err != nil {
    return err
}

handler, err := instrumentation.Handler(queue, applicationHandler)
if err != nil {
    return err
}
```

The compiling [`Example`](example_test.go) shows the complete imports and call
sequence.

## Contract

- `Publisher` creates one `PRODUCER` span around one synchronous publish,
  including broker confirmation, and injects its W3C creation context.
- `Handler` extracts W3C context and creates one `CONSUMER` span around one
  push-delivery handler invocation.
- Only `traceparent` and `tracestate` are propagated. Existing propagation
  fields and `baggage` are removed from the owned outbound snapshot.
- Payloads, headers, credentials, endpoints, error text, and arbitrary
  application values are never recorded.
- The adapter starts no goroutines and does not create, flush, or shut down an
  OpenTelemetry provider or exporter.
- Tracer-provider failures are contained and do not replace RabbitMQ publish
  results or handler results.

The process span records the handler's requested ACK/NACK/reject/delegate
choice. The root client performs the broker settlement after the handler
returns, so this adapter does not claim a separate broker-settlement span or
transactional coupling between application effects and settlement.

See the [technical reference](docs/reference.md) for destination naming,
ownership, error, privacy, and failure semantics.

## Compatibility

This stable module requires Go 1.27.0 and follows Semantic Versioning. It is an
optional nested module released with `adapters/otel/v` tags.

## License

MIT. See [LICENSE](LICENSE).
