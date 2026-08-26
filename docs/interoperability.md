# AMQP interoperability corpus

The version 1 corpus defines one language-neutral AMQP 0-9-1 message example.
The JSON file is a transport-independent fixture envelope; it does not require
applications to use JSON payloads. `body_base64` and byte-valued headers decode
to opaque bytes before publication. Every value is synthetic and non-sensitive;
live application messages must never be copied into this corpus or test output.

The canonical fixture is
[`testdata/interoperability/message-v1.json`](../testdata/interoperability/message-v1.json).
Its local Go contract test validates the public policy, converts it to AMQP
properties, and converts the equivalent delivery back to an owned snapshot.
This proves the package's in-process mapping only. It does not contact a broker
or establish PHP, Laravel, TLS, routing, confirmation, or settlement behavior.

## Field mapping

| Fixture field | AMQP 0-9-1 field | Rule |
|---|---|---|
| `routing.exchange` | publish exchange | Explicit UTF-8 identity |
| `routing.routing_key` | publish routing key | Explicit UTF-8 identity |
| `routing.mandatory` | mandatory flag | Must remain `true` for reliable routing evidence |
| `properties.delivery_mode` | `delivery-mode` | `persistent` maps to octet `2` |
| `properties.content_type` | `content-type` | Describes opaque body bytes; JSON is not mandatory |
| `properties.content_encoding` | `content-encoding` | Application-defined encoding name |
| `properties.message_id` | `message-id` | Stable application message identity |
| `properties.correlation_id` | `correlation-id` | Application request or workflow correlation |
| `properties.reply_to` | `reply-to` | Application-owned reply queue or routing identity for request/reply |
| `properties.timestamp` | `timestamp` | UTC RFC 3339 input, represented at AMQP second precision |
| `properties.type` | `type` | Application message type |
| `properties.app_id` | `app-id` | Publishing application identity |
| `properties.expiration_ms` | `expiration` | Non-negative decimal milliseconds |
| `properties.priority` | `priority` | Unsigned AMQP octet; queue support remains topology-specific |
| `headers` | application headers | Unique string, bool, signed int64, or byte entries; order has no AMQP meaning |
| `body_base64` | body | Base64 fixture representation decoded to exact opaque bytes |

`traceparent` and `tracestate` are ordinary string application headers. Their
W3C semantics and trust policy remain application responsibilities.
`schema-version` is an example signed 64-bit application header and is not a
package-mandated schema mechanism. The package-owned publish-correlation header
is deliberately absent from public deliveries and must never become part of an
interoperability contract.

`reply-to` and `correlation-id` expose the AMQP metadata needed for an
application-owned request/reply flow. The package does not create reply queues,
dispatch responses, enforce RPC timeouts, or claim exactly-once RPC behavior.

## PHP proof gate

Laravel compatibility remains unproved until a supported PHP AMQP client is
pinned and the same corpus passes in both directions through a live broker.
That gate must compare exact body bytes, property values, header value types,
routing, mandatory returns, confirms, and manual settlement. The run must also
record broker, PHP, extension or library, and package versions. A fixture-only
or Go-only pass must not be reported as PHP or Laravel interoperability.
