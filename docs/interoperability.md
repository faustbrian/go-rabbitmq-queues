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
| `properties.expiration_ms` | `expiration` | Optional non-negative decimal milliseconds; zero means immediate expiry |
| `properties.priority` | `priority` | Unsigned AMQP octet; queue support remains topology-specific |
| `headers` | application headers | Unique non-reserved string, bool, signed int64, or byte entries; order has no AMQP meaning |
| `body_base64` | body | Base64 fixture representation decoded to exact opaque bytes |

`traceparent` and `tracestate` are ordinary string application headers. Their
W3C semantics and trust policy remain application responsibilities.
`schema-version` is an example signed 64-bit application header and is not a
package-mandated schema mechanism. The package-owned publish-correlation header
is deliberately absent from public deliveries and must never become part of an
interoperability contract.
RabbitMQ-owned `x-death`, first/last-death summary, acquired-count, and
delivery-count headers are also excluded from application publication headers;
typed delivery metadata owns those broker values.

String application-header values are bounded message metadata rather than
identities. Their bytes, including control characters, are preserved across
publication and delivery and are never copied into package observations.
AMQP unsigned byte, uint16, and uint32 delivery headers are losslessly
normalized into the package's signed int64 header policy so clients using those
wire types remain interoperable without expanding the public type surface.
RabbitMQ's optional AMQP 0-9-1 `x-death.original-expiration` string is exposed
as a bounded duration pointer so an explicit zero remains distinct from an
omitted original TTL.
Publication and delivery expiration use the same presence semantics: omission
means no per-message TTL, while explicit zero requests RabbitMQ's immediate
expiration behavior when direct delivery is unavailable.

`reply-to` and `correlation-id` expose the AMQP metadata needed for an
application-owned request/reply flow. The package does not create reply queues,
dispatch responses, enforce RPC timeouts, or claim exactly-once RPC behavior.

## PHP proof gate

The opt-in live harness pins PHP 8.5.9, Composer 2.10.1, and
`php-amqplib/php-amqplib` v3.7.4 at Git
`381b6f7c600e0e0c7463cdd7f7a1a3bc6268e5fd`. Its lock file and runner live in
[`testdata/interoperability/php`](../testdata/interoperability/php). The runner
requires verified TLS 1.2 or 1.3, supports a custom root CA and mTLS, uses
publisher confirms and mandatory-return reconciliation, and acknowledges a
Go publication only after validating the canonical corpus.

`php-amqplib` exposes received application-table values as native PHP scalars;
it does not retain the received integer-width or byte/string wire-type tag in
that API. The Go-to-PHP direction therefore proves exact semantic values. In
the PHP-to-Go direction, the runner deliberately publishes the fixture's
signed integer as `T_INT_LONGLONG` and its opaque header as `T_BYTES`, and the
Go delivery assertion proves those exact public header kinds and values.
The Go producer also carries one reserved
`x-rabbitmqqueue-publish-token` string on the wire so mandatory returns can be
correlated with the exact publish. The PHP runner requires that single bounded
additional header without exposing its value. It is package policy metadata,
not application metadata, and Go removes it from public delivery snapshots.

Laravel compatibility remains unproved until the same corpus passes in both
directions through a retained live-broker run. That run must also prove routing,
mandatory returns, confirms, manual settlement, and record the broker, PHP,
extension, client-library, and package versions. A local runner self-test,
tagged compilation, fixture-only pass, or Go-only pass must not be reported as
PHP or Laravel interoperability.
