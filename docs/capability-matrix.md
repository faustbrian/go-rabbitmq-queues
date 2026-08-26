# RabbitMQ 4.3 capability matrix

This matrix is policy input for `rabbitmqqueue`; it is not a claim that every
row is implemented yet. The source snapshot and products are pinned in
[`COMPATIBILITY.md`](../COMPATIBILITY.md).

| Capability | Classic queue | Quorum queue | Streams |
|---|---|---|---|
| Process-and-remove consumption | Yes | Yes | No; retained log |
| Durable | Yes | Required | Required |
| Non-durable | Exclusive queues; non-exclusive requires a deprecated feature disabled by default in 4.3 | No | No |
| Replicated in RabbitMQ 4.x | No | Yes, Raft quorum | Yes |
| Exclusive/server-named | Yes | No | No |
| Queue/message TTL | Yes | Yes; queue TTL has renewal caveat | Retention policy instead |
| Length limit | Yes | Yes, with overflow restrictions | Retention policy instead |
| Message priority | Configured with `x-max-priority`; 1-255, low single digits recommended | Always enabled in 4.3; strict levels 0-31 | No queue-priority contract |
| Consumer priority | Yes | Yes | Consumer groups differ |
| Single active consumer | Yes | Yes | Single active consumer per partition differs |
| Broker delivery limit | No poison-message counter | Default 20; `x-delivery-count` semantics changed in 4.3 | No queue delivery limit |
| Dead lettering | Yes; no at-least-once dead-lettering | Yes; at-least-once mode when configured | Not a queue/DLX model |
| Consumer timeout in 4.3 | No queue-specific support | Yes | Different protocol lifecycle |
| Global QoS | Deprecated/removed policy surface | Unsupported | Not applicable |

Important RabbitMQ 4.3 distinctions:

- classic queue mirroring is removed; classic queues are non-replicated;
- quorum publishers need confirms, and consumer processing needs manual
  acknowledgements for data-safety claims;
- quorum confirms are emitted after replication to a member quorum;
- AMQP 0-9-1 `basic.nack` does not increment the 4.3 quorum
  `x-delivery-count`, while `basic.reject` and connection loss do;
- quorum `reject-publish-dlx` overflow is unsupported and `reject-publish` may
  overshoot a length limit by in-flight messages;
- requeue loops remain an application/client-policy risk even where a quorum
  delivery limit exists;
- streams remain a separate retained-history product and must not be used as a
  process-and-remove queue abstraction.

## Policy ownership

| Owner | Responsibilities |
|---|---|
| RabbitMQ broker | Routing, queue storage, replication, confirms, redelivery, DLX mechanics |
| `rabbitmqqueue` | Bounds, lifecycle, exact publish correlation, ambiguity, settlement policy, observations |
| Infrastructure | Exchanges, queues, bindings, policies, TLS identities, users, permissions, cluster sizing |
| Application | Payload schema, idempotency, business retries, outbox transaction, handler side effects |
