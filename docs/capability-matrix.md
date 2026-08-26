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

AMQP passive declarations compare exchange and queue identity and declaration
arguments, closing the channel when the entity is missing or inequivalent.
AMQP 0-9-1 provides no passive binding inspection method. Production binding
equivalence therefore remains an operator/infrastructure gate; the package
rejects passive binding requests instead of calling the mutating bind method.
Development-only declarations can create an explicitly modeled binding after
`PermitDevelopmentTopology` is supplied.

## Declaration-equivalent queue policy

`Queue` can represent RabbitMQ declaration arguments for per-queue message TTL,
unused-queue expiry, maximum ready-message count, maximum ready-message bytes,
overflow behavior, dead-letter exchange and routing key, and quorum dead-letter
strategy. Optional numeric fields distinguish an explicit zero from an omitted
argument and are bounded to millisecond or signed AMQP integer values. A nil
dead-letter routing key preserves the broker's original-routing-key behavior,
while a pointer to an empty string represents an explicit empty argument.

Queue-type validation preserves the RabbitMQ 4.3 distinctions:

- non-durable classic queues must be exclusive;
- classic queues support `drop-head`, `reject-publish`, and
  `reject-publish-dlx`, but not an explicit dead-letter strategy;
- quorum queues support `drop-head` and `reject-publish`, but not
  `reject-publish-dlx`;
- quorum at-least-once dead-lettering requires a dead-letter exchange and
  `reject-publish` overflow; and
- quorum priority remains intrinsic and does not emit `x-max-priority`.

RabbitMQ policies remain preferred for production because they can change
without redeploying applications. Declaration arguments override policy values.
When infrastructure supplies a setting only through policy, omit the matching
`Queue` field and verify the effective policy through operator evidence. Use
the field during passive verification only when the existing queue was declared
with that same `x-` argument.
