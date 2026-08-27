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
| Exclusive/server-named | Yes; must share the declaring consumer connection | No | No |
| Queue/message TTL | Yes | Yes; queue TTL has renewal caveat | Retention policy instead |
| Length limit | Yes | Yes, with overflow restrictions | Retention policy instead |
| Message priority | Configured with `x-max-priority`; 1-255, low single digits recommended | Always enabled in 4.3; strict levels 0-31 | No queue-priority contract |
| Consumer priority | Signed `x-priority`; higher active consumers receive first | Signed `x-priority`; also affects SAC promotion | Consumer groups differ |
| Exclusive consumer | Yes; application must replace a failed consumer | No; broker ignores the flag, so client policy rejects it | No |
| Single active consumer | Yes | Yes | Single active consumer per partition differs |
| Broker delivery limit | No poison-message counter | Default 20; explicit bounded values include zero; `x-delivery-count` semantics changed in 4.3 | No queue delivery limit |
| Dead lettering | Yes; no at-least-once dead-lettering | Yes; at-least-once mode when configured | Not a queue/DLX model |
| Consumer timeout in 4.3 | No queue-specific support | Yes | Different protocol lifecycle |
| Disconnected-consumer timeout in 4.3 | No | Yes; broker default is 60 seconds | Different protocol lifecycle |
| Delayed retry in 4.3 | No | `disabled`, `all`, `failed`, or `returned` linear backoff | Different protocol lifecycle |
| Global QoS | Deprecated/removed policy surface | Unsupported | Not applicable |

Important RabbitMQ 4.3 distinctions:

- classic queue mirroring is removed; classic queues are non-replicated;
- quorum publishers need confirms, and consumer processing needs manual
  acknowledgements for data-safety claims;
- quorum confirms are emitted after replication to a member quorum;
- `Publication.ExchangeKind` records the locally expected exchange semantic.
  Direct and topic publications preserve any bounded routing key, including
  RabbitMQ's native empty key; fanout and headers publications require the
  empty key. Passive topology verification remains the broker evidence for the
  exchange's actual kind;
- RabbitMQ's predeclared default direct exchange is represented only by an
  empty `Publication.Exchange` paired with an explicit `ExchangeDirect` kind.
  Its routing key is the destination queue identity;
- headers bindings accept RabbitMQ 4.3's optional `x-match` values `all`,
  `any`, `all-with-x`, and `any-with-x`. Omission defaults to `all`. The first
  two modes ignore `x-*` arguments, while the `with-x` modes include them; the
  package requires at least one criterion that the selected mode evaluates so
  a binding cannot become an unintended match-all route;
- AMQP 0-9-1 `basic.nack` does not increment the 4.3 quorum
  `x-delivery-count`, while `basic.reject` and connection loss do;
- `Delivery.AcquiredCount` preserves RabbitMQ 4.3's `x-acquired-count`, which
  tracks assignments to a consumer, while `Delivery.DeliveryCount` preserves
  the distinct failed-delivery counter used by the broker delivery limit;
- quorum `reject-publish-dlx` overflow is unsupported and `reject-publish` may
  overshoot a length limit by in-flight messages;
- requeue loops remain an application/client-policy risk even where a quorum
  delivery limit exists. The package uses `x-acquired-count` to apply
  `MaxRequeues` to quorum returns even when `x-delivery-count` is unchanged;
- `Queue.DeliveryLimit` distinguishes omission from an explicit zero. Omission
  leaves the RabbitMQ policy or 4.3 default of 20 effective; zero makes the
  first failed redelivery exceed the limit. The package intentionally cannot
  declare RabbitMQ's `-1` unlimited compatibility mode because unbounded
  failure redelivery conflicts with its retry-loop safety policy;
- an omitted consumer priority uses RabbitMQ's default zero, while an explicit
  zero is emitted as a signed `x-priority` argument; positive and negative
  priorities are supported on classic and quorum queues;
- per-queue delivery-acknowledgement timeout is quorum-only in 4.3;
  `ConsumerTimeout` emits `x-consumer-timeout`, accepts the broker minimum of
  one minute at millisecond precision, and remains distinct from the package's
  handler deadline;
- disconnected-consumer timeout is quorum-only in 4.3.
  `DisconnectedConsumerTimeout` emits `x-consumer-disconnected-timeout` and
  bounds how long the broker waits before returning deliveries held by a
  consumer node that becomes unreachable. Omission retains the broker's
  60-second default; explicit values are non-negative milliseconds;
- delayed retry is quorum-only in 4.3. `Queue.DelayedRetry` distinguishes
  omission from explicit disabling and can apply linear backoff to all returned
  messages, only delivery-count-incrementing failures, or only returns that do
  not increment the delivery count. Enabled retry requires a positive
  millisecond minimum; an omitted maximum produces a fixed delay, while an
  explicit maximum cannot precede the minimum;
- exclusive consumption is classic-only and mutually exclusive with a queue's
  single-active-consumer declaration; `QueueReference.SingleActiveConsumer`
  records the expected topology semantic for local validation but is not live
  broker evidence;
- a client-owned transient consumer passively verifies its existing exchange,
  then declares, binds, and consumes a server-named exclusive classic queue on
  one connection. Recovery creates a new empty queue; the deleted generation's
  backlog is not recovered;
- classic SAC initially selects a consumer without applying priority, while a
  higher-priority quorum SAC consumer takes over after outstanding deliveries
  from the current active consumer are acknowledged;
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
arguments, closing the channel when the entity is missing or inequivalent. A
`RESOURCE_LOCKED` response during detached passive inspection proves that the
name belongs to an exclusive queue on another connection and is classified as
inequivalent topology. AMQP 0-9-1 provides no passive binding inspection
method. Production binding equivalence therefore remains an
operator/infrastructure gate; the package rejects passive binding requests
instead of calling the mutating bind method. Development-only declarations can
create an explicitly modeled binding after `PermitDevelopmentTopology` is
supplied.

`ApplyTopology` rejects every exclusive queue, including queues with static
names, because it closes its connection after verification or declaration and
RabbitMQ would immediately delete the queue. Use `QueueReference.Transient`
when a consumer explicitly owns a connection-scoped queue. This is the bounded
exception to operator-owned queue topology; its exchange remains pre-existing
and is passively verified.

## Declaration-equivalent queue policy

`Queue` can represent RabbitMQ declaration arguments for quorum delivery limit,
per-queue message TTL, unused-queue expiry, quorum delivery-acknowledgement and
disconnected-consumer timeouts, quorum delayed retry, maximum ready-message
count, maximum ready-message bytes, overflow behavior, dead-letter exchange and
routing key, and quorum dead-letter strategy. Optional numeric fields
distinguish an explicit zero from an omitted argument and are bounded to
millisecond or signed AMQP integer values. A nil dead-letter routing key
preserves the broker's original-routing-key behavior, while a pointer to an
empty string represents an explicit empty argument.

Queue-type validation preserves the RabbitMQ 4.3 distinctions:

- non-durable classic queues must be exclusive;
- classic queues support `drop-head`, `reject-publish`, and
  `reject-publish-dlx`, but not an explicit dead-letter strategy;
- quorum queues support `drop-head` and `reject-publish`, but not
  `reject-publish-dlx`;
- quorum delivery limit is optional, accepts bounded unsigned values including
  zero, and is rejected for classic queues;
- quorum consumer timeout is optional, must be at least one minute, and is
  rejected for classic queues;
- quorum disconnected-consumer timeout is optional, accepts non-negative
  millisecond values, and is rejected for classic queues;
- quorum delayed retry is optional, requires a positive millisecond minimum
  when enabled, and is rejected for classic queues;
- quorum at-least-once dead-lettering requires a dead-letter exchange and
  `reject-publish` overflow; and
- quorum priority remains intrinsic and does not emit `x-max-priority`.

RabbitMQ policies remain preferred for production because they can change
without redeploying applications. Declaration arguments override policy values.
When infrastructure supplies a setting only through policy, omit the matching
`Queue` field and verify the effective policy through operator evidence. Use
the field during passive verification only when the existing queue was declared
with that same `x-` argument.
