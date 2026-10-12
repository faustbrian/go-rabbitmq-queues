package rabbitmqqueue

import (
	"math"
	"strings"
	"time"
)

const (
	// MaxTopologyExchanges bounds one topology operation's exchange set.
	MaxTopologyExchanges = 128
	// MaxTopologyQueues bounds one topology operation's queue set.
	MaxTopologyQueues = 128
	// MaxTopologyBindings bounds one development declaration's binding set.
	MaxTopologyBindings = 512
)

// ExchangeKind selects a RabbitMQ built-in AMQP exchange algorithm.
type ExchangeKind string

const (
	// ExchangeDirect selects direct routing by exact key.
	ExchangeDirect ExchangeKind = "direct"
	// ExchangeTopic selects topic-pattern routing.
	ExchangeTopic ExchangeKind = "topic"
	// ExchangeFanout selects fanout routing without a key criterion.
	ExchangeFanout ExchangeKind = "fanout"
	// ExchangeHeaders selects field-table binding criteria rather than a routing key.
	ExchangeHeaders ExchangeKind = "headers"
)

// Exchange is a stable exchange identity and equivalence policy.
type Exchange struct {
	// Name is a required named exchange identity; the default exchange is not declared.
	Name string
	// Kind selects direct, topic, fanout or headers semantics for local validation.
	Kind ExchangeKind
	// Durable is supplied declaration intent, not independently queried broker state.
	Durable bool
	// AutoDelete is supplied broker lifecycle intent.
	AutoDelete bool
	// Internal requests broker-only publication access when declaring the exchange.
	Internal bool
}

// Validate checks exchange identity and supported kind.
func (exchange Exchange) Validate() error {
	if invalidIdentity(exchange.Name, 255) {
		return ErrInvalidTopology
	}
	switch exchange.Kind {
	case ExchangeDirect, ExchangeTopic, ExchangeFanout, ExchangeHeaders:
		return nil
	default:
		return ErrUnsupportedExchangeKind
	}
}

// QueueType distinguishes queue implementations whose policies are not interchangeable.
type QueueType string

const (
	// QueueClassic selects the package-supported classic queue capability subset.
	QueueClassic QueueType = "classic"
	// QueueQuorum selects durable quorum queue policy with no exclusive/auto-delete flags.
	QueueQuorum QueueType = "quorum"
)

// QueueOverflow selects RabbitMQ's queue-length overflow behavior.
type QueueOverflow string

const (
	// QueueOverflowDropHead requests removal of ready head messages on overflow.
	QueueOverflowDropHead QueueOverflow = "drop-head"
	// QueueOverflowRejectPublish requests broker publication rejection on overflow.
	QueueOverflowRejectPublish QueueOverflow = "reject-publish"
	// QueueOverflowRejectPublishDeadLetter is classic-only reject-publish-dlx policy.
	QueueOverflowRejectPublishDeadLetter QueueOverflow = "reject-publish-dlx"
)

// DeadLetterStrategy selects quorum queue dead-letter transfer guarantees.
// The zero value leaves the broker's at-most-once default implicit.
type DeadLetterStrategy string

const (
	// DeadLetterAtMostOnce requests broker at-most-once dead-letter transfer.
	DeadLetterAtMostOnce DeadLetterStrategy = "at-most-once"
	// DeadLetterAtLeastOnce requests quorum transfer retries, which can duplicate effects.
	DeadLetterAtLeastOnce DeadLetterStrategy = "at-least-once"
)

// QueueDeadLetter describes declaration-time dead-letter arguments. An empty
// Exchange explicitly selects the AMQP default exchange. A nil RoutingKey
// omits the argument and preserves original routing keys; a pointer to an empty
// string emits an explicit empty routing key. RabbitMQ policies are preferred
// for production configuration because they remain mutable.
type QueueDeadLetter struct {
	// Exchange is the target exchange; empty explicitly selects the default exchange.
	Exchange string
	// RoutingKey is nil to omit the argument or points to an explicit key, including empty.
	RoutingKey *string
	// Strategy is omitted at zero; at-least-once requires quorum and reject-publish overflow.
	// It describes broker transfer policy, not exactly-once application effects.
	Strategy DeadLetterStrategy
}

// QueueDeliveryLimit bounds RabbitMQ quorum failed redeliveries. The unsigned
// policy intentionally cannot represent RabbitMQ's unsafe unlimited value -1.
type QueueDeliveryLimit uint32

// DelayedRetryType selects which RabbitMQ 4.3 quorum redeliveries receive
// broker-managed linear backoff.
type DelayedRetryType string

const (
	// DelayedRetryDisabled explicitly disables broker delayed retry and forbids delays.
	DelayedRetryDisabled DelayedRetryType = "disabled"
	// DelayedRetryAll requests broker delay for all returned deliveries.
	DelayedRetryAll DelayedRetryType = "all"
	// DelayedRetryFailed requests broker delay for failed-delivery-count increments.
	DelayedRetryFailed DelayedRetryType = "failed"
	// DelayedRetryReturned requests broker delay for returns without that increment.
	DelayedRetryReturned DelayedRetryType = "returned"
)

// QueueDelayedRetry describes RabbitMQ 4.3 quorum delayed-retry arguments.
// Enabled retry requires a positive millisecond Minimum. A nil Maximum uses a
// fixed delay equal to Minimum; otherwise Maximum must not precede Minimum.
type QueueDelayedRetry struct {
	// Type selects a RabbitMQ 4.3 quorum policy; disabled forbids delay values.
	Type DelayedRetryType
	// Minimum is positive whole milliseconds when enabled, not an application sleep.
	Minimum time.Duration
	// Maximum is optional whole milliseconds at least Minimum; nil emits no maximum.
	Maximum *time.Duration
}

// Queue describes declaration-equivalent queue policy. A zero Name requests a
// server-generated name and is valid only for an exclusive classic queue.
// MessageTTL and the length pointers distinguish an explicit zero argument
// from omission. Expires, when present, must be a positive millisecond value.
// A nil DeliveryLimit preserves the broker policy or default; a pointer emits
// an explicit bounded value, including zero.
// ConsumerTimeout is RabbitMQ 4.3's quorum-only delivery-acknowledgement
// timeout and accepts non-negative values with millisecond precision.
// DisconnectedConsumerTimeout is RabbitMQ 4.3's quorum-only wait before held
// deliveries are returned after a consumer node becomes unreachable.
// DelayedRetry is RabbitMQ 4.3's quorum-only linear-backoff policy.
type Queue struct {
	// Name is a bounded queue identity; empty is only valid for exclusive classic values.
	// ApplyTopology still rejects exclusive queues it cannot retain.
	Name string
	// Type is explicitly classic or quorum; no broker default/type inference is used.
	Type QueueType
	// Durable is required for quorum and nonexclusive classic under package policy.
	Durable bool
	// AutoDelete is declaration lifecycle intent, rejected for quorum.
	AutoDelete bool
	// Exclusive is connection ownership intent, classic-only and incompatible with Durable.
	Exclusive bool
	// SingleActiveConsumer requests broker SAC declaration; it is not a local handler mutex.
	SingleActiveConsumer bool
	// DeliveryLimit is optional quorum-only bounded failed-redelivery policy, including zero.
	DeliveryLimit *QueueDeliveryLimit
	// MaxPriority is classic-only x-max-priority in 1..255 when nonzero; zero omits it.
	MaxPriority uint8
	// MessageTTL is optional nonnegative whole-millisecond queue message TTL.
	MessageTTL *time.Duration
	// Expires is optional positive whole-millisecond unused-queue expiry.
	Expires *time.Duration
	// ConsumerTimeout is optional quorum-only nonnegative millisecond acknowledgement timeout.
	ConsumerTimeout *time.Duration
	// DisconnectedConsumerTimeout is optional quorum-only nonnegative millisecond partition wait.
	DisconnectedConsumerTimeout *time.Duration
	// DelayedRetry is optional quorum-only broker linear-backoff policy, not republishing.
	DelayedRetry *QueueDelayedRetry
	// MaxLength is optional ready-message count within the signed 64-bit argument range.
	MaxLength *uint64
	// MaxLengthBytes is optional ready-message byte count within the signed 64-bit range.
	MaxLengthBytes *uint64
	// Overflow is optional queue-type-specific overflow policy; zero omits the argument.
	Overflow QueueOverflow
	// DeadLetter supplies optional declaration-time DLX policy; effective operator policy
	// and permissions require separate infrastructure evidence.
	DeadLetter *QueueDeadLetter
}

// Validate enforces the package's RabbitMQ 4.3 queue-type subset, identity,
// optional argument ranges/presence and supported combinations (DEC-005).
// It performs no broker I/O, authorization or effective-policy inspection.
func (queue Queue) Validate() error {
	if queue.Name == "" {
		if queue.Type != QueueClassic || !queue.Exclusive {
			return ErrInvalidTopology
		}
	} else if invalidIdentity(queue.Name, 255) {
		return ErrInvalidTopology
	}

	switch queue.Type {
	case QueueClassic:
		if queue.DeliveryLimit != nil || (queue.Exclusive && queue.Durable) ||
			(!queue.Durable && !queue.Exclusive) ||
			queue.ConsumerTimeout != nil ||
			queue.DisconnectedConsumerTimeout != nil ||
			queue.DelayedRetry != nil ||
			(queue.DeadLetter != nil && queue.DeadLetter.Strategy != "") {
			return ErrUnsupportedQueuePolicy
		}
	case QueueQuorum:
		if !queue.Durable || queue.Exclusive || queue.AutoDelete || queue.MaxPriority != 0 ||
			queue.Overflow == QueueOverflowRejectPublishDeadLetter {
			return ErrUnsupportedQueuePolicy
		}
	default:
		return ErrUnsupportedQueuePolicy
	}

	if !validQueueDuration(queue.MessageTTL, true) || !validQueueDuration(queue.Expires, false) ||
		!validConsumerTimeout(queue.ConsumerTimeout) ||
		!validQueueDuration(queue.DisconnectedConsumerTimeout, true) ||
		!validDelayedRetry(queue.DelayedRetry) ||
		!validQueueLength(queue.MaxLength) || !validQueueLength(queue.MaxLengthBytes) {
		return ErrUnsupportedQueuePolicy
	}
	switch queue.Overflow {
	case "", QueueOverflowDropHead, QueueOverflowRejectPublish, QueueOverflowRejectPublishDeadLetter:
	default:
		return ErrUnsupportedQueuePolicy
	}
	if queue.Overflow == QueueOverflowRejectPublishDeadLetter && queue.DeadLetter == nil {
		return ErrUnsupportedQueuePolicy
	}
	if queue.DeadLetter != nil {
		if len(queue.DeadLetter.Exchange) > 255 || containsControl(queue.DeadLetter.Exchange) ||
			(queue.DeadLetter.RoutingKey != nil &&
				(len(*queue.DeadLetter.RoutingKey) > 255 || containsControl(*queue.DeadLetter.RoutingKey))) {
			return ErrUnsupportedQueuePolicy
		}
		switch queue.DeadLetter.Strategy {
		case "", DeadLetterAtMostOnce:
		case DeadLetterAtLeastOnce:
			if queue.Type != QueueQuorum || queue.Overflow != QueueOverflowRejectPublish {
				return ErrUnsupportedQueuePolicy
			}
		default:
			return ErrUnsupportedQueuePolicy
		}
	}

	return nil
}

func validQueueDuration(value *time.Duration, allowZero bool) bool {
	if value == nil {
		return true
	}
	return (*value > 0 || (allowZero && *value == 0)) && *value%time.Millisecond == 0
}

func validConsumerTimeout(value *time.Duration) bool {
	return validQueueDuration(value, true)
}

func validDelayedRetry(value *QueueDelayedRetry) bool {
	if value == nil {
		return true
	}
	if value.Type == DelayedRetryDisabled {
		return value.Minimum == 0 && value.Maximum == nil
	}
	switch value.Type {
	case DelayedRetryAll, DelayedRetryFailed, DelayedRetryReturned:
	default:
		return false
	}
	if value.Minimum <= 0 || value.Minimum%time.Millisecond != 0 {
		return false
	}
	return value.Maximum == nil ||
		(*value.Maximum >= value.Minimum && *value.Maximum%time.Millisecond == 0)
}

func validQueueLength(value *uint64) bool {
	return value == nil || *value <= math.MaxInt64
}

// TopologyMode selects passive broker checks or active development declaration.
type TopologyMode string

const (
	// TopologyPassive requests passive broker checks without declarations or binding mutation.
	TopologyPassive TopologyMode = "passive"
	// TopologyDeclare requests nontransactional development mutation with an explicit permit.
	TopologyDeclare TopologyMode = "declare"
)

// DevelopmentTopologyPermit is an explicit capability for test and local
// topology declaration. Its zero value never permits mutation. This is a local
// opt-in value, not an unforgeable security principal or broker permission.
type DevelopmentTopologyPermit struct{ allowed bool }

// PermitDevelopmentTopology explicitly opts a development or test process
// into topology mutation. Production applications must not call this function.
func PermitDevelopmentTopology() DevelopmentTopologyPermit {
	return DevelopmentTopologyPermit{allowed: true}
}

// TopologyPolicy keeps production verification distinct from development declaration.
type TopologyPolicy struct {
	// Mode must explicitly select passive or declare; the zero policy is invalid.
	Mode TopologyMode
	// Development is required only for declare. The permit is explicit caller intent,
	// not environment detection or RabbitMQ authorization.
	Development DevelopmentTopologyPermit
}

// Validate prevents declaration without an explicit development-only capability.
func (policy TopologyPolicy) Validate() error {
	switch policy.Mode {
	case TopologyPassive:
		return nil
	case TopologyDeclare:
		if policy.Development.allowed {
			return nil
		}
		return ErrTopologyMutationDenied
	default:
		return ErrInvalidTopology
	}
}

// Binding identifies one queue binding without exposing a raw AMQP field table.
// Arguments are supported only for headers exchanges; other built-in exchange
// kinds use the explicit routing key.
type Binding struct {
	// Exchange references a named exchange in the same Topology.
	Exchange string
	// Queue references a named queue in the same Topology.
	Queue string
	// RoutingKey is exchange-specific; headers/fanout require empty and direct/topic allow it.
	RoutingKey string
	// Arguments is a bounded headers-exchange criterion set; omitted x-match means all.
	// Other kinds forbid arguments, and every match mode needs an effective criterion.
	Arguments []Header
}

// Topology is one bounded exchange, queue, and binding graph. Passive AMQP
// checks depend on the broker passive response and do not independently prove
// all declaration properties. AMQP 0-9-1 has
// no passive binding method. Bindings therefore require development declaration
// or separate infrastructure/operator verification.
type Topology struct {
	// Exchanges is a bounded, unique named exchange list, processed first in input order.
	Exchanges []Exchange
	// Queues is a bounded unique named nonexclusive queue list, processed after exchanges.
	Queues []Queue
	// Bindings is a bounded graph reference list, processed last only in declare mode.
	Bindings []Binding
}

// TopologyResult returns verified or declared queue names in Topology.Queues order.
type TopologyResult struct {
	// QueueNames is an owned result slice in input queue order, not a broker topology inventory.
	QueueNames []string
}

// Validate checks graph bounds, identities, references, exchange-specific
// binding rules, lifecycle-safe queues, and the passive-binding protocol
// limitation. Every exclusive queue belongs to its declaring connection;
// ApplyTopology cannot retain one because it closes that connection on return.
func (topology Topology) Validate(policy TopologyPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if len(topology.Exchanges)+len(topology.Queues) == 0 ||
		len(topology.Exchanges) > MaxTopologyExchanges ||
		len(topology.Queues) > MaxTopologyQueues ||
		len(topology.Bindings) > MaxTopologyBindings {
		return ErrInvalidTopology
	}
	if policy.Mode == TopologyPassive && len(topology.Bindings) > 0 {
		return ErrPassiveBindingVerificationUnsupported
	}

	exchanges := make(map[string]ExchangeKind, len(topology.Exchanges))
	for _, exchange := range topology.Exchanges {
		if err := exchange.Validate(); err != nil {
			return err
		}
		if _, exists := exchanges[exchange.Name]; exists {
			return ErrInvalidTopology
		}
		exchanges[exchange.Name] = exchange.Kind
	}
	queues := make(map[string]struct{}, len(topology.Queues))
	for _, queue := range topology.Queues {
		if err := queue.Validate(); err != nil {
			return err
		}
		if queue.Exclusive || queue.Name == "" {
			return ErrInvalidTopology
		}
		if _, exists := queues[queue.Name]; exists {
			return ErrInvalidTopology
		}
		queues[queue.Name] = struct{}{}
	}
	for _, binding := range topology.Bindings {
		kind, exchangeExists := exchanges[binding.Exchange]
		_, queueExists := queues[binding.Queue]
		if !exchangeExists || !queueExists ||
			!validExchangeBinding(kind, binding.RoutingKey, binding.Arguments) {
			return ErrInvalidTopology
		}
	}
	return nil
}

func validExchangeBinding(kind ExchangeKind, routingKey string, arguments []Header) bool {
	return validExchangeBindingWithLimits(kind, routingKey, arguments, DefaultLimits())
}

func validExchangeBindingWithLimits(
	kind ExchangeKind,
	routingKey string,
	arguments []Header,
	limits Limits,
) bool {
	if len(routingKey) > limits.MaxRoutingKeyBytes || containsControl(routingKey) ||
		!validBindingArgumentsWithLimits(arguments, limits) {
		return false
	}
	switch kind {
	case ExchangeDirect, ExchangeTopic:
		return len(arguments) == 0
	case ExchangeFanout:
		return routingKey == "" && len(arguments) == 0
	case ExchangeHeaders:
		return routingKey == "" && validHeadersBindingArguments(arguments)
	default:
		return false
	}
}

func validHeadersBindingArguments(arguments []Header) bool {
	match := "all"
	for _, argument := range arguments {
		if argument.Key != "x-match" {
			continue
		}
		if argument.Kind != HeaderString {
			return false
		}
		switch argument.String {
		case "all", "any", "all-with-x", "any-with-x":
			match = argument.String
		default:
			return false
		}
	}
	includeExtensions := match == "all-with-x" || match == "any-with-x"
	for _, argument := range arguments {
		if argument.Key != "x-match" && (includeExtensions || !strings.HasPrefix(argument.Key, "x-")) {
			return true
		}
	}
	return false
}

func validBindingArguments(arguments []Header) bool {
	return validBindingArgumentsWithLimits(arguments, DefaultLimits())
}

func validBindingArgumentsWithLimits(arguments []Header, limits Limits) bool {
	if len(arguments) > limits.MaxHeaderEntries {
		return false
	}
	seen := make(map[string]struct{}, len(arguments))
	bytes := 0
	for _, argument := range arguments {
		if invalidIdentity(argument.Key, limits.MaxNameBytes) || argument.Key == publishTokenHeader {
			return false
		}
		if _, exists := seen[argument.Key]; exists {
			return false
		}
		seen[argument.Key] = struct{}{}
		bytes += len(argument.Key)
		switch argument.Kind {
		case HeaderString:
			if argument.Bool || argument.Int64 != 0 || argument.Bytes != nil || containsControl(argument.String) {
				return false
			}
			bytes += len(argument.String)
		case HeaderBool:
			if argument.String != "" || argument.Int64 != 0 || argument.Bytes != nil {
				return false
			}
			bytes++
		case HeaderInt64:
			if argument.String != "" || argument.Bool || argument.Bytes != nil {
				return false
			}
			bytes += 8
		case HeaderBytes:
			if argument.String != "" || argument.Bool || argument.Int64 != 0 {
				return false
			}
			bytes += len(argument.Bytes)
		default:
			return false
		}
		if bytes > limits.MaxHeaderBytes {
			return false
		}
	}
	return true
}
