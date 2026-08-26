package rabbitmqqueue

import (
	"math"
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
	ExchangeDirect  ExchangeKind = "direct"
	ExchangeTopic   ExchangeKind = "topic"
	ExchangeFanout  ExchangeKind = "fanout"
	ExchangeHeaders ExchangeKind = "headers"
)

// Exchange is a stable exchange identity and equivalence policy.
type Exchange struct {
	Name       string
	Kind       ExchangeKind
	Durable    bool
	AutoDelete bool
	Internal   bool
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
	QueueClassic QueueType = "classic"
	QueueQuorum  QueueType = "quorum"
)

// QueueOverflow selects RabbitMQ's queue-length overflow behavior.
type QueueOverflow string

const (
	QueueOverflowDropHead                QueueOverflow = "drop-head"
	QueueOverflowRejectPublish           QueueOverflow = "reject-publish"
	QueueOverflowRejectPublishDeadLetter QueueOverflow = "reject-publish-dlx"
)

// DeadLetterStrategy selects quorum queue dead-letter transfer guarantees.
// The zero value leaves the broker's at-most-once default implicit.
type DeadLetterStrategy string

const (
	DeadLetterAtMostOnce  DeadLetterStrategy = "at-most-once"
	DeadLetterAtLeastOnce DeadLetterStrategy = "at-least-once"
)

// QueueDeadLetter describes declaration-time dead-letter arguments. An empty
// Exchange explicitly selects the AMQP default exchange. A nil RoutingKey
// omits the argument and preserves original routing keys; a pointer to an empty
// string emits an explicit empty routing key. RabbitMQ policies are preferred
// for production configuration because they remain mutable.
type QueueDeadLetter struct {
	Exchange   string
	RoutingKey *string
	Strategy   DeadLetterStrategy
}

// Queue describes declaration-equivalent queue policy. A zero Name requests a
// server-generated name and is valid only for an exclusive classic queue.
// MessageTTL and the length pointers distinguish an explicit zero argument
// from omission. Expires, when present, must be a positive millisecond value.
type Queue struct {
	Name                 string
	Type                 QueueType
	Durable              bool
	AutoDelete           bool
	Exclusive            bool
	SingleActiveConsumer bool
	DeliveryLimit        uint32
	MaxPriority          uint8
	MessageTTL           *time.Duration
	Expires              *time.Duration
	MaxLength            *uint64
	MaxLengthBytes       *uint64
	Overflow             QueueOverflow
	DeadLetter           *QueueDeadLetter
}

// Validate rejects policies that RabbitMQ cannot apply to the selected queue type.
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
		if queue.DeliveryLimit != 0 || (queue.Exclusive && queue.Durable) ||
			(!queue.Durable && !queue.Exclusive) ||
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

func validQueueLength(value *uint64) bool {
	return value == nil || *value <= math.MaxInt64
}

// TopologyMode selects passive equivalence verification or active declaration.
type TopologyMode string

const (
	TopologyPassive TopologyMode = "passive"
	TopologyDeclare TopologyMode = "declare"
)

// DevelopmentTopologyPermit is an explicit capability for test and local
// topology declaration. Its zero value never permits mutation.
type DevelopmentTopologyPermit struct{ allowed bool }

// PermitDevelopmentTopology explicitly opts a development or test process
// into topology mutation. Production applications must not call this function.
func PermitDevelopmentTopology() DevelopmentTopologyPermit {
	return DevelopmentTopologyPermit{allowed: true}
}

// TopologyPolicy keeps production verification distinct from development declaration.
type TopologyPolicy struct {
	Mode        TopologyMode
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
	Exchange   string
	Queue      string
	RoutingKey string
	Arguments  []Header
}

// Topology is one bounded exchange, queue, and binding graph. Passive AMQP
// verification can compare exchange and queue declarations, but AMQP 0-9-1 has
// no passive binding method. Bindings therefore require development declaration
// or separate infrastructure/operator verification.
type Topology struct {
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

// TopologyResult returns resolved queue names in Topology.Queues order. This is
// relevant to development-only server-named queue declaration.
type TopologyResult struct {
	QueueNames []string
}

// Validate checks graph bounds, identities, references, exchange-specific
// binding rules, and the passive-binding protocol limitation.
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
		if queue.Name == "" {
			if policy.Mode != TopologyDeclare {
				return ErrInvalidTopology
			}
			continue
		}
		if _, exists := queues[queue.Name]; exists {
			return ErrInvalidTopology
		}
		queues[queue.Name] = struct{}{}
	}
	for _, binding := range topology.Bindings {
		kind, exchangeExists := exchanges[binding.Exchange]
		_, queueExists := queues[binding.Queue]
		if !exchangeExists || !queueExists || invalidIdentity(binding.Exchange, 255) ||
			invalidIdentity(binding.Queue, 255) || len(binding.RoutingKey) > 255 ||
			containsControl(binding.RoutingKey) || !validBindingArguments(binding.Arguments) {
			return ErrInvalidTopology
		}
		switch kind {
		case ExchangeDirect, ExchangeTopic:
			if binding.RoutingKey == "" || len(binding.Arguments) != 0 {
				return ErrInvalidTopology
			}
		case ExchangeFanout:
			if binding.RoutingKey != "" || len(binding.Arguments) != 0 {
				return ErrInvalidTopology
			}
		case ExchangeHeaders:
			if binding.RoutingKey != "" || len(binding.Arguments) == 0 {
				return ErrInvalidTopology
			}
		default:
			return ErrUnsupportedExchangeKind
		}
	}
	return nil
}

func validBindingArguments(arguments []Header) bool {
	limits := DefaultLimits()
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
