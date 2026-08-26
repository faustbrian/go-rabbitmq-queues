package rabbitmqqueue

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

// Queue describes declaration-equivalent queue policy. A zero Name requests a
// server-generated name and is valid only for an exclusive classic queue.
type Queue struct {
	Name                 string
	Type                 QueueType
	Durable              bool
	AutoDelete           bool
	Exclusive            bool
	SingleActiveConsumer bool
	DeliveryLimit        uint32
	MaxPriority          uint8
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
		if queue.DeliveryLimit != 0 || (queue.Exclusive && queue.Durable) {
			return ErrUnsupportedQueuePolicy
		}
	case QueueQuorum:
		if !queue.Durable || queue.Exclusive || queue.AutoDelete || queue.MaxPriority != 0 {
			return ErrUnsupportedQueuePolicy
		}
	default:
		return ErrUnsupportedQueuePolicy
	}

	return nil
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
