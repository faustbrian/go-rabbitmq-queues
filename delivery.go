package rabbitmqqueue

import (
	"sort"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MaxConsumerPrefetch    = 4096
	MaxConsumerConcurrency = 256
	MaxConsumerRequeues    = 100
	MaxDeathRecords        = 128
	MaxDeathRoutingKeys    = 32
)

const (
	acquiredCountHeader = "x-acquired-count"
	deliveryCountHeader = "x-delivery-count"
	deathHeader         = "x-death"
)

// TransientQueue describes an explicitly client-owned, connection-scoped,
// server-named classic queue bound to an existing exchange. The consumer
// declares and consumes it on the same connection so RabbitMQ can retain the
// exclusive queue for exactly that generation.
type TransientQueue struct {
	Exchange   Exchange
	RoutingKey string
	Arguments  []Header
}

// QueueReference identifies either an existing operator-owned queue or an
// explicitly client-owned transient queue. SingleActiveConsumer records
// declaration intent for local policy validation; callers use passive topology
// verification when they need broker evidence for a named queue.
type QueueReference struct {
	Name                 string
	Type                 QueueType
	SingleActiveConsumer bool
	Transient            *TransientQueue
}

// Validate rejects missing queue identities and unsupported queue types.
func (reference QueueReference) Validate() error {
	if reference.Transient != nil {
		if reference.Name != "" || reference.Type != QueueClassic || reference.SingleActiveConsumer ||
			reference.Transient.Exchange.Validate() != nil ||
			!validExchangeBinding(
				reference.Transient.Exchange.Kind,
				reference.Transient.RoutingKey,
				reference.Transient.Arguments,
			) {
			return ErrInvalidConsumer
		}
		return nil
	}
	if invalidIdentity(reference.Name, 255) {
		return ErrInvalidConsumer
	}
	switch reference.Type {
	case QueueClassic, QueueQuorum:
		return nil
	default:
		return ErrInvalidConsumer
	}
}

// ConsumerConfig bounds one independent manual-settlement consumer. Priority
// distinguishes an omitted RabbitMQ default from an explicit signed value,
// including zero. Exclusive requests classic-queue exclusivity and cannot be
// combined with single-active-consumer topology. HandlerTimeout also bounds
// settlement and supplies the shutdown fallback; handlers must observe their
// context for graceful draining.
type ConsumerConfig struct {
	Limits         Limits
	Queue          QueueReference
	Name           string
	Priority       *int32
	Exclusive      bool
	Prefetch       int
	Concurrency    int
	HandlerTimeout time.Duration
	MaxRequeues    uint32
	Failure        Settlement
}

// Validate rejects unbounded consumption and unsafe automatic failure outcomes.
func (config ConsumerConfig) Validate() error {
	if !config.Limits.valid() || config.Queue.Validate() != nil || invalidIdentity(config.Name, 255) ||
		(config.Queue.Transient != nil && !validExchangeBindingWithLimits(
			config.Queue.Transient.Exchange.Kind,
			config.Queue.Transient.RoutingKey,
			config.Queue.Transient.Arguments,
			config.Limits,
		)) ||
		(config.Exclusive && (config.Queue.Type != QueueClassic || config.Queue.SingleActiveConsumer)) ||
		config.Prefetch < 1 || config.Prefetch > MaxConsumerPrefetch ||
		config.Concurrency < 1 || config.Concurrency > MaxConsumerConcurrency ||
		config.Concurrency > config.Prefetch || config.HandlerTimeout <= 0 ||
		config.HandlerTimeout > maximumDialTimeout || config.MaxRequeues > MaxConsumerRequeues ||
		config.Failure.Validate() != nil ||
		config.Failure.Method == SettlementAcknowledge || config.Failure.Method == SettlementDelegate {
		return ErrInvalidConsumer
	}
	return nil
}

func ownConsumerConfig(config ConsumerConfig) ConsumerConfig {
	if config.Priority != nil {
		priority := *config.Priority
		config.Priority = &priority
	}
	if config.Queue.Transient != nil {
		transient := *config.Queue.Transient
		transient.Arguments = append([]Header(nil), transient.Arguments...)
		for index := range transient.Arguments {
			transient.Arguments[index].Bytes = append([]byte(nil), transient.Arguments[index].Bytes...)
		}
		config.Queue.Transient = &transient
	}
	return config
}

// Death preserves one bounded RabbitMQ x-death record without exposing a raw field table.
type Death struct {
	Count       uint64
	Reason      string
	Queue       string
	Exchange    string
	RoutingKeys []string
	Time        time.Time
}

// Delivery is an owned, bounded AMQP delivery snapshot. Delivery tags and the
// underlying client delivery never cross the public API boundary.
type Delivery struct {
	Body            []byte
	Headers         []Header
	MessageID       string
	CorrelationID   string
	ContentType     string
	ContentEncoding string
	ReplyTo         string
	Type            string
	UserID          string
	AppID           string
	Timestamp       time.Time
	Expiration      time.Duration
	Priority        uint8
	DeliveryMode    DeliveryMode
	Consumer        string
	Exchange        string
	RoutingKey      string
	Redelivered     bool
	AcquiredCount   *uint64
	DeliveryCount   *uint64
	Deaths          []Death
}

func deliveryFromAMQP(source amqp.Delivery, config ConsumerConfig) (Delivery, error) {
	if err := config.Validate(); err != nil || source.DeliveryTag == 0 ||
		invalidIdentity(source.ConsumerTag, config.Limits.MaxNameBytes) ||
		source.RoutingKey == "" || len(source.RoutingKey) > config.Limits.MaxRoutingKeyBytes ||
		containsControl(source.RoutingKey) || len(source.Exchange) > config.Limits.MaxNameBytes ||
		containsControl(source.Exchange) || len(source.Body) > config.Limits.MaxPayloadBytes {
		return Delivery{}, ErrInvalidDelivery
	}
	values := []string{source.MessageId, source.CorrelationId, source.ContentType,
		source.ContentEncoding, source.ReplyTo, source.Type, source.UserId, source.AppId}
	for _, value := range values {
		if len(value) > config.Limits.MaxNameBytes || containsControl(value) {
			return Delivery{}, ErrInvalidDelivery
		}
	}
	if !source.Timestamp.IsZero() && source.Timestamp.Before(time.Unix(0, 0)) {
		return Delivery{}, ErrInvalidDelivery
	}
	expiration, err := parseDeliveryExpiration(source.Expiration)
	if err != nil {
		return Delivery{}, ErrInvalidDelivery
	}
	headers, metadataBytes, err := deliveryHeaders(source.Headers, config.Limits)
	if err != nil {
		return Delivery{}, err
	}
	acquiredCount, err := deliveryCounter(source.Headers, acquiredCountHeader)
	if err != nil {
		return Delivery{}, err
	}
	deliveryCount, err := deliveryCounter(source.Headers, deliveryCountHeader)
	if err != nil {
		return Delivery{}, err
	}
	for _, count := range []*uint64{acquiredCount, deliveryCount} {
		if count == nil {
			continue
		}
		metadataBytes += 8
		if metadataBytes > config.Limits.MaxHeaderBytes {
			return Delivery{}, ErrInvalidDelivery
		}
	}
	deaths, err := deliveryDeaths(source.Headers, config.Limits, metadataBytes)
	if err != nil {
		return Delivery{}, err
	}
	mode := DeliveryTransient
	if source.DeliveryMode == amqp.Persistent {
		mode = DeliveryPersistent
	} else if source.DeliveryMode != 0 && source.DeliveryMode != amqp.Transient {
		return Delivery{}, ErrInvalidDelivery
	}
	return Delivery{
		Body: append([]byte(nil), source.Body...), Headers: headers,
		MessageID: source.MessageId, CorrelationID: source.CorrelationId,
		ContentType: source.ContentType, ContentEncoding: source.ContentEncoding,
		ReplyTo: source.ReplyTo, Type: source.Type, UserID: source.UserId, AppID: source.AppId,
		Timestamp: source.Timestamp, Expiration: expiration, Priority: source.Priority, DeliveryMode: mode,
		Consumer: source.ConsumerTag, Exchange: source.Exchange, RoutingKey: source.RoutingKey,
		Redelivered: source.Redelivered, AcquiredCount: acquiredCount,
		DeliveryCount: deliveryCount, Deaths: deaths,
	}, nil
}

func parseDeliveryExpiration(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	milliseconds, err := strconv.ParseUint(value, 10, 63)
	if err != nil || milliseconds > uint64((time.Duration(1<<63-1))/time.Millisecond) {
		return 0, ErrInvalidDelivery
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

func deliveryHeaders(table amqp.Table, limits Limits) ([]Header, int, error) {
	keys := make([]string, 0, len(table))
	for key := range table {
		if key != acquiredCountHeader && key != deathHeader &&
			key != deliveryCountHeader && key != publishTokenHeader {
			keys = append(keys, key)
		}
	}
	if len(keys) > limits.MaxHeaderEntries {
		return nil, 0, ErrInvalidDelivery
	}
	sort.Strings(keys)
	headers := make([]Header, 0, len(keys))
	bytes := 0
	for _, key := range keys {
		if invalidIdentity(key, limits.MaxNameBytes) {
			return nil, 0, ErrInvalidDelivery
		}
		bytes += len(key)
		header, size, ok := stableDeliveryHeader(key, table[key])
		if !ok {
			return nil, 0, ErrInvalidDelivery
		}
		bytes += size
		if bytes > limits.MaxHeaderBytes {
			return nil, 0, ErrInvalidDelivery
		}
		headers = append(headers, header)
	}
	return headers, bytes, nil
}

func stableDeliveryHeader(key string, value any) (Header, int, bool) {
	switch value := value.(type) {
	case string:
		return StringHeader(key, value), len(value), !containsControl(value)
	case bool:
		return BoolHeader(key, value), 1, true
	case []byte:
		return BytesHeader(key, value), len(value), true
	case int8:
		return Int64Header(key, int64(value)), 8, true
	case int16:
		return Int64Header(key, int64(value)), 8, true
	case int32:
		return Int64Header(key, int64(value)), 8, true
	case int64:
		return Int64Header(key, value), 8, true
	default:
		return Header{}, 0, false
	}
}

func deliveryCounter(table amqp.Table, key string) (*uint64, error) {
	value, exists := table[key]
	if !exists {
		return nil, nil
	}
	count, ok := unsignedAMQPInteger(value)
	if !ok {
		return nil, ErrInvalidDelivery
	}
	return &count, nil
}

func deliveryDeaths(table amqp.Table, limits Limits, metadataBytes int) ([]Death, error) {
	value, exists := table[deathHeader]
	if !exists {
		return nil, nil
	}
	records, ok := value.([]any)
	if !ok || len(records) > MaxDeathRecords {
		return nil, ErrInvalidDelivery
	}
	deaths := make([]Death, 0, len(records))
	for _, record := range records {
		fields, ok := record.(amqp.Table)
		if !ok {
			return nil, ErrInvalidDelivery
		}
		death, size, err := deliveryDeath(fields, limits)
		if err != nil {
			return nil, err
		}
		if size > limits.MaxHeaderBytes-metadataBytes {
			return nil, ErrInvalidDelivery
		}
		metadataBytes += size
		deaths = append(deaths, death)
	}
	return deaths, nil
}

func deliveryDeath(fields amqp.Table, limits Limits) (Death, int, error) {
	count, ok := unsignedAMQPInteger(fields["count"])
	if !ok {
		return Death{}, 0, ErrInvalidDelivery
	}
	reason, reasonOK := fields["reason"].(string)
	queue, queueOK := fields["queue"].(string)
	exchange, exchangeOK := fields["exchange"].(string)
	deathTime, timeOK := fields["time"].(time.Time)
	if !reasonOK || !queueOK || !exchangeOK || !timeOK || deathTime.Before(time.Unix(0, 0)) ||
		invalidIdentity(reason, limits.MaxNameBytes) || invalidIdentity(queue, limits.MaxNameBytes) ||
		len(exchange) > limits.MaxNameBytes || containsControl(exchange) {
		return Death{}, 0, ErrInvalidDelivery
	}
	routingValues, ok := fields["routing-keys"].([]any)
	if !ok || len(routingValues) > MaxDeathRoutingKeys {
		return Death{}, 0, ErrInvalidDelivery
	}
	routingKeys := make([]string, 0, len(routingValues))
	metadataBytes := 16 + len(reason) + len(queue) + len(exchange)
	for _, value := range routingValues {
		routingKey, ok := value.(string)
		if !ok || routingKey == "" || len(routingKey) > limits.MaxRoutingKeyBytes || containsControl(routingKey) {
			return Death{}, 0, ErrInvalidDelivery
		}
		metadataBytes += len(routingKey)
		routingKeys = append(routingKeys, routingKey)
	}
	return Death{Count: count, Reason: reason, Queue: queue, Exchange: exchange, RoutingKeys: routingKeys, Time: deathTime}, metadataBytes, nil
}

func unsignedAMQPInteger(value any) (uint64, bool) {
	switch value := value.(type) {
	case int8:
		return uint64(value), value >= 0
	case int16:
		return uint64(value), value >= 0
	case int32:
		return uint64(value), value >= 0
	case int64:
		return uint64(value), value >= 0
	case uint8:
		return uint64(value), true
	case uint16:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case uint64:
		return value, true
	default:
		return 0, false
	}
}
