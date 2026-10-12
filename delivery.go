package rabbitmqqueue

import (
	"sort"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// MaxConsumerPrefetch caps per-consumer unsettled delivery QoS.
	MaxConsumerPrefetch = 4096
	// MaxConsumerConcurrency caps concurrent handler workers.
	MaxConsumerConcurrency = 256
	// MaxConsumerRequeues caps configured local requeue policy, not broker-wide attempts.
	MaxConsumerRequeues = 100
	// MaxDeathRecords caps retained x-death records per delivery.
	MaxDeathRecords = 128
	// MaxDeathRoutingKeys caps routing keys retained in each x-death record.
	MaxDeathRoutingKeys = 32
)

const (
	acquiredCountHeader      = "x-acquired-count"
	deliveryCountHeader      = "x-delivery-count"
	deathHeader              = "x-death"
	firstDeathQueueHeader    = "x-first-death-queue"
	firstDeathReasonHeader   = "x-first-death-reason"
	firstDeathExchangeHeader = "x-first-death-exchange"
	lastDeathQueueHeader     = "x-last-death-queue"
	lastDeathReasonHeader    = "x-last-death-reason"
	lastDeathExchangeHeader  = "x-last-death-exchange"
)

func reservedDeliveryMetadataHeader(key string) bool {
	switch key {
	case publishTokenHeader, acquiredCountHeader, deliveryCountHeader, deathHeader,
		firstDeathQueueHeader, firstDeathReasonHeader, firstDeathExchangeHeader,
		lastDeathQueueHeader, lastDeathReasonHeader, lastDeathExchangeHeader:
		return true
	default:
		return false
	}
}

// TransientQueue describes an explicitly client-owned, connection-scoped,
// server-named classic queue bound to an existing exchange. The consumer
// declares and consumes it on the same connection so RabbitMQ can retain the
// exclusive queue for exactly that generation.
type TransientQueue struct {
	// Exchange is an existing exchange checked passively before transient queue setup.
	Exchange Exchange
	// RoutingKey and Arguments follow the exchange-specific binding policy.
	RoutingKey string
	// Arguments contains bounded headers-exchange criteria, copied during opening.
	Arguments []Header
}

// QueueReference identifies either an existing operator-owned queue or an
// explicitly client-owned transient queue. SingleActiveConsumer records
// declaration intent for local policy validation; callers use passive topology
// verification when they need broker evidence for a named queue.
type QueueReference struct {
	// Name is the existing named queue identity, empty only with Transient.
	Name string
	// Type is explicitly QueueClassic or QueueQuorum, not inferred from the broker.
	Type QueueType
	// SingleActiveConsumer asserts expected declaration intent for local exclusivity checks,
	// not verified named-queue topology.
	SingleActiveConsumer bool
	// Transient opts into one server-named exclusive classic queue per generation.
	// Recovery creates a new empty queue; lost-generation backlog is not recovered.
	Transient *TransientQueue
}

// Validate rejects missing queue identities and unsupported queue types.
func (reference QueueReference) Validate() error {
	return reference.validate(DefaultLimits())
}

func (reference QueueReference) validate(limits Limits) error {
	if reference.Transient != nil {
		if reference.Name != "" || reference.Type != QueueClassic || reference.SingleActiveConsumer ||
			reference.Transient.Exchange.Validate() != nil ||
			invalidIdentity(reference.Transient.Exchange.Name, limits.MaxNameBytes) ||
			!validExchangeBindingWithLimits(
				reference.Transient.Exchange.Kind,
				reference.Transient.RoutingKey,
				reference.Transient.Arguments,
				limits,
			) {
			return ErrInvalidConsumer
		}
		return nil
	}
	if invalidIdentity(reference.Name, limits.MaxNameBytes) {
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
// context for graceful draining. MaxRequeues uses RabbitMQ 4.3's quorum
// acquired count first, then the failed-delivery count for quorum Reject when
// available. Otherwise it permits only an initial non-redelivered request.
type ConsumerConfig struct {
	// Limits supplies explicit message/identity/metadata bounds.
	Limits Limits
	// Queue selects a named operator-owned queue or client-owned transient generation.
	Queue QueueReference
	// Name is a required bounded consumer tag, preserved across recovery.
	Name string
	// Priority is optional signed x-priority; nil omits it and explicit zero is emitted.
	Priority *int32
	// Exclusive requests classic-only consumer exclusivity, incompatible with SAC intent.
	Exclusive bool
	// Prefetch is 1..MaxConsumerPrefetch for per-consumer manual unsettled delivery QoS.
	Prefetch int
	// Concurrency is 1..MaxConsumerConcurrency and at most Prefetch, with concurrent handlers.
	Concurrency int
	// HandlerTimeout is positive and at most two minutes, used for cooperative handlers,
	// settlement/setup waits and shared shutdown fallback, not callback preemption.
	HandlerTimeout time.Duration
	// MaxRequeues is at most MaxConsumerRequeues; zero clears every requeue request.
	// It bounds observed broker state, not application-side republishing.
	MaxRequeues uint32
	// Failure is NACK or Reject policy used after handler error, expired context or
	// invalid settlement. It cannot ACK or Delegate; its requeue flag is also bounded.
	Failure Settlement
}

// Validate rejects unbounded consumption and unsafe automatic failure outcomes.
func (config ConsumerConfig) Validate() error {
	if !config.Limits.valid() || config.Queue.validate(config.Limits) != nil ||
		invalidIdentity(config.Name, config.Limits.MaxNameBytes) ||
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
	// Count is the nonnegative broker dead-letter count, not a handler attempt count.
	Count uint64
	// Reason is bounded broker reason text, not an authenticated application decision.
	Reason string
	// Queue is the bounded queue identity where dead lettering occurred.
	Queue string
	// Exchange is the bounded dead-letter source exchange and may be empty.
	Exchange string
	// RoutingKeys is an owned bounded list preserving empty routing keys and broker order.
	RoutingKeys []string
	// Time is the nonnegative broker timestamp, not a package observation clock.
	Time time.Time
	// OriginalExpiration preserves omitted versus explicit-zero original TTL.
	OriginalExpiration *time.Duration
}

// Delivery is an owned, bounded AMQP delivery snapshot. Delivery tags and the
// underlying client delivery never cross the public API boundary. The native
// client has already decoded/allocated it; wrapper limits govern retained copies,
// not hostile wire allocation. Copies share their settlement result and mutable
// fields, so ownership does not mean concurrent mutation safety. Handlers must
// leave acquired/delivery counter pointees unchanged for the observed-state
// requeue bound; the consumer reads the shared values after return. See DEC-001/004.
type Delivery struct {
	// Body is copied opaque payload owned by this delivery; copies of Delivery alias it.
	Body []byte
	// Headers is a sorted, copied application-header subset, excluding reserved metadata.
	Headers []Header
	// MessageID is optional bounded broker message identity, not deduplicated by the package.
	MessageID string
	// CorrelationID is optional application correlation, not a provided RPC lifecycle.
	CorrelationID string
	// ContentType is descriptive metadata without body validation.
	ContentType string
	// ContentEncoding is descriptive metadata without decoding or decompression.
	ContentEncoding string
	// ReplyTo is optional application-owned reply routing metadata.
	ReplyTo string
	// Type is bounded application type metadata.
	Type string
	// UserID is bounded broker property text; wrapper conversion does not authorize it.
	UserID string
	// AppID is bounded publisher property text, not wrapper-verified identity.
	AppID string
	// Timestamp is the broker property, with zero allowed; conversion does not normalize it.
	Timestamp time.Time
	// Expiration distinguishes an omitted TTL from RabbitMQ's explicit
	// zero-duration immediate-expiration value.
	Expiration *time.Duration
	// Priority is the AMQP octet; broker queue-type policy controls its effect.
	Priority uint8
	// DeliveryMode is normalized to transient for native zero/transient and persistent for two.
	DeliveryMode DeliveryMode
	// Consumer is the bounded broker consumer tag, without exposing the delivery tag.
	Consumer string
	// Exchange is the bounded broker routing exchange, which can be empty.
	Exchange string
	// RoutingKey is the bounded broker route, preserving native empty keys.
	RoutingKey string
	// Redelivered is the broker flag, not proof of a prior handler invocation.
	Redelivered bool
	// AcquiredCount is optional nonnegative RabbitMQ 4.3 assignment count, preferred for
	// local quorum requeue bounds.
	AcquiredCount *uint64
	// DeliveryCount is optional nonnegative failed-delivery count, distinct from assignments.
	DeliveryCount *uint64
	// Deaths is owned bounded x-death history. Copies of Delivery share this slice
	// and pointer fields; synchronize any mutations across copies.
	Deaths     []Death
	settlement *deliverySettlement
}

func deliveryFromAMQP(source amqp.Delivery, config ConsumerConfig) (Delivery, error) {
	if consumerOperationFailed(config.Validate()) {
		return Delivery{}, ErrInvalidDelivery
	}
	if !validDeliveryTag(source.DeliveryTag) {
		return Delivery{}, ErrInvalidDelivery
	}
	if invalidIdentity(source.ConsumerTag, config.Limits.MaxNameBytes) ||
		len(source.RoutingKey) > config.Limits.MaxRoutingKeyBytes ||
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
	deathSummaryBytes, err := deliveryDeathSummaryBytes(source.Headers, config.Limits)
	if err != nil || !fitsRemainingHeaderBudget(deathSummaryBytes, metadataBytes, config.Limits.MaxHeaderBytes) {
		return Delivery{}, ErrInvalidDelivery
	}
	metadataBytes += deathSummaryBytes
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
		DeliveryCount: deliveryCount, Deaths: deaths, settlement: newDeliverySettlement(),
	}, nil
}

func validDeliveryTag(tag uint64) bool {
	return tag > 0
}

func fitsRemainingHeaderBudget(additional, used, maximum int) bool {
	return additional <= maximum-used
}

func parseDeliveryExpiration(value string) (*time.Duration, error) {
	if value == "" {
		return nil, nil
	}
	milliseconds, err := strconv.ParseUint(value, 10, 63)
	if err != nil || milliseconds > uint64((time.Duration(1<<63-1))/time.Millisecond) {
		return nil, ErrInvalidDelivery
	}
	expiration := time.Duration(milliseconds) * time.Millisecond
	return &expiration, nil
}

func deliveryHeaders(table amqp.Table, limits Limits) ([]Header, int, error) {
	if value, exists := table[publishTokenHeader]; exists {
		token, ok := value.(string)
		if !ok || invalidIdentity(token, maxPublishTokenBytes) {
			return nil, 0, ErrInvalidDelivery
		}
	}
	count, err := admittedDeliveryHeaderCount(table, limits)
	if err != nil {
		return nil, 0, err
	}
	keys := make([]string, 0, count)
	for key := range table {
		if !reservedDeliveryMetadataHeader(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	headers := make([]Header, 0, len(keys))
	bytes := 0
	for _, key := range keys {
		bytes += len(key)
		// The borrowed table is immutable during this conversion. Admission
		// already validated the same authoritative type and size mapping.
		header, size, _ := borrowedDeliveryHeader(key, table[key])
		bytes += size
		if header.Kind == HeaderBytes {
			header.Bytes = append([]byte(nil), header.Bytes...)
		}
		headers = append(headers, header)
	}
	return headers, bytes, nil
}

// Inspect borrowed values before allocating key storage or owned byte copies.
// Reserved metadata has its own bounded validators and is not application data.
func admittedDeliveryHeaderCount(table amqp.Table, limits Limits) (int, error) {
	count, bytes := 0, 0
	for key, value := range table {
		if reservedDeliveryMetadataHeader(key) {
			continue
		}
		count++
		if count > limits.MaxHeaderEntries || invalidIdentity(key, limits.MaxNameBytes) ||
			!fitsRemainingHeaderBudget(len(key), bytes, limits.MaxHeaderBytes) {
			return 0, ErrInvalidDelivery
		}
		bytes += len(key)
		_, size, ok := borrowedDeliveryHeader(key, value)
		if !ok || !fitsRemainingHeaderBudget(size, bytes, limits.MaxHeaderBytes) {
			return 0, ErrInvalidDelivery
		}
		bytes += size
	}
	return count, nil
}

func deliveryDeathSummaryBytes(table amqp.Table, limits Limits) (int, error) {
	type summaryField struct {
		key        string
		allowEmpty bool
	}
	fields := [...]summaryField{
		{key: firstDeathQueueHeader},
		{key: firstDeathReasonHeader},
		{key: firstDeathExchangeHeader, allowEmpty: true},
		{key: lastDeathQueueHeader},
		{key: lastDeathReasonHeader},
		{key: lastDeathExchangeHeader, allowEmpty: true},
	}
	bytes := 0
	for _, field := range fields {
		value, exists := table[field.key]
		if !exists {
			continue
		}
		text, ok := validDeathSummaryField(value, field.allowEmpty, limits)
		if !ok {
			return 0, ErrInvalidDelivery
		}
		bytes += len(text)
		if bytes > limits.MaxHeaderBytes {
			return 0, ErrInvalidDelivery
		}
	}
	return bytes, nil
}

func validDeathSummaryField(value any, allowEmpty bool, limits Limits) (string, bool) {
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	if len(text) > limits.MaxNameBytes {
		return "", false
	}
	if containsControl(text) {
		return "", false
	}
	if !allowEmpty && invalidIdentity(text, limits.MaxNameBytes) {
		return "", false
	}
	return text, true
}

// Return a borrowed descriptor; only the admitted materialization owns bytes.
func borrowedDeliveryHeader(key string, value any) (Header, int, bool) {
	switch value := value.(type) {
	case string:
		return StringHeader(key, value), len(value), true
	case bool:
		return BoolHeader(key, value), 1, true
	case []byte:
		return Header{Key: key, Kind: HeaderBytes, Bytes: value}, len(value), true
	case int8:
		return Int64Header(key, int64(value)), 8, true
	case int16:
		return Int64Header(key, int64(value)), 8, true
	case int32:
		return Int64Header(key, int64(value)), 8, true
	case int64:
		return Int64Header(key, value), 8, true
	case uint8:
		return Int64Header(key, int64(value)), 8, true
	case uint16:
		return Int64Header(key, int64(value)), 8, true
	case uint32:
		return Int64Header(key, int64(value)), 8, true
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
	if !validDeliveryDeathFields(reason, reasonOK, queue, queueOK, exchange, exchangeOK, deathTime, timeOK, limits) {
		return Death{}, 0, ErrInvalidDelivery
	}
	routingValues, ok := fields["routing-keys"].([]any)
	if !ok || len(routingValues) > MaxDeathRoutingKeys {
		return Death{}, 0, ErrInvalidDelivery
	}
	originalExpiration, expirationBytes, err := deathOriginalExpiration(fields)
	if err != nil {
		return Death{}, 0, err
	}
	routingKeys := make([]string, 0, len(routingValues))
	metadataBytes := deliveryDeathMetadataBytes(reason, queue, exchange, expirationBytes)
	for _, value := range routingValues {
		routingKey, ok := value.(string)
		if !ok || len(routingKey) > limits.MaxRoutingKeyBytes || containsControl(routingKey) {
			return Death{}, 0, ErrInvalidDelivery
		}
		metadataBytes += len(routingKey)
		routingKeys = append(routingKeys, routingKey)
	}
	return Death{
		Count: count, Reason: reason, Queue: queue, Exchange: exchange,
		RoutingKeys: routingKeys, Time: deathTime, OriginalExpiration: originalExpiration,
	}, metadataBytes, nil
}

func validDeliveryDeathFields(
	reason string,
	reasonOK bool,
	queue string,
	queueOK bool,
	exchange string,
	exchangeOK bool,
	deathTime time.Time,
	timeOK bool,
	limits Limits,
) bool {
	if !reasonOK || invalidIdentity(reason, limits.MaxNameBytes) {
		return false
	}
	if !queueOK || invalidIdentity(queue, limits.MaxNameBytes) {
		return false
	}
	if !exchangeOK || len(exchange) > limits.MaxNameBytes || containsControl(exchange) {
		return false
	}
	return timeOK && !deathTime.Before(time.Unix(0, 0))
}

func deliveryDeathMetadataBytes(reason, queue, exchange string, expirationBytes int) int {
	return 16 + len(reason) + len(queue) + len(exchange) + expirationBytes
}

func deathOriginalExpiration(fields amqp.Table) (*time.Duration, int, error) {
	value, exists := fields["original-expiration"]
	if !exists {
		return nil, 0, nil
	}
	encoded, ok := value.(string)
	if !ok || encoded == "" {
		return nil, 0, ErrInvalidDelivery
	}
	expiration, err := parseDeliveryExpiration(encoded)
	if err != nil {
		return nil, 0, err
	}
	return expiration, len(encoded), nil
}

func unsignedAMQPInteger(value any) (uint64, bool) {
	switch value := value.(type) {
	case int8:
		// #nosec G115 -- Nonnegative signed values fit uint64; negative values return invalid and callers reject them.
		return uint64(value), value >= 0
	case int16:
		// #nosec G115 -- Nonnegative signed values fit uint64; negative values return invalid and callers reject them.
		return uint64(value), value >= 0
	case int32:
		// #nosec G115 -- Nonnegative signed values fit uint64; negative values return invalid and callers reject them.
		return uint64(value), value >= 0
	case int64:
		// #nosec G115 -- Nonnegative signed values fit uint64; negative values return invalid and callers reject them.
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
