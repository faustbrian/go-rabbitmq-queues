package rabbitmqqueue

import "time"

const (
	publishTokenHeader      = "x-rabbitmqqueue-publish-token" // #nosec G101 -- Public AMQP header name, not a token value or credential.
	maxProducerSessionBytes = 128
	maxPublishTokenBytes    = maxProducerSessionBytes + 1 + 20
)

// Limits bounds untrusted message and topology-controlled allocation. Values
// may be lowered from DefaultLimits but cannot raise the package safety caps.
type Limits struct {
	// MaxPayloadBytes bounds one opaque body; the package cap is 16 MiB.
	MaxPayloadBytes int
	// MaxHeaderEntries bounds application headers; the package cap is 128.
	MaxHeaderEntries int
	// MaxHeaderBytes bounds cumulative key/value data; the cap is 64 KiB.
	// This is retained wrapper data, not the native AMQP decoder allocation.
	MaxHeaderBytes int
	// MaxNameBytes bounds message/consumer identities and properties; the cap is 255 bytes.
	MaxNameBytes int
	// MaxRoutingKeyBytes bounds a routing key; the cap is 255 bytes.
	MaxRoutingKeyBytes int
}

// DefaultLimits returns conservative RabbitMQ 4.x policy bounds.
func DefaultLimits() Limits {
	return Limits{
		MaxPayloadBytes:    16 << 20,
		MaxHeaderEntries:   128,
		MaxHeaderBytes:     64 << 10,
		MaxNameBytes:       255,
		MaxRoutingKeyBytes: 255,
	}
}

func (limits Limits) valid() bool {
	maximum := DefaultLimits()
	return limits.MaxPayloadBytes > 0 && limits.MaxPayloadBytes <= maximum.MaxPayloadBytes &&
		limits.MaxHeaderEntries > 0 && limits.MaxHeaderEntries <= maximum.MaxHeaderEntries &&
		limits.MaxHeaderBytes > 0 && limits.MaxHeaderBytes <= maximum.MaxHeaderBytes &&
		limits.MaxNameBytes > 0 && limits.MaxNameBytes <= maximum.MaxNameBytes &&
		limits.MaxRoutingKeyBytes > 0 && limits.MaxRoutingKeyBytes <= maximum.MaxRoutingKeyBytes
}

// HeaderKind identifies a bounded, language-neutral AMQP field-table value.
type HeaderKind uint8

const (
	// HeaderString selects an opaque string field-table value.
	HeaderString HeaderKind = iota + 1
	// HeaderBool selects a boolean field-table value.
	HeaderBool
	// HeaderInt64 selects a signed 64-bit field-table integer.
	HeaderInt64
	// HeaderBytes selects opaque byte-string field-table data.
	HeaderBytes
)

// Header is one ordered AMQP application header. Nested tables and arrays are
// intentionally excluded to keep allocation and interoperability bounded.
type Header struct {
	// Key is unique within a message and cannot use package/broker reserved keys.
	Key string
	// Kind selects the sole active value field; unused fields must be zero/nil.
	Kind HeaderKind
	// String contains opaque bounded string bytes for HeaderString, including controls.
	String string
	// Bool contains the HeaderBool value.
	Bool bool
	// Int64 contains the HeaderInt64 value, including negative values.
	Int64 int64
	// Bytes contains opaque HeaderBytes data. Direct struct assignment borrows the slice;
	// BytesHeader copies it, and publication admission later copies admitted bytes.
	Bytes []byte
}

// StringHeader creates a string application header.
func StringHeader(key, value string) Header {
	return Header{Key: key, Kind: HeaderString, String: value}
}

// BoolHeader creates a boolean application header.
func BoolHeader(key string, value bool) Header {
	return Header{Key: key, Kind: HeaderBool, Bool: value}
}

// Int64Header creates a signed 64-bit integer application header.
func Int64Header(key string, value int64) Header {
	return Header{Key: key, Kind: HeaderInt64, Int64: value}
}

// BytesHeader creates a byte-string application header with an owned value copy.
func BytesHeader(key string, value []byte) Header {
	return Header{Key: key, Kind: HeaderBytes, Bytes: append([]byte(nil), value...)}
}

// Message contains AMQP message properties and opaque payload bytes.
type Message struct {
	// Body is opaque caller data, borrowed until publication validation/admission copy.
	Body []byte
	// MessageID is required for publication, bounded but not deduplicated or authenticated.
	MessageID string
	// CorrelationID is optional application correlation; no request/reply lifecycle is provided.
	CorrelationID string
	// ReplyTo is optional application-owned reply routing metadata, not a created queue.
	ReplyTo string
	// ContentType is bounded descriptive metadata; no body parsing or schema validation occurs.
	ContentType string
	// ContentEncoding is bounded descriptive metadata; no compression/decompression is performed.
	ContentEncoding string
	// Type is optional bounded application type metadata.
	Type string
	// AppID is optional bounded application identity, not verified authorization.
	AppID string
	// Timestamp is either zero or a non-negative whole-second AMQP timestamp.
	Timestamp time.Time
	// Expiration distinguishes an omitted TTL from an explicit zero-duration
	// TTL, which RabbitMQ interprets as immediate expiration when the message
	// cannot be delivered directly.
	Expiration *time.Duration
	// Priority distinguishes omission from an explicit value in 0..255 for validation.
	// AMQP publication encodes omission and zero identically; queue support is external.
	Priority *uint16
	// Headers is ordered caller metadata validated for kinds, uniqueness and budgets.
	// AMQP table ordering is not a wire contract.
	Headers []Header
}

// DeliveryMode selects broker persistence intent.
type DeliveryMode uint8

const (
	// DeliveryTransient requests transient AMQP delivery mode, without persistence intent.
	DeliveryTransient DeliveryMode = iota + 1
	// DeliveryPersistent requests persistent AMQP delivery mode, not exactly-once effects.
	DeliveryPersistent
)

// Publication binds one message to explicit AMQP routing policy.
type Publication struct {
	// Exchange is a named exchange identity, or the empty default-exchange
	// identity when ExchangeKind is explicitly ExchangeDirect.
	Exchange string
	// ExchangeKind records the expected routing semantic for local validation.
	// Omit it only for non-empty direct/topic-compatible routing keys. An
	// explicit direct or topic kind may use RabbitMQ's native empty key; fanout
	// and headers publications must name their kind and use an empty key.
	ExchangeKind ExchangeKind
	// RoutingKey follows ExchangeKind policy; explicit direct/topic kinds allow empty keys.
	RoutingKey string
	// Mandatory requests a return for unroutable publications; false can confirm a drop.
	Mandatory bool
	// DeliveryMode expresses broker persistence intent, not application-effect durability.
	DeliveryMode DeliveryMode
	// Message is validated before admission and deep-copied only once admitted.
	Message Message
}

// Validate checks explicit routing, required MessageID, property byte/control
// limits, whole-second nonnegative timestamps, millisecond nonnegative expiration
// and header kind/count/byte/uniqueness rules. It performs no I/O, body/schema
// validation, UTF-8 normalization, authorization or broker topology check.
// Caller-owned slices and pointer values must remain stable while it reads them.
func (publication Publication) Validate(limits Limits) error {
	if !limits.valid() {
		return ErrInvalidBounds
	}
	if !validPublicationExchange(publication.Exchange, publication.ExchangeKind, limits) ||
		!validPublicationRouting(publication.ExchangeKind, publication.RoutingKey, limits) ||
		publication.DeliveryMode < DeliveryTransient || publication.DeliveryMode > DeliveryPersistent {
		return ErrInvalidPublication
	}
	message := publication.Message
	if message.MessageID == "" {
		return ErrMessageIDRequired
	}
	if len(message.Body) > limits.MaxPayloadBytes {
		return ErrPayloadTooLarge
	}
	for _, value := range []string{message.MessageID, message.CorrelationID, message.ReplyTo, message.ContentType,
		message.ContentEncoding, message.Type, message.AppID} {
		if len(value) > limits.MaxNameBytes || containsControl(value) {
			return ErrInvalidPublication
		}
	}
	if message.Priority != nil && *message.Priority > 255 {
		return ErrInvalidPriority
	}
	if !message.Timestamp.IsZero() &&
		(message.Timestamp.Before(time.Unix(0, 0)) || message.Timestamp.Nanosecond() != 0) {
		return ErrInvalidPublication
	}
	if message.Expiration != nil &&
		(*message.Expiration < 0 || *message.Expiration%time.Millisecond != 0) {
		return ErrInvalidExpiration
	}
	if len(message.Headers) > limits.MaxHeaderEntries {
		return ErrHeadersTooLarge
	}
	seen := make(map[string]struct{}, len(message.Headers))
	headerBytes := 0
	for _, header := range message.Headers {
		if invalidIdentity(header.Key, limits.MaxNameBytes) {
			return ErrInvalidHeader
		}
		if reservedDeliveryMetadataHeader(header.Key) {
			return ErrReservedHeader
		}
		if _, exists := seen[header.Key]; exists {
			return ErrDuplicateHeader
		}
		seen[header.Key] = struct{}{}
		headerBytes += len(header.Key)
		switch header.Kind {
		case HeaderString:
			if header.Bool || header.Int64 != 0 || header.Bytes != nil {
				return ErrInvalidHeader
			}
			headerBytes += len(header.String)
		case HeaderBool:
			if header.String != "" || header.Int64 != 0 || header.Bytes != nil {
				return ErrInvalidHeader
			}
			headerBytes++
		case HeaderInt64:
			if header.String != "" || header.Bool || header.Bytes != nil {
				return ErrInvalidHeader
			}
			headerBytes += 8
		case HeaderBytes:
			if header.String != "" || header.Bool || header.Int64 != 0 {
				return ErrInvalidHeader
			}
			headerBytes += len(header.Bytes)
		default:
			return ErrInvalidHeader
		}
		if headerBytes > limits.MaxHeaderBytes {
			return ErrHeadersTooLarge
		}
	}

	return nil
}

func validPublicationExchange(exchange string, kind ExchangeKind, limits Limits) bool {
	if exchange == "" {
		return kind == ExchangeDirect
	}
	return !invalidIdentity(exchange, limits.MaxNameBytes)
}

func validPublicationRouting(kind ExchangeKind, routingKey string, limits Limits) bool {
	if len(routingKey) > limits.MaxRoutingKeyBytes || containsControl(routingKey) {
		return false
	}
	switch kind {
	case "":
		return routingKey != ""
	case ExchangeDirect, ExchangeTopic:
		return true
	case ExchangeFanout, ExchangeHeaders:
		return routingKey == ""
	default:
		return false
	}
}

func containsControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}
