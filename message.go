package rabbitmqqueue

import "time"

const (
	publishTokenHeader      = "x-rabbitmqqueue-publish-token"
	maxProducerSessionBytes = 128
	maxPublishTokenBytes    = maxProducerSessionBytes + 1 + 20
)

// Limits bounds untrusted message and topology-controlled allocation.
type Limits struct {
	MaxPayloadBytes    int
	MaxHeaderEntries   int
	MaxHeaderBytes     int
	MaxNameBytes       int
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
	return limits.MaxPayloadBytes > 0 && limits.MaxHeaderEntries > 0 &&
		limits.MaxHeaderBytes > 0 && limits.MaxNameBytes > 0 &&
		limits.MaxRoutingKeyBytes > 0
}

// HeaderKind identifies a bounded, language-neutral AMQP field-table value.
type HeaderKind uint8

const (
	HeaderString HeaderKind = iota + 1
	HeaderBool
	HeaderInt64
	HeaderBytes
)

// Header is one ordered AMQP application header. Nested tables and arrays are
// intentionally excluded to keep allocation and interoperability bounded.
type Header struct {
	Key    string
	Kind   HeaderKind
	String string
	Bool   bool
	Int64  int64
	Bytes  []byte
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
	Body            []byte
	MessageID       string
	CorrelationID   string
	ReplyTo         string
	ContentType     string
	ContentEncoding string
	Type            string
	AppID           string
	// Timestamp is either zero or a non-negative whole-second AMQP timestamp.
	Timestamp  time.Time
	Expiration time.Duration
	Priority   *uint16
	Headers    []Header
}

// DeliveryMode selects broker persistence intent.
type DeliveryMode uint8

const (
	DeliveryTransient DeliveryMode = iota + 1
	DeliveryPersistent
)

// Publication binds one message to explicit AMQP routing policy.
type Publication struct {
	// Exchange is a named exchange identity, or the empty default-exchange
	// identity when ExchangeKind is explicitly ExchangeDirect.
	Exchange string
	// ExchangeKind records the expected routing semantic for local validation.
	// Omit it only for non-empty direct/topic-compatible routing keys; fanout
	// and headers publications must name their kind and use an empty key.
	ExchangeKind ExchangeKind
	RoutingKey   string
	Mandatory    bool
	DeliveryMode DeliveryMode
	Message      Message
}

// Validate bounds and validates publication routing, properties, and headers.
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
	if message.Expiration < 0 ||
		(message.Expiration > 0 && message.Expiration%time.Millisecond != 0) {
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
		if header.Key == publishTokenHeader {
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
	case "", ExchangeDirect, ExchangeTopic:
		return routingKey != ""
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
