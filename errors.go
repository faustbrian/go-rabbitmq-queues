package rabbitmqqueue

import "errors"

var (
	// ErrInvalidEndpoint means a connection endpoint is missing or unsafe.
	ErrInvalidEndpoint = errors.New("rabbitmqqueue: invalid endpoint")
	// ErrCredentialsRequired means no rotating credential provider was supplied.
	ErrCredentialsRequired = errors.New("rabbitmqqueue: credentials are required")
	// ErrInvalidTLS means verified TLS configuration is incomplete or unsafe.
	ErrInvalidTLS = errors.New("rabbitmqqueue: invalid TLS configuration")
	// ErrInvalidBounds means a configured resource or time bound is invalid.
	ErrInvalidBounds = errors.New("rabbitmqqueue: invalid resource bounds")
	// ErrInvalidVirtualHost means the AMQP virtual-host identity is invalid.
	ErrInvalidVirtualHost = errors.New("rabbitmqqueue: invalid virtual host")
	// ErrUnsupportedQueuePolicy means a queue option is not supported by its queue type.
	ErrUnsupportedQueuePolicy = errors.New("rabbitmqqueue: unsupported queue policy")
	// ErrUnsupportedExchangeKind means the exchange kind is not an AMQP built-in supported here.
	ErrUnsupportedExchangeKind = errors.New("rabbitmqqueue: unsupported exchange kind")
	// ErrInvalidTopology means a topology identity or property is invalid.
	ErrInvalidTopology = errors.New("rabbitmqqueue: invalid topology")
	// ErrTopologyMutationDenied means active declaration lacks a development-only permit.
	ErrTopologyMutationDenied = errors.New("rabbitmqqueue: topology mutation denied")
	// ErrMessageIDRequired means a publication has no stable application message identity.
	ErrMessageIDRequired = errors.New("rabbitmqqueue: message ID is required")
	// ErrPayloadTooLarge means a payload exceeds the configured byte limit.
	ErrPayloadTooLarge = errors.New("rabbitmqqueue: payload is too large")
	// ErrHeadersTooLarge means header count or bytes exceed configured limits.
	ErrHeadersTooLarge = errors.New("rabbitmqqueue: headers are too large")
	// ErrDuplicateHeader means a message repeats a header key.
	ErrDuplicateHeader = errors.New("rabbitmqqueue: duplicate header")
	// ErrInvalidHeader means a header key or value is outside the stable policy surface.
	ErrInvalidHeader = errors.New("rabbitmqqueue: invalid header")
	// ErrInvalidPriority means a message priority is outside the AMQP octet range.
	ErrInvalidPriority = errors.New("rabbitmqqueue: invalid priority")
	// ErrInvalidExpiration means a message expiration is negative or cannot be encoded safely.
	ErrInvalidExpiration = errors.New("rabbitmqqueue: invalid expiration")
	// ErrInvalidPublication means publication routing or properties are invalid.
	ErrInvalidPublication = errors.New("rabbitmqqueue: invalid publication")
)
