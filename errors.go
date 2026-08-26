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
	// ErrPassiveBindingVerificationUnsupported means AMQP cannot inspect a binding without mutating it.
	ErrPassiveBindingVerificationUnsupported = errors.New("rabbitmqqueue: passive binding verification unsupported")
	// ErrTopologyUnavailable means required topology is missing or could not be inspected.
	ErrTopologyUnavailable = errors.New("rabbitmqqueue: topology unavailable")
	// ErrTopologyInequivalent means broker topology exists with incompatible declaration properties.
	ErrTopologyInequivalent = errors.New("rabbitmqqueue: topology is inequivalent")
	// ErrTopologyUnauthorized means broker permissions denied topology inspection or declaration.
	ErrTopologyUnauthorized = errors.New("rabbitmqqueue: topology access denied")
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
	// ErrOutstandingConfirmLimit means the bounded in-flight publish window is full.
	ErrOutstandingConfirmLimit = errors.New("rabbitmqqueue: outstanding confirm limit reached")
	// ErrInvalidBatch means a publish batch is empty, oversized, or contains invalid work.
	ErrInvalidBatch = errors.New("rabbitmqqueue: invalid publish batch")
	// ErrInvalidPublishCorrelation means a publish sequence or internal token is invalid or reused.
	ErrInvalidPublishCorrelation = errors.New("rabbitmqqueue: invalid publish correlation")
	// ErrContextRequired means an operation received a nil context.
	ErrContextRequired = errors.New("rabbitmqqueue: context is required")
	// ErrProducerClosed means the producer no longer accepts publications.
	ErrProducerClosed = errors.New("rabbitmqqueue: producer is closed")
	// ErrProducerUnavailable means producer setup or its event channel failed.
	ErrProducerUnavailable = errors.New("rabbitmqqueue: producer is unavailable")
	// ErrPublishReturned means mandatory routing returned the publication.
	ErrPublishReturned = errors.New("rabbitmqqueue: publication was returned")
	// ErrPublishRejected means the broker negatively confirmed the publication.
	ErrPublishRejected = errors.New("rabbitmqqueue: publication was rejected")
	// ErrPublishAmbiguous means transmission began but no definitive broker result was observed.
	ErrPublishAmbiguous = errors.New("rabbitmqqueue: publication outcome is ambiguous")
	// ErrReservedHeader means application metadata collides with package-owned correlation state.
	ErrReservedHeader = errors.New("rabbitmqqueue: reserved header")
	// ErrInvalidConsumer means consumer identity, bounds, or failure policy is invalid.
	ErrInvalidConsumer = errors.New("rabbitmqqueue: invalid consumer")
	// ErrInvalidDelivery means broker delivery data exceeds the safe public policy surface.
	ErrInvalidDelivery = errors.New("rabbitmqqueue: invalid delivery")
	// ErrInvalidSettlement means a handler requested an undefined settlement operation.
	ErrInvalidSettlement = errors.New("rabbitmqqueue: invalid settlement")
	// ErrConsumerUnavailable means consumer setup, delivery, or settlement reached a terminal state.
	ErrConsumerUnavailable = errors.New("rabbitmqqueue: consumer is unavailable")
)
