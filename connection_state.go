package rabbitmqqueue

// ConnectionBlockedState reports whether RabbitMQ has temporarily blocked
// publishing on the owned connection. Broker-provided reason text is omitted.
type ConnectionBlockedState struct {
	Active bool
}
