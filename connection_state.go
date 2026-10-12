package rabbitmqqueue

// ConnectionBlockedState reports whether RabbitMQ has temporarily blocked
// publishing on the owned connection. Broker-provided reason text is omitted.
type ConnectionBlockedState struct {
	// Active is the latest broker blocked flag, not a complete transition history
	// or evidence that a pending publication was accepted or retried.
	Active bool
}
