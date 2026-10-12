package rabbitmqqueue

// Liveness is process-supervision state for one package resource. Temporary
// dependency outages remain live while bounded recovery is active.
type Liveness string

const (
	// LivenessLive means the resource supervisor remains live, including recovery.
	LivenessLive Liveness = "live"
	// LivenessFailed means terminal resource failure, not a process health probe.
	LivenessFailed Liveness = "failed"
	// LivenessStopped means the resource lifecycle stopped normally.
	LivenessStopped Liveness = "stopped"
)

// Readiness reports whether one resource can currently accept useful work.
type Readiness string

const (
	// ReadinessReady is local advisory readiness, not an admission reservation.
	ReadinessReady Readiness = "ready"
	// ReadinessNotReady means local lifecycle, pause, recovery or blocking denies readiness.
	ReadinessNotReady Readiness = "not_ready"
)

// DependencyHealth reports the owned RabbitMQ dependency state separately
// from process liveness.
type DependencyHealth string

const (
	// DependencyAvailable is the current local connected state, not a fresh broker probe.
	DependencyAvailable DependencyHealth = "available"
	// DependencyBlocked means RabbitMQ reported producer flow blocking.
	DependencyBlocked DependencyHealth = "blocked"
	// DependencyRecovering means bounded generation replacement is underway.
	DependencyRecovering DependencyHealth = "recovering"
	// DependencyUnavailable means local resource failure/exhausted recovery.
	DependencyUnavailable DependencyHealth = "unavailable"
	// DependencyUnknown means the lifecycle no longer reports a connected dependency.
	DependencyUnknown DependencyHealth = "unknown"
)

// Liveness reports producer supervision state without probing RabbitMQ.
func (producer *Producer) Liveness() Liveness {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	if producer.terminal {
		return LivenessFailed
	}
	if producer.stopped {
		return LivenessStopped
	}
	return LivenessLive
}

// Readiness reports advisory local lifecycle/recovery/blocking state, without
// probing RabbitMQ or reserving capacity; a full window can still reject work.
func (producer *Producer) Readiness() Readiness {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	if producer.closed || producer.unavailable || producer.recovering || producer.blocked || producer.terminal {
		return ReadinessNotReady
	}
	return ReadinessReady
}

// DependencyHealth reports producer connection state independently of liveness.
func (producer *Producer) DependencyHealth() DependencyHealth {
	producer.stateMu.Lock()
	defer producer.stateMu.Unlock()
	if producer.terminal {
		return DependencyUnavailable
	}
	if producer.stopped || producer.closed {
		return DependencyUnknown
	}
	if producer.recovering {
		return DependencyRecovering
	}
	if producer.blocked {
		return DependencyBlocked
	}
	if producer.unavailable {
		return DependencyUnavailable
	}
	return DependencyAvailable
}

// Liveness reports consumer supervision state without probing RabbitMQ.
func (consumer *Consumer) Liveness() Liveness {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	if consumer.terminalErr != nil {
		return LivenessFailed
	}
	if consumer.stopped {
		return LivenessStopped
	}
	return LivenessLive
}

// Readiness reports whether the consumer currently admits broker deliveries.
func (consumer *Consumer) Readiness() Readiness {
	consumer.admissionMu.Lock()
	paused := consumer.paused
	consumer.admissionMu.Unlock()
	if paused {
		return ReadinessNotReady
	}
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	if consumer.stopping || consumer.recovering || consumer.terminalErr != nil || consumer.stopped {
		return ReadinessNotReady
	}
	return ReadinessReady
}

// DependencyHealth reports consumer connection state independently of liveness.
func (consumer *Consumer) DependencyHealth() DependencyHealth {
	consumer.stateMu.Lock()
	defer consumer.stateMu.Unlock()
	if consumer.terminalErr != nil {
		return DependencyUnavailable
	}
	if consumer.stopped || consumer.stopping {
		return DependencyUnknown
	}
	if consumer.recovering {
		return DependencyRecovering
	}
	return DependencyAvailable
}
