package rabbitmqqueue

// SettlementMethod identifies one AMQP manual-settlement operation.
type SettlementMethod string

const (
	// SettlementAcknowledge requests a single-delivery positive acknowledgement.
	SettlementAcknowledge SettlementMethod = "ack"
	// SettlementNegativeAcknowledge requests a single-delivery NACK, with bounded requeue.
	SettlementNegativeAcknowledge SettlementMethod = "nack"
	// SettlementReject requests a single-delivery rejection, with bounded requeue.
	SettlementReject SettlementMethod = "reject"
	// SettlementDelegate leaves the tracked delivery to generation closure/redelivery.
	SettlementDelegate SettlementMethod = "delegate"
)

// Settlement is a handler's explicit request for one delivery. Delegate leaves
// the delivery unsettled until the consumer drains, closes, or loses its
// connection.
type Settlement struct {
	// Method selects ACK, NACK, Reject or lifecycle delegation; zero is invalid.
	Method SettlementMethod
	// Requeue requests broker redelivery only with NACK/Reject, then is reduced by
	// ConsumerConfig.MaxRequeues and observed delivery state.
	Requeue bool
}

// Acknowledge requests a single-delivery ACK after handler success.
func Acknowledge() Settlement { return Settlement{Method: SettlementAcknowledge} }

// NegativeAcknowledge requests a single-delivery NACK.
func NegativeAcknowledge(requeue bool) Settlement {
	return Settlement{Method: SettlementNegativeAcknowledge, Requeue: requeue}
}

// Reject requests a single-delivery reject.
func Reject(requeue bool) Settlement {
	return Settlement{Method: SettlementReject, Requeue: requeue}
}

// Delegate explicitly leaves settlement to the consumer connection lifecycle.
func Delegate() Settlement { return Settlement{Method: SettlementDelegate} }

// Validate rejects unknown methods and impossible requeue flags.
func (settlement Settlement) Validate() error {
	switch settlement.Method {
	case SettlementAcknowledge, SettlementDelegate:
		if settlement.Requeue {
			return ErrInvalidSettlement
		}
		return nil
	case SettlementNegativeAcknowledge, SettlementReject:
		return nil
	default:
		return ErrInvalidSettlement
	}
}

// RabbitMQ 4.3 acquired counts cover returns even when NACK does not increment
// the failed-delivery count. Quorum Reject can use the latter if acquired count
// is absent; classic/NACK fallback refuses an already-redelivered message. A cap
// only clears Requeue; it does not replace the method or publish another message.
// See TestSettlementPolicyPreventsUnboundedRequeue and DEC-004.
func boundedSettlement(delivery Delivery, requested Settlement, config ConsumerConfig) Settlement {
	if !requested.Requeue {
		return requested
	}
	requested.Requeue = false
	if config.MaxRequeues == 0 {
		return requested
	}
	if config.Queue.Type == QueueQuorum && delivery.AcquiredCount != nil {
		requested.Requeue = *delivery.AcquiredCount < uint64(config.MaxRequeues)
		return requested
	}
	if requested.Method == SettlementNegativeAcknowledge && delivery.Redelivered {
		return requested
	}
	if config.Queue.Type == QueueQuorum && requested.Method == SettlementReject && delivery.DeliveryCount != nil {
		requested.Requeue = *delivery.DeliveryCount < uint64(config.MaxRequeues)
		return requested
	}
	requested.Requeue = !delivery.Redelivered
	return requested
}
