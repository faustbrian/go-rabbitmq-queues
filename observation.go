package rabbitmqqueue

import (
	"sync"
	"sync/atomic"
	"time"
)

const observationBufferSize = 256

// ObservationResource identifies the bounded package resource that emitted an
// observation without exposing a connection, route, message, or consumer ID.
type ObservationResource string

const (
	// ObservationProducer labels events from a producer resource.
	ObservationProducer ObservationResource = "producer"
	// ObservationConsumer labels events from a consumer resource.
	ObservationConsumer ObservationResource = "consumer"
)

// ObservationKind is a fixed low-cardinality operational event category.
type ObservationKind string

const (
	// ObservationConnectionState labels local connection/lifecycle transitions.
	ObservationConnectionState ObservationKind = "connection_state"
	// ObservationConnectionBlocked labels broker flow-control transitions.
	ObservationConnectionBlocked ObservationKind = "connection_blocked"
	// ObservationReconnect labels a bounded connection replacement attempt.
	ObservationReconnect ObservationKind = "reconnect"
	// ObservationPublish labels a terminal observed publication outcome.
	ObservationPublish ObservationKind = "publish"
	// ObservationReturn labels receipt of mandatory-return metadata.
	ObservationReturn ObservationKind = "return"
	// ObservationConfirm labels receipt of a positive or negative publisher confirm.
	ObservationConfirm ObservationKind = "confirm"
	// ObservationConfirmationLatency labels time since client-send initiation for a definitive result.
	ObservationConfirmationLatency ObservationKind = "confirmation_latency"
	// ObservationAmbiguous labels uncertain possible transmission.
	ObservationAmbiguous ObservationKind = "ambiguous"
	// ObservationDelivery labels receipt before retained-delivery conversion.
	ObservationDelivery ObservationKind = "delivery"
	// ObservationRedelivery labels a received broker redelivered flag.
	ObservationRedelivery ObservationKind = "redelivery"
	// ObservationConsumerCancellation labels a matching broker consumer cancellation.
	ObservationConsumerCancellation ObservationKind = "consumer_cancellation"
	// ObservationAcknowledgement labels local ACK-send success, not broker receipt.
	ObservationAcknowledgement ObservationKind = "acknowledgement"
	// ObservationSettlement labels local settlement-send success.
	ObservationSettlement ObservationKind = "settlement"
	// ObservationHandlerFailure labels a handler error, expired context or invalid request.
	ObservationHandlerFailure ObservationKind = "handler_failure"
	// ObservationDeadLetter labels received nonempty dead-letter history.
	ObservationDeadLetter ObservationKind = "dead_letter"
	// ObservationBacklogPressure labels observed admission/backlog saturation.
	ObservationBacklogPressure ObservationKind = "backlog_pressure"
	// ObservationShutdown labels local shutdown start/completion.
	ObservationShutdown ObservationKind = "shutdown"
	// ObservationStreamClosed is the final stream event with accumulated tail loss.
	ObservationStreamClosed ObservationKind = "stream_closed"
)

// ObservationOutcome is a fixed low-cardinality event result or transition.
type ObservationOutcome string

const (
	// ObservationConnected marks initial local connection establishment.
	ObservationConnected ObservationOutcome = "connected"
	// ObservationRecovering marks entry into generation recovery.
	ObservationRecovering ObservationOutcome = "recovering"
	// ObservationRecovered marks successful generation replacement.
	ObservationRecovered ObservationOutcome = "recovered"
	// ObservationUnavailable marks terminal unavailable state.
	ObservationUnavailable ObservationOutcome = "unavailable"
	// ObservationBlocked marks broker-reported producer blocking.
	ObservationBlocked ObservationOutcome = "blocked"
	// ObservationUnblocked marks local cleared blocking state.
	ObservationUnblocked ObservationOutcome = "unblocked"
	// ObservationAttempted marks a recovery attempt, not success.
	ObservationAttempted ObservationOutcome = "attempted"
	// ObservationConfirmed marks a positive confirm/classified confirmed outcome.
	ObservationConfirmed ObservationOutcome = "confirmed"
	// ObservationRejected marks a negative confirm or local reject-send success.
	ObservationRejected ObservationOutcome = "rejected"
	// ObservationReturned marks a mandatory return/classified returned outcome.
	ObservationReturned ObservationOutcome = "returned"
	// ObservationNotSent marks a classified publication not committed to transmission.
	ObservationNotSent ObservationOutcome = "not_sent"
	// ObservationAmbiguousOutcome marks uncertain publication outcome.
	ObservationAmbiguousOutcome ObservationOutcome = "ambiguous"
	// ObservationDelivered marks a broker delivery received, not processed.
	ObservationDelivered ObservationOutcome = "delivered"
	// ObservationRedelivered marks receipt with the broker redelivered flag.
	ObservationRedelivered ObservationOutcome = "redelivered"
	// ObservationCancelled marks a matching basic.cancel notification.
	ObservationCancelled ObservationOutcome = "cancelled"
	// ObservationAcknowledged marks local single-delivery ACK-send success.
	ObservationAcknowledged ObservationOutcome = "acknowledged"
	// ObservationNegativeAcknowledged marks local single-delivery NACK-send success.
	ObservationNegativeAcknowledged ObservationOutcome = "negative_acknowledged"
	// ObservationHandlerFailed marks failed/expired/invalid handler outcome.
	ObservationHandlerFailed ObservationOutcome = "failed"
	// ObservationDeadLettered marks received dead-letter history, not a fresh transfer.
	ObservationDeadLettered ObservationOutcome = "dead_lettered"
	// ObservationBacklogFull marks observed bounded backlog/admission pressure.
	ObservationBacklogFull ObservationOutcome = "full"
	// ObservationShutdownStarted marks initiation of resource shutdown.
	ObservationShutdownStarted ObservationOutcome = "started"
	// ObservationShutdownCompleted marks local shutdown processing completion.
	ObservationShutdownCompleted ObservationOutcome = "completed"
	// ObservationClosed marks observation-stream closure.
	ObservationClosed ObservationOutcome = "closed"
)

// Observation is a payload-free, identifier-free operational event. Duration
// is populated only for confirmation latency. Dropped reports observations
// discarded since the previous delivered event because the bounded stream was
// full. Stream closure emits ObservationStreamClosed and reserves a buffered
// slot when necessary so undisclosed tail loss remains visible.
type Observation struct {
	// Resource is the fixed producer/consumer category, not a resource identifier.
	Resource ObservationResource
	// Kind is the fixed event category, not broker/application text.
	Kind ObservationKind
	// Outcome is the fixed transition/result category.
	Outcome ObservationOutcome
	// Duration is populated for confirmation latency only, not every operation timer.
	Duration time.Duration
	// Dropped reports preceding stream losses; final closure can displace one
	// buffered event so tail loss is included in the terminal observation.
	Dropped uint64
}

type observationStream struct {
	mu        sync.RWMutex
	channel   chan Observation
	resource  ObservationResource
	closed    bool
	dropped   atomic.Uint64
	closeOnce sync.Once
}

func newObservationStream(resource ObservationResource, capacity int) *observationStream {
	return &observationStream{channel: make(chan Observation, capacity), resource: resource}
}

func (stream *observationStream) emit(observation Observation) {
	stream.mu.RLock()
	defer stream.mu.RUnlock()
	if stream.closed {
		return
	}
	observation.Resource = stream.resource
	observation.Dropped = stream.dropped.Swap(0)
	select {
	case stream.channel <- observation:
	default:
		stream.dropped.Add(observation.Dropped + 1)
	}
}

func (stream *observationStream) close() {
	stream.closeOnce.Do(func() {
		stream.mu.Lock()
		stream.closed = true
		dropped := stream.dropped.Swap(0)
		terminal := Observation{
			Resource: stream.resource, Kind: ObservationStreamClosed,
			Outcome: ObservationClosed, Dropped: dropped,
		}
		select {
		case stream.channel <- terminal:
		default:
			// Reserve terminal visibility even for a saturated stream. Include both
			// the displaced event and its accumulated loss, or tail drops vanish.
			if displaced, ok := takeObservation(stream.channel); ok {
				terminal.Dropped += displaced.Dropped + 1
			}
			stream.channel <- terminal
		}
		close(stream.channel)
		stream.mu.Unlock()
	})
}

func takeObservation(channel <-chan Observation) (Observation, bool) {
	select {
	case observation := <-channel:
		return observation, true
	default:
		return Observation{}, false
	}
}

func publishObservationOutcome(state PublishState) ObservationOutcome {
	switch state {
	case PublishConfirmed:
		return ObservationConfirmed
	case PublishRejected:
		return ObservationRejected
	case PublishReturned:
		return ObservationReturned
	case PublishAmbiguous:
		return ObservationAmbiguousOutcome
	default:
		return ObservationNotSent
	}
}
