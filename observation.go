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
	ObservationProducer ObservationResource = "producer"
	ObservationConsumer ObservationResource = "consumer"
)

// ObservationKind is a fixed low-cardinality operational event category.
type ObservationKind string

const (
	ObservationConnectionState      ObservationKind = "connection_state"
	ObservationConnectionBlocked    ObservationKind = "connection_blocked"
	ObservationReconnect            ObservationKind = "reconnect"
	ObservationPublish              ObservationKind = "publish"
	ObservationReturn               ObservationKind = "return"
	ObservationConfirm              ObservationKind = "confirm"
	ObservationConfirmationLatency  ObservationKind = "confirmation_latency"
	ObservationAmbiguous            ObservationKind = "ambiguous"
	ObservationDelivery             ObservationKind = "delivery"
	ObservationRedelivery           ObservationKind = "redelivery"
	ObservationConsumerCancellation ObservationKind = "consumer_cancellation"
	ObservationAcknowledgement      ObservationKind = "acknowledgement"
	ObservationSettlement           ObservationKind = "settlement"
	ObservationHandlerFailure       ObservationKind = "handler_failure"
	ObservationDeadLetter           ObservationKind = "dead_letter"
	ObservationBacklogPressure      ObservationKind = "backlog_pressure"
	ObservationShutdown             ObservationKind = "shutdown"
	ObservationStreamClosed         ObservationKind = "stream_closed"
)

// ObservationOutcome is a fixed low-cardinality event result or transition.
type ObservationOutcome string

const (
	ObservationConnected            ObservationOutcome = "connected"
	ObservationRecovering           ObservationOutcome = "recovering"
	ObservationRecovered            ObservationOutcome = "recovered"
	ObservationUnavailable          ObservationOutcome = "unavailable"
	ObservationBlocked              ObservationOutcome = "blocked"
	ObservationUnblocked            ObservationOutcome = "unblocked"
	ObservationAttempted            ObservationOutcome = "attempted"
	ObservationConfirmed            ObservationOutcome = "confirmed"
	ObservationRejected             ObservationOutcome = "rejected"
	ObservationReturned             ObservationOutcome = "returned"
	ObservationNotSent              ObservationOutcome = "not_sent"
	ObservationAmbiguousOutcome     ObservationOutcome = "ambiguous"
	ObservationDelivered            ObservationOutcome = "delivered"
	ObservationRedelivered          ObservationOutcome = "redelivered"
	ObservationCancelled            ObservationOutcome = "cancelled"
	ObservationAcknowledged         ObservationOutcome = "acknowledged"
	ObservationNegativeAcknowledged ObservationOutcome = "negative_acknowledged"
	ObservationHandlerFailed        ObservationOutcome = "failed"
	ObservationDeadLettered         ObservationOutcome = "dead_lettered"
	ObservationBacklogFull          ObservationOutcome = "full"
	ObservationShutdownStarted      ObservationOutcome = "started"
	ObservationShutdownCompleted    ObservationOutcome = "completed"
	ObservationClosed               ObservationOutcome = "closed"
)

// Observation is a payload-free, identifier-free operational event. Duration
// is populated only for confirmation latency. Dropped reports observations
// discarded since the previous delivered event because the bounded stream was
// full. Stream closure emits ObservationStreamClosed and reserves a buffered
// slot when necessary so undisclosed tail loss remains visible.
type Observation struct {
	Resource ObservationResource
	Kind     ObservationKind
	Outcome  ObservationOutcome
	Duration time.Duration
	Dropped  uint64
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
