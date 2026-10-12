package rabbitmqqueue

// PublishState distinguishes broker outcomes without collapsing post-send
// cancellation or connection loss into a definitive result.
type PublishState string

const (
	// PublishNotSent means the attempt was not committed to client transmission.
	PublishNotSent PublishState = "not_sent"
	// PublishRejected means a negative broker confirm without a correlated return.
	PublishRejected PublishState = "rejected"
	// PublishReturned means a correlated mandatory return, even with a positive confirm.
	PublishReturned PublishState = "returned"
	// PublishConfirmed means a positive broker confirm without a correlated return;
	// non-mandatory unroutable work can still be dropped.
	PublishConfirmed PublishState = "confirmed"
	// PublishAmbiguous means no definitive result was observed after possible transmission;
	// reconcile before replay to avoid duplicate effects.
	PublishAmbiguous PublishState = "ambiguous"
)

// Valid reports whether state is a defined publication outcome.
func (state PublishState) Valid() bool {
	switch state {
	case PublishNotSent, PublishRejected, PublishReturned, PublishConfirmed, PublishAmbiguous:
		return true
	default:
		return false
	}
}

// Return describes a mandatory unroutable outcome without carrying payloads or
// headers. Exchange and RoutingKey come from the exact registered publication,
// not untrusted broker return metadata.
type Return struct {
	// Code is the broker reply code for the correlated mandatory return.
	Code uint16
	// Reason is broker reply text retained only if at most 255 bytes and without ASCII
	// controls; otherwise empty. This is not secret redaction or trusted log text.
	Reason string
	// Exchange is the registered publication exchange, not the broker-supplied field.
	Exchange string
	// RoutingKey is the registered publication routing key, not the broker-supplied field.
	RoutingKey string
}

// PublishResult is the terminal observed state for exactly one publish attempt.
type PublishResult struct {
	// State is the terminal local/broker classification; zero is invalid.
	State PublishState
	// Return is present only for PublishReturned; callers own mutable result details.
	Return *Return
}

// PublishOutcome pairs one terminal result with its sanitized operation error.
// Batch outcomes preserve input order; asynchronous outcomes are delivered once.
type PublishOutcome struct {
	// Result preserves the observed classification even when Err is nonnil.
	Result PublishResult
	// Err is nil for confirmation or a sanitized classification/context error.
	// An ambiguous outcome requires application reconciliation before retry.
	Err error
}

// Valid reports whether the state and mandatory-return detail form a canonical outcome.
func (result PublishResult) Valid() bool {
	if !result.State.Valid() {
		return false
	}
	if result.State == PublishReturned {
		return result.Return != nil
	}
	return result.Return == nil
}
