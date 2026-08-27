package rabbitmqqueue

// PublishState distinguishes broker outcomes without collapsing post-send
// cancellation or connection loss into a definitive result.
type PublishState string

const (
	PublishNotSent   PublishState = "not_sent"
	PublishRejected  PublishState = "rejected"
	PublishReturned  PublishState = "returned"
	PublishConfirmed PublishState = "confirmed"
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
	Code       uint16
	Reason     string
	Exchange   string
	RoutingKey string
}

// PublishResult is the terminal observed state for exactly one publish attempt.
type PublishResult struct {
	State  PublishState
	Return *Return
}

// PublishOutcome pairs one terminal result with its sanitized operation error.
// Batch outcomes preserve input order; asynchronous outcomes are delivered once.
type PublishOutcome struct {
	Result PublishResult
	Err    error
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
