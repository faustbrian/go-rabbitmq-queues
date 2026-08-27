package rabbitmqqueue

import "sync"

// publishAttempt is owned by publishTracker until one terminal result is sent.
type publishAttempt struct {
	sequence uint64
	token    string
	exchange string
	route    string
	returned *Return
	outcome  chan PublishResult
}

// publishTracker correlates channel-scoped confirmation sequences and
// generation-scoped return tokens. Its mutex is the sole owner of both maps.
type publishTracker struct {
	mu       sync.Mutex
	maximum  int
	sequence map[uint64]*publishAttempt
	token    map[string]*publishAttempt
}

func newPublishTracker(maximum int) *publishTracker {
	return &publishTracker{
		maximum:  maximum,
		sequence: make(map[uint64]*publishAttempt),
		token:    make(map[string]*publishAttempt),
	}
}

func (tracker *publishTracker) register(
	sequence uint64,
	token string,
	exchange string,
	route string,
) (*publishAttempt, error) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if tracker.maximum < 1 {
		return nil, ErrInvalidBounds
	}
	if sequence == 0 || token == "" {
		return nil, ErrInvalidPublishCorrelation
	}
	if _, exists := tracker.sequence[sequence]; exists {
		return nil, ErrInvalidPublishCorrelation
	}
	if _, exists := tracker.token[token]; exists {
		return nil, ErrInvalidPublishCorrelation
	}
	if len(tracker.sequence) >= tracker.maximum {
		return nil, ErrOutstandingConfirmLimit
	}

	attempt := &publishAttempt{
		sequence: sequence,
		token:    token,
		exchange: exchange,
		route:    route,
		outcome:  make(chan PublishResult, 1),
	}
	tracker.sequence[sequence] = attempt
	tracker.token[token] = attempt

	return attempt, nil
}

func (tracker *publishTracker) returned(token string, returned Return) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	attempt, exists := tracker.token[token]
	if !exists {
		return false
	}
	returned.Exchange = attempt.exchange
	returned.RoutingKey = attempt.route
	attempt.returned = &returned
	return true
}

func (tracker *publishTracker) confirm(sequence uint64, acknowledged bool) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	attempt, exists := tracker.sequence[sequence]
	if !exists {
		return false
	}
	result := PublishResult{State: PublishRejected}
	if attempt.returned != nil {
		result.State = PublishReturned
		result.Return = attempt.returned
	} else if acknowledged {
		result.State = PublishConfirmed
	}
	tracker.complete(attempt, result)
	return true
}

func (tracker *publishTracker) abandon(sequence uint64, state PublishState) bool {
	if state != PublishNotSent && state != PublishAmbiguous {
		return false
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	attempt, exists := tracker.sequence[sequence]
	if !exists {
		return false
	}
	tracker.complete(attempt, PublishResult{State: state})
	return true
}

func (tracker *publishTracker) failAll(state PublishState) int {
	if state != PublishNotSent && state != PublishAmbiguous {
		return 0
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	count := len(tracker.sequence)
	for _, attempt := range tracker.sequence {
		tracker.complete(attempt, PublishResult{State: state})
	}
	return count
}

func (tracker *publishTracker) complete(attempt *publishAttempt, result PublishResult) {
	delete(tracker.sequence, attempt.sequence)
	delete(tracker.token, attempt.token)
	attempt.outcome <- result
	close(attempt.outcome)
}
