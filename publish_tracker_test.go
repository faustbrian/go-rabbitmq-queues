package rabbitmqqueue

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestPublishTrackerReconcilesExactConfirmAndMandatoryReturn(t *testing.T) {
	t.Parallel()

	tracker := newPublishTracker(2)
	first, err := tracker.register(41, "session-a/41")
	if err != nil {
		t.Fatalf("register first publish: %v", err)
	}
	second, err := tracker.register(42, "session-a/42")
	if err != nil {
		t.Fatalf("register second publish: %v", err)
	}

	returned := Return{Code: 312, Reason: "NO_ROUTE", Exchange: "events", RoutingKey: "missing"}
	if !tracker.returned("session-a/42", returned) {
		t.Fatal("return did not correlate to the second publish")
	}
	if !tracker.confirm(41, true) {
		t.Fatal("first confirmation was not correlated")
	}
	if !tracker.confirm(42, true) {
		t.Fatal("second confirmation was not correlated")
	}

	if result := <-first.outcome; result.State != PublishConfirmed || result.Return != nil {
		t.Fatalf("first outcome = %#v, want confirmed", result)
	}
	if result := <-second.outcome; result.State != PublishReturned || result.Return == nil || *result.Return != returned {
		t.Fatalf("second outcome = %#v, want returned %#v", result, returned)
	}
}

func TestPublishTrackerNeverAppliesLateEventsToLaterPublishes(t *testing.T) {
	t.Parallel()

	tracker := newPublishTracker(1)
	first, err := tracker.register(7, "old-session/7")
	if err != nil {
		t.Fatalf("register first publish: %v", err)
	}
	if !tracker.abandon(7, PublishAmbiguous) {
		t.Fatal("first publish was not abandoned")
	}
	if result := <-first.outcome; result.State != PublishAmbiguous {
		t.Fatalf("abandoned outcome = %#v, want ambiguous", result)
	}

	second, err := tracker.register(8, "new-session/8")
	if err != nil {
		t.Fatalf("register second publish: %v", err)
	}
	if tracker.confirm(7, true) {
		t.Fatal("late confirmation matched a completed publish")
	}
	if tracker.returned("old-session/7", Return{Code: 312}) {
		t.Fatal("late return matched a completed publish")
	}
	select {
	case result := <-second.outcome:
		t.Fatalf("late event completed later publish: %#v", result)
	default:
	}
	if !tracker.confirm(8, false) {
		t.Fatal("negative confirmation was not correlated")
	}
	if result := <-second.outcome; result.State != PublishRejected {
		t.Fatalf("negative confirmation outcome = %#v, want rejected", result)
	}
}

func TestPublishTrackerBoundsOutstandingAndFailsGeneration(t *testing.T) {
	t.Parallel()

	tracker := newPublishTracker(1)
	first, err := tracker.register(1, "session/1")
	if err != nil {
		t.Fatalf("register first publish: %v", err)
	}
	if _, err := tracker.register(2, "session/2"); !errors.Is(err, ErrOutstandingConfirmLimit) {
		t.Fatalf("register over limit error = %v, want %v", err, ErrOutstandingConfirmLimit)
	}
	if count := tracker.failAll(PublishAmbiguous); count != 1 {
		t.Fatalf("failAll() count = %d, want 1", count)
	}
	if result := <-first.outcome; result.State != PublishAmbiguous {
		t.Fatalf("failed generation outcome = %#v, want ambiguous", result)
	}
	if count := tracker.failAll(PublishAmbiguous); count != 0 {
		t.Fatalf("second failAll() count = %d, want 0", count)
	}
}

func TestPublishTrackerCorrelatesConcurrentCompletions(t *testing.T) {
	t.Parallel()

	const count = 100
	tracker := newPublishTracker(count)
	attempts := make([]*publishAttempt, count)
	for index := range count {
		sequence := uint64(index + 1)
		attempt, err := tracker.register(sequence, fmt.Sprintf("session/%d", sequence))
		if err != nil {
			t.Fatalf("register publish %d: %v", sequence, err)
		}
		attempts[index] = attempt
	}

	var completions sync.WaitGroup
	completions.Add(count)
	for index := range count {
		go func() {
			defer completions.Done()
			sequence := uint64(index + 1)
			if index%2 == 0 {
				tracker.returned(fmt.Sprintf("session/%d", sequence), Return{Code: 312})
			}
			tracker.confirm(sequence, true)
		}()
	}
	completions.Wait()

	for index, attempt := range attempts {
		result := <-attempt.outcome
		want := PublishConfirmed
		if index%2 == 0 {
			want = PublishReturned
		}
		if result.State != want {
			t.Fatalf("publish %d state = %s, want %s", index+1, result.State, want)
		}
	}
}

func TestPublishTrackerRejectsInvalidRegistrationAndCompletion(t *testing.T) {
	t.Parallel()

	if _, err := newPublishTracker(0).register(1, "session/1"); !errors.Is(err, ErrInvalidBounds) {
		t.Fatalf("unbounded tracker error = %v, want %v", err, ErrInvalidBounds)
	}
	tracker := newPublishTracker(2)
	if _, err := tracker.register(0, "session/0"); !errors.Is(err, ErrInvalidPublishCorrelation) {
		t.Fatalf("zero sequence error = %v, want %v", err, ErrInvalidPublishCorrelation)
	}
	if _, err := tracker.register(1, ""); !errors.Is(err, ErrInvalidPublishCorrelation) {
		t.Fatalf("empty token error = %v, want %v", err, ErrInvalidPublishCorrelation)
	}
	if _, err := tracker.register(1, "session/1"); err != nil {
		t.Fatalf("register publish: %v", err)
	}
	if _, err := tracker.register(1, "session/other"); !errors.Is(err, ErrInvalidPublishCorrelation) {
		t.Fatalf("duplicate sequence error = %v, want %v", err, ErrInvalidPublishCorrelation)
	}
	if _, err := tracker.register(2, "session/1"); !errors.Is(err, ErrInvalidPublishCorrelation) {
		t.Fatalf("duplicate token error = %v, want %v", err, ErrInvalidPublishCorrelation)
	}
	if tracker.abandon(99, PublishAmbiguous) {
		t.Fatal("unknown publish was abandoned")
	}
	if tracker.abandon(1, PublishConfirmed) {
		t.Fatal("abandon accepted a definitive success state")
	}
	if count := tracker.failAll(PublishConfirmed); count != 0 {
		t.Fatalf("failAll accepted a definitive success state: %d", count)
	}
}

func TestPublishResultValidation(t *testing.T) {
	t.Parallel()

	returned := Return{Code: 312, Reason: "NO_ROUTE", Exchange: "events", RoutingKey: "missing"}
	tests := []struct {
		result PublishResult
		valid  bool
	}{
		{result: PublishResult{State: PublishConfirmed}, valid: true},
		{result: PublishResult{State: PublishRejected}, valid: true},
		{result: PublishResult{State: PublishNotSent}, valid: true},
		{result: PublishResult{State: PublishAmbiguous}, valid: true},
		{result: PublishResult{State: PublishReturned, Return: &returned}, valid: true},
		{result: PublishResult{State: PublishReturned}},
		{result: PublishResult{State: PublishConfirmed, Return: &returned}},
		{result: PublishResult{State: PublishState("unknown")}},
	}
	for _, test := range tests {
		if got := test.result.Valid(); got != test.valid {
			t.Fatalf("PublishResult%#v.Valid() = %t, want %t", test.result, got, test.valid)
		}
	}
}
