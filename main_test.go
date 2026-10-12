package rabbitmqqueue

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain checks process goroutine ownership even for selected examples;
// livebroker files require separate explicit infrastructure and are build-tagged.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
