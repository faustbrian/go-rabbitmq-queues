//go:build livebroker

package rabbitmqqueue_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

const (
	liveClusterConfigEnvironment = "RABBITMQ_QUEUE_CLUSTER_CONFIG"
	minimumFaultWindowMessages   = 64
	maximumFaultWindowMessages   = 10_000
	postFaultMessages            = 16
	clusterPublishTimeout        = 2 * time.Second
	clusterDeliveryTimeout       = 60 * time.Second
)

type liveClusterLedger struct {
	mu         sync.Mutex
	attempts   map[string]rabbitmqqueue.PublishState
	deliveries map[string]int
	observed   chan struct{}
}

func TestLiveClusterLedgerAccounting(t *testing.T) {
	ledger := &liveClusterLedger{
		attempts:   make(map[string]rabbitmqqueue.PublishState),
		deliveries: make(map[string]int),
		observed:   make(chan struct{}, 1),
	}
	ledger.recordAttempt("confirmed", rabbitmqqueue.PublishConfirmed)
	ledger.recordAttempt("ambiguous", rabbitmqqueue.PublishAmbiguous)
	ledger.recordAttempt("not-sent", rabbitmqqueue.PublishNotSent)
	if ledger.allConfirmedObserved([]string{"confirmed", "ambiguous", "not-sent"}) {
		t.Fatal("confirmed publication was reported observed before delivery")
	}
	ledger.recordDelivery("confirmed")
	ledger.recordDelivery("confirmed")
	ledger.recordDelivery("ambiguous")
	if !ledger.allConfirmedObserved([]string{"confirmed", "ambiguous", "not-sent"}) {
		t.Fatal("confirmed publication remained missing after delivery")
	}
	confirmed, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	if confirmed != 1 || ambiguous != 1 || notSent != 1 || delivered != 3 || duplicates != 1 {
		t.Fatalf(
			"ledger summary = (%d, %d, %d, %d, %d), want (1, 1, 1, 3, 1)",
			confirmed, ambiguous, notSent, delivered, duplicates,
		)
	}
}

func TestLiveBrokerThreeNodeInterruption(t *testing.T) {
	fixture := readLiveBrokerFixtureForEnvironment(t, liveClusterConfigEnvironment)
	if len(fixture.Endpoints) != 3 || fixture.FaultStartGateFile == "" ||
		fixture.FaultCompleteGateFile == "" ||
		fixture.FaultStartGateFile == fixture.FaultCompleteGateFile ||
		(fixture.FaultQueueType != rabbitmqqueue.QueueClassic &&
			fixture.FaultQueueType != rabbitmqqueue.QueueQuorum) ||
		fixture.FaultWindowMessages < minimumFaultWindowMessages ||
		fixture.FaultWindowMessages > maximumFaultWindowMessages {
		t.Fatal("three-node configuration requires three endpoints, a classic or quorum queue type, distinct fault gates, and a bounded message window")
	}
	for _, gate := range []string{fixture.FaultStartGateFile, fixture.FaultCompleteGateFile} {
		if faultGateExists(t, gate) {
			t.Fatal("fault gates must not exist before the test starts")
		}
	}

	connection := fixture.connection(t)
	verifyLiveTopology(t, connection, fixture)
	faultQueue := fixture.Classic
	if fixture.FaultQueueType == rabbitmqqueue.QueueQuorum {
		faultQueue = fixture.Quorum
	}
	runToken := randomLiveToken(t)
	ledger := &liveClusterLedger{
		attempts:   make(map[string]rabbitmqqueue.PublishState),
		deliveries: make(map[string]int),
		observed:   make(chan struct{}, 1),
	}
	consumer := openLiveConsumer(t, connection, faultQueue, fixture.FaultQueueType, 2, func(
		_ context.Context,
		delivery rabbitmqqueue.Delivery,
	) (rabbitmqqueue.Settlement, error) {
		ledger.recordDelivery(delivery.MessageID)
		return rabbitmqqueue.Acknowledge(), nil
	})
	defer closeLiveConsumer(t, consumer)
	producer := openLiveProducer(t, connection)
	defer closeLiveProducer(t, producer)

	baselineIDs := publishLiveRange(t, producer, ledger, fixture, faultQueue, runToken, "baseline", 0, 8, false)
	waitForConfirmedDeliveries(t, ledger, baselineIDs)
	t.Log("FAULT_WINDOW_READY")
	waitForFaultGate(t, fixture.FaultStartGateFile)
	t.Log("FAULT_WINDOW_STARTED")

	faultAttempts := make([]string, 0, fixture.FaultWindowMessages)
	gateObserved := false
	for index := 0; index < fixture.FaultWindowMessages; index++ {
		if faultGateExists(t, fixture.FaultCompleteGateFile) {
			gateObserved = true
			break
		}
		faultAttempts = append(
			faultAttempts,
			publishLiveRange(t, producer, ledger, fixture, faultQueue, runToken, "fault", index, 1, true)...,
		)
	}
	if !gateObserved && faultGateExists(t, fixture.FaultCompleteGateFile) {
		gateObserved = true
	}
	if !gateObserved || len(faultAttempts) == 0 {
		t.Fatal("fault gate was not observed during the bounded publication window")
	}

	postFaultIDs := publishLiveRange(
		t, producer, ledger, fixture, faultQueue, runToken, "post", 0, postFaultMessages, false,
	)
	allAttempts := append(append(baselineIDs, faultAttempts...), postFaultIDs...)
	waitForConfirmedDeliveries(t, ledger, allAttempts)
	waitForDeliveryQuiet(t, ledger)
	confirmed, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	t.Logf(
		"MESSAGE_OUTCOMES queue_type=%s attempted=%d confirmed=%d ambiguous=%d not_sent=%d delivered=%d duplicates=%d",
		fixture.FaultQueueType, len(allAttempts), confirmed, ambiguous, notSent, delivered, duplicates,
	)
}

func publishLiveRange(
	t *testing.T,
	producer *rabbitmqqueue.Producer,
	ledger *liveClusterLedger,
	fixture liveBrokerFixture,
	queue liveQueue,
	runToken string,
	phase string,
	start int,
	count int,
	allowUnavailable bool,
) []string {
	t.Helper()
	ids := make([]string, 0, count)
	for index := start; index < start+count; index++ {
		messageID := "live-cluster-" + runToken + "-" + phase + "-" + strconv.Itoa(index)
		ctx, cancel := context.WithTimeout(context.Background(), clusterPublishTimeout)
		result, err := producer.Publish(ctx, rabbitmqqueue.Publication{
			Exchange: fixture.Exchange, ExchangeKind: rabbitmqqueue.ExchangeDirect,
			RoutingKey: queue.RoutingKey, Mandatory: true,
			DeliveryMode: rabbitmqqueue.DeliveryPersistent,
			Message:      rabbitmqqueue.Message{Body: []byte("cluster-interruption"), MessageID: messageID},
		})
		cancel()
		if !result.Valid() {
			t.Fatalf("publication %s returned an invalid outcome", messageID)
		}
		switch result.State {
		case rabbitmqqueue.PublishConfirmed:
			if err != nil {
				t.Fatalf("confirmed publication %s returned an error: %v", messageID, err)
			}
		case rabbitmqqueue.PublishAmbiguous, rabbitmqqueue.PublishNotSent:
			if err == nil {
				t.Fatalf("unavailable publication %s returned no error", messageID)
			}
			if !allowUnavailable {
				t.Fatalf("publication %s was %s outside the fault window: %v", messageID, result.State, err)
			}
		case rabbitmqqueue.PublishRejected, rabbitmqqueue.PublishReturned:
			t.Fatalf("publication %s was %s on the bound route: %v", messageID, result.State, err)
		default:
			t.Fatalf("publication %s returned unknown state %q", messageID, result.State)
		}
		ledger.recordAttempt(messageID, result.State)
		ids = append(ids, messageID)
	}
	return ids
}

func waitForConfirmedDeliveries(t *testing.T, ledger *liveClusterLedger, ids []string) {
	t.Helper()
	timer := time.NewTimer(clusterDeliveryTimeout)
	defer timer.Stop()
	for {
		if ledger.allConfirmedObserved(ids) {
			return
		}
		select {
		case <-ledger.observed:
		case <-timer.C:
			t.Fatal("timed out waiting for every confirmed publication to be delivered")
		}
	}
}

func waitForDeliveryQuiet(t *testing.T, ledger *liveClusterLedger) {
	t.Helper()
	quiet := time.NewTimer(500 * time.Millisecond)
	defer quiet.Stop()
	maximum := time.NewTimer(5 * time.Second)
	defer maximum.Stop()
	for {
		select {
		case <-ledger.observed:
			if !quiet.Stop() {
				select {
				case <-quiet.C:
				default:
				}
			}
			quiet.Reset(500 * time.Millisecond)
		case <-quiet.C:
			return
		case <-maximum.C:
			t.Fatal("deliveries did not become quiet after recovery")
		}
	}
}

func faultGateExists(t *testing.T, filename string) bool {
	t.Helper()
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect fault gate: %v", err)
	}
	return false
}

func waitForFaultGate(t *testing.T, filename string) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(clusterDeliveryTimeout)
	defer timer.Stop()
	for {
		if faultGateExists(t, filename) {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("timed out waiting for the external fault-start gate")
		}
	}
}

func (ledger *liveClusterLedger) recordAttempt(messageID string, state rabbitmqqueue.PublishState) {
	ledger.mu.Lock()
	ledger.attempts[messageID] = state
	ledger.mu.Unlock()
}

func (ledger *liveClusterLedger) recordDelivery(messageID string) {
	ledger.mu.Lock()
	ledger.deliveries[messageID]++
	ledger.mu.Unlock()
	select {
	case ledger.observed <- struct{}{}:
	default:
	}
}

func (ledger *liveClusterLedger) allConfirmedObserved(ids []string) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for _, messageID := range ids {
		if ledger.attempts[messageID] == rabbitmqqueue.PublishConfirmed && ledger.deliveries[messageID] == 0 {
			return false
		}
	}
	return true
}

func (ledger *liveClusterLedger) summary(t *testing.T) (
	confirmed int,
	ambiguous int,
	notSent int,
	delivered int,
	duplicates int,
) {
	t.Helper()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for messageID, state := range ledger.attempts {
		switch state {
		case rabbitmqqueue.PublishConfirmed:
			confirmed++
		case rabbitmqqueue.PublishAmbiguous:
			ambiguous++
		case rabbitmqqueue.PublishNotSent:
			notSent++
		}
		count := ledger.deliveries[messageID]
		if state == rabbitmqqueue.PublishNotSent && count > 0 {
			t.Fatalf("not-sent publication %s was delivered", messageID)
		}
		if state == rabbitmqqueue.PublishConfirmed && count == 0 {
			t.Fatalf("confirmed publication %s was not delivered", messageID)
		}
		delivered += count
		if count > 1 {
			duplicates += count - 1
		}
	}
	for messageID := range ledger.deliveries {
		if _, exists := ledger.attempts[messageID]; !exists {
			t.Fatalf("delivery %s had no publication attempt in this run", messageID)
		}
	}
	return confirmed, ambiguous, notSent, delivered, duplicates
}
