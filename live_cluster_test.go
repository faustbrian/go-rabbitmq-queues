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
	liveClusterConfigEnvironment  = "RABBITMQ_QUEUE_CLUSTER_CONFIG"
	minimumFaultWindowMessages    = 64
	maximumFaultWindowMessages    = 10_000
	minimumReconnectStormCycles   = 3
	maximumReconnectStormCycles   = 8
	minimumReconnectResourcePairs = 4
	maximumReconnectResourcePairs = 16
	postFaultMessages             = 16
	clusterPublishTimeout         = 2 * time.Second
	clusterDeliveryTimeout        = 60 * time.Second
	clusterRecoveryAttempts       = rabbitmqqueue.MaxReconnectAttempts
	clusterRecoveryMaxDelay       = 15 * time.Second
	prolongedOutageSampleInterval = 10 * time.Second
	minimumProlongedOutage        = 60 * time.Second
	minimumProlongedOutageSamples = 6
)

var errInvalidLiveCluster = errors.New("invalid live cluster fixture")

type liveFaultScenario string

const (
	liveFaultClassicNodeLoss        liveFaultScenario = "classic-node-loss"
	liveFaultQuorumLeaderLoss       liveFaultScenario = "quorum-leader-loss"
	liveFaultQuorumNetworkPartition liveFaultScenario = "quorum-network-partition"
	liveFaultClusterRestart         liveFaultScenario = "cluster-restart"
	liveFaultReconnectStorm         liveFaultScenario = "reconnect-storm"
	liveFaultRollingUpgrade         liveFaultScenario = "rolling-upgrade"
	liveFaultApplicationRollout     liveFaultScenario = "application-rolling-deployment"
	liveFaultProlongedOutage        liveFaultScenario = "prolonged-outage"
	liveFaultQuorumPerformanceLoss  liveFaultScenario = "quorum-performance-leader-loss"
)

type liveClusterLedger struct {
	mu         sync.Mutex
	attempts   map[string]rabbitmqqueue.PublishState
	deliveries map[string]int
	observed   chan struct{}
}

type liveRecoveryObservationLedger struct {
	mu         sync.Mutex
	counts     map[rabbitmqqueue.ObservationResource]map[rabbitmqqueue.ObservationOutcome]int
	changed    chan struct{}
	reconnects map[rabbitmqqueue.ObservationResource]int
}

type liveRecoveryCheckpoint struct {
	producerRecovered int
	consumerRecovered int
}

type liveApplicationRolloutLedger struct {
	mu            sync.Mutex
	oldDeliveries map[string]int
	newDeliveries map[string]int
}

func TestLiveClusterFixtureValidation(t *testing.T) {
	valid := liveBrokerFixture{
		Endpoints:             []liveEndpoint{{}, {}, {}},
		FaultStartGateFile:    "fault-started",
		FaultCompleteGateFile: "fault-complete",
		FaultWindowMessages:   minimumFaultWindowMessages,
		FaultQueueType:        rabbitmqqueue.QueueClassic,
		FaultScenario:         liveFaultClassicNodeLoss,
	}
	if err := validateLiveClusterFixture(valid); err != nil {
		t.Fatalf("valid classic host-loss fixture: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*liveBrokerFixture)
	}{
		{name: "missing endpoint", mutate: func(value *liveBrokerFixture) { value.Endpoints = value.Endpoints[:2] }},
		{name: "same gate", mutate: func(value *liveBrokerFixture) { value.FaultCompleteGateFile = value.FaultStartGateFile }},
		{name: "small window", mutate: func(value *liveBrokerFixture) { value.FaultWindowMessages-- }},
		{name: "unknown scenario", mutate: func(value *liveBrokerFixture) { value.FaultScenario = "unknown" }},
		{name: "classic leader loss", mutate: func(value *liveBrokerFixture) { value.FaultScenario = liveFaultQuorumLeaderLoss }},
		{name: "classic partition", mutate: func(value *liveBrokerFixture) { value.FaultScenario = liveFaultQuorumNetworkPartition }},
		{name: "classic cluster restart", mutate: func(value *liveBrokerFixture) { value.FaultScenario = liveFaultClusterRestart }},
		{name: "classic prolonged outage", mutate: func(value *liveBrokerFixture) { value.FaultScenario = liveFaultProlongedOutage }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := valid
			test.mutate(&fixture)
			if err := validateLiveClusterFixture(fixture); err == nil {
				t.Fatal("invalid cluster fixture was accepted")
			}
		})
	}

	for _, scenario := range []liveFaultScenario{
		liveFaultQuorumLeaderLoss,
		liveFaultQuorumNetworkPartition,
		liveFaultClusterRestart,
	} {
		fixture := valid
		fixture.FaultQueueType = rabbitmqqueue.QueueQuorum
		fixture.FaultScenario = scenario
		if err := validateLiveClusterFixture(fixture); err != nil {
			t.Fatalf("valid %s fixture: %v", scenario, err)
		}
	}
	prolongedOutage := valid
	prolongedOutage.FaultQueueType = rabbitmqqueue.QueueQuorum
	prolongedOutage.FaultScenario = liveFaultProlongedOutage
	if err := validateLiveClusterFixture(prolongedOutage); err != nil {
		t.Fatalf("valid prolonged-outage fixture: %v", err)
	}
	performanceLeaderLoss := prolongedOutage
	performanceLeaderLoss.FaultScenario = liveFaultQuorumPerformanceLoss
	performanceLeaderLoss.Performance = livePerformanceFixture{
		QueueType: rabbitmqqueue.QueueQuorum,
		Queues: []liveQueue{
			{Name: "performance.one", RoutingKey: "performance.one"},
			{Name: "performance.two", RoutingKey: "performance.two"},
			{Name: "performance.three", RoutingKey: "performance.three"},
			{Name: "performance.four", RoutingKey: "performance.four"},
		},
		DailyMessages: 100_000_000, WarmupSeconds: 5, SampleSeconds: 30, Samples: 3,
		BurstMultiplier: 4, BurstSeconds: 5, PublisherConcurrency: 64,
		ConsumerConcurrency: 16, PayloadBytes: []int{256, 1024, 4096},
		HeaderBytes: []int{0, 64, 512},
	}
	if err := validateLiveClusterFixture(performanceLeaderLoss); err != nil {
		t.Fatalf("valid quorum performance leader-loss fixture: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*liveBrokerFixture)
	}{
		{name: "missing performance profile", mutate: func(value *liveBrokerFixture) {
			value.Performance = livePerformanceFixture{}
		}},
		{name: "classic performance queues", mutate: func(value *liveBrokerFixture) {
			value.Performance.QueueType = rabbitmqqueue.QueueClassic
		}},
	} {
		t.Run("invalid quorum performance leader loss/"+test.name, func(t *testing.T) {
			fixture := performanceLeaderLoss
			test.mutate(&fixture)
			if err := validateLiveClusterFixture(fixture); err == nil {
				t.Fatal("invalid quorum performance leader-loss fixture was accepted")
			}
		})
	}

	rollingUpgrade := valid
	rollingUpgrade.FaultStartGateFile = ""
	rollingUpgrade.FaultCompleteGateFile = ""
	rollingUpgrade.FaultQueueType = rabbitmqqueue.QueueQuorum
	rollingUpgrade.FaultScenario = liveFaultRollingUpgrade
	rollingUpgrade.FaultCycleGateFiles = []string{"upgrade-1", "upgrade-2", "upgrade-3"}
	rollingUpgrade.FaultCycleCompleteGateFiles = []string{"complete-1", "complete-2", "complete-3"}
	if err := validateLiveClusterFixture(rollingUpgrade); err != nil {
		t.Fatalf("valid rolling-upgrade fixture: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*liveBrokerFixture)
	}{
		{name: "wrong cycle count", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleGateFiles = value.FaultCycleGateFiles[:2]
		}},
		{name: "duplicate completion gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleCompleteGateFiles[2] = value.FaultCycleCompleteGateFiles[1]
		}},
		{name: "mixed gate protocols", mutate: func(value *liveBrokerFixture) {
			value.FaultStartGateFile = "fault-started"
		}},
		{name: "classic queue", mutate: func(value *liveBrokerFixture) {
			value.FaultQueueType = rabbitmqqueue.QueueClassic
		}},
		{name: "resource pairs", mutate: func(value *liveBrokerFixture) {
			value.FaultResourcePairs = 1
		}},
	} {
		t.Run("invalid rolling upgrade/"+test.name, func(t *testing.T) {
			fixture := rollingUpgrade
			fixture.FaultCycleGateFiles = append([]string(nil), rollingUpgrade.FaultCycleGateFiles...)
			fixture.FaultCycleCompleteGateFiles = append(
				[]string(nil), rollingUpgrade.FaultCycleCompleteGateFiles...,
			)
			test.mutate(&fixture)
			if err := validateLiveClusterFixture(fixture); err == nil {
				t.Fatal("invalid rolling-upgrade fixture was accepted")
			}
		})
	}

	applicationRollout := valid
	applicationRollout.FaultQueueType = rabbitmqqueue.QueueQuorum
	applicationRollout.FaultScenario = liveFaultApplicationRollout
	applicationRollout.FaultCycleGateFiles = []string{"new-consumer-verified"}
	if err := validateLiveClusterFixture(applicationRollout); err != nil {
		t.Fatalf("valid application rolling-deployment fixture: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*liveBrokerFixture)
	}{
		{name: "missing old-drain gate", mutate: func(value *liveBrokerFixture) {
			value.FaultStartGateFile = ""
		}},
		{name: "missing new-consumer gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleGateFiles = nil
		}},
		{name: "aliased new-consumer gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleGateFiles[0] = value.FaultCompleteGateFile
		}},
		{name: "unexpected completion gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleCompleteGateFiles = []string{"unexpected"}
		}},
	} {
		t.Run("invalid application rolling deployment/"+test.name, func(t *testing.T) {
			fixture := applicationRollout
			fixture.FaultCycleGateFiles = append([]string(nil), applicationRollout.FaultCycleGateFiles...)
			test.mutate(&fixture)
			if err := validateLiveClusterFixture(fixture); err == nil {
				t.Fatal("invalid application rolling-deployment fixture was accepted")
			}
		})
	}

	reconnectStorm := valid
	reconnectStorm.FaultStartGateFile = ""
	reconnectStorm.FaultCompleteGateFile = ""
	reconnectStorm.FaultQueueType = rabbitmqqueue.QueueQuorum
	reconnectStorm.FaultScenario = liveFaultReconnectStorm
	reconnectStorm.FaultCycleGateFiles = []string{"cycle-1", "cycle-2", "cycle-3"}
	reconnectStorm.FaultCycleCompleteGateFiles = []string{"complete-1", "complete-2", "complete-3"}
	reconnectStorm.FaultResourcePairs = minimumReconnectResourcePairs
	if err := validateLiveClusterFixture(reconnectStorm); err != nil {
		t.Fatalf("valid reconnect-storm fixture: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*liveBrokerFixture)
	}{
		{name: "too few cycles", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleGateFiles = value.FaultCycleGateFiles[:minimumReconnectStormCycles-1]
		}},
		{name: "duplicate cycle gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleGateFiles[1] = value.FaultCycleGateFiles[0]
		}},
		{name: "missing cycle completion gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleCompleteGateFiles = nil
		}},
		{name: "duplicate cycle completion gate", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleCompleteGateFiles[1] = value.FaultCycleCompleteGateFiles[0]
		}},
		{name: "overlapping cycle gates", mutate: func(value *liveBrokerFixture) {
			value.FaultCycleCompleteGateFiles[1] = value.FaultCycleGateFiles[1]
		}},
		{name: "mixed gate protocols", mutate: func(value *liveBrokerFixture) {
			value.FaultStartGateFile = "fault-started"
		}},
		{name: "classic reconnect storm", mutate: func(value *liveBrokerFixture) {
			value.FaultQueueType = rabbitmqqueue.QueueClassic
		}},
		{name: "too few resource pairs", mutate: func(value *liveBrokerFixture) {
			value.FaultResourcePairs = minimumReconnectResourcePairs - 1
		}},
	} {
		t.Run("invalid reconnect storm/"+test.name, func(t *testing.T) {
			fixture := reconnectStorm
			fixture.FaultCycleGateFiles = append([]string(nil), reconnectStorm.FaultCycleGateFiles...)
			test.mutate(&fixture)
			if err := validateLiveClusterFixture(fixture); err == nil {
				t.Fatal("invalid reconnect-storm fixture was accepted")
			}
		})
	}
}

func TestLiveClusterConsumerDependencyRecoveryAllowsPausedReadiness(t *testing.T) {
	if !liveClusterConsumerDependencyRecovered(
		rabbitmqqueue.DependencyAvailable,
	) {
		t.Fatal("paused consumer with an available dependency was not recovered")
	}
}

func TestLiveClusterLedgerAccounting(t *testing.T) {
	ledger := &liveClusterLedger{
		attempts:   make(map[string]rabbitmqqueue.PublishState),
		deliveries: make(map[string]int),
		observed:   make(chan struct{}, 1),
	}
	ledger.recordAttempt("confirmed", rabbitmqqueue.PublishConfirmed)
	ledger.recordAttempt("rejected", rabbitmqqueue.PublishRejected)
	ledger.recordAttempt("ambiguous", rabbitmqqueue.PublishAmbiguous)
	ledger.recordAttempt("not-sent", rabbitmqqueue.PublishNotSent)
	if ledger.allConfirmedObserved([]string{"confirmed", "rejected", "ambiguous", "not-sent"}) {
		t.Fatal("confirmed publication was reported observed before delivery")
	}
	ledger.recordDelivery("confirmed")
	ledger.recordDelivery("confirmed")
	ledger.recordDelivery("ambiguous")
	if !ledger.allConfirmedObserved([]string{"confirmed", "rejected", "ambiguous", "not-sent"}) {
		t.Fatal("confirmed publication remained missing after delivery")
	}
	confirmed, rejected, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	if confirmed != 1 || rejected != 1 || ambiguous != 1 || notSent != 1 || delivered != 3 || duplicates != 1 {
		t.Fatalf(
			"ledger summary = (%d, %d, %d, %d, %d, %d), want (1, 1, 1, 1, 3, 1)",
			confirmed, rejected, ambiguous, notSent, delivered, duplicates,
		)
	}
}

func TestLiveRecoveryObservationLedgerRequiresNewRecoveryPerCycle(t *testing.T) {
	ledger := newLiveRecoveryObservationLedger()
	for _, resource := range []rabbitmqqueue.ObservationResource{
		rabbitmqqueue.ObservationProducer, rabbitmqqueue.ObservationConsumer,
	} {
		ledger.record(rabbitmqqueue.Observation{
			Resource: resource, Kind: rabbitmqqueue.ObservationReconnect,
			Outcome: rabbitmqqueue.ObservationAttempted,
		})
		ledger.record(rabbitmqqueue.Observation{
			Resource: resource, Kind: rabbitmqqueue.ObservationConnectionState,
			Outcome: rabbitmqqueue.ObservationRecovered,
		})
	}
	if !ledger.cycleObserved(1, liveRecoveryCheckpoint{}) {
		t.Fatal("first producer and consumer recovery cycle was not observed")
	}
	checkpoint := ledger.checkpoint()
	ledger.record(rabbitmqqueue.Observation{
		Resource: rabbitmqqueue.ObservationProducer, Kind: rabbitmqqueue.ObservationReconnect,
		Outcome: rabbitmqqueue.ObservationAttempted,
	})
	ledger.record(rabbitmqqueue.Observation{
		Resource: rabbitmqqueue.ObservationProducer, Kind: rabbitmqqueue.ObservationConnectionState,
		Outcome: rabbitmqqueue.ObservationRecovered,
	})
	if ledger.cycleObserved(2, checkpoint) {
		t.Fatal("producer-only recovery satisfied a complete recovery cycle")
	}
	ledger.record(rabbitmqqueue.Observation{
		Resource: rabbitmqqueue.ObservationConsumer, Kind: rabbitmqqueue.ObservationReconnect,
		Outcome: rabbitmqqueue.ObservationAttempted,
	})
	ledger.record(rabbitmqqueue.Observation{
		Resource: rabbitmqqueue.ObservationConsumer, Kind: rabbitmqqueue.ObservationConnectionState,
		Outcome: rabbitmqqueue.ObservationRecovered,
	})
	if !ledger.cycleObserved(2, checkpoint) {
		t.Fatal("second producer and consumer recovery cycle was not observed")
	}
}

func validateLiveClusterFixture(fixture liveBrokerFixture) error {
	if len(fixture.Endpoints) != 3 ||
		fixture.FaultWindowMessages < minimumFaultWindowMessages ||
		fixture.FaultWindowMessages > maximumFaultWindowMessages {
		return errInvalidLiveCluster
	}
	if fixture.FaultScenario == liveFaultReconnectStorm {
		if fixture.FaultStartGateFile != "" || fixture.FaultCompleteGateFile != "" ||
			!validReconnectStormGates(fixture.FaultCycleGateFiles, fixture.FaultCycleCompleteGateFiles) ||
			fixture.FaultResourcePairs < minimumReconnectResourcePairs ||
			fixture.FaultResourcePairs > maximumReconnectResourcePairs {
			return errInvalidLiveCluster
		}
	} else if fixture.FaultScenario == liveFaultRollingUpgrade {
		if fixture.FaultStartGateFile != "" || fixture.FaultCompleteGateFile != "" ||
			!validRollingUpgradeGates(fixture.FaultCycleGateFiles, fixture.FaultCycleCompleteGateFiles) ||
			fixture.FaultResourcePairs != 0 {
			return errInvalidLiveCluster
		}
	} else if fixture.FaultScenario == liveFaultApplicationRollout {
		if fixture.FaultStartGateFile == "" || fixture.FaultCompleteGateFile == "" ||
			fixture.FaultStartGateFile == fixture.FaultCompleteGateFile ||
			len(fixture.FaultCycleGateFiles) != 1 || len(fixture.FaultCycleCompleteGateFiles) != 0 ||
			fixture.FaultResourcePairs != 0 ||
			!uniqueFaultGates(
				[]string{fixture.FaultStartGateFile, fixture.FaultCompleteGateFile},
				fixture.FaultCycleGateFiles,
			) {
			return errInvalidLiveCluster
		}
	} else if fixture.FaultStartGateFile == "" || fixture.FaultCompleteGateFile == "" ||
		fixture.FaultStartGateFile == fixture.FaultCompleteGateFile || len(fixture.FaultCycleGateFiles) != 0 ||
		len(fixture.FaultCycleCompleteGateFiles) != 0 ||
		fixture.FaultResourcePairs != 0 {
		return errInvalidLiveCluster
	}
	switch fixture.FaultScenario {
	case liveFaultClassicNodeLoss:
		if fixture.FaultQueueType != rabbitmqqueue.QueueClassic {
			return errInvalidLiveCluster
		}
	case liveFaultQuorumLeaderLoss, liveFaultQuorumNetworkPartition, liveFaultClusterRestart,
		liveFaultReconnectStorm, liveFaultRollingUpgrade, liveFaultApplicationRollout,
		liveFaultProlongedOutage,
		liveFaultQuorumPerformanceLoss:
		if fixture.FaultQueueType != rabbitmqqueue.QueueQuorum {
			return errInvalidLiveCluster
		}
	default:
		return errInvalidLiveCluster
	}
	if fixture.FaultScenario == liveFaultQuorumPerformanceLoss &&
		(fixture.Performance.QueueType != rabbitmqqueue.QueueQuorum || fixture.Performance.Validate() != nil) {
		return errInvalidLiveCluster
	}
	return nil
}

func TestLiveBrokerApplicationRollingDeployment(t *testing.T) {
	fixture := readLiveBrokerFixtureForEnvironment(t, liveClusterConfigEnvironment)
	if err := validateLiveClusterFixture(fixture); err != nil ||
		fixture.FaultScenario != liveFaultApplicationRollout {
		t.Fatalf("validate application rolling-deployment configuration: %v", err)
	}
	gates := []string{
		fixture.FaultStartGateFile,
		fixture.FaultCompleteGateFile,
		fixture.FaultCycleGateFiles[0],
	}
	for _, gate := range gates {
		if faultGateExists(t, gate) {
			t.Fatal("application rollout gates must not exist before the test starts")
		}
	}

	connection := fixture.connection(t)
	connection.Recovery.MaxAttempts = clusterRecoveryAttempts
	connection.Recovery.MaxDelay = clusterRecoveryMaxDelay
	verifyLiveTopology(t, connection, fixture)
	queue := fixture.Quorum
	producer := openLiveProducer(t, connection)
	defer closeLiveProducer(t, producer)
	ledger := &liveClusterLedger{
		attempts: make(map[string]rabbitmqqueue.PublishState), deliveries: make(map[string]int),
		observed: make(chan struct{}, 1),
	}
	rollout := &liveApplicationRolloutLedger{
		oldDeliveries: make(map[string]int), newDeliveries: make(map[string]int),
	}
	runToken := randomLiveToken(t)
	oldAdmitted := make(chan struct{}, 1)
	releaseOld := make(chan struct{})
	oldConsumer := openLiveConsumerWithBounds(
		t, connection, queue, rabbitmqqueue.QueueQuorum, 0, 1, 1, 2*clusterDeliveryTimeout,
		func(_ context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			rollout.recordOld(delivery.MessageID)
			select {
			case oldAdmitted <- struct{}{}:
			default:
			}
			<-releaseOld
			ledger.recordDelivery(delivery.MessageID)
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	oldClosed := false
	oldReleased := false
	defer func() {
		if !oldClosed {
			if !oldReleased {
				close(releaseOld)
			}
			closeLiveConsumer(t, oldConsumer)
		}
	}()
	oldIDs := publishLiveRange(
		t, producer, ledger, fixture, queue, runToken, "rollout-old-admitted", 0, 1, false,
	)
	select {
	case <-oldAdmitted:
	case <-time.After(clusterDeliveryTimeout):
		t.Fatal("timed out waiting for the old consumer to admit work")
	}
	t.Log("APPLICATION_ROLLOUT_OLD_ADMITTED")
	waitForFaultGate(t, fixture.FaultStartGateFile)

	oldClose := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		oldClose <- oldConsumer.Close(ctx)
	}()
	close(releaseOld)
	oldReleased = true
	if err := <-oldClose; err != nil {
		t.Fatalf("drain old application consumer: %v", err)
	}
	oldClosed = true
	t.Log("APPLICATION_ROLLOUT_OLD_DRAINED")
	waitForFaultGate(t, fixture.FaultCompleteGateFile)

	newConsumer := openLiveConsumer(
		t, connection, queue, rabbitmqqueue.QueueQuorum, 0,
		func(_ context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			rollout.recordNew(delivery.MessageID)
			ledger.recordDelivery(delivery.MessageID)
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	defer closeLiveConsumer(t, newConsumer)
	t.Log("APPLICATION_ROLLOUT_NEW_READY")
	waitForFaultGate(t, fixture.FaultCycleGateFiles[0])
	newIDs := publishLiveRange(
		t, producer, ledger, fixture, queue, runToken, "rollout-new", 0, postFaultMessages, false,
	)
	allIDs := append(append([]string(nil), oldIDs...), newIDs...)
	waitForConfirmedDeliveries(t, ledger, allIDs)
	waitForDeliveryQuiet(t, ledger)
	rollout.assert(t, oldIDs, newIDs)
	confirmed, rejected, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	if confirmed != len(allIDs) || rejected != 0 || ambiguous != 0 || notSent != 0 ||
		delivered != len(allIDs) || duplicates != 0 {
		t.Fatal("application rollout did not preserve exact message accounting")
	}
	t.Logf(
		"APPLICATION_ROLLOUT_OUTCOMES old_admitted=%d old_delivered=%d new_attempted=%d new_delivered=%d confirmed=%d duplicates=%d",
		len(oldIDs), len(oldIDs), len(newIDs), len(newIDs), confirmed, duplicates,
	)
}

func validReconnectStormGates(start []string, complete []string) bool {
	if len(start) < minimumReconnectStormCycles || len(start) > maximumReconnectStormCycles ||
		len(start) != len(complete) {
		return false
	}
	return uniqueFaultGates(start, complete)
}

func validRollingUpgradeGates(start []string, complete []string) bool {
	return len(start) == 3 && len(complete) == 3 && uniqueFaultGates(start, complete)
}

func uniqueFaultGates(start []string, complete []string) bool {
	seen := make(map[string]struct{}, len(start)+len(complete))
	for _, gate := range append(append([]string(nil), start...), complete...) {
		if gate == "" {
			return false
		}
		if _, exists := seen[gate]; exists {
			return false
		}
		seen[gate] = struct{}{}
	}
	return true
}

func TestLiveBrokerThreeNodeInterruption(t *testing.T) {
	fixture := readLiveBrokerFixtureForEnvironment(t, liveClusterConfigEnvironment)
	if err := validateLiveClusterFixture(fixture); err != nil {
		t.Fatalf("validate three-node configuration: %v", err)
	}
	gates := []string{fixture.FaultStartGateFile, fixture.FaultCompleteGateFile}
	if fixture.FaultScenario == liveFaultReconnectStorm || fixture.FaultScenario == liveFaultRollingUpgrade {
		gates = append(append([]string(nil), fixture.FaultCycleGateFiles...), fixture.FaultCycleCompleteGateFiles...)
	}
	for _, gate := range gates {
		if faultGateExists(t, gate) {
			t.Fatal("fault gates must not exist before the test starts")
		}
	}

	connection := fixture.connection(t)
	connection.Recovery.MaxAttempts = clusterRecoveryAttempts
	connection.Recovery.MaxDelay = clusterRecoveryMaxDelay
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
	handler := func(
		_ context.Context,
		delivery rabbitmqqueue.Delivery,
	) (rabbitmqqueue.Settlement, error) {
		ledger.recordDelivery(delivery.MessageID)
		return rabbitmqqueue.Acknowledge(), nil
	}
	consumer := openLiveConsumer(t, connection, faultQueue, fixture.FaultQueueType, 2, handler)
	defer closeLiveConsumer(t, consumer)
	producer := openLiveProducer(t, connection)
	defer closeLiveProducer(t, producer)
	producers := []*rabbitmqqueue.Producer{producer}
	consumers := []*rabbitmqqueue.Consumer{consumer}
	if fixture.FaultScenario == liveFaultReconnectStorm {
		for len(producers) < fixture.FaultResourcePairs {
			additionalConsumer := openLiveConsumer(
				t, connection, faultQueue, fixture.FaultQueueType, 2, handler,
			)
			defer closeLiveConsumer(t, additionalConsumer)
			consumers = append(consumers, additionalConsumer)
			additionalProducer := openLiveProducer(t, connection)
			defer closeLiveProducer(t, additionalProducer)
			producers = append(producers, additionalProducer)
		}
	}
	var recoveryObservations []*liveRecoveryObservationLedger
	if fixture.FaultScenario == liveFaultReconnectStorm {
		var stopObservations func()
		recoveryObservations, stopObservations = startLiveRecoveryObservationCollectors(producers, consumers)
		defer stopObservations()
	}

	baselineIDs := publishLiveRange(t, producer, ledger, fixture, faultQueue, runToken, "baseline", 0, 8, false)
	waitForConfirmedDeliveries(t, ledger, baselineIDs)
	t.Log("FAULT_WINDOW_READY")
	if fixture.FaultScenario == liveFaultReconnectStorm {
		runLiveReconnectStorm(
			t, producers, consumers, ledger, recoveryObservations, fixture, faultQueue, runToken, baselineIDs,
		)
		return
	}
	if fixture.FaultScenario == liveFaultRollingUpgrade {
		runLiveRollingUpgrade(t, producer, consumer, ledger, fixture, faultQueue, runToken, baselineIDs)
		return
	}
	if fixture.FaultScenario == liveFaultProlongedOutage {
		runLiveProlongedOutage(t, producer, consumer, ledger, fixture, faultQueue, runToken, baselineIDs)
		return
	}
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
	if len(faultAttempts) == 0 {
		t.Fatal("fault window completed without a publication attempt")
	}
	if !gateObserved {
		waitForFaultGate(t, fixture.FaultCompleteGateFile)
	}
	waitForLiveClusterRecovery(t, producer, consumer)

	postFaultIDs := publishLiveRange(
		t, producer, ledger, fixture, faultQueue, runToken, "post", 0, postFaultMessages, false,
	)
	allAttempts := append(append(baselineIDs, faultAttempts...), postFaultIDs...)
	waitForConfirmedDeliveries(t, ledger, allAttempts)
	waitForDeliveryQuiet(t, ledger)
	confirmed, rejected, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	t.Logf(
		"MESSAGE_OUTCOMES scenario=%s queue_type=%s attempted=%d confirmed=%d rejected=%d ambiguous=%d not_sent=%d delivered=%d duplicates=%d",
		fixture.FaultScenario, fixture.FaultQueueType, len(allAttempts), confirmed, rejected, ambiguous,
		notSent, delivered, duplicates,
	)
}

func runLiveProlongedOutage(
	t *testing.T,
	producer *rabbitmqqueue.Producer,
	consumer *rabbitmqqueue.Consumer,
	ledger *liveClusterLedger,
	fixture liveBrokerFixture,
	faultQueue liveQueue,
	runToken string,
	baselineIDs []string,
) {
	t.Helper()
	waitForFaultGate(t, fixture.FaultStartGateFile)
	waitForLiveClusterResourcesOutage(t, []*rabbitmqqueue.Producer{producer}, []*rabbitmqqueue.Consumer{consumer})
	waitForLiveProlongedOutageHealth(t, producer, consumer)
	outageStarted := time.Now()
	t.Log("PROLONGED_OUTAGE_STARTED")

	allAttempts := append([]string(nil), baselineIDs...)
	samples := 0
	for !faultGateExists(t, fixture.FaultCompleteGateFile) {
		assertLiveProlongedOutageHealth(t, producer, consumer)
		faultIDs := publishLiveRange(
			t, producer, ledger, fixture, faultQueue, runToken, "prolonged-fault", samples, 1, true,
		)
		allAttempts = append(allAttempts, faultIDs...)
		samples++
		t.Logf(
			"PROLONGED_OUTAGE_SAMPLE sample=%d producer_liveness=%s producer_readiness=%s producer_dependency=%s consumer_liveness=%s consumer_readiness=%s consumer_dependency=%s",
			samples, producer.Liveness(), producer.Readiness(), producer.DependencyHealth(),
			consumer.Liveness(), consumer.Readiness(), consumer.DependencyHealth(),
		)
		waitForFaultGateInterval(t, fixture.FaultCompleteGateFile, prolongedOutageSampleInterval)
	}
	outageDuration := time.Since(outageStarted)
	if outageDuration < minimumProlongedOutage || samples < minimumProlongedOutageSamples {
		t.Fatalf(
			"prolonged outage evidence too short: duration=%s samples=%d",
			outageDuration.Round(time.Second), samples,
		)
	}
	t.Logf(
		"PROLONGED_OUTAGE_HEALTH duration_seconds=%d samples=%d liveness=live readiness=not_ready dependency=recovering",
		int(outageDuration/time.Second), samples,
	)
	waitForLiveClusterRecovery(t, producer, consumer)
	postFaultIDs := publishLiveRange(
		t, producer, ledger, fixture, faultQueue, runToken, "prolonged-post", 0, postFaultMessages, false,
	)
	allAttempts = append(allAttempts, postFaultIDs...)
	waitForConfirmedDeliveries(t, ledger, allAttempts)
	waitForDeliveryQuiet(t, ledger)
	confirmed, rejected, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	t.Logf(
		"MESSAGE_OUTCOMES scenario=%s queue_type=%s attempted=%d confirmed=%d rejected=%d ambiguous=%d not_sent=%d delivered=%d duplicates=%d",
		fixture.FaultScenario, fixture.FaultQueueType, len(allAttempts), confirmed, rejected, ambiguous,
		notSent, delivered, duplicates,
	)
}

func waitForLiveProlongedOutageHealth(
	t *testing.T,
	producer *rabbitmqqueue.Producer,
	consumer *rabbitmqqueue.Consumer,
) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if liveProlongedOutageHealthMatches(producer, consumer) {
			return
		}
		select {
		case <-timer.C:
			assertLiveProlongedOutageHealth(t, producer, consumer)
		case <-ticker.C:
		}
	}
}

func assertLiveProlongedOutageHealth(
	t *testing.T,
	producer *rabbitmqqueue.Producer,
	consumer *rabbitmqqueue.Consumer,
) {
	t.Helper()
	if !liveProlongedOutageHealthMatches(producer, consumer) {
		t.Fatalf(
			"prolonged outage health changed: producer=(%s, %s, %s) consumer=(%s, %s, %s)",
			producer.Liveness(), producer.Readiness(), producer.DependencyHealth(),
			consumer.Liveness(), consumer.Readiness(), consumer.DependencyHealth(),
		)
	}
}

func liveProlongedOutageHealthMatches(
	producer *rabbitmqqueue.Producer,
	consumer *rabbitmqqueue.Consumer,
) bool {
	return producer.Liveness() == rabbitmqqueue.LivenessLive &&
		producer.Readiness() == rabbitmqqueue.ReadinessNotReady &&
		producer.DependencyHealth() == rabbitmqqueue.DependencyRecovering &&
		consumer.Liveness() == rabbitmqqueue.LivenessLive &&
		consumer.Readiness() == rabbitmqqueue.ReadinessNotReady &&
		consumer.DependencyHealth() == rabbitmqqueue.DependencyRecovering
}

func waitForFaultGateInterval(t *testing.T, filename string, interval time.Duration) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		if faultGateExists(t, filename) {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return
		}
	}
}

func runLiveRollingUpgrade(
	t *testing.T,
	producer *rabbitmqqueue.Producer,
	consumer *rabbitmqqueue.Consumer,
	ledger *liveClusterLedger,
	fixture liveBrokerFixture,
	faultQueue liveQueue,
	runToken string,
	baselineIDs []string,
) {
	t.Helper()
	allAttempts := append([]string(nil), baselineIDs...)
	for index, gate := range fixture.FaultCycleGateFiles {
		cycle := index + 1
		t.Logf("UPGRADE_CYCLE_WAITING cycle=%d", cycle)
		waitForFaultGate(t, gate)
		t.Logf("UPGRADE_CYCLE_STARTED cycle=%d", cycle)
		faultIDs := publishLiveRange(
			t, producer, ledger, fixture, faultQueue, runToken, "upgrade-fault", index*fixture.FaultWindowMessages,
			1, true,
		)
		allAttempts = append(allAttempts, faultIDs...)
		t.Logf("UPGRADE_CYCLE_FAULT_OBSERVED cycle=%d", cycle)
		faultIDs = publishLiveRange(
			t, producer, ledger, fixture, faultQueue, runToken, "upgrade-fault",
			index*fixture.FaultWindowMessages+1, fixture.FaultWindowMessages-1, true,
		)
		allAttempts = append(allAttempts, faultIDs...)
		t.Logf("UPGRADE_CYCLE_MESSAGES_READY cycle=%d", cycle)
		waitForLiveClusterRecovery(t, producer, consumer)
		t.Logf("UPGRADE_CYCLE_CLIENT_READY cycle=%d", cycle)
		waitForFaultGate(t, fixture.FaultCycleCompleteGateFiles[index])
		postCycleIDs := publishLiveRange(
			t, producer, ledger, fixture, faultQueue, runToken, "upgrade-post", index, 1, false,
		)
		allAttempts = append(allAttempts, postCycleIDs...)
		waitForConfirmedDeliveries(t, ledger, postCycleIDs)
		t.Logf("UPGRADE_CYCLE_VERIFIED cycle=%d", cycle)
	}
	waitForConfirmedDeliveries(t, ledger, allAttempts)
	waitForDeliveryQuiet(t, ledger)
	confirmed, rejected, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	t.Logf(
		"MESSAGE_OUTCOMES scenario=%s queue_type=%s attempted=%d confirmed=%d rejected=%d ambiguous=%d not_sent=%d delivered=%d duplicates=%d",
		fixture.FaultScenario, fixture.FaultQueueType, len(allAttempts), confirmed, rejected, ambiguous,
		notSent, delivered, duplicates,
	)
}

func runLiveReconnectStorm(
	t *testing.T,
	producers []*rabbitmqqueue.Producer,
	consumers []*rabbitmqqueue.Consumer,
	ledger *liveClusterLedger,
	recoveryObservations []*liveRecoveryObservationLedger,
	fixture liveBrokerFixture,
	faultQueue liveQueue,
	runToken string,
	baselineIDs []string,
) {
	t.Helper()
	producer := producers[0]
	allAttempts := append([]string(nil), baselineIDs...)
	for index, gate := range fixture.FaultCycleGateFiles {
		cycle := index + 1
		t.Logf("RECONNECT_CYCLE_WAITING cycle=%d", cycle)
		waitForFaultGate(t, gate)
		t.Logf("RECONNECT_CYCLE_STARTED cycle=%d", cycle)
		faultIDs := publishLiveRange(
			t, producer, ledger, fixture, faultQueue, runToken, "reconnect-fault", index, 1, true,
		)
		allAttempts = append(allAttempts, faultIDs...)
		waitForLiveClusterResourcesOutage(t, producers, consumers)
		checkpoints := make([]liveRecoveryCheckpoint, len(recoveryObservations))
		for observationIndex, observations := range recoveryObservations {
			checkpoints[observationIndex] = observations.checkpoint()
		}
		for observationIndex, observations := range recoveryObservations {
			observations.waitForCycle(t, cycle, checkpoints[observationIndex])
		}
		waitForLiveClusterResourcesRecovery(t, producers, consumers)
		postCycleIDs := publishLiveRange(
			t, producer, ledger, fixture, faultQueue, runToken, "reconnect-post", index, 1, false,
		)
		allAttempts = append(allAttempts, postCycleIDs...)
		waitForConfirmedDeliveries(t, ledger, postCycleIDs)
		t.Logf("RECOVERY_CYCLE_READY cycle=%d", cycle)
		waitForFaultGate(t, fixture.FaultCycleCompleteGateFiles[index])
		t.Logf("RECOVERY_CYCLE_VERIFIED cycle=%d", cycle)
	}
	waitForConfirmedDeliveries(t, ledger, allAttempts)
	waitForDeliveryQuiet(t, ledger)
	confirmed, rejected, ambiguous, notSent, delivered, duplicates := ledger.summary(t)
	producerReconnects, consumerReconnects := 0, 0
	for _, observations := range recoveryObservations {
		pairProducerReconnects, pairConsumerReconnects := observations.reconnectAttempts()
		producerReconnects += pairProducerReconnects
		consumerReconnects += pairConsumerReconnects
	}
	t.Logf(
		"RECOVERY_OUTCOMES scenario=%s cycles=%d resource_pairs=%d producer_reconnects=%d consumer_reconnects=%d",
		fixture.FaultScenario, len(fixture.FaultCycleGateFiles), len(recoveryObservations),
		producerReconnects, consumerReconnects,
	)
	t.Logf(
		"MESSAGE_OUTCOMES scenario=%s queue_type=%s attempted=%d confirmed=%d rejected=%d ambiguous=%d not_sent=%d delivered=%d duplicates=%d",
		fixture.FaultScenario, fixture.FaultQueueType, len(allAttempts), confirmed, rejected, ambiguous,
		notSent, delivered, duplicates,
	)
}

func waitForLiveClusterResourcesOutage(
	t *testing.T,
	producers []*rabbitmqqueue.Producer,
	consumers []*rabbitmqqueue.Consumer,
) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if liveClusterResourcesUnavailable(producers, consumers) {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("cluster outage was not observed by every producer and consumer")
		case <-ticker.C:
		}
	}
}

func liveClusterResourcesUnavailable(
	producers []*rabbitmqqueue.Producer,
	consumers []*rabbitmqqueue.Consumer,
) bool {
	for _, producer := range producers {
		if producer.Readiness() == rabbitmqqueue.ReadinessReady {
			return false
		}
	}
	for _, consumer := range consumers {
		if consumer.Readiness() == rabbitmqqueue.ReadinessReady {
			return false
		}
	}
	return true
}

func liveClusterConsumerDependencyRecovered(
	dependency rabbitmqqueue.DependencyHealth,
) bool {
	return dependency == rabbitmqqueue.DependencyAvailable
}

func waitForLiveClusterResourceDependenciesRecovery(
	t *testing.T,
	producers []*rabbitmqqueue.Producer,
	consumers []*rabbitmqqueue.Consumer,
) {
	t.Helper()
	timer := time.NewTimer(clusterDeliveryTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		recovered := true
		for _, producer := range producers {
			recovered = recovered && producer.Readiness() == rabbitmqqueue.ReadinessReady
		}
		for _, consumer := range consumers {
			recovered = recovered && liveClusterConsumerDependencyRecovered(
				consumer.DependencyHealth(),
			)
		}
		if recovered {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("not every cluster producer and consumer dependency recovered")
		case <-ticker.C:
		}
	}
}

func waitForLiveClusterResourcesRecovery(
	t *testing.T,
	producers []*rabbitmqqueue.Producer,
	consumers []*rabbitmqqueue.Consumer,
) {
	t.Helper()
	timer := time.NewTimer(clusterDeliveryTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for _, producer := range producers {
			ready = ready && producer.Readiness() == rabbitmqqueue.ReadinessReady
		}
		for _, consumer := range consumers {
			ready = ready && consumer.Readiness() == rabbitmqqueue.ReadinessReady
		}
		if ready {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("not every cluster producer and consumer recovered")
		case <-ticker.C:
		}
	}
}

func waitForLiveClusterRecovery(
	t *testing.T,
	producer *rabbitmqqueue.Producer,
	consumer *rabbitmqqueue.Consumer,
) {
	t.Helper()
	timer := time.NewTimer(clusterDeliveryTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if producer.Readiness() == rabbitmqqueue.ReadinessReady &&
			consumer.Readiness() == rabbitmqqueue.ReadinessReady {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf(
				"cluster resources did not recover: producer=(%s, %s) consumer=(%s, %s)",
				producer.Readiness(), producer.DependencyHealth(),
				consumer.Readiness(), consumer.DependencyHealth(),
			)
		case <-ticker.C:
		}
	}
}

func startLiveRecoveryObservationCollectors(
	producers []*rabbitmqqueue.Producer,
	consumers []*rabbitmqqueue.Consumer,
) ([]*liveRecoveryObservationLedger, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var collectors sync.WaitGroup
	ledgers := make([]*liveRecoveryObservationLedger, len(producers))
	for index := range producers {
		ledger := newLiveRecoveryObservationLedger()
		ledgers[index] = ledger
		for _, observations := range []<-chan rabbitmqqueue.Observation{
			producers[index].Observations(), consumers[index].Observations(),
		} {
			collectors.Add(1)
			go func() {
				defer collectors.Done()
				ledger.collect(ctx, observations)
			}()
		}
	}
	return ledgers, func() {
		cancel()
		collectors.Wait()
	}
}

func newLiveRecoveryObservationLedger() *liveRecoveryObservationLedger {
	return &liveRecoveryObservationLedger{
		counts: map[rabbitmqqueue.ObservationResource]map[rabbitmqqueue.ObservationOutcome]int{
			rabbitmqqueue.ObservationProducer: {},
			rabbitmqqueue.ObservationConsumer: {},
		},
		changed: make(chan struct{}, 1),
		reconnects: map[rabbitmqqueue.ObservationResource]int{
			rabbitmqqueue.ObservationProducer: 0,
			rabbitmqqueue.ObservationConsumer: 0,
		},
	}
}

func (ledger *liveRecoveryObservationLedger) collect(
	ctx context.Context,
	observations <-chan rabbitmqqueue.Observation,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case observation, open := <-observations:
			if !open {
				return
			}
			if ledger.record(observation) {
				select {
				case ledger.changed <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (ledger *liveRecoveryObservationLedger) record(observation rabbitmqqueue.Observation) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if _, exists := ledger.counts[observation.Resource]; !exists {
		return false
	}
	changed := false
	if observation.Kind == rabbitmqqueue.ObservationConnectionState &&
		(observation.Outcome == rabbitmqqueue.ObservationRecovering ||
			observation.Outcome == rabbitmqqueue.ObservationRecovered) {
		ledger.counts[observation.Resource][observation.Outcome]++
		changed = true
	}
	if observation.Kind == rabbitmqqueue.ObservationReconnect &&
		observation.Outcome == rabbitmqqueue.ObservationAttempted {
		ledger.reconnects[observation.Resource]++
		changed = true
	}
	return changed
}

func (ledger *liveRecoveryObservationLedger) waitForCycle(
	t *testing.T,
	cycle int,
	checkpoint liveRecoveryCheckpoint,
) {
	t.Helper()
	timer := time.NewTimer(clusterDeliveryTimeout)
	defer timer.Stop()
	for {
		if ledger.cycleObserved(cycle, checkpoint) {
			return
		}
		select {
		case <-ledger.changed:
		case <-timer.C:
			producerReconnects, consumerReconnects := ledger.reconnectAttempts()
			t.Fatalf(
				"reconnect cycle %d was not observed: producer_reconnects=%d consumer_reconnects=%d",
				cycle, producerReconnects, consumerReconnects,
			)
		}
	}
}

func (ledger *liveRecoveryObservationLedger) checkpoint() liveRecoveryCheckpoint {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return liveRecoveryCheckpoint{
		producerRecovered: ledger.counts[rabbitmqqueue.ObservationProducer][rabbitmqqueue.ObservationRecovered],
		consumerRecovered: ledger.counts[rabbitmqqueue.ObservationConsumer][rabbitmqqueue.ObservationRecovered],
	}
}

func (ledger *liveRecoveryObservationLedger) cycleObserved(
	cycle int,
	checkpoint liveRecoveryCheckpoint,
) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.counts[rabbitmqqueue.ObservationProducer][rabbitmqqueue.ObservationRecovered] >
		checkpoint.producerRecovered &&
		ledger.counts[rabbitmqqueue.ObservationConsumer][rabbitmqqueue.ObservationRecovered] >
			checkpoint.consumerRecovered &&
		ledger.reconnects[rabbitmqqueue.ObservationProducer] >= cycle &&
		ledger.reconnects[rabbitmqqueue.ObservationConsumer] >= cycle
}

func (ledger *liveRecoveryObservationLedger) reconnectAttempts() (producer int, consumer int) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.reconnects[rabbitmqqueue.ObservationProducer], ledger.reconnects[rabbitmqqueue.ObservationConsumer]
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
		case rabbitmqqueue.PublishRejected, rabbitmqqueue.PublishAmbiguous, rabbitmqqueue.PublishNotSent:
			if err == nil {
				t.Fatalf("unavailable publication %s returned no error", messageID)
			}
			if !allowUnavailable {
				t.Fatalf("publication %s was %s outside the fault window: %v", messageID, result.State, err)
			}
		case rabbitmqqueue.PublishReturned:
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

func (ledger *liveApplicationRolloutLedger) recordOld(messageID string) {
	ledger.mu.Lock()
	ledger.oldDeliveries[messageID]++
	ledger.mu.Unlock()
}

func (ledger *liveApplicationRolloutLedger) recordNew(messageID string) {
	ledger.mu.Lock()
	ledger.newDeliveries[messageID]++
	ledger.mu.Unlock()
}

func (ledger *liveApplicationRolloutLedger) assert(t *testing.T, oldIDs []string, newIDs []string) {
	t.Helper()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for _, messageID := range oldIDs {
		if ledger.oldDeliveries[messageID] != 1 || ledger.newDeliveries[messageID] != 0 {
			t.Fatalf("old application delivery %s crossed the rollout boundary", messageID)
		}
	}
	for _, messageID := range newIDs {
		if ledger.oldDeliveries[messageID] != 0 || ledger.newDeliveries[messageID] != 1 {
			t.Fatalf("new application delivery %s crossed the rollout boundary", messageID)
		}
	}
	if len(ledger.oldDeliveries) != len(oldIDs) || len(ledger.newDeliveries) != len(newIDs) {
		t.Fatal("application rollout observed an unexpected delivery identity")
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
	rejected int,
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
		case rabbitmqqueue.PublishRejected:
			rejected++
		case rabbitmqqueue.PublishAmbiguous:
			ambiguous++
		case rabbitmqqueue.PublishNotSent:
			notSent++
		}
		count := ledger.deliveries[messageID]
		if (state == rabbitmqqueue.PublishRejected || state == rabbitmqqueue.PublishNotSent) && count > 0 {
			t.Fatalf("%s publication %s was delivered", state, messageID)
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
	return confirmed, rejected, ambiguous, notSent, delivered, duplicates
}
