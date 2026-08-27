//go:build livebroker

package rabbitmqqueue_test

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

const (
	livePerformanceConfigEnvironment = "RABBITMQ_QUEUE_PERFORMANCE_CONFIG"
	secondsPerDay                    = 86_400
	maximumPerformanceMessages       = 250_000
	performanceOfferHeadroom         = 1.01
)

var errInvalidLivePerformance = errors.New("invalid live performance fixture")

type livePerformanceSample struct {
	Mode        string
	Index       int
	TargetRate  float64
	OfferedRate float64
	Messages    int
	Elapsed     time.Duration
	Drain       time.Duration
	Achieved    float64
	Confirmed   int
	Rejected    int
	Ambiguous   int
	NotSent     int
	Delivered   int
	Duplicates  int
	Invalid     int64
}

type livePerformanceRateSummary struct {
	Samples   int
	MetTarget int
	Minimum   float64
	Median    float64
	Maximum   float64
}

func (summary livePerformanceRateSummary) MeetsTarget() bool {
	return summary.Samples > 0 && summary.MetTarget > summary.Samples/2
}

type livePerformanceJob struct {
	MessageID string
	Queue     int
	Shape     int
}

type livePerformanceReceiver struct {
	mu     sync.RWMutex
	ledger *liveClusterLedger
	orphan atomic.Int64
}

type livePerformanceSession struct {
	producer *rabbitmqqueue.Producer
	receiver *livePerformanceReceiver
	payloads [][]byte
	headers  [][]byte
}

func TestLivePerformanceFixtureValidation(t *testing.T) {
	valid := livePerformanceFixture{
		QueueType: rabbitmqqueue.QueueQuorum,
		Queues: []liveQueue{
			{Name: "one", RoutingKey: "one"},
			{Name: "two", RoutingKey: "two"},
			{Name: "three", RoutingKey: "three"},
			{Name: "four", RoutingKey: "four"},
		},
		DailyMessages: 100_000_000, WarmupSeconds: 5, SampleSeconds: 30, Samples: 3,
		BurstMultiplier: 4, BurstSeconds: 10, PublisherConcurrency: 32,
		ConsumerConcurrency: 8, PayloadBytes: []int{256, 1024, 4096},
		HeaderBytes: []int{0, 64, 512}, HandlerDelayMillis: 1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid performance fixture: %v", err)
	}
	tests := []func(*livePerformanceFixture){
		func(value *livePerformanceFixture) { value.QueueType = "stream" },
		func(value *livePerformanceFixture) { value.Queues = value.Queues[:3] },
		func(value *livePerformanceFixture) { value.Queues[3] = value.Queues[0] },
		func(value *livePerformanceFixture) { value.DailyMessages = 2_000_000 },
		func(value *livePerformanceFixture) { value.SampleSeconds = 1 },
		func(value *livePerformanceFixture) { value.Samples = 1 },
		func(value *livePerformanceFixture) { value.BurstMultiplier = 1 },
		func(value *livePerformanceFixture) { value.PublisherConcurrency = 0 },
		func(value *livePerformanceFixture) { value.ConsumerConcurrency = 0 },
		func(value *livePerformanceFixture) { value.PayloadBytes = nil },
		func(value *livePerformanceFixture) { value.HeaderBytes = nil },
		func(value *livePerformanceFixture) {
			value.PayloadBytes = []int{
				rabbitmqqueue.DefaultLimits().MaxPayloadBytes,
				rabbitmqqueue.DefaultLimits().MaxPayloadBytes,
			}
		},
		func(value *livePerformanceFixture) {
			value.HeaderBytes = []int{
				rabbitmqqueue.DefaultLimits().MaxHeaderBytes - 256,
				rabbitmqqueue.DefaultLimits().MaxHeaderBytes - 256,
			}
		},
		func(value *livePerformanceFixture) { value.HandlerDelayMillis = -1 },
	}
	for index, mutate := range tests {
		candidate := valid
		candidate.Queues = append([]liveQueue(nil), valid.Queues...)
		candidate.PayloadBytes = append([]int(nil), valid.PayloadBytes...)
		candidate.HeaderBytes = append([]int(nil), valid.HeaderBytes...)
		mutate(&candidate)
		if err := candidate.Validate(); !errors.Is(err, errInvalidLivePerformance) {
			t.Fatalf("invalid fixture %d error = %v, want %v", index, err, errInvalidLivePerformance)
		}
	}
}

func TestLivePerformanceRateProfileValidation(t *testing.T) {
	tests := []struct {
		name    string
		rates   []float64
		target  float64
		wantMet bool
	}{
		{name: "every sample meets target", rates: []float64{101, 102, 103}, target: 100, wantMet: true},
		{name: "one runner pause is retained", rates: []float64{101, 90, 102}, target: 100, wantMet: true},
		{name: "persistent miss fails", rates: []float64{99, 98, 101}, target: 100, wantMet: false},
		{name: "even sample set requires a majority", rates: []float64{99, 98, 101, 102}, target: 100, wantMet: false},
		{name: "empty sample set fails", target: 100, wantMet: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			samples := make([]livePerformanceSample, len(test.rates))
			for index, rate := range test.rates {
				samples[index] = livePerformanceSample{Achieved: rate}
			}
			summary := summarizeLivePerformanceRates(samples, test.target)
			if got := summary.MeetsTarget(); got != test.wantMet {
				t.Fatalf("MeetsTarget() = %t, want %t; summary = %#v", got, test.wantMet, summary)
			}
		})
	}
}

func TestLiveBrokerPerformanceProfiles(t *testing.T) {
	fixture := decodeLiveBrokerFixture(t, livePerformanceConfigEnvironment)
	performance := fixture.Performance
	if err := performance.Validate(); err != nil ||
		(len(fixture.Endpoints) != 1 && len(fixture.Endpoints) != 3) {
		t.Fatalf("validate live performance fixture: %v", errInvalidLivePerformance)
	}
	connection := fixture.connection(t)
	verifyLivePerformanceTopology(t, connection, fixture.Exchange, performance)
	session := openLivePerformanceSession(t, connection, performance)
	baseRate := float64(performance.DailyMessages) / secondsPerDay
	t.Logf(
		"PERFORMANCE_RUN go=%s os=%s arch=%s endpoints=%d queue_type=%s daily_messages=%d samples=%d",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, len(fixture.Endpoints),
		performance.QueueType, performance.DailyMessages, performance.Samples,
	)

	runLivePerformanceSample(
		t, session, fixture.Exchange, performance,
		livePerformanceSample{Mode: "warmup", TargetRate: baseRate},
		performance.WarmupSeconds,
	)
	steadySamples := make([]livePerformanceSample, 0, performance.Samples)
	for sample := 1; sample <= performance.Samples; sample++ {
		steadySamples = append(steadySamples, runLivePerformanceSample(
			t, session, fixture.Exchange, performance,
			livePerformanceSample{Mode: "steady", Index: sample, TargetRate: baseRate},
			performance.SampleSeconds,
		))
	}
	requireLivePerformanceRateProfile(t, performance.QueueType, "steady", steadySamples)
	burstRate := baseRate * float64(performance.BurstMultiplier)
	burstSamples := make([]livePerformanceSample, 0, performance.Samples)
	for sample := 1; sample <= performance.Samples; sample++ {
		burstSamples = append(burstSamples, runLivePerformanceSample(
			t, session, fixture.Exchange, performance,
			livePerformanceSample{Mode: "burst", Index: sample, TargetRate: burstRate},
			performance.BurstSeconds,
		))
	}
	requireLivePerformanceRateProfile(t, performance.QueueType, "burst", burstSamples)
}

func summarizeLivePerformanceRates(
	samples []livePerformanceSample,
	target float64,
) livePerformanceRateSummary {
	summary := livePerformanceRateSummary{Samples: len(samples)}
	if len(samples) == 0 {
		return summary
	}
	rates := make([]float64, len(samples))
	for index, sample := range samples {
		rates[index] = sample.Achieved
		if sample.Achieved >= target {
			summary.MetTarget++
		}
	}
	sort.Float64s(rates)
	summary.Minimum = rates[0]
	summary.Median = rates[len(rates)/2]
	summary.Maximum = rates[len(rates)-1]
	return summary
}

func requireLivePerformanceRateProfile(
	t *testing.T,
	queueType rabbitmqqueue.QueueType,
	mode string,
	samples []livePerformanceSample,
) {
	t.Helper()
	target := 0.0
	if len(samples) > 0 {
		target = samples[0].TargetRate
	}
	summary := summarizeLivePerformanceRates(samples, target)
	t.Logf(
		"PERFORMANCE_PROFILE mode=%s queue_type=%s samples=%d target_per_second=%.2f met_target=%d minimum_per_second=%.2f median_per_second=%.2f maximum_per_second=%.2f",
		mode, queueType, summary.Samples, target, summary.MetTarget,
		summary.Minimum, summary.Median, summary.Maximum,
	)
	if !summary.MeetsTarget() {
		t.Fatal("live performance profile did not sustain its required throughput across a majority of samples")
	}
}

func (fixture livePerformanceFixture) Validate() error {
	if (fixture.QueueType != rabbitmqqueue.QueueClassic && fixture.QueueType != rabbitmqqueue.QueueQuorum) ||
		len(fixture.Queues) != 4 ||
		(fixture.DailyMessages != 1_000_000 && fixture.DailyMessages != 10_000_000 &&
			fixture.DailyMessages != 100_000_000) ||
		fixture.WarmupSeconds < 5 || fixture.WarmupSeconds > 30 ||
		fixture.SampleSeconds < 30 || fixture.SampleSeconds > 300 ||
		fixture.Samples < 3 || fixture.Samples > 10 ||
		fixture.BurstMultiplier < 2 || fixture.BurstMultiplier > 20 ||
		fixture.BurstSeconds < 5 || fixture.BurstSeconds > 60 ||
		fixture.PublisherConcurrency < 1 || fixture.PublisherConcurrency > 512 ||
		fixture.ConsumerConcurrency < 1 || fixture.ConsumerConcurrency > rabbitmqqueue.MaxConsumerConcurrency ||
		len(fixture.PayloadBytes) == 0 || len(fixture.PayloadBytes) > 128 ||
		len(fixture.HeaderBytes) == 0 || len(fixture.HeaderBytes) > 128 ||
		fixture.HandlerDelayMillis < 0 || fixture.HandlerDelayMillis > 1_000 {
		return errInvalidLivePerformance
	}
	payloadBytes := 0
	for _, size := range fixture.PayloadBytes {
		if size < 1 || size > rabbitmqqueue.DefaultLimits().MaxPayloadBytes-payloadBytes {
			return errInvalidLivePerformance
		}
		payloadBytes += size
	}
	headerBytes := 0
	for _, size := range fixture.HeaderBytes {
		if size < 0 || size > rabbitmqqueue.DefaultLimits().MaxHeaderBytes-256-headerBytes {
			return errInvalidLivePerformance
		}
		headerBytes += size
	}
	names := make(map[string]struct{}, len(fixture.Queues))
	routes := make(map[string]struct{}, len(fixture.Queues))
	for _, queue := range fixture.Queues {
		if (rabbitmqqueue.Queue{
			Name: queue.Name, Type: fixture.QueueType, Durable: true,
		}).Validate() != nil || (rabbitmqqueue.Publication{
			Exchange: "performance", ExchangeKind: rabbitmqqueue.ExchangeDirect,
			RoutingKey: queue.RoutingKey, DeliveryMode: rabbitmqqueue.DeliveryPersistent,
			Message: rabbitmqqueue.Message{MessageID: "validation"},
		}).Validate(rabbitmqqueue.DefaultLimits()) != nil {
			return errInvalidLivePerformance
		}
		if _, exists := names[queue.Name]; exists {
			return errInvalidLivePerformance
		}
		if _, exists := routes[queue.RoutingKey]; exists {
			return errInvalidLivePerformance
		}
		names[queue.Name] = struct{}{}
		routes[queue.RoutingKey] = struct{}{}
	}
	maximumRate := float64(fixture.DailyMessages) / secondsPerDay * float64(fixture.BurstMultiplier)
	if math.Ceil(maximumRate*float64(fixture.BurstSeconds)) > maximumPerformanceMessages ||
		math.Ceil(float64(fixture.DailyMessages)/secondsPerDay*float64(fixture.SampleSeconds)) >
			maximumPerformanceMessages {
		return errInvalidLivePerformance
	}
	return nil
}

func runLivePerformanceSample(
	t *testing.T,
	session *livePerformanceSession,
	exchange string,
	fixture livePerformanceFixture,
	sample livePerformanceSample,
	durationSeconds int,
) livePerformanceSample {
	t.Helper()
	sample.OfferedRate = sample.TargetRate * performanceOfferHeadroom
	sample.Messages = int(math.Ceil(sample.OfferedRate * float64(durationSeconds)))
	ledger := &liveClusterLedger{
		attempts:   make(map[string]rabbitmqqueue.PublishState, sample.Messages),
		deliveries: make(map[string]int, sample.Messages),
		observed:   make(chan struct{}, 1),
	}
	if orphans := session.receiver.orphan.Swap(0); orphans != 0 {
		t.Fatalf("received %d deliveries outside a measured sample", orphans)
	}
	session.receiver.setLedger(ledger)
	attached := true
	defer func() {
		if attached {
			session.receiver.setLedger(nil)
		}
	}()
	runToken := randomLiveToken(t)
	ids := make([]string, sample.Messages)
	jobs := make(chan livePerformanceJob, fixture.PublisherConcurrency)
	var invalid atomic.Int64
	var workers sync.WaitGroup
	workers.Add(fixture.PublisherConcurrency)
	for range fixture.PublisherConcurrency {
		go func() {
			defer workers.Done()
			for job := range jobs {
				queue := fixture.Queues[job.Queue]
				ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
				result, err := session.producer.Publish(ctx, rabbitmqqueue.Publication{
					Exchange: exchange, ExchangeKind: rabbitmqqueue.ExchangeDirect,
					RoutingKey: queue.RoutingKey, Mandatory: true,
					DeliveryMode: rabbitmqqueue.DeliveryPersistent,
					Message: rabbitmqqueue.Message{
						Body:      session.payloads[job.Shape%len(session.payloads)],
						MessageID: job.MessageID, ContentType: "application/octet-stream",
						Headers: []rabbitmqqueue.Header{
							rabbitmqqueue.Int64Header("schema-version", 1),
							rabbitmqqueue.StringHeader("profile", sample.Mode),
							rabbitmqqueue.BytesHeader(
								"metadata", session.headers[job.Shape%len(session.headers)],
							),
						},
					},
				})
				cancel()
				if !result.Valid() ||
					(result.State == rabbitmqqueue.PublishConfirmed) != (err == nil) {
					invalid.Add(1)
				}
				ledger.recordAttempt(job.MessageID, result.State)
			}
		}()
	}

	started := time.Now()
	interval := float64(time.Second) / sample.OfferedRate
	for index := 0; index < sample.Messages; index++ {
		messageID := "live-performance-" + runToken + "-" + sample.Mode + "-" + strconv.Itoa(index)
		ids[index] = messageID
		due := started.Add(time.Duration(float64(index) * interval))
		if remaining := time.Until(due); remaining > 0 {
			timer := time.NewTimer(remaining)
			<-timer.C
		}
		jobs <- livePerformanceJob{
			MessageID: messageID, Queue: index % len(fixture.Queues), Shape: index,
		}
	}
	close(jobs)
	workers.Wait()
	sample.Elapsed = time.Since(started)
	drainStarted := time.Now()
	waitForConfirmedDeliveries(t, ledger, ids)
	sample.Drain = time.Since(drainStarted)
	waitForDeliveryQuiet(t, ledger)
	session.receiver.setLedger(nil)
	attached = false
	sample.Confirmed, sample.Rejected, sample.Ambiguous, sample.NotSent,
		sample.Delivered, sample.Duplicates = ledger.summary(t)
	sample.Invalid = invalid.Load()
	sample.Achieved = float64(sample.Messages) / sample.Elapsed.Seconds()
	errorRate := float64(sample.Messages-sample.Confirmed) / float64(sample.Messages)
	t.Logf(
		"PERFORMANCE_SAMPLE mode=%s sample=%d queue_type=%s messages=%d target_per_second=%.2f offered_per_second=%.2f achieved_per_second=%.2f publish_elapsed=%s backlog_drain=%s confirmed=%d rejected=%d ambiguous=%d not_sent=%d delivered=%d duplicates=%d invalid=%d error_rate=%.6f",
		sample.Mode, sample.Index, fixture.QueueType, sample.Messages, sample.TargetRate, sample.OfferedRate,
		sample.Achieved, sample.Elapsed, sample.Drain, sample.Confirmed, sample.Rejected, sample.Ambiguous,
		sample.NotSent, sample.Delivered, sample.Duplicates, sample.Invalid, errorRate,
	)
	if sample.Invalid != 0 || sample.Confirmed != sample.Messages || sample.Rejected != 0 || sample.Ambiguous != 0 ||
		sample.NotSent != 0 || sample.Delivered != sample.Messages || sample.Duplicates != 0 {
		t.Fatal("live performance sample did not satisfy its correctness profile")
	}
	return sample
}

func openLivePerformanceSession(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	fixture livePerformanceFixture,
) *livePerformanceSession {
	t.Helper()
	receiver := &livePerformanceReceiver{}
	t.Cleanup(func() {
		if orphans := receiver.orphan.Load(); orphans != 0 {
			t.Errorf("received %d deliveries outside a measured sample", orphans)
		}
	})
	handler := func(ctx context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
		if fixture.HandlerDelayMillis > 0 {
			timer := time.NewTimer(time.Duration(fixture.HandlerDelayMillis) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return rabbitmqqueue.Reject(false), ctx.Err()
			case <-timer.C:
			}
		}
		receiver.record(delivery.MessageID)
		return rabbitmqqueue.Acknowledge(), nil
	}
	for _, queue := range fixture.Queues {
		consumer := openLiveConsumerWithBounds(
			t, connection, queue, fixture.QueueType, 1,
			fixture.ConsumerConcurrency*4, fixture.ConsumerConcurrency,
			liveOperationTimeout, handler,
		)
		t.Cleanup(func() { closeLiveConsumer(t, consumer) })
	}
	producer := openLiveProducerWithBounds(
		t, connection, fixture.PublisherConcurrency, liveOperationTimeout,
	)
	t.Cleanup(func() { closeLiveProducer(t, producer) })
	payloads := performanceByteShapes(fixture.PayloadBytes)
	headers := performanceByteShapes(fixture.HeaderBytes)
	return &livePerformanceSession{
		producer: producer, receiver: receiver, payloads: payloads, headers: headers,
	}
}

func (receiver *livePerformanceReceiver) setLedger(ledger *liveClusterLedger) {
	receiver.mu.Lock()
	receiver.ledger = ledger
	receiver.mu.Unlock()
}

func (receiver *livePerformanceReceiver) record(messageID string) {
	receiver.mu.RLock()
	defer receiver.mu.RUnlock()
	ledger := receiver.ledger
	if ledger == nil {
		receiver.orphan.Add(1)
		return
	}
	ledger.recordDelivery(messageID)
}

func performanceByteShapes(sizes []int) [][]byte {
	shapes := make([][]byte, len(sizes))
	for shape, size := range sizes {
		shapes[shape] = make([]byte, size)
		for index := range shapes[shape] {
			shapes[shape][index] = byte((shape + index) % 251)
		}
	}
	return shapes
}

func verifyLivePerformanceTopology(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	exchange string,
	fixture livePerformanceFixture,
) {
	t.Helper()
	queues := make([]rabbitmqqueue.Queue, len(fixture.Queues))
	for index, queue := range fixture.Queues {
		queues[index] = rabbitmqqueue.Queue{Name: queue.Name, Type: fixture.QueueType, Durable: true}
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	result, err := rabbitmqqueue.ApplyTopology(
		ctx, connection,
		rabbitmqqueue.TopologyPolicy{Mode: rabbitmqqueue.TopologyPassive},
		rabbitmqqueue.Topology{
			Exchanges: []rabbitmqqueue.Exchange{{
				Name: exchange, Kind: rabbitmqqueue.ExchangeDirect, Durable: true,
			}},
			Queues: queues,
		},
	)
	if err != nil || len(result.QueueNames) != len(fixture.Queues) {
		t.Fatalf("passively verify live performance topology: %v", err)
	}
	for index, queue := range fixture.Queues {
		if result.QueueNames[index] != queue.Name {
			t.Fatal("passive performance topology returned an unexpected queue identity")
		}
	}
}
