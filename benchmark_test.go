package rabbitmqqueue

import (
	"strconv"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// BenchmarkPublicationValidate measures in-process default-limit validation of one
// fixed publication; it excludes broker I/O and has no throughput guarantee.
func BenchmarkPublicationValidate(b *testing.B) {
	publication := testPublication()
	limits := DefaultLimits()
	b.ReportAllocs()
	for b.Loop() {
		if err := publication.Validate(limits); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeliveryFromAMQP measures owned conversion of fixed native metadata;
// compare the same header/body shape, not native wire decode or consumer work.
func BenchmarkDeliveryFromAMQP(b *testing.B) {
	delivery := testAMQPDelivery(1)
	delivery.Headers = amqp.Table{
		"schema-version": "1", "attempt": int64(2), "binary": []byte{1, 2, 3, 4},
	}
	config := testConsumerConfig()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := deliveryFromAMQP(delivery, config); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAMQPPublishing measures property/header/body copying for one fixed
// message; it does not send or await confirms.
func BenchmarkAMQPPublishing(b *testing.B) {
	message := testPublication().Message
	b.ReportAllocs()
	for b.Loop() {
		_ = amqpPublishing(message, DeliveryPersistent, "benchmark-token")
	}
}

// BenchmarkPublishTrackerRegisterConfirm excludes token construction and measures
// one-at-a-time registration/positive completion using 4096 reusable identities;
// it is not a concurrent broker-confirm latency benchmark.
func BenchmarkPublishTrackerRegisterConfirm(b *testing.B) {
	const tokens = 4096
	values := make([]string, tokens)
	for index := range tokens {
		values[index] = "token-" + strconv.Itoa(index)
	}
	tracker := newPublishTracker(1)
	index := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sequence := uint64(index + 1)
		attempt, err := tracker.register(sequence, values[index], "events", "route")
		if err != nil {
			b.Fatal(err)
		}
		if !tracker.confirm(sequence, true) || (<-attempt.outcome).State != PublishConfirmed {
			b.Fatal("tracker did not confirm the exact attempt")
		}
		index = (index + 1) % tokens
	}
}
