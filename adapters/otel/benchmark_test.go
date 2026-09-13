package rabbitmqqueueotel_test

import (
	"context"
	"testing"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	rabbitmqqueueotel "github.com/faustbrian/go-rabbitmq-queues/adapters/otel"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func BenchmarkPublisher(b *testing.B) {
	instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: tracenoop.NewTracerProvider(),
		Limits:         rabbitmqqueue.DefaultLimits(),
	})
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	publisher, err := instrumentation.Publisher(benchmarkPublisher{})
	if err != nil {
		b.Fatalf("Publisher() error = %v", err)
	}
	ctx := context.Background()
	publication := validPublication()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := publisher.Publish(ctx, publication); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
	}
}

type benchmarkPublisher struct{}

func (benchmarkPublisher) Publish(
	context.Context,
	rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error) {
	return rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}, nil
}
