package rabbitmqqueueotel_test

import (
	"context"
	"reflect"
	"testing"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	rabbitmqqueueotel "github.com/faustbrian/go-rabbitmq-queues/adapters/otel"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func FuzzPropagationOwnershipAndBounds(f *testing.F) {
	f.Add([]byte("payload"), "traceparent", "stale")
	f.Add([]byte{}, "schema-version", "1")

	f.Fuzz(func(t *testing.T, body []byte, key, value string) {
		limits := rabbitmqqueue.DefaultLimits()
		instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
			TracerProvider: tracenoop.NewTracerProvider(),
			Limits:         limits,
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		next := &recordingPublisher{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
		publisher, err := instrumentation.Publisher(next)
		if err != nil {
			t.Fatalf("Publisher() error = %v", err)
		}
		publication := validPublication()
		publication.Message.Body = append([]byte(nil), body...)
		publication.Message.Headers = []rabbitmqqueue.Header{
			rabbitmqqueue.StringHeader(key, value),
		}
		original := clonePublication(publication)

		_, _ = publisher.Publish(context.Background(), publication)

		if !reflect.DeepEqual(publication, original) {
			t.Fatalf("Publish() mutated caller publication")
		}
	})
}
