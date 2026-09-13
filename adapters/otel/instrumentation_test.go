package rabbitmqqueueotel_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	rabbitmqqueueotel "github.com/faustbrian/go-rabbitmq-queues/adapters/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestPublisherCreatesProducerSpanAndInjectsItsContext(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	parentContext, parent := provider.Tracer("test").Start(context.Background(), "request")
	publication := validPublication()
	original := clonePublication(publication)
	next := &recordingPublisher{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	instrumentation := newInstrumentation(t, provider)
	publisher, err := instrumentation.Publisher(next)
	if err != nil {
		t.Fatalf("Publisher() error = %v", err)
	}

	result, err := publisher.Publish(parentContext, publication)
	parent.End()

	if err != nil || result.State != rabbitmqqueue.PublishConfirmed {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
	if !reflect.DeepEqual(publication, original) {
		t.Fatalf("Publish() mutated caller publication = %#v", publication)
	}
	traceparent := stringHeader(next.publication.Message.Headers, "traceparent")
	if traceparent == "" || stringHeader(next.publication.Message.Headers, "tracestate") != "" {
		t.Fatalf("propagation headers = %#v", next.publication.Message.Headers)
	}
	ended := recorder.Ended()
	if len(ended) != 2 {
		t.Fatalf("ended spans = %d, want producer and parent", len(ended))
	}
	producer := ended[0]
	if producer.Name() != "publish tracking.events:tracking.event.v1" ||
		producer.SpanKind() != trace.SpanKindProducer ||
		producer.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("producer span = %q/%s parent=%s", producer.Name(), producer.SpanKind(), producer.Parent().SpanID())
	}
	if traceparent != "00-"+producer.SpanContext().TraceID().String()+"-"+
		producer.SpanContext().SpanID().String()+"-01" {
		t.Fatalf("traceparent = %q", traceparent)
	}
	assertAttributes(t, producer.Attributes(), map[string]any{
		"messaging.system":                           "rabbitmq",
		"messaging.destination.name":                 "tracking.events:tracking.event.v1",
		"messaging.operation.name":                   "publish",
		"messaging.operation.type":                   "send",
		"messaging.rabbitmq.destination.routing_key": "tracking.event.v1",
		"messaging.message.id":                       "event-1",
		"messaging.message.body.size":                int64(7),
	})
}

func TestPublisherRecordsFailedOutcomeWithoutChangingIt(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	want := rabbitmqqueue.ErrPublishAmbiguous
	next := &recordingPublisher{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishAmbiguous},
		err:    want,
	}
	publisher, err := newInstrumentation(t, provider).Publisher(next)
	if err != nil {
		t.Fatalf("Publisher() error = %v", err)
	}

	result, err := publisher.Publish(context.Background(), validPublication())

	if result.State != rabbitmqqueue.PublishAmbiguous || !errors.Is(err, want) {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
	span := recorder.Ended()[0]
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %s", span.Status().Code)
	}
	assertAttributes(t, span.Attributes(), map[string]any{
		"error.type":                 "ambiguous",
		"messaging.rabbitmq.outcome": "ambiguous",
	})
}

func TestHandlerExtractsProducerContextAndCreatesConsumerSpan(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	instrumentation := newInstrumentation(t, provider)
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{17, 18, 19, 20, 21, 22, 23, 24},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	delivery := rabbitmqqueue.Delivery{
		Body: []byte("payload"), MessageID: "event-1", Exchange: "tracking.events",
		RoutingKey: "tracking.event.v1", Headers: []rabbitmqqueue.Header{
			rabbitmqqueue.StringHeader("traceparent", "00-"+remote.TraceID().String()+"-"+remote.SpanID().String()+"-01"),
		},
	}
	var handlerContext trace.SpanContext
	handler, err := instrumentation.Handler(
		rabbitmqqueue.QueueReference{Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum},
		func(ctx context.Context, _ rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			handlerContext = trace.SpanContextFromContext(ctx)
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	if err != nil {
		t.Fatalf("Handler() error = %v", err)
	}

	settlement, err := handler(context.Background(), delivery)

	if err != nil || settlement != rabbitmqqueue.Acknowledge() {
		t.Fatalf("handler() = %#v, %v", settlement, err)
	}
	if !handlerContext.IsValid() || handlerContext.TraceID() != remote.TraceID() {
		t.Fatalf("handler span context = %#v", handlerContext)
	}
	span := recorder.Ended()[0]
	if span.Name() != "process tracking.events:tracking.event.v1:tracking.billing" ||
		span.SpanKind() != trace.SpanKindConsumer ||
		span.Parent().SpanID() != remote.SpanID() || !span.Parent().IsRemote() {
		t.Fatalf("consumer span = %q/%s parent=%#v", span.Name(), span.SpanKind(), span.Parent())
	}
	assertAttributes(t, span.Attributes(), map[string]any{
		"messaging.system":                           "rabbitmq",
		"messaging.destination.name":                 "tracking.events:tracking.event.v1:tracking.billing",
		"messaging.operation.name":                   "process",
		"messaging.operation.type":                   "process",
		"messaging.rabbitmq.destination.routing_key": "tracking.event.v1",
		"messaging.message.id":                       "event-1",
		"messaging.message.body.size":                int64(7),
	})
}

func newInstrumentation(t *testing.T, provider trace.TracerProvider) *rabbitmqqueueotel.Instrumentation {
	t.Helper()
	instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: provider,
		Limits:         rabbitmqqueue.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return instrumentation
}

func validPublication() rabbitmqqueue.Publication {
	return rabbitmqqueue.Publication{
		Exchange: "tracking.events", ExchangeKind: rabbitmqqueue.ExchangeTopic,
		RoutingKey: "tracking.event.v1", Mandatory: true,
		DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{
			Body: []byte("payload"), MessageID: "event-1", ContentType: "application/json",
			Headers: []rabbitmqqueue.Header{rabbitmqqueue.StringHeader("schema-version", "1")},
		},
	}
}

type recordingPublisher struct {
	publication rabbitmqqueue.Publication
	result      rabbitmqqueue.PublishResult
	err         error
}

func (publisher *recordingPublisher) Publish(
	_ context.Context,
	publication rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error) {
	publisher.publication = clonePublication(publication)
	return publisher.result, publisher.err
}

func clonePublication(publication rabbitmqqueue.Publication) rabbitmqqueue.Publication {
	publication.Message.Body = append([]byte(nil), publication.Message.Body...)
	publication.Message.Headers = append([]rabbitmqqueue.Header(nil), publication.Message.Headers...)
	for index := range publication.Message.Headers {
		publication.Message.Headers[index].Bytes = append([]byte(nil), publication.Message.Headers[index].Bytes...)
	}
	return publication
}

func stringHeader(headers []rabbitmqqueue.Header, key string) string {
	for _, header := range headers {
		if header.Key == key && header.Kind == rabbitmqqueue.HeaderString {
			return header.String
		}
	}
	return ""
}

func assertAttributes(t *testing.T, attributes []attribute.KeyValue, want map[string]any) {
	t.Helper()
	got := make(map[string]any, len(attributes))
	for _, item := range attributes {
		got[string(item.Key)] = item.Value.AsInterface()
	}
	for key, value := range want {
		if !reflect.DeepEqual(got[key], value) {
			t.Fatalf("attribute %q = %#v, want %#v; all=%#v", key, got[key], value, got)
		}
	}
}
