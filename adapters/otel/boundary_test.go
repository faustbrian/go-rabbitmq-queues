package rabbitmqqueueotel_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	rabbitmqqueueotel "github.com/faustbrian/go-rabbitmq-queues/adapters/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestPublisherReplacesStaleTraceHeadersAndExcludesBaggage(t *testing.T) {
	t.Parallel()

	provider := sdktrace.NewTracerProvider()
	ctx, span := provider.Tracer("test").Start(context.Background(), "request")
	defer span.End()
	member, err := baggage.NewMember("tenant", "secret-tenant")
	if err != nil {
		t.Fatalf("NewMember() error = %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("New() baggage error = %v", err)
	}
	publication := validPublication()
	publication.Message.Headers = append(publication.Message.Headers,
		rabbitmqqueue.StringHeader("TraceParent", "stale"),
		rabbitmqqueue.StringHeader("TraceState", "stale=value"),
		rabbitmqqueue.StringHeader("baggage", "application-owned"),
	)
	original := clonePublication(publication)
	next := &recordingPublisher{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	publisher, err := newInstrumentation(t, provider).Publisher(next)
	if err != nil {
		t.Fatalf("Publisher() error = %v", err)
	}

	if _, err := publisher.Publish(baggage.ContextWithBaggage(ctx, bag), publication); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if !reflect.DeepEqual(publication, original) {
		t.Fatalf("Publish() mutated caller publication = %#v", publication)
	}
	if got := stringHeader(next.publication.Message.Headers, "traceparent"); got == "" || got == "stale" {
		t.Fatalf("traceparent = %q", got)
	}
	if got := stringHeader(next.publication.Message.Headers, "tracestate"); got != "" {
		t.Fatalf("tracestate = %q", got)
	}
	if got := stringHeader(next.publication.Message.Headers, "baggage"); got != "" {
		t.Fatalf("baggage header = %q", got)
	}
}

func TestPublisherRejectsPropagationThatExceedsConfiguredBounds(t *testing.T) {
	t.Parallel()

	limits := rabbitmqqueue.DefaultLimits()
	limits.MaxHeaderBytes = 32
	instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: sdktrace.NewTracerProvider(),
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
	publication.Message.Headers = nil
	ctx, span := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "request")
	defer span.End()

	result, err := publisher.Publish(ctx, publication)

	if !errors.Is(err, rabbitmqqueue.ErrHeadersTooLarge) || result.State != rabbitmqqueue.PublishNotSent {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
	if next.publication.Message.MessageID != "" {
		t.Fatalf("wrapped publisher received %#v", next.publication)
	}
}

func TestHandlerRejectsAmbiguousTraceHeadersAndPreservesAmbientContext(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	ambientContext, ambient := provider.Tracer("test").Start(context.Background(), "ambient")
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled,
		Remote: true,
	})
	traceparent := "00-" + remote.TraceID().String() + "-" + remote.SpanID().String() + "-01"
	delivery := rabbitmqqueue.Delivery{Headers: []rabbitmqqueue.Header{
		rabbitmqqueue.StringHeader("traceparent", traceparent),
		rabbitmqqueue.StringHeader("TraceParent", traceparent),
	}}
	handler, err := newInstrumentation(t, provider).Handler(
		rabbitmqqueue.QueueReference{Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum},
		func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	if err != nil {
		t.Fatalf("Handler() error = %v", err)
	}

	if _, err := handler(ambientContext, delivery); err != nil {
		t.Fatalf("handler() error = %v", err)
	}
	ambiguous := recorder.Ended()[0]
	if ambiguous.Parent().SpanID() != ambient.SpanContext().SpanID() || ambiguous.Parent().IsRemote() {
		t.Fatalf("consumer parent = %#v", ambiguous.Parent())
	}
	ambient.End()
}

func TestHandlerRecordsBoundedFailureAndPreservesResult(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	want := errors.New("sensitive handler detail")
	handler, err := newInstrumentation(t, provider).Handler(
		rabbitmqqueue.QueueReference{Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum},
		func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			return rabbitmqqueue.NegativeAcknowledge(true), want
		},
	)
	if err != nil {
		t.Fatalf("Handler() error = %v", err)
	}

	settlement, err := handler(context.Background(), rabbitmqqueue.Delivery{
		Exchange: "tracking.events", RoutingKey: "tracking.event.v1",
	})

	if settlement != rabbitmqqueue.NegativeAcknowledge(true) || !errors.Is(err, want) {
		t.Fatalf("handler() = %#v, %v", settlement, err)
	}
	span := recorder.Ended()[0]
	if span.Status().Code != codes.Error || span.Status().Description != "handler" {
		t.Fatalf("span status = %#v", span.Status())
	}
	if strings.Contains(spanText(span), want.Error()) {
		t.Fatalf("span contains handler detail: %s", spanText(span))
	}
}

func TestHandlerRecordsInvalidSettlementWithoutChangingIt(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	want := rabbitmqqueue.Settlement{Method: rabbitmqqueue.SettlementMethod("unknown")}
	handler, err := newInstrumentation(t, provider).Handler(
		rabbitmqqueue.QueueReference{Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum},
		func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			return want, nil
		},
	)
	if err != nil {
		t.Fatalf("Handler() error = %v", err)
	}

	settlement, err := handler(context.Background(), rabbitmqqueue.Delivery{})

	if err != nil || settlement != want {
		t.Fatalf("handler() = %#v, %v", settlement, err)
	}
	span := recorder.Ended()[0]
	if span.Status().Code != codes.Error || span.Status().Description != "invalid settlement" {
		t.Fatalf("span status = %#v", span.Status())
	}
	assertAttributes(t, span.Attributes(), map[string]any{
		"error.type":                    "invalid_settlement",
		"messaging.rabbitmq.settlement": "unknown",
	})
}

func TestRabbitMQDestinationNamesFollowSemanticConventions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		publication rabbitmqqueue.Publication
		queue       rabbitmqqueue.QueueReference
		delivery    rabbitmqqueue.Delivery
		wantSpan    string
	}{
		{
			name: "default exchange publish",
			publication: rabbitmqqueue.Publication{
				ExchangeKind: rabbitmqqueue.ExchangeDirect,
				DeliveryMode: rabbitmqqueue.DeliveryPersistent,
				Message:      rabbitmqqueue.Message{MessageID: "event-1"},
			},
			wantSpan: "publish amq.default",
		},
		{
			name: "routing key equal to queue",
			queue: rabbitmqqueue.QueueReference{
				Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum,
			},
			delivery: rabbitmqqueue.Delivery{
				Exchange: "tracking.events", RoutingKey: "tracking.billing",
			},
			wantSpan: "process tracking.events:tracking.billing",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			instrumentation := newInstrumentation(t, provider)
			if test.publication.Message.MessageID != "" {
				next := &recordingPublisher{
					result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
				}
				publisher, err := instrumentation.Publisher(next)
				if err != nil {
					t.Fatalf("Publisher() error = %v", err)
				}
				if _, err := publisher.Publish(context.Background(), test.publication); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
			} else {
				handler, err := instrumentation.Handler(
					test.queue,
					func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
						return rabbitmqqueue.Acknowledge(), nil
					},
				)
				if err != nil {
					t.Fatalf("Handler() error = %v", err)
				}
				if _, err := handler(context.Background(), test.delivery); err != nil {
					t.Fatalf("handler() error = %v", err)
				}
			}
			if got := recorder.Ended()[0].Name(); got != test.wantSpan {
				t.Fatalf("span name = %q, want %q", got, test.wantSpan)
			}
		})
	}
}

func TestHandlerOmitsOutOfBoundsDeliveryIdentifiers(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handler, err := newInstrumentation(t, provider).Handler(
		rabbitmqqueue.QueueReference{Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum},
		func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	if err != nil {
		t.Fatalf("Handler() error = %v", err)
	}
	oversized := strings.Repeat("x", rabbitmqqueue.DefaultLimits().MaxNameBytes+1)

	if _, err := handler(context.Background(), rabbitmqqueue.Delivery{
		Exchange: oversized, RoutingKey: oversized, MessageID: oversized,
	}); err != nil {
		t.Fatalf("handler() error = %v", err)
	}

	attributes := attributeMap(recorder.Ended()[0].Attributes())
	for _, key := range []string{
		"messaging.rabbitmq.destination.routing_key",
		"messaging.message.id",
	} {
		if _, exists := attributes[key]; exists {
			t.Fatalf("attribute %q = %#v", key, attributes[key])
		}
	}
	if got := attributes["messaging.destination.name"]; got != "tracking.billing" {
		t.Fatalf("destination = %#v", got)
	}
}

func TestInvalidConfigurationAndDependenciesAreRejected(t *testing.T) {
	t.Parallel()

	limits := rabbitmqqueue.DefaultLimits()
	limits.MaxPayloadBytes = 0
	if instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{}); instrumentation != nil || !errors.Is(err, rabbitmqqueueotel.ErrInvalidConfiguration) {
		t.Fatalf("New(empty) = %#v, %v", instrumentation, err)
	}
	if instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: sdktrace.NewTracerProvider(), Limits: limits,
	}); instrumentation != nil || !errors.Is(err, rabbitmqqueueotel.ErrInvalidConfiguration) {
		t.Fatalf("New(invalid limits) = %#v, %v", instrumentation, err)
	}
	instrumentation := newInstrumentation(t, sdktrace.NewTracerProvider())
	var typedNil *recordingPublisher
	if publisher, err := instrumentation.Publisher(typedNil); publisher != nil || !errors.Is(err, rabbitmqqueueotel.ErrInvalidPublisher) {
		t.Fatalf("Publisher(typed nil) = %#v, %v", publisher, err)
	}
	if handler, err := instrumentation.Handler(
		rabbitmqqueue.QueueReference{}, nil,
	); handler != nil || !errors.Is(err, rabbitmqqueueotel.ErrInvalidHandler) {
		t.Fatalf("Handler(invalid) = %#v, %v", handler, err)
	}
}

func TestTracerFailureDoesNotBlockPublishing(t *testing.T) {
	t.Parallel()

	instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: staticTracerProvider{tracer: panickingTracer{
			Tracer: sdktrace.NewTracerProvider().Tracer("fallback"),
		}},
		Limits: rabbitmqqueue.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	next := &recordingPublisher{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	publisher, err := instrumentation.Publisher(next)
	if err != nil {
		t.Fatalf("Publisher() error = %v", err)
	}

	result, err := publisher.Publish(context.Background(), validPublication())

	if err != nil || result.State != rabbitmqqueue.PublishConfirmed {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
	if next.publication.Message.MessageID != "event-1" {
		t.Fatalf("wrapped publisher received %#v", next.publication)
	}
}

func TestPanickingTracerProviderIsRejected(t *testing.T) {
	t.Parallel()

	instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: panickingTracerProvider{
			TracerProvider: sdktrace.NewTracerProvider(),
		},
		Limits: rabbitmqqueue.DefaultLimits(),
	})

	if instrumentation != nil || !errors.Is(err, rabbitmqqueueotel.ErrInvalidConfiguration) {
		t.Fatalf("New() = %#v, %v", instrumentation, err)
	}
}

func TestTraceContextCarrierInteroperatesWithStandardPropagator(t *testing.T) {
	t.Parallel()

	provider := sdktrace.NewTracerProvider()
	ctx, span := provider.Tracer("test").Start(context.Background(), "request")
	defer span.End()
	next := &recordingPublisher{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	publisher, err := newInstrumentation(t, provider).Publisher(next)
	if err != nil {
		t.Fatalf("Publisher() error = %v", err)
	}
	if _, err := publisher.Publish(ctx, validPublication()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	carrier := propagation.MapCarrier{}
	for _, header := range next.publication.Message.Headers {
		if header.Kind == rabbitmqqueue.HeaderString {
			carrier.Set(header.Key, header.String)
		}
	}
	extracted := propagation.TraceContext{}.Extract(context.Background(), carrier)
	if got := trace.SpanContextFromContext(extracted); !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("extracted context = %#v", got)
	}
}

func attributeMap(attributes []attribute.KeyValue) map[string]any {
	values := make(map[string]any, len(attributes))
	for _, item := range attributes {
		values[string(item.Key)] = item.Value.AsInterface()
	}
	return values
}

func spanText(span sdktrace.ReadOnlySpan) string {
	return strings.Join([]string{
		span.Name(), span.Status().Description,
		strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(strings.Join(attributeStrings(span.Attributes()), " ")), "\n", " ")),
	}, " ")
}

func attributeStrings(attributes []attribute.KeyValue) []string {
	values := make([]string, 0, len(attributes)*2)
	for _, item := range attributes {
		values = append(values, string(item.Key), item.Value.String())
	}
	return values
}

type staticTracerProvider struct {
	trace.TracerProvider
	tracer trace.Tracer
}

func (provider staticTracerProvider) Tracer(
	string,
	...trace.TracerOption,
) trace.Tracer {
	return provider.tracer
}

type panickingTracerProvider struct {
	trace.TracerProvider
}

func (provider panickingTracerProvider) Tracer(
	string,
	...trace.TracerOption,
) trace.Tracer {
	panic("telemetry provider failure")
}

type panickingTracer struct {
	trace.Tracer
}

func (tracer panickingTracer) Start(
	context.Context,
	string,
	...trace.SpanStartOption,
) (context.Context, trace.Span) {
	panic("telemetry tracer failure")
}
