package rabbitmqqueueotel

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestInstrumentedBoundariesRejectInvalidCalls(t *testing.T) {
	t.Parallel()

	instrumentation, err := New(Config{
		TracerProvider: sdktrace.NewTracerProvider(),
		Limits:         rabbitmqqueue.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	publisher, err := instrumentation.Publisher(publisherFunc(func(
		context.Context,
		rabbitmqqueue.Publication,
	) (rabbitmqqueue.PublishResult, error) {
		t.Fatal("invalid publication reached wrapped publisher")
		return rabbitmqqueue.PublishResult{}, nil
	}))
	if err != nil {
		t.Fatalf("Publisher() error = %v", err)
	}

	var missingContext context.Context
	result, err := publisher.Publish(missingContext, validInternalPublication())
	if result.State != rabbitmqqueue.PublishNotSent || !errors.Is(err, rabbitmqqueue.ErrContextRequired) {
		t.Fatalf("Publish(nil) = %#v, %v", result, err)
	}
	invalid := validInternalPublication()
	invalid.Message.MessageID = ""
	result, err = publisher.Publish(context.Background(), invalid)
	if result.State != rabbitmqqueue.PublishNotSent || !errors.Is(err, rabbitmqqueue.ErrMessageIDRequired) {
		t.Fatalf("Publish(invalid) = %#v, %v", result, err)
	}

	handler, err := instrumentation.Handler(rabbitmqqueue.QueueReference{
		Type: rabbitmqqueue.QueueClassic,
		Transient: &rabbitmqqueue.TransientQueue{
			Exchange:   rabbitmqqueue.Exchange{Name: "events", Kind: rabbitmqqueue.ExchangeTopic},
			RoutingKey: "tracking.#",
		},
	}, func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
		return rabbitmqqueue.Acknowledge(), nil
	})
	if err != nil {
		t.Fatalf("Handler(transient) error = %v", err)
	}
	if settlement, err := handler(missingContext, rabbitmqqueue.Delivery{}); !errors.Is(err, rabbitmqqueue.ErrContextRequired) || settlement.Method != "" {
		t.Fatalf("handler(nil) = %#v, %v", settlement, err)
	}
}

func TestDeliveryAttributesIncludeBoundedConversationID(t *testing.T) {
	t.Parallel()

	attributes := deliveryAttributes("events:key:queue", rabbitmqqueue.Delivery{
		CorrelationID: "conversation-1",
	}, rabbitmqqueue.DefaultLimits())
	if got := attributeValues(attributes)["messaging.message.conversation_id"]; got != "conversation-1" {
		t.Fatalf("conversation ID = %#v", got)
	}
}

func TestHeaderCarrierRejectsDuplicateValuesAndListsKeys(t *testing.T) {
	t.Parallel()

	carrier := headerCarrier{headers: []rabbitmqqueue.Header{
		rabbitmqqueue.StringHeader("traceparent", "first"),
		rabbitmqqueue.StringHeader("TraceParent", "second"),
		rabbitmqqueue.BoolHeader("sampled", true),
	}}
	if got := carrier.Get("TRACEPARENT"); got != "" {
		t.Fatalf("Get(duplicate) = %q", got)
	}
	if got := carrier.Keys(); !reflect.DeepEqual(got, []string{"traceparent", "TraceParent", "sampled"}) {
		t.Fatalf("Keys() = %#v", got)
	}
	if !equalASCIIFold("traceparent", "TRACEPARENT") || equalASCIIFold("traceparent", "trace-state") {
		t.Fatal("ASCII folding accepted unequal keys")
	}
}

func TestDestinationAndOutcomeClassification(t *testing.T) {
	t.Parallel()

	destinations := []struct {
		publication rabbitmqqueue.Publication
		want        string
	}{
		{publication: rabbitmqqueue.Publication{Exchange: "events"}, want: "events"},
		{publication: rabbitmqqueue.Publication{RoutingKey: "tracking.event"}, want: "tracking.event"},
	}
	for _, test := range destinations {
		if got := publicationDestination(test.publication); got != test.want {
			t.Fatalf("publicationDestination(%#v) = %q", test.publication, got)
		}
	}

	outcomes := []struct {
		result rabbitmqqueue.PublishResult
		err    error
		want   string
	}{
		{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}, err: errors.New("invalid"), want: "invalid"},
		{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishRejected}, want: "rejected"},
		{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishReturned}, want: "returned"},
		{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishNotSent}, want: "not_sent"},
		{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishState("unknown")}, want: "invalid"},
	}
	for _, test := range outcomes {
		if got := publishOutcome(test.result, test.err); got != test.want {
			t.Fatalf("publishOutcome(%q) = %q", test.result.State, got)
		}
	}
}

func TestConfiguredQueueIdentityBounds(t *testing.T) {
	t.Parallel()

	limits := rabbitmqqueue.DefaultLimits()
	valid := rabbitmqqueue.QueueReference{
		Type: rabbitmqqueue.QueueClassic,
		Transient: &rabbitmqqueue.TransientQueue{
			Exchange: rabbitmqqueue.Exchange{Name: "events", Kind: rabbitmqqueue.ExchangeHeaders},
			Arguments: []rabbitmqqueue.Header{
				rabbitmqqueue.StringHeader("kind", "tracking"),
				rabbitmqqueue.BoolHeader("durable", true),
				rabbitmqqueue.Int64Header("version", 1),
				rabbitmqqueue.BytesHeader("digest", []byte{1}),
			},
		},
	}
	if !validQueueIdentity(valid, limits) {
		t.Fatal("valid transient queue rejected")
	}
	invalidIdentity := valid
	invalidIdentity.Transient = cloneTransient(valid.Transient)
	invalidIdentity.Transient.Exchange.Name = ""
	if validQueueIdentity(invalidIdentity, limits) {
		t.Fatal("missing transient exchange accepted")
	}

	cases := []struct {
		name    string
		headers []rabbitmqqueue.Header
	}{
		{name: "missing key", headers: []rabbitmqqueue.Header{rabbitmqqueue.StringHeader("", "value")}},
		{name: "duplicate key", headers: []rabbitmqqueue.Header{
			rabbitmqqueue.StringHeader("key", "a"), rabbitmqqueue.StringHeader("key", "b"),
		}},
		{name: "malformed string", headers: []rabbitmqqueue.Header{{Key: "key", Kind: rabbitmqqueue.HeaderString, Bool: true}}},
		{name: "malformed bool", headers: []rabbitmqqueue.Header{{Key: "key", Kind: rabbitmqqueue.HeaderBool, String: "true"}}},
		{name: "malformed integer", headers: []rabbitmqqueue.Header{{Key: "key", Kind: rabbitmqqueue.HeaderInt64, Bool: true}}},
		{name: "malformed bytes", headers: []rabbitmqqueue.Header{{Key: "key", Kind: rabbitmqqueue.HeaderBytes, String: "bytes"}}},
		{name: "unknown kind", headers: []rabbitmqqueue.Header{{Key: "key", Kind: rabbitmqqueue.HeaderKind(255)}}},
		{name: "aggregate too large", headers: []rabbitmqqueue.Header{
			rabbitmqqueue.StringHeader("key", strings.Repeat("x", limits.MaxHeaderBytes)),
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if validHeaders(test.headers, limits) {
				t.Fatalf("validHeaders(%#v) = true", test.headers)
			}
		})
	}
	if !containsControl("value\x7f") {
		t.Fatal("DEL control character accepted")
	}
}

func TestNilSpanStatusIsIgnored(t *testing.T) {
	t.Parallel()

	setSpanStatus(nil, codes.Error, "ignored")
}

type publisherFunc func(
	context.Context,
	rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error)

func (publisher publisherFunc) Publish(
	ctx context.Context,
	publication rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error) {
	return publisher(ctx, publication)
}

func validInternalPublication() rabbitmqqueue.Publication {
	return rabbitmqqueue.Publication{
		Exchange: "events", ExchangeKind: rabbitmqqueue.ExchangeTopic,
		RoutingKey: "tracking.event", Mandatory: true,
		DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message:      rabbitmqqueue.Message{MessageID: "event-1", Body: []byte("payload")},
	}
}

func cloneTransient(queue *rabbitmqqueue.TransientQueue) *rabbitmqqueue.TransientQueue {
	cloned := *queue
	cloned.Arguments = append([]rabbitmqqueue.Header(nil), queue.Arguments...)
	return &cloned
}

func attributeValues(attributes []attribute.KeyValue) map[string]any {
	values := make(map[string]any, len(attributes))
	for _, item := range attributes {
		values[string(item.Key)] = item.Value.AsInterface()
	}
	return values
}
