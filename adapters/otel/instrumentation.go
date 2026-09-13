package rabbitmqqueueotel

import (
	"context"
	"errors"
	"reflect"
	"strings"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/faustbrian/go-rabbitmq-queues/adapters/otel"

var (
	// ErrInvalidConfiguration identifies a missing provider or invalid bounds.
	ErrInvalidConfiguration = errors.New("rabbitmqqueueotel: invalid configuration")
	// ErrInvalidPublisher identifies a missing publication dependency.
	ErrInvalidPublisher = errors.New("rabbitmqqueueotel: invalid publisher")
	// ErrInvalidHandler identifies an invalid queue or delivery handler.
	ErrInvalidHandler = errors.New("rabbitmqqueueotel: invalid handler")
)

// Config supplies a caller-owned provider and the root client's message
// limits. The instrumentation never configures exporters or owns shutdown.
type Config struct {
	TracerProvider trace.TracerProvider
	Limits         rabbitmqqueue.Limits
}

// Publisher is the narrow synchronous publication boundary instrumented by
// this adapter.
type Publisher interface {
	Publish(context.Context, rabbitmqqueue.Publication) (rabbitmqqueue.PublishResult, error)
}

// Instrumentation owns no goroutines or provider lifecycle and is safe for
// concurrent use when its caller-owned provider is safe for concurrent use.
type Instrumentation struct {
	tracer     trace.Tracer
	limits     rabbitmqqueue.Limits
	propagator propagation.TraceContext
}

// New validates and constructs RabbitMQ queue instrumentation.
func New(config Config) (*Instrumentation, error) {
	if nilInterface(config.TracerProvider) || !validLimits(config.Limits) {
		return nil, ErrInvalidConfiguration
	}
	tracer, ok := tracerFromProvider(config.TracerProvider)
	if !ok {
		return nil, ErrInvalidConfiguration
	}

	return &Instrumentation{
		tracer:     tracer,
		limits:     config.Limits,
		propagator: propagation.TraceContext{},
	}, nil
}

// Publisher decorates synchronous publication with one producer span and
// injects that span's W3C Trace Context into an owned message snapshot.
func (instrumentation *Instrumentation) Publisher(next Publisher) (Publisher, error) {
	if instrumentation == nil || instrumentation.tracer == nil || nilInterface(next) {
		return nil, ErrInvalidPublisher
	}

	return instrumentedPublisher{instrumentation: instrumentation, next: next}, nil
}

// Handler decorates one push-based delivery handler with W3C extraction and
// one consumer span covering application processing and settlement choice.
func (instrumentation *Instrumentation) Handler(
	queue rabbitmqqueue.QueueReference,
	next rabbitmqqueue.DeliveryHandler,
) (rabbitmqqueue.DeliveryHandler, error) {
	if instrumentation == nil || instrumentation.tracer == nil || next == nil ||
		queue.Validate() != nil || !validQueueIdentity(queue, instrumentation.limits) {
		return nil, ErrInvalidHandler
	}
	queueName := queue.Name
	if queue.Transient != nil {
		queueName = "(temporary)"
	}

	return func(ctx context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
		if ctx == nil {
			return rabbitmqqueue.Settlement{}, rabbitmqqueue.ErrContextRequired
		}
		destination := consumerDestination(queueName, delivery, instrumentation.limits)
		parent := instrumentation.extract(ctx, delivery.Headers)
		spanContext, span := startSpan(
			instrumentation.tracer,
			parent,
			"process "+destination,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(deliveryAttributes(destination, delivery, instrumentation.limits)...),
		)
		defer endSpan(span)

		settlement, err := next(spanContext, delivery)
		setSpanAttributes(span, attribute.String(
			"messaging.rabbitmq.settlement",
			string(settlement.Method),
		))
		if err != nil {
			setSpanAttributes(span, attribute.String("error.type", "handler"))
			setSpanStatus(span, codes.Error, "handler")
		} else if settlement.Validate() != nil {
			setSpanAttributes(span, attribute.String("error.type", "invalid_settlement"))
			setSpanStatus(span, codes.Error, "invalid settlement")
		}

		return settlement, err
	}, nil
}

type instrumentedPublisher struct {
	instrumentation *Instrumentation
	next            Publisher
}

func (publisher instrumentedPublisher) Publish(
	ctx context.Context,
	publication rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error) {
	if ctx == nil {
		return rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishNotSent},
			rabbitmqqueue.ErrContextRequired
	}
	if err := publication.Validate(publisher.instrumentation.limits); err != nil {
		return rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishNotSent}, err
	}
	destination := publicationDestination(publication)
	spanContext, span := startSpan(
		publisher.instrumentation.tracer,
		ctx,
		"publish "+destination,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(publicationAttributes(destination, publication)...),
	)
	defer endSpan(span)

	owned := ownPublication(publication)
	owned.Message.Headers = removePropagationHeaders(owned.Message.Headers)
	carrier := headerCarrier{headers: owned.Message.Headers}
	publisher.instrumentation.propagator.Inject(spanContext, &carrier)
	owned.Message.Headers = carrier.headers
	if err := owned.Validate(publisher.instrumentation.limits); err != nil {
		setSpanAttributes(span, attribute.String("error.type", "propagation"))
		setSpanStatus(span, codes.Error, "propagation")

		return rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishNotSent}, err
	}

	result, err := publisher.next.Publish(spanContext, owned)
	outcome := publishOutcome(result, err)
	setSpanAttributes(span, attribute.String("messaging.rabbitmq.outcome", outcome))
	if err != nil || result.State != rabbitmqqueue.PublishConfirmed {
		setSpanAttributes(span, attribute.String("error.type", outcome))
		setSpanStatus(span, codes.Error, outcome)
	}

	return result, err
}

func publicationAttributes(
	destination string,
	publication rabbitmqqueue.Publication,
) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", destination),
		attribute.String("messaging.operation.name", "publish"),
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.rabbitmq.destination.routing_key", publication.RoutingKey),
		attribute.String("messaging.message.id", publication.Message.MessageID),
		attribute.Int("messaging.message.body.size", len(publication.Message.Body)),
	}
}

func deliveryAttributes(
	destination string,
	delivery rabbitmqqueue.Delivery,
	limits rabbitmqqueue.Limits,
) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", destination),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.operation.type", "process"),
		attribute.Int("messaging.message.body.size", len(delivery.Body)),
		attribute.Bool("messaging.message.redelivered", delivery.Redelivered),
	}
	if validBoundedValue(delivery.RoutingKey, limits.MaxRoutingKeyBytes) {
		attributes = append(attributes, attribute.String(
			"messaging.rabbitmq.destination.routing_key",
			delivery.RoutingKey,
		))
	}
	if validBoundedValue(delivery.MessageID, limits.MaxNameBytes) {
		attributes = append(attributes, attribute.String("messaging.message.id", delivery.MessageID))
	}
	if validBoundedValue(delivery.CorrelationID, limits.MaxNameBytes) {
		attributes = append(attributes, attribute.String(
			"messaging.message.conversation_id",
			delivery.CorrelationID,
		))
	}

	return attributes
}

func (instrumentation *Instrumentation) extract(
	ctx context.Context,
	headers []rabbitmqqueue.Header,
) context.Context {
	if duplicatePropagationHeader(headers) {
		return ctx
	}

	return instrumentation.propagator.Extract(ctx, &headerCarrier{headers: headers})
}

type headerCarrier struct {
	headers []rabbitmqqueue.Header
}

func (carrier *headerCarrier) Get(key string) string {
	value := ""
	found := false
	for _, header := range carrier.headers {
		if !equalASCIIFold(header.Key, key) || header.Kind != rabbitmqqueue.HeaderString {
			continue
		}
		if found {
			return ""
		}
		value = header.String
		found = true
	}

	return value
}

func (carrier *headerCarrier) Set(key, value string) {
	carrier.headers = removeHeader(carrier.headers, key)
	carrier.headers = append(carrier.headers, rabbitmqqueue.StringHeader(strings.ToLower(key), value))
}

func (carrier *headerCarrier) Keys() []string {
	keys := make([]string, 0, len(carrier.headers))
	for _, header := range carrier.headers {
		keys = append(keys, header.Key)
	}

	return keys
}

func ownPublication(publication rabbitmqqueue.Publication) rabbitmqqueue.Publication {
	publication.Message.Body = append([]byte(nil), publication.Message.Body...)
	publication.Message.Headers = append([]rabbitmqqueue.Header(nil), publication.Message.Headers...)
	for index := range publication.Message.Headers {
		publication.Message.Headers[index].Bytes = append(
			[]byte(nil),
			publication.Message.Headers[index].Bytes...,
		)
	}

	return publication
}

func removePropagationHeaders(headers []rabbitmqqueue.Header) []rabbitmqqueue.Header {
	without := removeHeader(headers, "traceparent")
	without = removeHeader(without, "tracestate")
	return removeHeader(without, "baggage")
}

func removeHeader(headers []rabbitmqqueue.Header, key string) []rabbitmqqueue.Header {
	kept := make([]rabbitmqqueue.Header, 0, len(headers))
	for _, header := range headers {
		if !equalASCIIFold(header.Key, key) {
			kept = append(kept, header)
		}
	}

	return kept
}

func duplicatePropagationHeader(headers []rabbitmqqueue.Header) bool {
	for _, key := range []string{"traceparent", "tracestate"} {
		count := 0
		for _, header := range headers {
			if equalASCIIFold(header.Key, key) {
				count++
			}
		}
		if count > 1 {
			return true
		}
	}

	return false
}

func equalASCIIFold(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range len(left) {
		leftByte := left[index]
		rightByte := right[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		if leftByte != rightByte {
			return false
		}
	}

	return true
}

func publicationDestination(publication rabbitmqqueue.Publication) string {
	if publication.Exchange != "" && publication.RoutingKey != "" {
		return publication.Exchange + ":" + publication.RoutingKey
	}
	if publication.Exchange != "" {
		return publication.Exchange
	}
	if publication.RoutingKey != "" {
		return publication.RoutingKey
	}

	return "amq.default"
}

func consumerDestination(
	queue string,
	delivery rabbitmqqueue.Delivery,
	limits rabbitmqqueue.Limits,
) string {
	parts := make([]string, 0, 3)
	if validBoundedValue(delivery.Exchange, limits.MaxNameBytes) {
		parts = append(parts, delivery.Exchange)
	}
	if validBoundedValue(delivery.RoutingKey, limits.MaxRoutingKeyBytes) {
		parts = append(parts, delivery.RoutingKey)
	}
	if queue != delivery.RoutingKey || len(parts) == 0 {
		parts = append(parts, queue)
	}

	return strings.Join(parts, ":")
}

func publishOutcome(result rabbitmqqueue.PublishResult, err error) string {
	switch result.State {
	case rabbitmqqueue.PublishConfirmed:
		if err == nil {
			return "confirmed"
		}
		return "invalid"
	case rabbitmqqueue.PublishRejected:
		return "rejected"
	case rabbitmqqueue.PublishReturned:
		return "returned"
	case rabbitmqqueue.PublishAmbiguous:
		return "ambiguous"
	case rabbitmqqueue.PublishNotSent:
		return "not_sent"
	default:
		return "invalid"
	}
}

func validLimits(limits rabbitmqqueue.Limits) bool {
	maximum := rabbitmqqueue.DefaultLimits()
	return limits.MaxPayloadBytes > 0 && limits.MaxPayloadBytes <= maximum.MaxPayloadBytes &&
		limits.MaxHeaderEntries > 0 && limits.MaxHeaderEntries <= maximum.MaxHeaderEntries &&
		limits.MaxHeaderBytes > 0 && limits.MaxHeaderBytes <= maximum.MaxHeaderBytes &&
		limits.MaxNameBytes > 0 && limits.MaxNameBytes <= maximum.MaxNameBytes &&
		limits.MaxRoutingKeyBytes > 0 && limits.MaxRoutingKeyBytes <= maximum.MaxRoutingKeyBytes
}

func validQueueIdentity(queue rabbitmqqueue.QueueReference, limits rabbitmqqueue.Limits) bool {
	if queue.Transient != nil {
		if !validBoundedValue(queue.Transient.Exchange.Name, limits.MaxNameBytes) ||
			len(queue.Transient.RoutingKey) > limits.MaxRoutingKeyBytes ||
			containsControl(queue.Transient.RoutingKey) ||
			len(queue.Transient.Arguments) > limits.MaxHeaderEntries {
			return false
		}
		return validHeaders(queue.Transient.Arguments, limits)
	}

	return validBoundedValue(queue.Name, limits.MaxNameBytes)
}

func validHeaders(headers []rabbitmqqueue.Header, limits rabbitmqqueue.Limits) bool {
	seen := make(map[string]struct{}, len(headers))
	used := 0
	for _, header := range headers {
		if !validBoundedValue(header.Key, limits.MaxNameBytes) {
			return false
		}
		if _, exists := seen[header.Key]; exists {
			return false
		}
		seen[header.Key] = struct{}{}
		used += len(header.Key)
		switch header.Kind {
		case rabbitmqqueue.HeaderString:
			if header.Bool || header.Int64 != 0 || header.Bytes != nil || containsControl(header.String) {
				return false
			}
			used += len(header.String)
		case rabbitmqqueue.HeaderBool:
			if header.String != "" || header.Int64 != 0 || header.Bytes != nil {
				return false
			}
			used++
		case rabbitmqqueue.HeaderInt64:
			if header.String != "" || header.Bool || header.Bytes != nil {
				return false
			}
			used += 8
		case rabbitmqqueue.HeaderBytes:
			if header.String != "" || header.Bool || header.Int64 != 0 {
				return false
			}
			used += len(header.Bytes)
		default:
			return false
		}
		if used > limits.MaxHeaderBytes {
			return false
		}
	}

	return true
}

func validBoundedValue(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && !containsControl(value)
}

func containsControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func tracerFromProvider(provider trace.TracerProvider) (tracer trace.Tracer, ok bool) {
	defer func() {
		if recover() != nil {
			tracer = nil
			ok = false
		}
	}()
	tracer = provider.Tracer(instrumentationName)
	return tracer, !nilInterface(tracer)
}

func startSpan(
	tracer trace.Tracer,
	ctx context.Context,
	name string,
	options ...trace.SpanStartOption,
) (spanContext context.Context, span trace.Span) {
	spanContext = ctx
	defer func() {
		if recover() != nil {
			spanContext = ctx
			span = nil
		}
	}()
	return tracer.Start(ctx, name, options...)
}

func setSpanAttributes(span trace.Span, attributes ...attribute.KeyValue) {
	if nilInterface(span) {
		return
	}
	defer func() { _ = recover() }()
	span.SetAttributes(attributes...)
}

func setSpanStatus(span trace.Span, code codes.Code, description string) {
	if nilInterface(span) {
		return
	}
	defer func() { _ = recover() }()
	span.SetStatus(code, description)
}

func endSpan(span trace.Span) {
	if nilInterface(span) {
		return
	}
	defer func() { _ = recover() }()
	span.End()
}
