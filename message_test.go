package rabbitmqqueue

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPublicationValidation(t *testing.T) {
	t.Parallel()

	limits := Limits{
		MaxPayloadBytes:    8,
		MaxHeaderEntries:   2,
		MaxHeaderBytes:     128,
		MaxNameBytes:       32,
		MaxRoutingKeyBytes: 32,
	}
	valid := Publication{
		Exchange:     "events",
		RoutingKey:   "orders.created",
		Mandatory:    true,
		DeliveryMode: DeliveryPersistent,
		Message: Message{
			Body:          []byte("payload"),
			MessageID:     "event-1",
			CorrelationID: "request-1",
			ReplyTo:       "rpc.responses",
			ContentType:   "application/json",
			Timestamp:     time.Unix(1, 0).UTC(),
			Headers: []Header{
				StringHeader("schema-version", "1"),
				StringHeader("traceparent", "00-00000000000000000000000000000001-0000000000000001-01"),
			},
		},
	}

	if err := valid.Validate(limits); err != nil {
		t.Fatalf("valid publication rejected: %v", err)
	}
	immediateExpiration := time.Duration(0)
	immediate := valid
	immediate.Message.Expiration = &immediateExpiration
	if err := immediate.Validate(limits); err != nil {
		t.Fatalf("explicit immediate expiration rejected: %v", err)
	}
	exactReplyToLimit := valid
	exactReplyToLimit.Message.ReplyTo = strings.Repeat("r", limits.MaxNameBytes)
	if err := exactReplyToLimit.Validate(limits); err != nil {
		t.Fatalf("maximum-length reply-to rejected: %v", err)
	}

	tests := map[string]struct {
		mutate func(*Publication)
		want   error
	}{
		"message id required": {
			mutate: func(publication *Publication) { publication.Message.MessageID = "" },
			want:   ErrMessageIDRequired,
		},
		"payload bounded": {
			mutate: func(publication *Publication) { publication.Message.Body = make([]byte, 9) },
			want:   ErrPayloadTooLarge,
		},
		"headers bounded": {
			mutate: func(publication *Publication) {
				publication.Message.Headers = append(publication.Message.Headers, BoolHeader("sampled", true))
			},
			want: ErrHeadersTooLarge,
		},
		"duplicate headers rejected": {
			mutate: func(publication *Publication) {
				publication.Message.Headers[1] = StringHeader("schema-version", "2")
			},
			want: ErrDuplicateHeader,
		},
		"priority is AMQP octet": {
			mutate: func(publication *Publication) {
				priority := uint16(256)
				publication.Message.Priority = &priority
			},
			want: ErrInvalidPriority,
		},
		"expiration is positive": {
			mutate: func(publication *Publication) {
				expiration := -time.Second
				publication.Message.Expiration = &expiration
			},
			want: ErrInvalidExpiration,
		},
		"expiration preserves milliseconds": {
			mutate: func(publication *Publication) {
				expiration := 1500 * time.Microsecond
				publication.Message.Expiration = &expiration
			},
			want: ErrInvalidExpiration,
		},
		"timestamp preserves AMQP seconds": {
			mutate: func(publication *Publication) { publication.Message.Timestamp = time.Unix(1, 1) },
			want:   ErrInvalidPublication,
		},
		"reply to rejects control characters": {
			mutate: func(publication *Publication) { publication.Message.ReplyTo = "rpc\nresponses" },
			want:   ErrInvalidPublication,
		},
		"reply to is bounded": {
			mutate: func(publication *Publication) {
				publication.Message.ReplyTo = strings.Repeat("r", limits.MaxNameBytes+1)
			},
			want: ErrInvalidPublication,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publication := valid
			publication.Message.Body = append([]byte(nil), valid.Message.Body...)
			publication.Message.Headers = append([]Header(nil), valid.Message.Headers...)
			test.mutate(&publication)

			if err := publication.Validate(limits); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPublicationRejectsLimitsAbovePolicyMaximum(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Limits){
		"payload bytes":     func(limits *Limits) { limits.MaxPayloadBytes++ },
		"header entries":    func(limits *Limits) { limits.MaxHeaderEntries++ },
		"header bytes":      func(limits *Limits) { limits.MaxHeaderBytes++ },
		"name bytes":        func(limits *Limits) { limits.MaxNameBytes++ },
		"routing key bytes": func(limits *Limits) { limits.MaxRoutingKeyBytes++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			limits := DefaultLimits()
			mutate(&limits)
			if err := testPublication().Validate(limits); !errors.Is(err, ErrInvalidBounds) {
				t.Fatalf("Validate() error = %v, want %v", err, ErrInvalidBounds)
			}
		})
	}
}

func TestPublicationSupportsBuiltInExchangeRoutingSemantics(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		kind       ExchangeKind
		routingKey string
		want       error
	}{
		"unspecified kind preserves non-empty routing": {routingKey: "orders.created"},
		"direct uses a routing key":                    {kind: ExchangeDirect, routingKey: "orders.created"},
		"topic uses a routing key":                     {kind: ExchangeTopic, routingKey: "orders.*"},
		"direct accepts an empty routing key":          {kind: ExchangeDirect},
		"topic accepts an empty routing key":           {kind: ExchangeTopic},
		"fanout uses an empty routing key":             {kind: ExchangeFanout},
		"headers uses an empty routing key":            {kind: ExchangeHeaders},
		"unspecified kind cannot omit routing":         {want: ErrInvalidPublication},
		"fanout cannot carry routing": {
			kind: ExchangeFanout, routingKey: "ignored", want: ErrInvalidPublication,
		},
		"headers cannot carry routing": {
			kind: ExchangeHeaders, routingKey: "ignored", want: ErrInvalidPublication,
		},
		"unknown kind is rejected": {
			kind: ExchangeKind("plugin"), routingKey: "orders.created", want: ErrInvalidPublication,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publication := testPublication()
			publication.ExchangeKind = test.kind
			publication.RoutingKey = test.routingKey
			if err := publication.Validate(DefaultLimits()); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPublicationSupportsExplicitDefaultDirectExchange(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		kind ExchangeKind
		want error
	}{
		"explicit default direct exchange":  {kind: ExchangeDirect},
		"omitted exchange is not implicit":  {want: ErrInvalidPublication},
		"topic cannot be default exchange":  {kind: ExchangeTopic, want: ErrInvalidPublication},
		"fanout cannot be default exchange": {kind: ExchangeFanout, want: ErrInvalidPublication},
		"headers cannot be default exchange": {
			kind: ExchangeHeaders, want: ErrInvalidPublication,
		},
		"unknown kind cannot be default exchange": {
			kind: ExchangeKind("plugin"), want: ErrInvalidPublication,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publication := testPublication()
			publication.Exchange = ""
			publication.ExchangeKind = test.kind
			publication.RoutingKey = "orders"
			if err := publication.Validate(DefaultLimits()); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPublishStatesRemainDistinct(t *testing.T) {
	t.Parallel()

	states := []PublishState{
		PublishNotSent,
		PublishRejected,
		PublishReturned,
		PublishConfirmed,
		PublishAmbiguous,
	}
	seen := make(map[PublishState]struct{}, len(states))
	for _, state := range states {
		if !state.Valid() {
			t.Fatalf("state %q is not valid", state)
		}
		if _, exists := seen[state]; exists {
			t.Fatalf("duplicate publish state %q", state)
		}
		seen[state] = struct{}{}
	}
}
