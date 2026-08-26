package rabbitmqqueue

import (
	"errors"
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
			mutate: func(publication *Publication) { publication.Message.Expiration = -time.Second },
			want:   ErrInvalidExpiration,
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
