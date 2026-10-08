package rabbitmqqueue

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMixedDeliveryHeadersRejectBeforeOwnedSnapshots(t *testing.T) {
	for _, test := range []struct {
		name        string
		application amqp.Table
		limits      func(*Limits)
	}{
		{name: "unsupported value", application: amqp.Table{"application": []string{"value"}}},
		{name: "empty key", application: amqp.Table{"": "value"}},
		{name: "entry budget", application: amqp.Table{"a": "value", "b": "value"}, limits: func(limits *Limits) { limits.MaxHeaderEntries = 1 }},
		{name: "byte budget", application: amqp.Table{"a": []byte("value")}, limits: func(limits *Limits) { limits.MaxHeaderBytes = 5 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			if test.limits != nil {
				test.limits(&limits)
			}
			table := amqp.Table{publishTokenHeader: "session/1"}
			for key, value := range test.application {
				table[key] = value
			}
			allocations := testing.AllocsPerRun(1, func() {
				headers, bytes, err := deliveryHeaders(table, limits)
				if !errors.Is(err, ErrInvalidDelivery) || headers != nil || bytes != 0 {
					t.Errorf("mixed header rejection produced owned data or lost its error")
				}
			})
			if allocations != 0 {
				t.Errorf("rejected mixed headers allocated %.0f snapshots", allocations)
			}
		})
	}
}

func TestMixedDeliveryHeadersPreserveOwnedApplicationBytes(t *testing.T) {
	value := []byte("value")
	headers, bytes, err := deliveryHeaders(amqp.Table{
		publishTokenHeader: "session/1", "a": value,
	}, DefaultLimits())
	if err != nil || len(headers) != 1 || bytes != 6 || headers[0].Key != "a" || headers[0].Kind != HeaderBytes {
		t.Fatal("valid mixed headers were not admitted as application data")
	}
	value[0] = 'x'
	if string(headers[0].Bytes) != "value" {
		t.Fatal("accepted application bytes alias the borrowed table")
	}
}
