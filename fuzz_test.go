package rabbitmqqueue

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func FuzzPublicationValidation(f *testing.F) {
	f.Add(uint16(6), uint16(12), uint16(7), uint16(8), uint8(0), uint8(HeaderString), uint8(DeliveryPersistent))
	f.Add(uint16(33), uint16(33), uint16(65), uint16(33), uint8(7), uint8(0), uint8(0))
	f.Fuzz(func(t *testing.T, nameSize, routingSize, bodySize, headerSize uint16, flags, kind, mode uint8) {
		limits := Limits{
			MaxPayloadBytes: 64, MaxHeaderEntries: 2, MaxHeaderBytes: 64,
			MaxNameBytes: 32, MaxRoutingKeyBytes: 32,
		}
		header := fuzzHeader(headerSize, flags, kind)
		var expiration *time.Duration
		if flags&1 != 0 {
			value := time.Duration((flags>>1)%3) * 500 * time.Microsecond
			expiration = &value
		}
		publication := Publication{
			Exchange:     fuzzText(nameSize, flags&8 != 0),
			RoutingKey:   fuzzText(routingSize, flags&16 != 0),
			DeliveryMode: DeliveryMode(mode % 4),
			Message: Message{
				Body: make([]byte, int(bodySize%96)), MessageID: fuzzText(nameSize, flags&32 != 0),
				Headers: []Header{header}, Expiration: expiration,
			},
		}

		err := publication.Validate(limits)
		if !knownPublicationValidationError(err) {
			t.Fatalf("Validate() returned an undocumented error type: %T", err)
		}
		if repeat := publication.Validate(limits); !errors.Is(repeat, err) {
			t.Fatalf("Validate() was not deterministic: first %T, second %T", err, repeat)
		}
	})
}

func FuzzDeliveryConversion(f *testing.F) {
	f.Add(uint16(7), uint16(5), uint8(2), uint8(1))
	f.Add(uint16(80), uint16(80), uint8(5), uint8(31))
	f.Fuzz(func(t *testing.T, bodySize, metadataSize uint16, headerKind, flags uint8) {
		config := testConsumerConfig()
		config.Limits.MaxPayloadBytes = 64
		config.Limits.MaxHeaderBytes = 64
		config.Limits.MaxNameBytes = 32
		config.Limits.MaxRoutingKeyBytes = 32
		source := amqp.Delivery{
			ConsumerTag: fuzzText(metadataSize, flags&1 != 0), DeliveryTag: uint64(flags),
			Exchange:   fuzzText(metadataSize, flags&2 != 0),
			RoutingKey: fuzzText(metadataSize, flags&4 != 0),
			Body:       make([]byte, int(bodySize%96)), MessageId: fuzzText(metadataSize, flags&8 != 0),
			DeliveryMode: headerKind % 4,
			Headers:      fuzzAMQPHeaders(headerKind, metadataSize),
		}

		delivery, err := deliveryFromAMQP(source, config)
		if err != nil && !errors.Is(err, ErrInvalidDelivery) {
			t.Fatalf("deliveryFromAMQP() returned an undocumented error type: %T", err)
		}
		if err != nil {
			return
		}
		if len(delivery.Body) > config.Limits.MaxPayloadBytes || len(delivery.Headers) > config.Limits.MaxHeaderEntries {
			t.Fatal("successful conversion exceeded configured bounds")
		}
		if len(source.Body) > 0 {
			before := delivery.Body[0]
			source.Body[0]++
			if delivery.Body[0] != before {
				t.Fatal("converted delivery retained a body alias")
			}
		}
	})
}

func FuzzPublishTrackerCorrelation(f *testing.F) {
	f.Add(uint64(1), uint64(7), true, true)
	f.Add(^uint64(0), uint64(0), false, false)
	f.Fuzz(func(t *testing.T, sequence, tokenSeed uint64, acknowledged, returned bool) {
		if sequence == 0 {
			sequence = 1
		}
		token := fmt.Sprintf("token-%016x", tokenSeed)
		tracker := newPublishTracker(1)
		attempt, err := tracker.register(sequence, token, "events", "route")
		if err != nil {
			t.Fatalf("register(): %v", err)
		}
		if tracker.confirm(sequence^1, acknowledged) || tracker.returned(token+"-other", Return{}) {
			t.Fatal("cross-correlated event completed the registered attempt")
		}
		if returned && !tracker.returned(token, Return{Code: 312}) {
			t.Fatal("exact return was not correlated")
		}
		if !tracker.confirm(sequence, acknowledged) {
			t.Fatal("exact confirmation was not correlated")
		}
		result := <-attempt.outcome
		want := PublishRejected
		if returned {
			want = PublishReturned
		} else if acknowledged {
			want = PublishConfirmed
		}
		if result.State != want {
			t.Fatalf("result state = %s, want %s", result.State, want)
		}
		if tracker.confirm(sequence, true) || tracker.returned(token, Return{}) {
			t.Fatal("late event correlated after terminal completion")
		}
	})
}

func FuzzTopologyValidation(f *testing.F) {
	f.Add(uint8(1), uint8(1), uint16(0), uint8(0), uint8(0))
	f.Add(uint8(129), uint8(129), uint16(513), uint8(7), uint8(4))
	f.Fuzz(func(t *testing.T, exchangeCount, queueCount uint8, bindingCount uint16, flags, kind uint8) {
		topology := Topology{
			Exchanges: make([]Exchange, int(exchangeCount)),
			Queues:    make([]Queue, int(queueCount)),
			Bindings:  make([]Binding, int(bindingCount%600)),
		}
		exchangeKinds := []ExchangeKind{
			ExchangeDirect, ExchangeTopic, ExchangeFanout, ExchangeHeaders,
			ExchangeKind("unsupported"),
		}
		for index := range topology.Exchanges {
			topology.Exchanges[index] = Exchange{
				Name: fmt.Sprintf("exchange-%d", index),
				Kind: exchangeKinds[int(kind)%len(exchangeKinds)], Durable: true,
			}
		}
		for index := range topology.Queues {
			queueType := QueueClassic
			if flags&1 != 0 {
				queueType = QueueQuorum
			}
			topology.Queues[index] = Queue{
				Name: fmt.Sprintf("queue-%d", index), Type: queueType, Durable: true,
			}
		}
		for index := range topology.Bindings {
			topology.Bindings[index] = Binding{
				Exchange: "exchange-0", Queue: "queue-0", RoutingKey: "route",
			}
		}
		if flags&2 != 0 && len(topology.Exchanges) > 1 {
			topology.Exchanges[1].Name = topology.Exchanges[0].Name
		}
		policy := TopologyPolicy{Mode: TopologyPassive}
		if flags&4 != 0 {
			policy = TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}
		}

		err := topology.Validate(policy)
		if !knownTopologyValidationError(err) {
			t.Fatalf("Topology.Validate() returned an undocumented error type: %T", err)
		}
		if repeat := topology.Validate(policy); !errors.Is(repeat, err) {
			t.Fatalf("Topology.Validate() was not deterministic: first %T, second %T", err, repeat)
		}
	})
}

func fuzzText(size uint16, control bool) string {
	value := strings.Repeat("a", int(size%48))
	if control {
		value += "\n"
	}
	return value
}

func fuzzHeader(size uint16, flags, kind uint8) Header {
	header := Header{Key: fuzzText(size, flags&1 != 0), Kind: HeaderKind(kind % 6)}
	switch header.Kind {
	case HeaderString:
		header.String = fuzzText(size, flags&2 != 0)
	case HeaderBool:
		header.Bool = flags&4 != 0
	case HeaderInt64:
		header.Int64 = int64(size)
	case HeaderBytes:
		header.Bytes = make([]byte, int(size%80))
	}
	if flags&64 != 0 {
		header.String = "conflict"
		header.Int64 = 1
	}
	return header
}

func fuzzAMQPHeaders(kind uint8, size uint16) amqp.Table {
	switch kind % 8 {
	case 0:
		return amqp.Table{"metadata": fuzzText(size, false)}
	case 1:
		return amqp.Table{"metadata": kind&1 != 0}
	case 2:
		return amqp.Table{"metadata": int64(size)}
	case 3:
		return amqp.Table{"metadata": make([]byte, int(size%80))}
	case 4:
		return amqp.Table{"metadata": amqp.Table{"nested": "unsupported"}}
	case 5:
		return amqp.Table{deliveryCountHeader: int64(size)}
	case 6:
		return amqp.Table{deliveryCountHeader: int64(-1)}
	default:
		return amqp.Table{deathHeader: []any{amqp.Table{
			"count": int64(size), "reason": "rejected", "queue": "orders",
			"exchange": "events", "routing-keys": []any{"orders.created"},
			"time": time.Unix(100, 0),
		}}}
	}
}

func knownPublicationValidationError(err error) bool {
	if err == nil {
		return true
	}
	for _, known := range []error{
		ErrInvalidBounds, ErrInvalidPublication, ErrMessageIDRequired, ErrPayloadTooLarge,
		ErrInvalidPriority, ErrInvalidExpiration, ErrHeadersTooLarge, ErrInvalidHeader,
		ErrReservedHeader, ErrDuplicateHeader,
	} {
		if errors.Is(err, known) {
			return true
		}
	}
	return false
}

func knownTopologyValidationError(err error) bool {
	if err == nil {
		return true
	}
	for _, known := range []error{
		ErrInvalidTopology, ErrUnsupportedExchangeKind, ErrUnsupportedQueuePolicy,
		ErrTopologyMutationDenied, ErrPassiveBindingVerificationUnsupported,
	} {
		if errors.Is(err, known) {
			return true
		}
	}
	return false
}
