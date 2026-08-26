package rabbitmqqueue

import (
	"errors"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConsumerConfigRequiresBoundedQueueAndFailurePolicy(t *testing.T) {
	t.Parallel()

	valid := testConsumerConfig()
	tests := map[string]func(*ConsumerConfig){
		"limits":           func(config *ConsumerConfig) { config.Limits = Limits{} },
		"queue name":       func(config *ConsumerConfig) { config.Queue.Name = "" },
		"queue type":       func(config *ConsumerConfig) { config.Queue.Type = QueueType("stream") },
		"consumer name":    func(config *ConsumerConfig) { config.Name = "bad\nname" },
		"zero prefetch":    func(config *ConsumerConfig) { config.Prefetch = 0 },
		"large prefetch":   func(config *ConsumerConfig) { config.Prefetch = MaxConsumerPrefetch + 1 },
		"zero concurrency": func(config *ConsumerConfig) { config.Concurrency = 0 },
		"over prefetch":    func(config *ConsumerConfig) { config.Concurrency = config.Prefetch + 1 },
		"large concurrency": func(config *ConsumerConfig) {
			config.Concurrency = MaxConsumerConcurrency + 1
			config.Prefetch = MaxConsumerConcurrency + 1
		},
		"zero timeout":     func(config *ConsumerConfig) { config.HandlerTimeout = 0 },
		"large timeout":    func(config *ConsumerConfig) { config.HandlerTimeout = maximumDialTimeout + time.Nanosecond },
		"large requeues":   func(config *ConsumerConfig) { config.MaxRequeues = MaxConsumerRequeues + 1 },
		"ack on failure":   func(config *ConsumerConfig) { config.Failure = Acknowledge() },
		"delegate failure": func(config *ConsumerConfig) { config.Failure = Delegate() },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := valid
			mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("Validate() accepted an unsafe consumer policy")
			}
		})
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
}

func TestDeliveryConversionOwnsBoundedMetadataAndDeadLetterHistory(t *testing.T) {
	t.Parallel()

	body := []byte("payload")
	binary := []byte{1, 2}
	deathTime := time.Unix(100, 0)
	source := amqp.Delivery{
		Headers: amqp.Table{
			"schema-version":   "1",
			"attempt":          int64(2),
			"binary":           binary,
			"x-delivery-count": int64(3),
			"x-death": []any{amqp.Table{
				"count": int64(2), "reason": "rejected", "queue": "orders",
				"exchange": "events", "routing-keys": []any{"orders.created"}, "time": deathTime,
			}},
		},
		ContentType: "application/json", ContentEncoding: "identity",
		DeliveryMode: amqp.Persistent, Priority: 7, CorrelationId: "request-1",
		ReplyTo: "rpc.reply", MessageId: "event-1", Timestamp: time.Unix(50, 0),
		Type: "order.created", UserId: "orders", AppId: "orders-api",
		ConsumerTag: "orders-worker", DeliveryTag: 42, Redelivered: true,
		Exchange: "events", RoutingKey: "orders.created", Body: body,
	}

	delivery, err := deliveryFromAMQP(source, testConsumerConfig())
	if err != nil {
		t.Fatalf("deliveryFromAMQP(): %v", err)
	}
	body[0] = 'X'
	binary[0] = 9
	if string(delivery.Body) != "payload" || delivery.MessageID != "event-1" ||
		delivery.CorrelationID != "request-1" || delivery.ContentType != "application/json" ||
		delivery.ContentEncoding != "identity" || delivery.Type != "order.created" ||
		delivery.UserID != "orders" || delivery.AppID != "orders-api" || delivery.ReplyTo != "rpc.reply" ||
		delivery.Consumer != "orders-worker" || delivery.Exchange != "events" ||
		delivery.RoutingKey != "orders.created" || !delivery.Redelivered || delivery.Priority != 7 ||
		delivery.DeliveryMode != DeliveryPersistent || !delivery.Timestamp.Equal(time.Unix(50, 0)) {
		t.Fatalf("converted delivery lost stable metadata: %#v", delivery)
	}
	if delivery.DeliveryCount == nil || *delivery.DeliveryCount != 3 {
		t.Fatalf("delivery count = %#v, want 3", delivery.DeliveryCount)
	}
	if len(delivery.Deaths) != 1 || delivery.Deaths[0].Count != 2 ||
		delivery.Deaths[0].Reason != "rejected" || delivery.Deaths[0].Queue != "orders" ||
		delivery.Deaths[0].Exchange != "events" || len(delivery.Deaths[0].RoutingKeys) != 1 ||
		delivery.Deaths[0].RoutingKeys[0] != "orders.created" || !delivery.Deaths[0].Time.Equal(deathTime) {
		t.Fatalf("dead-letter history = %#v", delivery.Deaths)
	}
	if len(delivery.Headers) != 3 || delivery.Headers[2].Key == "x-death" {
		t.Fatalf("application headers = %#v, want bounded non-broker metadata", delivery.Headers)
	}
	for _, header := range delivery.Headers {
		if header.Key == "binary" && header.Bytes[0] != 1 {
			t.Fatal("binary header was aliased")
		}
	}
}

func TestDeliveryConversionHidesProducerCorrelationWithinApplicationHeaderLimit(t *testing.T) {
	t.Parallel()

	config := testConsumerConfig()
	headers := make(amqp.Table, config.Limits.MaxHeaderEntries+1)
	for index := range config.Limits.MaxHeaderEntries {
		headers[fmt.Sprintf("header-%03d", index)] = "value"
	}
	headers[publishTokenHeader] = "internal-correlation"
	source := testAMQPDelivery(43)
	source.Headers = headers
	delivery, err := deliveryFromAMQP(source, config)
	if err != nil {
		t.Fatalf("deliveryFromAMQP(): %v", err)
	}
	if len(delivery.Headers) != config.Limits.MaxHeaderEntries {
		t.Fatalf("application headers = %d, want %d", len(delivery.Headers), config.Limits.MaxHeaderEntries)
	}
	for _, header := range delivery.Headers {
		if header.Key == publishTokenHeader {
			t.Fatal("package correlation header leaked into public delivery")
		}
	}
}

func TestDeliveryConversionBoundsCombinedApplicationAndDeathMetadata(t *testing.T) {
	t.Parallel()

	config := testConsumerConfig()
	config.Limits.MaxHeaderBytes = 16
	source := testAMQPDelivery(44)
	source.Headers = amqp.Table{
		"application": "value",
		deathHeader: []any{amqp.Table{
			"count": int64(1), "reason": "rejected", "queue": "orders",
			"exchange": "events", "routing-keys": []any{"orders.created"},
			"time": time.Unix(100, 0),
		}},
	}
	if _, err := deliveryFromAMQP(source, config); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("deliveryFromAMQP() error = %v, want invalid delivery", err)
	}
}

func TestDeliveryConversionRejectsUnsafeBrokerInput(t *testing.T) {
	t.Parallel()

	invalidDeath := amqp.Table{
		"count": int64(1), "reason": "rejected", "queue": "orders",
		"exchange": "events", "routing-keys": []any{"orders.created"},
		"time": time.Unix(-1, 0),
	}
	tests := map[string]amqp.Delivery{
		"payload":            {Body: make([]byte, DefaultLimits().MaxPayloadBytes+1), ConsumerTag: "consumer", RoutingKey: "key", DeliveryTag: 1},
		"zero tag":           {ConsumerTag: "consumer", RoutingKey: "key"},
		"routing":            {ConsumerTag: "consumer", RoutingKey: "bad\nkey", DeliveryTag: 1},
		"unsupported header": {Headers: amqp.Table{"nested": amqp.Table{"key": "value"}}, ConsumerTag: "consumer", RoutingKey: "key", DeliveryTag: 1},
		"invalid count":      {Headers: amqp.Table{"x-delivery-count": int64(-1)}, ConsumerTag: "consumer", RoutingKey: "key", DeliveryTag: 1},
		"too many deaths":    {Headers: amqp.Table{"x-death": make([]any, MaxDeathRecords+1)}, ConsumerTag: "consumer", RoutingKey: "key", DeliveryTag: 1},
		"pre-epoch death":    {Headers: amqp.Table{"x-death": []any{invalidDeath}}, ConsumerTag: "consumer", RoutingKey: "key", DeliveryTag: 1},
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := deliveryFromAMQP(source, testConsumerConfig()); !errors.Is(err, ErrInvalidDelivery) {
				t.Fatalf("deliveryFromAMQP() error = %v, want %v", err, ErrInvalidDelivery)
			}
		})
	}
}

func TestSettlementPolicyPreventsUnboundedRequeue(t *testing.T) {
	t.Parallel()

	config := testConsumerConfig()
	tests := []struct {
		name      string
		queueType QueueType
		delivery  Delivery
		requested Settlement
		requeue   bool
	}{
		{name: "first classic reject", queueType: QueueClassic, requested: Reject(true), requeue: true},
		{name: "redelivered classic reject", queueType: QueueClassic, delivery: Delivery{Redelivered: true}, requested: Reject(true)},
		{name: "redelivered classic nack", queueType: QueueClassic, delivery: Delivery{Redelivered: true}, requested: NegativeAcknowledge(true)},
		{name: "quorum below limit", queueType: QueueQuorum, delivery: deliveryWithCount(1), requested: Reject(true), requeue: true},
		{name: "quorum at limit", queueType: QueueQuorum, delivery: deliveryWithCount(2), requested: Reject(true)},
		{name: "quorum nack redelivery", queueType: QueueQuorum, delivery: Delivery{Redelivered: true}, requested: NegativeAcknowledge(true)},
		{name: "no configured requeue", queueType: QueueClassic, requested: Reject(true)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			local := config
			local.Queue.Type = test.queueType
			local.MaxRequeues = 2
			if test.name == "no configured requeue" {
				local.MaxRequeues = 0
			}
			settlement := boundedSettlement(test.delivery, test.requested, local)
			if settlement.Requeue != test.requeue {
				t.Fatalf("bounded settlement = %#v, want requeue %t", settlement, test.requeue)
			}
		})
	}
}

func deliveryWithCount(count uint64) Delivery {
	return Delivery{Redelivered: true, DeliveryCount: &count}
}

func testConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Limits:         DefaultLimits(),
		Queue:          QueueReference{Name: "orders", Type: QueueClassic},
		Name:           "orders-worker",
		Prefetch:       8,
		Concurrency:    4,
		HandlerTimeout: time.Second,
		MaxRequeues:    2,
		Failure:        Reject(false),
	}
}
