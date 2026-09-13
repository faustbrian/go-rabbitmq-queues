package rabbitmqqueueotel_test

import (
	"context"
	"fmt"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
	rabbitmqqueueotel "github.com/faustbrian/go-rabbitmq-queues/adapters/otel"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func Example() {
	instrumentation, err := rabbitmqqueueotel.New(rabbitmqqueueotel.Config{
		TracerProvider: tracenoop.NewTracerProvider(),
		Limits:         rabbitmqqueue.DefaultLimits(),
	})
	if err != nil {
		panic(err)
	}
	handler, err := instrumentation.Handler(
		rabbitmqqueue.QueueReference{Name: "tracking.billing", Type: rabbitmqqueue.QueueQuorum},
		func(context.Context, rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	if err != nil {
		panic(err)
	}
	settlement, err := handler(context.Background(), rabbitmqqueue.Delivery{
		Exchange: "tracking.events", RoutingKey: "tracking.event.v1",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(settlement.Method)
	// Output: ack
}
