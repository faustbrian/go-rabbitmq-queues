package rabbitmqqueue

import (
	"context"
	"sync"
)

type deliverySettlement struct {
	once sync.Once
	done chan struct{}
	err  error
}

func newDeliverySettlement() *deliverySettlement {
	return &deliverySettlement{done: make(chan struct{})}
}

// AwaitSettlement waits for the broker result of the settlement selected by
// the delivery handler. The handler must first return its Settlement; waiting
// synchronously inside that handler cannot complete. A separate goroutine may
// wait on a copied Delivery while the handler returns. Copies share the same
// bounded result. The method returns ErrSettlementResultUnavailable for
// deliveries not created by a Consumer or whose handler delegated settlement.
func (delivery Delivery) AwaitSettlement(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	if delivery.settlement == nil {
		return ErrSettlementResultUnavailable
	}
	select {
	case <-delivery.settlement.done:
		return delivery.settlement.err
	default:
	}
	select {
	case <-delivery.settlement.done:
		return delivery.settlement.err
	case <-ctx.Done():
		select {
		case <-delivery.settlement.done:
			return delivery.settlement.err
		default:
			return ctx.Err()
		}
	}
}

func (delivery Delivery) completeSettlement(err error) {
	if delivery.settlement == nil {
		return
	}
	delivery.settlement.once.Do(func() {
		delivery.settlement.err = err
		close(delivery.settlement.done)
	})
}
