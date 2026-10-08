package rabbitmqqueue

import (
	"context"
	"time"
)

// Native Channel has no context and the connection handshake clears its socket
// deadline. Closing the owned connection releases the pending channel RPC.
func boundedAMQPChannel(ctx context.Context, client amqpConnection, deadline time.Time) (producerChannel, error) {
	setupContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := setupContext.Err(); err != nil {
		_ = client.CloseDeadline(time.Now())
		return nil, err
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(setupContext, func() {
		defer close(closed)
		_ = client.CloseDeadline(time.Now())
	})
	channel, err := client.Channel()
	if !stop() {
		// Join connection cleanup before returning; no callback can outlive setup
		// or close a successfully transferred runtime connection later.
		<-closed
	}
	if contextErr := setupContext.Err(); contextErr != nil {
		_ = client.CloseDeadline(time.Now())
		return nil, contextErr
	}
	return channel, err
}
