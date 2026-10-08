package rabbitmqqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The fake models the native channel RPC: closing the connection releases it.
// Cleanup also releases it on the pre-fix implementation, so failure is finite.
type blockedChannelConnection struct {
	entered chan struct{}
	closed  chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (c *blockedChannelConnection) Channel() (producerChannel, error) {
	close(c.entered)
	defer close(c.exited)
	<-c.closed
	return nil, errors.New("connection closed")
}

func (c *blockedChannelConnection) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *blockedChannelConnection) CloseDeadline(time.Time) error { return c.Close() }

func TestChannelOpeningHonorsDialDeadline(t *testing.T) {
	for _, owner := range []string{"producer", "consumer", "topology"} {
		t.Run(owner, func(t *testing.T) {
			connection := &blockedChannelConnection{
				entered: make(chan struct{}), closed: make(chan struct{}), exited: make(chan struct{}),
			}
			dial := func(string, amqp.Config) (amqpConnection, error) { return connection, nil }
			finished := make(chan error, 1)
			returned := make(chan struct{})
			deadline := time.Now().Add(50 * time.Millisecond)
			go func() {
				defer close(returned)
				var err error
				switch owner {
				case "producer":
					_, _, err = openAMQPConnectionWith("", amqp.Config{}, deadline, dial)
				case "consumer":
					_, _, err = openAMQPConsumerConnectionWith("", amqp.Config{}, deadline, dial)
				case "topology":
					_, _, err = openAMQPTopologyConnectionWith("", amqp.Config{}, deadline, dial)
				}
				finished <- err
			}()
			defer func() {
				_ = connection.Close()
				select {
				case <-connection.exited:
				case <-time.After(time.Second):
					t.Error("channel worker did not exit after connection closure")
				}
				select {
				case <-returned:
				case <-time.After(time.Second):
					t.Error("opening owner did not return after connection closure")
				}
			}()
			select {
			case <-connection.entered:
			case <-time.After(time.Second):
				t.Fatal("channel opening did not start")
			}
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("blocked channel opening unexpectedly succeeded")
				}
				select {
				case <-connection.closed:
				default:
					t.Error("expired channel opening retained its connection")
				}
			case <-time.After(250 * time.Millisecond):
				t.Error("channel opening exceeded its dial deadline")
			}
		})
	}
}

func TestChannelOpeningHonorsCancellation(t *testing.T) {
	for _, owner := range []string{"producer", "consumer", "topology"} {
		t.Run(owner, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			connection := &blockedChannelConnection{
				entered: make(chan struct{}), closed: make(chan struct{}), exited: make(chan struct{}),
			}
			dial := func(string, amqp.Config) (amqpConnection, error) { return connection, nil }
			finished := make(chan error, 1)
			returned := make(chan struct{})
			go func() {
				defer close(returned)
				var err error
				deadline := time.Now().Add(time.Hour)
				switch owner {
				case "producer":
					_, _, err = openAMQPConnectionWithContext(ctx, "", amqp.Config{}, deadline, dial)
				case "consumer":
					_, _, err = openAMQPConsumerConnectionWithContext(ctx, "", amqp.Config{}, deadline, dial)
				case "topology":
					_, _, err = openAMQPTopologyConnectionWithContext(ctx, "", amqp.Config{}, deadline, dial)
				}
				finished <- err
			}()
			defer func() {
				_ = connection.Close()
				select {
				case <-returned:
				case <-time.After(time.Second):
					t.Error("opening owner did not return after connection closure")
				}
			}()
			select {
			case <-connection.entered:
			case <-time.After(time.Second):
				t.Fatal("channel opening did not start")
			}
			cancel()
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("cancelled channel opening unexpectedly succeeded")
				}
				select {
				case <-connection.exited:
				default:
					t.Error("channel RPC outlived cancelled opening")
				}
			case <-time.After(250 * time.Millisecond):
				t.Error("channel opening ignored cancellation")
			}
		})
	}
}
