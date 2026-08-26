package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumerDialFunc func(
	context.Context,
	Endpoint,
	ConnectionConfig,
	Credentials,
) (consumerChannel, io.Closer, error)

type consumerAMQPOpenFunc func(string, amqp.Config, time.Time) (consumerChannel, io.Closer, error)

// OpenConsumer establishes an independent consumer-only AMQP connection,
// applies bounded per-consumer QoS, and starts manual-settlement workers.
func OpenConsumer(
	ctx context.Context,
	connection ConnectionConfig,
	config ConsumerConfig,
	handler DeliveryHandler,
) (*Consumer, error) {
	return openConsumerWith(ctx, connection, config, handler, dialAMQPConsumer)
}

func openConsumerWith(
	ctx context.Context,
	connection ConnectionConfig,
	config ConsumerConfig,
	handler DeliveryHandler,
	dial consumerDialFunc,
) (*Consumer, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if err := connection.Validate(); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, ErrInvalidConsumer
	}
	if dial == nil {
		return nil, ErrConsumerUnavailable
	}
	delay := connection.Recovery.InitialDelay
	for attempt := 0; attempt < connection.Recovery.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, errors.Join(ErrConsumerUnavailable, ctx.Err())
		default:
		}
		attemptContext, cancel := context.WithTimeout(ctx, connection.DialTimeout)
		attemptDeadline, _ := attemptContext.Deadline()
		credentials, credentialErr := connection.Credentials.Credentials(attemptContext)
		if credentialErr != nil || !validCredentials(credentials) {
			cancel()
			wipe(credentials.Password)
		} else {
			channel, resource, dialErr := dial(
				attemptContext,
				connection.Endpoints[attempt%len(connection.Endpoints)],
				connection,
				credentials,
			)
			wipe(credentials.Password)
			if dialErr == nil && channel != nil && resource != nil {
				consumer, consumerErr := newConsumerFromChannel(attemptContext, config, handler, channel, resource)
				cancel()
				if consumerErr == nil {
					return consumer, nil
				}
			} else {
				cancel()
				_ = boundedCloseConsumerResources(resource, channel, attemptDeadline)
			}
		}
		if attempt == connection.Recovery.MaxAttempts-1 {
			break
		}
		if err := waitForRecovery(ctx, delay); err != nil {
			return nil, errors.Join(ErrConsumerUnavailable, err)
		}
		if delay > connection.Recovery.MaxDelay/2 {
			delay = connection.Recovery.MaxDelay
		} else {
			delay *= 2
		}
	}
	return nil, ErrConsumerUnavailable
}

func dialAMQPConsumer(
	ctx context.Context,
	endpoint Endpoint,
	connection ConnectionConfig,
	credentials Credentials,
) (consumerChannel, io.Closer, error) {
	address, config, deadline, err := buildAMQPClientConfig(ctx, endpoint, connection, credentials)
	if err != nil {
		return nil, nil, ErrConsumerUnavailable
	}
	return openAMQPConsumerConnection(address, config, deadline)
}

func openAMQPConsumerConnection(
	address string,
	config amqp.Config,
	deadline time.Time,
) (consumerChannel, io.Closer, error) {
	return openAMQPConsumerConnectionWith(address, config, deadline, dialAMQPConnection)
}

func openAMQPConsumerConnectionWith(
	address string,
	config amqp.Config,
	deadline time.Time,
	dial amqpConnectionDialFunc,
) (consumerChannel, io.Closer, error) {
	client, err := dial(address, config)
	if err != nil || client == nil {
		if client != nil {
			_ = boundedCloseConsumerResources(client, nil, deadline)
		}
		return nil, nil, ErrConsumerUnavailable
	}
	channel, err := client.Channel()
	if err != nil {
		_ = boundedCloseConsumerResources(client, nil, deadline)
		return nil, nil, ErrConsumerUnavailable
	}
	if channel == nil {
		_ = boundedCloseConsumerResources(client, nil, deadline)
		return nil, nil, ErrConsumerUnavailable
	}
	consumer, ok := channel.(consumerChannel)
	if !ok {
		_ = boundedCloseConsumerResources(client, channel, deadline)
		return nil, nil, ErrConsumerUnavailable
	}
	return consumer, client, nil
}
