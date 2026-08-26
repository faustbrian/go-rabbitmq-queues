package rabbitmqqueue

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type topologyChannel interface {
	ExchangeDeclarePassive(string, string, bool, bool, bool, bool, amqp.Table) error
	QueueDeclarePassive(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	QueueBind(string, string, string, bool, amqp.Table) error
	Close() error
}

var _ topologyChannel = (*amqp.Channel)(nil)

type topologyDialFunc func(
	context.Context,
	Endpoint,
	ConnectionConfig,
	Credentials,
) (topologyChannel, io.Closer, error)

type topologyOpenFunc func(
	string,
	amqp.Config,
	time.Time,
) (topologyChannel, io.Closer, error)

type topologyGeneration struct {
	channel  topologyChannel
	resource io.Closer
	once     sync.Once
	err      error
}

type retryableTopologyOperationError struct {
	cause error
}

func (err *retryableTopologyOperationError) Error() string {
	return ErrTopologyUnavailable.Error()
}

func (err *retryableTopologyOperationError) Unwrap() []error {
	if err.cause == nil {
		return []error{ErrTopologyUnavailable}
	}
	return []error{ErrTopologyUnavailable, err.cause}
}

// ApplyTopology passively verifies operator-owned exchange and queue
// equivalence, or performs explicitly permitted development-only declarations.
// AMQP cannot passively inspect bindings; Topology.Validate rejects passive
// binding requests rather than mutating production topology. Connection-scoped
// server-named queues are declared only by a client-owned transient consumer.
func ApplyTopology(
	ctx context.Context,
	connection ConnectionConfig,
	policy TopologyPolicy,
	topology Topology,
) (TopologyResult, error) {
	return applyTopologyWith(ctx, connection, policy, topology, dialAMQPTopology)
}

func applyTopologyWith(
	ctx context.Context,
	connection ConnectionConfig,
	policy TopologyPolicy,
	topology Topology,
	dial topologyDialFunc,
) (TopologyResult, error) {
	if ctx == nil {
		return TopologyResult{}, ErrContextRequired
	}
	if err := connection.Validate(); err != nil {
		return TopologyResult{}, err
	}
	if err := topology.Validate(policy); err != nil {
		return TopologyResult{}, err
	}
	if dial == nil {
		return TopologyResult{}, ErrTopologyUnavailable
	}
	connection = ownConnectionConfig(connection)
	topology = ownTopology(topology)
	if err := connection.Validate(); err != nil {
		return TopologyResult{}, err
	}
	if err := topology.Validate(policy); err != nil {
		return TopologyResult{}, err
	}
	delay := connection.Recovery.InitialDelay
	for attempt := 0; attempt < connection.Recovery.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return TopologyResult{}, errors.Join(ErrTopologyUnavailable, ctx.Err())
		default:
		}
		attemptContext, cancel := context.WithTimeout(ctx, connection.DialTimeout)
		deadline, _ := attemptContext.Deadline()
		var attemptErr error
		credentials, credentialErr := connection.Credentials.Credentials(attemptContext)
		if credentialErr != nil || !validCredentials(credentials) {
			attemptErr = attemptContext.Err()
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
				generation := &topologyGeneration{channel: channel, resource: resource}
				result, applyErr := applyTopologyChannel(attemptContext, generation, policy, topology)
				closeErr := generation.close(deadline)
				cancel()
				if applyErr != nil {
					if policy.Mode != TopologyPassive || !retryableTopologyOperation(applyErr) {
						return TopologyResult{}, applyErr
					}
					if ctxErr := ctx.Err(); ctxErr != nil {
						return TopologyResult{}, errors.Join(ErrTopologyUnavailable, ctxErr)
					}
					if closeErr != nil || attempt == connection.Recovery.MaxAttempts-1 {
						return TopologyResult{}, applyErr
					}
				} else {
					if closeErr != nil {
						return TopologyResult{}, ErrTopologyUnavailable
					}
					return result, nil
				}
			} else {
				_ = closeTopologyResources(channel, resource, deadline)
				attemptErr = attemptContext.Err()
				cancel()
			}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return TopologyResult{}, errors.Join(ErrTopologyUnavailable, ctxErr)
		}
		if attempt == connection.Recovery.MaxAttempts-1 {
			if attemptErr != nil {
				return TopologyResult{}, errors.Join(ErrTopologyUnavailable, attemptErr)
			}
			break
		}
		if err := waitForRecovery(ctx, delay); err != nil {
			return TopologyResult{}, errors.Join(ErrTopologyUnavailable, err)
		}
		if delay > connection.Recovery.MaxDelay/2 {
			delay = connection.Recovery.MaxDelay
		} else {
			delay *= 2
		}
	}
	return TopologyResult{}, ErrTopologyUnavailable
}

func applyTopologyChannel(
	ctx context.Context,
	generation *topologyGeneration,
	policy TopologyPolicy,
	topology Topology,
) (TopologyResult, error) {
	result := TopologyResult{QueueNames: make([]string, 0, len(topology.Queues))}
	for _, exchange := range topology.Exchanges {
		exchange := exchange
		err := runTopologyOperation(ctx, generation, func() error {
			if policy.Mode == TopologyPassive {
				return generation.channel.ExchangeDeclarePassive(
					exchange.Name, string(exchange.Kind), exchange.Durable,
					exchange.AutoDelete, exchange.Internal, false, nil,
				)
			}
			return generation.channel.ExchangeDeclare(
				exchange.Name, string(exchange.Kind), exchange.Durable,
				exchange.AutoDelete, exchange.Internal, false, nil,
			)
		})
		if err != nil {
			return TopologyResult{}, topologyOperationError(err)
		}
	}
	for _, queue := range topology.Queues {
		queue := queue
		var declared amqp.Queue
		err := runTopologyOperation(ctx, generation, func() error {
			var err error
			if policy.Mode == TopologyPassive {
				declared, err = generation.channel.QueueDeclarePassive(
					queue.Name, queue.Durable, queue.AutoDelete, queue.Exclusive,
					false, queueArguments(queue),
				)
			} else {
				declared, err = generation.channel.QueueDeclare(
					queue.Name, queue.Durable, queue.AutoDelete, queue.Exclusive,
					false, queueArguments(queue),
				)
			}
			return err
		})
		if err != nil {
			return TopologyResult{}, topologyOperationError(err)
		}
		if invalidIdentity(declared.Name, 255) {
			return TopologyResult{}, ErrTopologyUnavailable
		}
		if queue.Name != "" && declared.Name != queue.Name {
			return TopologyResult{}, ErrTopologyInequivalent
		}
		result.QueueNames = append(result.QueueNames, declared.Name)
	}
	for _, binding := range topology.Bindings {
		binding := binding
		if err := runTopologyOperation(ctx, generation, func() error {
			return generation.channel.QueueBind(
				binding.Queue, binding.RoutingKey, binding.Exchange,
				false, bindingArguments(binding.Arguments),
			)
		}); err != nil {
			return TopologyResult{}, topologyOperationError(err)
		}
	}
	return result, nil
}

func runTopologyOperation(
	ctx context.Context,
	generation *topologyGeneration,
	operation func() error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	completed := make(chan error, 1)
	go func() { completed <- operation() }()
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		deadline, _ := ctx.Deadline()
		_ = generation.close(deadline)
		return ctx.Err()
	}
}

func (generation *topologyGeneration) close(deadline time.Time) error {
	generation.once.Do(func() {
		generation.err = closeTopologyResources(generation.channel, generation.resource, deadline)
	})
	return generation.err
}

func closeTopologyResources(channel topologyChannel, resource io.Closer, deadline time.Time) error {
	var result error
	if resource != nil {
		if err := closeWithDeadline(resource, deadline); err != nil && !errors.Is(err, amqp.ErrClosed) {
			result = ErrTopologyUnavailable
		}
	}
	if channel != nil {
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			result = ErrTopologyUnavailable
		}
	}
	return result
}

func topologyOperationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &retryableTopologyOperationError{cause: err}
	}
	var broker *amqp.Error
	if errors.As(err, &broker) {
		switch broker.Code {
		case 403:
			return ErrTopologyUnauthorized
		case 406:
			return ErrTopologyInequivalent
		case 404:
			return ErrTopologyUnavailable
		}
	}
	return &retryableTopologyOperationError{}
}

func retryableTopologyOperation(err error) bool {
	var retryable *retryableTopologyOperationError
	return errors.As(err, &retryable)
}

func queueArguments(queue Queue) amqp.Table {
	arguments := amqp.Table{"x-queue-type": string(queue.Type)}
	if queue.SingleActiveConsumer {
		arguments["x-single-active-consumer"] = true
	}
	if queue.DeliveryLimit > 0 {
		arguments["x-delivery-limit"] = int64(queue.DeliveryLimit)
	}
	if queue.MaxPriority > 0 {
		arguments["x-max-priority"] = int32(queue.MaxPriority)
	}
	if queue.MessageTTL != nil {
		arguments["x-message-ttl"] = queue.MessageTTL.Milliseconds()
	}
	if queue.Expires != nil {
		arguments["x-expires"] = queue.Expires.Milliseconds()
	}
	if queue.MaxLength != nil {
		arguments["x-max-length"] = int64(*queue.MaxLength)
	}
	if queue.MaxLengthBytes != nil {
		arguments["x-max-length-bytes"] = int64(*queue.MaxLengthBytes)
	}
	if queue.Overflow != "" {
		arguments["x-overflow"] = string(queue.Overflow)
	}
	if queue.DeadLetter != nil {
		arguments["x-dead-letter-exchange"] = queue.DeadLetter.Exchange
		if queue.DeadLetter.RoutingKey != nil {
			arguments["x-dead-letter-routing-key"] = *queue.DeadLetter.RoutingKey
		}
		if queue.DeadLetter.Strategy != "" {
			arguments["x-dead-letter-strategy"] = string(queue.DeadLetter.Strategy)
		}
	}
	return arguments
}

func bindingArguments(arguments []Header) amqp.Table {
	table := make(amqp.Table, len(arguments))
	for _, argument := range arguments {
		switch argument.Kind {
		case HeaderString:
			table[argument.Key] = argument.String
		case HeaderBool:
			table[argument.Key] = argument.Bool
		case HeaderInt64:
			table[argument.Key] = argument.Int64
		case HeaderBytes:
			table[argument.Key] = append([]byte(nil), argument.Bytes...)
		}
	}
	return table
}

func ownTopology(topology Topology) Topology {
	topology.Exchanges = append([]Exchange(nil), topology.Exchanges...)
	topology.Queues = append([]Queue(nil), topology.Queues...)
	for index := range topology.Queues {
		queue := &topology.Queues[index]
		if queue.MessageTTL != nil {
			value := *queue.MessageTTL
			queue.MessageTTL = &value
		}
		if queue.Expires != nil {
			value := *queue.Expires
			queue.Expires = &value
		}
		if queue.MaxLength != nil {
			value := *queue.MaxLength
			queue.MaxLength = &value
		}
		if queue.MaxLengthBytes != nil {
			value := *queue.MaxLengthBytes
			queue.MaxLengthBytes = &value
		}
		if queue.DeadLetter != nil {
			deadLetter := *queue.DeadLetter
			if deadLetter.RoutingKey != nil {
				routingKey := *deadLetter.RoutingKey
				deadLetter.RoutingKey = &routingKey
			}
			queue.DeadLetter = &deadLetter
		}
	}
	topology.Bindings = append([]Binding(nil), topology.Bindings...)
	for index := range topology.Bindings {
		topology.Bindings[index].Arguments = append([]Header(nil), topology.Bindings[index].Arguments...)
		for argument := range topology.Bindings[index].Arguments {
			topology.Bindings[index].Arguments[argument].Bytes = append(
				[]byte(nil), topology.Bindings[index].Arguments[argument].Bytes...,
			)
		}
	}
	return topology
}

func dialAMQPTopology(
	ctx context.Context,
	endpoint Endpoint,
	connection ConnectionConfig,
	credentials Credentials,
) (topologyChannel, io.Closer, error) {
	return dialAMQPTopologyWith(ctx, endpoint, connection, credentials, openAMQPTopologyConnection)
}

func dialAMQPTopologyWith(
	ctx context.Context,
	endpoint Endpoint,
	connection ConnectionConfig,
	credentials Credentials,
	open topologyOpenFunc,
) (topologyChannel, io.Closer, error) {
	if open == nil {
		return nil, nil, ErrTopologyUnavailable
	}
	address, config, deadline, err := buildAMQPClientConfig(ctx, endpoint, connection, credentials)
	if err != nil {
		return nil, nil, ErrTopologyUnavailable
	}
	return open(address, config, deadline)
}

func openAMQPTopologyConnection(
	address string,
	config amqp.Config,
	deadline time.Time,
) (topologyChannel, io.Closer, error) {
	return openAMQPTopologyConnectionWith(address, config, deadline, dialAMQPConnection)
}

func openAMQPTopologyConnectionWith(
	address string,
	config amqp.Config,
	deadline time.Time,
	dial amqpConnectionDialFunc,
) (topologyChannel, io.Closer, error) {
	client, err := dial(address, config)
	if err != nil || client == nil {
		if client != nil {
			_ = closeWithDeadline(client, deadline)
		}
		return nil, nil, ErrTopologyUnavailable
	}
	channel, err := client.Channel()
	if err != nil || channel == nil {
		_ = closeWithDeadline(client, deadline)
		return nil, nil, ErrTopologyUnavailable
	}
	topology, ok := channel.(topologyChannel)
	if !ok {
		_ = channel.Close()
		_ = closeWithDeadline(client, deadline)
		return nil, nil, ErrTopologyUnavailable
	}
	return topology, client, nil
}
