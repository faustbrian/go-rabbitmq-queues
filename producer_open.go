package rabbitmqqueue

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxCredentialBytes = 4096

type producerDialFunc func(
	context.Context,
	Endpoint,
	ConnectionConfig,
	Credentials,
) (producerChannel, io.Closer, error)

type producerRecovery struct {
	connection   ConnectionConfig
	session      func() (string, error)
	dial         producerDialFunc
	nextEndpoint int
}

type amqpOpenFunc func(string, amqp.Config, time.Time) (producerChannel, io.Closer, error)

type amqpConnection interface {
	Channel() (producerChannel, error)
	Close() error
	CloseDeadline(time.Time) error
}

type producerConnectionEvents interface {
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	NotifyBlocked(chan amqp.Blocking) chan amqp.Blocking
}

type amqpConnectionDialFunc func(string, amqp.Config) (amqpConnection, error)

type nativeAMQPConnection struct {
	connection *amqp.Connection
}

func (connection *nativeAMQPConnection) Channel() (producerChannel, error) {
	return connection.connection.Channel()
}

func (connection *nativeAMQPConnection) Close() error {
	return connection.connection.Close()
}

func (connection *nativeAMQPConnection) CloseDeadline(deadline time.Time) error {
	return connection.connection.CloseDeadline(deadline)
}

func (connection *nativeAMQPConnection) NotifyClose(listener chan *amqp.Error) chan *amqp.Error {
	return connection.connection.NotifyClose(listener)
}

func (connection *nativeAMQPConnection) NotifyBlocked(listener chan amqp.Blocking) chan amqp.Blocking {
	return connection.connection.NotifyBlocked(listener)
}

// OpenProducer establishes an independent producer-only AMQP connection and
// confirm-enabled channel. Startup and bounded runtime recovery attempts rotate
// endpoints and credentials. Exhausted runtime recovery is terminal.
func OpenProducer(
	ctx context.Context,
	connection ConnectionConfig,
	config ProducerConfig,
) (*Producer, error) {
	return openProducerWith(ctx, connection, config, randomProducerSession, dialAMQPProducer)
}

func openProducerWith(
	ctx context.Context,
	connection ConnectionConfig,
	config ProducerConfig,
	session func() (string, error),
	dial producerDialFunc,
) (*Producer, error) {
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if err := connection.Validate(); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	connection = ownConnectionConfig(connection)
	if session == nil || dial == nil {
		return nil, ErrProducerUnavailable
	}
	sessionID, err := session()
	if err != nil || invalidIdentity(sessionID, 128) {
		return nil, ErrProducerUnavailable
	}

	delay := connection.Recovery.InitialDelay
	for attempt := 0; attempt < connection.Recovery.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, errors.Join(ErrProducerUnavailable, ctx.Err())
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
				recovery := &producerRecovery{
					connection: connection, session: session, dial: dial,
					nextEndpoint: attempt + 1,
				}
				producer, producerErr := newProducerFromChannelWithRecovery(attemptContext, config, sessionID, channel, resource, recovery)
				cancel()
				if producerErr == nil {
					return producer, nil
				}
			} else {
				cancel()
				if resource != nil {
					_ = closeWithDeadline(resource, attemptDeadline)
				}
				if channel != nil {
					_ = channel.Close()
				}
			}
		}
		if attempt == connection.Recovery.MaxAttempts-1 {
			break
		}
		if err := waitForRecovery(ctx, delay); err != nil {
			return nil, errors.Join(ErrProducerUnavailable, err)
		}
		if delay > connection.Recovery.MaxDelay/2 {
			delay = connection.Recovery.MaxDelay
		} else {
			delay *= 2
		}
	}

	return nil, ErrProducerUnavailable
}

func (producer *Producer) recoverRuntime() bool {
	if producer.recovery == nil {
		return false
	}
	delay := producer.recovery.connection.Recovery.InitialDelay
	for attempt := 0; attempt < producer.recovery.connection.Recovery.MaxAttempts; attempt++ {
		select {
		case <-producer.eventsContext.Done():
			return false
		default:
		}
		if attempt > 0 {
			if err := waitForRecovery(producer.eventsContext, delay); err != nil {
				return false
			}
			if delay > producer.recovery.connection.Recovery.MaxDelay/2 {
				delay = producer.recovery.connection.Recovery.MaxDelay
			} else {
				delay *= 2
			}
		}
		attemptContext, cancel := context.WithTimeout(producer.eventsContext, producer.recovery.connection.DialTimeout)
		deadline, _ := attemptContext.Deadline()
		credentials, credentialErr := producer.recovery.connection.Credentials.Credentials(attemptContext)
		if credentialErr != nil || !validCredentials(credentials) {
			cancel()
			wipe(credentials.Password)
			continue
		}
		session, sessionErr := producer.recovery.session()
		if sessionErr != nil || invalidIdentity(session, 128) {
			cancel()
			wipe(credentials.Password)
			continue
		}
		endpointIndex := (producer.recovery.nextEndpoint + attempt) % len(producer.recovery.connection.Endpoints)
		channel, resource, dialErr := producer.recovery.dial(
			attemptContext,
			producer.recovery.connection.Endpoints[endpointIndex],
			producer.recovery.connection,
			credentials,
		)
		wipe(credentials.Password)
		if dialErr != nil || channel == nil || resource == nil {
			cancel()
			_ = closeProducerGeneration(channel, resource, &sync.Once{}, deadline)
			continue
		}
		returns, confirms, connectionClosed, connectionBlocked, setupErr := setupProducerChannel(
			attemptContext,
			producer.config,
			channel,
			resource,
		)
		cancel()
		if setupErr != nil {
			continue
		}

		producer.publishMu.Lock()
		producer.stateMu.Lock()
		if producer.closed {
			producer.stateMu.Unlock()
			producer.publishMu.Unlock()
			_ = closeProducerGeneration(channel, resource, &sync.Once{}, deadline)
			return false
		}
		producer.session = session
		producer.channel = channel
		producer.resource = resource
		producer.tracker = newPublishTracker(producer.config.MaxOutstanding)
		producer.returns = returns
		producer.confirms = confirms
		producer.connectionClosed = connectionClosed
		producer.connectionBlocked = connectionBlocked
		producer.generationClose = &sync.Once{}
		producer.failure = make(chan struct{}, 1)
		producer.unavailable = false
		producer.recovery.nextEndpoint = endpointIndex + 1
		producer.stateMu.Unlock()
		producer.publishMu.Unlock()
		return true
	}
	return false
}

func setupProducerChannel(
	ctx context.Context,
	config ProducerConfig,
	channel producerChannel,
	resource io.Closer,
) (<-chan amqp.Return, <-chan amqp.Confirmation, <-chan *amqp.Error, <-chan amqp.Blocking, error) {
	setupContext, cancel := context.WithTimeout(ctx, config.PublishTimeout)
	defer cancel()
	confirmed := make(chan error, 1)
	go func() { confirmed <- channel.Confirm(false) }()
	select {
	case err := <-confirmed:
		if err != nil {
			_ = closeProducerGeneration(channel, resource, &sync.Once{}, deadlineFor(setupContext, config.PublishTimeout))
			return nil, nil, nil, nil, ErrProducerUnavailable
		}
	case <-setupContext.Done():
		_ = closeProducerGeneration(channel, resource, &sync.Once{}, deadlineFor(setupContext, config.PublishTimeout))
		return nil, nil, nil, nil, ErrProducerUnavailable
	}
	returns := channel.NotifyReturn(make(chan amqp.Return, config.MaxOutstanding))
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, config.MaxOutstanding))
	connectionClosed, connectionBlocked := producerConnectionNotifications(resource)
	return returns, confirms, connectionClosed, connectionBlocked, nil
}

func producerConnectionNotifications(resource io.Closer) (<-chan *amqp.Error, <-chan amqp.Blocking) {
	events, ok := resource.(producerConnectionEvents)
	if !ok {
		return nil, nil
	}
	return events.NotifyClose(make(chan *amqp.Error, 1)), events.NotifyBlocked(make(chan amqp.Blocking, 1))
}

func validCredentials(credentials Credentials) bool {
	return !invalidIdentity(credentials.Username, 255) && len(credentials.Password) > 0 &&
		len(credentials.Password) <= maxCredentialBytes
}

func wipe(secret []byte) {
	for index := range secret {
		secret[index] = 0
	}
}

func waitForRecovery(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomProducerSession() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", ErrProducerUnavailable
	}
	return hex.EncodeToString(random), nil
}

func dialAMQPProducer(
	ctx context.Context,
	endpoint Endpoint,
	connection ConnectionConfig,
	credentials Credentials,
) (producerChannel, io.Closer, error) {
	return dialAMQPProducerWith(ctx, endpoint, connection, credentials, openAMQPConnection)
}

func dialAMQPProducerWith(
	ctx context.Context,
	endpoint Endpoint,
	connection ConnectionConfig,
	credentials Credentials,
	open amqpOpenFunc,
) (producerChannel, io.Closer, error) {
	address, amqpConfig, deadline, err := buildAMQPClientConfig(ctx, endpoint, connection, credentials)
	if err != nil {
		return nil, nil, ErrProducerUnavailable
	}
	channel, resource, err := open(address, amqpConfig, deadline)
	if err != nil {
		return nil, nil, ErrProducerUnavailable
	}
	return channel, resource, nil
}

type networkDialFunc func(context.Context, string, string) (net.Conn, error)

func boundedNetworkDial(
	ctx context.Context,
	dial networkDialFunc,
	network string,
	address string,
) (net.Conn, error) {
	connection, err := dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		_ = connection.Close()
		return nil, ErrProducerUnavailable
	}
	if err := connection.SetDeadline(deadline); err != nil {
		_ = connection.Close()
		return nil, ErrProducerUnavailable
	}
	return connection, nil
}

func openAMQPConnection(address string, config amqp.Config, deadline time.Time) (producerChannel, io.Closer, error) {
	return openAMQPConnectionWith(address, config, deadline, dialAMQPConnection)
}

func openAMQPConnectionWith(
	address string,
	config amqp.Config,
	deadline time.Time,
	dial amqpConnectionDialFunc,
) (producerChannel, io.Closer, error) {
	client, err := dial(address, config)
	if err != nil {
		if client != nil {
			_ = client.CloseDeadline(deadline)
		}
		return nil, nil, ErrProducerUnavailable
	}
	if client == nil {
		return nil, nil, ErrProducerUnavailable
	}
	channel, err := client.Channel()
	if err != nil {
		_ = client.CloseDeadline(deadline)
		return nil, nil, ErrProducerUnavailable
	}
	return channel, client, nil
}

func dialAMQPConnection(address string, config amqp.Config) (amqpConnection, error) {
	connection, err := amqp.DialConfig(address, config)
	if err != nil {
		if connection != nil {
			_ = connection.CloseDeadline(time.Now())
		}
		return nil, ErrProducerUnavailable
	}
	return &nativeAMQPConnection{connection: connection}, nil
}

func buildTLSConfig(config TLSConfig) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, ErrInvalidTLS
	}
	if roots == nil {
		roots = x509.NewCertPool()
	}
	for _, root := range config.RootCAs {
		if len(root) == 0 || !roots.AppendCertsFromPEM(root) {
			return nil, ErrInvalidTLS
		}
	}
	certificates := []tls.Certificate(nil)
	if len(config.ClientCertificate) > 0 {
		certificate, err := tls.X509KeyPair(config.ClientCertificate, config.ClientPrivateKey)
		if err != nil {
			return nil, ErrInvalidTLS
		}
		certificates = []tls.Certificate{certificate}
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		ServerName:   config.ServerName,
		RootCAs:      roots,
		Certificates: certificates,
	}, nil
}
