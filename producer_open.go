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

type nativeAMQPDialFunc func(string, amqp.Config) (*amqp.Connection, error)
type nativeAMQPWrapFunc func(*amqp.Connection) amqpConnection

type nativeChannelOpener interface {
	Channel() (*amqp.Channel, error)
}

type nativeAMQPConnection struct {
	*amqp.Connection
	opener nativeChannelOpener
}

func (connection *nativeAMQPConnection) Channel() (producerChannel, error) {
	return connection.opener.Channel()
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
	if !usableProducerSession(sessionID, err) {
		return nil, ErrProducerUnavailable
	}

	delay := connection.Recovery.InitialDelay
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Join(ErrProducerUnavailable, ctx.Err())
		default:
		}
		attemptContext, cancel := context.WithTimeout(ctx, connection.DialTimeout)
		attemptDeadline, _ := attemptContext.Deadline()
		credentials, credentialErr := connection.Credentials.Credentials(attemptContext)
		if !usableCredentials(credentials, credentialErr) {
			cancel()
			wipe(credentials.Password)
		} else {
			channel, resource, dialErr := dial(
				attemptContext,
				connection.Endpoints[recoveryEndpointIndex(0, attempt, len(connection.Endpoints))],
				connection,
				credentials,
			)
			wipe(credentials.Password)
			if usableProducerResources(channel, resource, dialErr) {
				recovery := &producerRecovery{
					connection: connection, session: session, dial: dial,
					nextEndpoint: nextRecoveryAttempt(attempt),
				}
				producer, producerErr := newProducerFromChannelWithRecovery(attemptContext, config, sessionID, channel, resource, recovery)
				cancel()
				if producerOpenSucceeded(producer, producerErr) {
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
		if finalRecoveryAttempt(attempt, connection.Recovery.MaxAttempts) {
			return nil, ErrProducerUnavailable
		}
		if err := waitForRecovery(ctx, delay); err != nil {
			return nil, errors.Join(ErrProducerUnavailable, err)
		}
		delay = nextRecoveryDelay(delay, connection.Recovery.MaxDelay)
		attempt = nextRecoveryAttempt(attempt)
	}
}

func producerOpenSucceeded(producer *Producer, err error) bool {
	return producer != nil && err == nil
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
		if shouldWaitForRecovery(attempt) {
			if err := waitForRecovery(producer.eventsContext, delay); err != nil {
				return false
			}
			delay = nextRecoveryDelay(delay, producer.recovery.connection.Recovery.MaxDelay)
		}
		producer.observe(Observation{Kind: ObservationReconnect, Outcome: ObservationAttempted})
		attemptContext, cancel := context.WithTimeout(producer.eventsContext, producer.recovery.connection.DialTimeout)
		deadline, _ := attemptContext.Deadline()
		credentials, credentialErr := producer.recovery.connection.Credentials.Credentials(attemptContext)
		if !usableCredentials(credentials, credentialErr) {
			cancel()
			wipe(credentials.Password)
			continue
		}
		session, sessionErr := producer.recovery.session()
		if !usableProducerSession(session, sessionErr) {
			cancel()
			wipe(credentials.Password)
			continue
		}
		endpointIndex := recoveryEndpointIndex(
			producer.recovery.nextEndpoint,
			attempt,
			len(producer.recovery.connection.Endpoints),
		)
		channel, resource, dialErr := producer.recovery.dial(
			attemptContext,
			producer.recovery.connection.Endpoints[endpointIndex],
			producer.recovery.connection,
			credentials,
		)
		wipe(credentials.Password)
		if !usableProducerResources(channel, resource, dialErr) {
			cancel()
			_ = closeProducerGeneration(channel, resource, &producerGenerationClose{}, deadline)
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
			_ = closeProducerGeneration(channel, resource, &producerGenerationClose{}, deadline)
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
		producer.generationClose = &producerGenerationClose{}
		producer.failure = make(chan struct{}, 1)
		producer.unavailable = false
		producer.recovering = false
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
			_ = closeProducerGeneration(channel, resource, &producerGenerationClose{}, deadlineFor(setupContext, config.PublishTimeout))
			return nil, nil, nil, nil, ErrProducerUnavailable
		}
	case <-setupContext.Done():
		_ = closeProducerGeneration(channel, resource, &producerGenerationClose{}, deadlineFor(setupContext, config.PublishTimeout))
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

func usableCredentials(credentials Credentials, err error) bool {
	return err == nil && validCredentials(credentials)
}

func usableProducerSession(session string, err error) bool {
	return err == nil && !invalidIdentity(session, maxProducerSessionBytes)
}

func usableProducerResources(channel producerChannel, resource io.Closer, err error) bool {
	return err == nil && channel != nil && resource != nil
}

func usableAMQPConnection(connection amqpConnection, err error) bool {
	return err == nil && connection != nil
}

func finalRecoveryAttempt(attempt, maximum int) bool {
	return attempt+1 == maximum
}

func shouldWaitForRecovery(attempt int) bool {
	return attempt > 0
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

func nextRecoveryDelay(current, maximum time.Duration) time.Duration {
	return min(current*2, maximum)
}

func recoveryEndpointIndex(start, attempt, count int) int {
	return (start + attempt) % count
}

func randomProducerSession() (string, error) {
	return producerSessionFrom(rand.Reader)
}

func producerSessionFrom(source io.Reader) (string, error) {
	random := make([]byte, 16)
	if _, err := io.ReadFull(source, random); err != nil {
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
	return dialAMQPConnectionWithNative(address, config, amqp.DialConfig, wrapNativeAMQPConnection)
}

func dialAMQPConnectionWithNative(
	address string,
	config amqp.Config,
	dial nativeAMQPDialFunc,
	wrap nativeAMQPWrapFunc,
) (amqpConnection, error) {
	connection, err := dial(address, config)
	var owned amqpConnection
	if connection != nil {
		owned = wrap(connection)
	}
	if err != nil {
		if owned != nil {
			_ = owned.CloseDeadline(time.Now())
		}
		return nil, ErrProducerUnavailable
	}
	if owned == nil {
		return nil, ErrProducerUnavailable
	}
	return owned, nil
}

func wrapNativeAMQPConnection(connection *amqp.Connection) amqpConnection {
	return &nativeAMQPConnection{Connection: connection, opener: connection}
}

func buildTLSConfig(config TLSConfig) (*tls.Config, error) {
	return buildTLSConfigWithSystemRoots(config, x509.SystemCertPool)
}

func buildTLSConfigWithSystemRoots(
	config TLSConfig,
	systemRoots func() (*x509.CertPool, error),
) (*tls.Config, error) {
	roots, err := systemRoots()
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
