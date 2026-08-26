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

type amqpOpenFunc func(string, amqp.Config, time.Time) (producerChannel, io.Closer, error)

type amqpConnection interface {
	Channel() (producerChannel, error)
	Close() error
	CloseDeadline(time.Time) error
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

// OpenProducer establishes an independent producer-only AMQP connection and
// confirm-enabled channel. Startup attempts rotate endpoints and credentials.
// A successfully opened producer is terminal on runtime connection loss;
// runtime recovery is not yet part of this lifecycle.
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
				producer, producerErr := newProducerFromChannelWithContext(attemptContext, config, sessionID, channel, resource)
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
