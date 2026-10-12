package rabbitmqqueue

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func buildAMQPClientConfig(
	ctx context.Context,
	endpoint Endpoint,
	connection ConnectionConfig,
	credentials Credentials,
) (string, amqp.Config, time.Time, error) {
	tlsConfig, err := buildTLSConfig(connection.TLS)
	if err != nil {
		return "", amqp.Config{}, time.Time{}, ErrProducerUnavailable
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", amqp.Config{}, time.Time{}, ErrProducerUnavailable
	}
	// Never put credentials in the URI/error surface. PlainAuth necessarily
	// converts password bytes to an immutable Go string; clearing the provider
	// buffer cannot erase that native copy or other retained credential copies.
	address := (&url.URL{
		Scheme: "amqps",
		Host:   net.JoinHostPort(endpoint.Host, strconv.Itoa(int(endpoint.Port))),
	}).String()
	dialer := &net.Dialer{Timeout: connection.DialTimeout}
	return address, amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{
			Username: credentials.Username,
			Password: string(credentials.Password),
		}},
		Vhost:           connection.VirtualHost,
		Heartbeat:       connection.Heartbeat,
		TLSClientConfig: tlsConfig,
		Dial: func(network, address string) (net.Conn, error) {
			return boundedNetworkDial(ctx, dialer.DialContext, network, address)
		},
	}, deadline, nil
}
