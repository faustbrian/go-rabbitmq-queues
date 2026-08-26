package rabbitmqqueue

import (
	"context"
	"strings"
	"time"
)

const (
	// MaxEndpoints bounds endpoint rotation and diagnostic state.
	MaxEndpoints = 16
	// MaxReconnectAttempts bounds one continuous recovery episode.
	MaxReconnectAttempts = 32
	// MaxRootCAs bounds custom trust-store parsing and retained certificate state.
	MaxRootCAs = 16
	// MaxTLSMaterialBytes bounds aggregate roots, certificate, and private-key bytes.
	MaxTLSMaterialBytes  = 1 << 20
	maxEndpointHostBytes = 255
	maxVirtualHostBytes  = 255
	minimumHeartbeat     = 3 * time.Second
	maximumHeartbeat     = 10 * time.Minute
	maximumDialTimeout   = 2 * time.Minute
	maximumRecoveryDelay = 5 * time.Minute
)

// Endpoint identifies one AMQP listener without embedding credentials.
type Endpoint struct {
	Host string
	Port uint16
}

// Credentials are an owned authentication snapshot. Providers should return a
// fresh password slice on every call so reconnection can observe rotation.
type Credentials struct {
	Username string
	Password []byte
}

// CredentialProvider resolves credentials for an individual connection attempt.
type CredentialProvider interface {
	Credentials(context.Context) (Credentials, error)
}

// CredentialProviderFunc adapts a function to CredentialProvider.
type CredentialProviderFunc func(context.Context) (Credentials, error)

// Credentials resolves a fresh credential snapshot.
func (provider CredentialProviderFunc) Credentials(ctx context.Context) (Credentials, error) {
	if provider == nil {
		return Credentials{}, ErrCredentialsRequired
	}
	return provider(ctx)
}

// TLSConfig owns verified TLS identity and optional custom trust material.
// Certificate and key bytes are secrets and must never be observed or logged.
type TLSConfig struct {
	ServerName        string
	RootCAs           [][]byte
	ClientCertificate []byte
	ClientPrivateKey  []byte
}

// RecoveryPolicy bounds reconnection attempts and exponential backoff.
type RecoveryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// ConnectionConfig owns connection, authentication, TLS, heartbeat, and
// recovery policy. It contains no formatted URI so credentials cannot leak
// through ordinary diagnostics.
type ConnectionConfig struct {
	Endpoints   []Endpoint
	VirtualHost string
	Credentials CredentialProvider
	TLS         TLSConfig
	DialTimeout time.Duration
	Heartbeat   time.Duration
	Recovery    RecoveryPolicy
}

// Validate rejects unbounded, secret-bearing, or unverifiable connection policy.
func (config ConnectionConfig) Validate() error {
	if len(config.Endpoints) == 0 || len(config.Endpoints) > MaxEndpoints {
		return ErrInvalidEndpoint
	}
	for _, endpoint := range config.Endpoints {
		if endpoint.Port == 0 || invalidIdentity(endpoint.Host, maxEndpointHostBytes) {
			return ErrInvalidEndpoint
		}
	}
	if config.Credentials == nil {
		return ErrCredentialsRequired
	}
	if invalidIdentity(config.VirtualHost, maxVirtualHostBytes) {
		return ErrInvalidVirtualHost
	}
	if invalidIdentity(config.TLS.ServerName, maxEndpointHostBytes) || len(config.TLS.RootCAs) > MaxRootCAs ||
		(len(config.TLS.ClientCertificate) == 0) != (len(config.TLS.ClientPrivateKey) == 0) {
		return ErrInvalidTLS
	}
	tlsBytes := 0
	materials := append([][]byte{config.TLS.ClientCertificate, config.TLS.ClientPrivateKey}, config.TLS.RootCAs...)
	for _, material := range materials {
		if len(material) > MaxTLSMaterialBytes-tlsBytes {
			return ErrInvalidTLS
		}
		tlsBytes += len(material)
	}
	if config.DialTimeout <= 0 || config.DialTimeout > maximumDialTimeout ||
		config.Heartbeat < minimumHeartbeat || config.Heartbeat > maximumHeartbeat {
		return ErrInvalidBounds
	}
	if config.Recovery.MaxAttempts < 1 || config.Recovery.MaxAttempts > MaxReconnectAttempts ||
		config.Recovery.InitialDelay <= 0 || config.Recovery.MaxDelay < config.Recovery.InitialDelay ||
		config.Recovery.MaxDelay > maximumRecoveryDelay {
		return ErrInvalidBounds
	}

	return nil
}

func ownConnectionConfig(config ConnectionConfig) ConnectionConfig {
	config.Endpoints = append([]Endpoint(nil), config.Endpoints...)
	config.TLS.RootCAs = append([][]byte(nil), config.TLS.RootCAs...)
	for index := range config.TLS.RootCAs {
		config.TLS.RootCAs[index] = append([]byte(nil), config.TLS.RootCAs[index]...)
	}
	config.TLS.ClientCertificate = append([]byte(nil), config.TLS.ClientCertificate...)
	config.TLS.ClientPrivateKey = append([]byte(nil), config.TLS.ClientPrivateKey...)
	return config
}

func invalidIdentity(value string, maximum int) bool {
	return value == "" || len(value) > maximum || strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, "\x00\r\n")
}
