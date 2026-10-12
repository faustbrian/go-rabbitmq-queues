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
	// Host is a nonblank, trimmed host of at most 255 bytes, without NUL/CR/LF.
	// It is caller-selected network authority, not an allowlist or URL.
	Host string
	// Port is a nonzero listener port; open operations always use verified AMQPS.
	Port uint16
}

// Credentials are an owned authentication snapshot. Providers should return a
// fresh password slice on every call so reconnection can observe rotation.
type Credentials struct {
	// Username is a nonblank, trimmed identity of at most 255 bytes without NUL/CR/LF.
	Username string
	// Password is 1..4096 secret bytes for an open attempt. The provider transfers
	// this buffer for in-place clearing; use a fresh buffer, not shared storage.
	Password []byte
}

// CredentialProvider resolves credentials for an individual connection attempt.
// Implementations may be called concurrently by independent producers and
// consumers. Package-owned connection attempts supply a non-nil, bounded
// context. Calls are synchronous, are never made while a package lock is held,
// and panics are not recovered. Implementations must return when the supplied
// context is cancelled.
type CredentialProvider interface {
	// Credentials returns fresh attempt credentials or an error. Open/apply
	// paths clear the returned password even on failure and hide callback errors;
	// direct adapter calls return values/errors unchanged. Do not retain mutable
	// aliases or require re-entry into the resource being opened.
	Credentials(context.Context) (Credentials, error)
}

// CredentialProviderFunc adapts a function to CredentialProvider. The function
// must follow CredentialProvider's concurrency, cancellation, and panic rules.
type CredentialProviderFunc func(context.Context) (Credentials, error)

// Credentials resolves a fresh caller-owned credential snapshot. OpenProducer,
// OpenConsumer, and ApplyTopology zero their returned password snapshot after
// each connection attempt. Direct callers and providers remain responsible for
// other secret copies they retain. This method itself does not copy or clear
// the callback result, contain panics or impose a deadline on direct calls.
func (provider CredentialProviderFunc) Credentials(ctx context.Context) (Credentials, error) {
	if ctx == nil {
		return Credentials{}, ErrContextRequired
	}
	if provider == nil {
		return Credentials{}, ErrCredentialsRequired
	}
	return provider(ctx)
}

// TLSConfig owns verified TLS identity and optional custom trust material.
// Certificate and key bytes are secrets and must never be observed or logged.
type TLSConfig struct {
	// ServerName is required, bounded verification/SNI identity, not inferred per endpoint.
	ServerName string
	// RootCAs adds bounded PEM roots to the system trust pool, not a replacement
	// trust store. PEM/key validity is checked during opening, not Validate.
	RootCAs [][]byte
	// ClientCertificate is optional PEM client identity, supplied together with the key.
	ClientCertificate []byte
	// ClientPrivateKey is optional PEM secret material paired with ClientCertificate.
	// Open operations copy it for recovery; Shutdown does not guarantee erasure.
	ClientPrivateKey []byte
}

// RecoveryPolicy bounds reconnection attempts and exponential backoff.
type RecoveryPolicy struct {
	// MaxAttempts is 1..MaxReconnectAttempts per startup or continuous recovery episode.
	MaxAttempts int
	// InitialDelay is a positive first backoff before retry; the initial attempt is immediate.
	InitialDelay time.Duration
	// MaxDelay caps doubling backoff, is at least InitialDelay and at most five minutes.
	// No jitter or publication replay is added.
	MaxDelay time.Duration
}

// ConnectionConfig owns connection, authentication, TLS, heartbeat, and
// recovery policy. It contains no formatted URI so credentials cannot leak
// through ordinary diagnostics.
type ConnectionConfig struct {
	// Endpoints is a nonempty ordered list of at most MaxEndpoints, rotated on retries.
	Endpoints []Endpoint
	// VirtualHost is a required trimmed AMQP virtual-host identity of at most 255 bytes.
	VirtualHost string
	// Credentials is a required borrowed provider, retained for credential rotation.
	// Shared providers must obey CredentialProvider; typed-nil usability is not tested.
	Credentials CredentialProvider
	// TLS supplies verified trust/client identity, copied for the resource lifetime.
	TLS TLSConfig
	// DialTimeout is positive and at most two minutes, covering credential, dial and setup
	// work per attempt; a synchronous provider must cooperate with its context.
	DialTimeout time.Duration
	// Heartbeat is negotiated AMQP heartbeat policy between three seconds and ten minutes.
	Heartbeat time.Duration
	// Recovery supplies explicit finite retries/backoff; zero configuration is invalid.
	Recovery RecoveryPolicy
}

// Validate checks structural identities, material byte/count budgets and finite
// timeout/recovery bounds. It neither invokes the provider nor parses PEM, dials
// endpoints, tests DNS/authorization or establishes a usable typed-nil callback.
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

// Own configuration bytes for later reconnects, but keep the provider borrowed.
// Caller buffers must stay stable while validation/copy runs; retained TLS and
// native SASL string copies are not erased by clearing one password slice.
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
