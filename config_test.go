package rabbitmqqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectionConfigValidation(t *testing.T) {
	t.Parallel()

	valid := ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
		VirtualHost: "/tracking",
		Credentials: CredentialProviderFunc(func(context.Context) (Credentials, error) {
			return Credentials{Username: "publisher", Password: []byte("secret")}, nil
		}),
		TLS:         TLSConfig{ServerName: "rabbitmq.internal"},
		DialTimeout: 5 * time.Second,
		Heartbeat:   30 * time.Second,
		Recovery: RecoveryPolicy{
			MaxAttempts:  8,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
		},
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}

	tests := map[string]struct {
		mutate func(*ConnectionConfig)
		want   error
	}{
		"endpoint required": {
			mutate: func(config *ConnectionConfig) { config.Endpoints = nil },
			want:   ErrInvalidEndpoint,
		},
		"endpoint host rejects controls": {
			mutate: func(config *ConnectionConfig) { config.Endpoints[0].Host = "rabbitmq.internal\nignored" },
			want:   ErrInvalidEndpoint,
		},
		"credentials required": {
			mutate: func(config *ConnectionConfig) { config.Credentials = nil },
			want:   ErrCredentialsRequired,
		},
		"tls identity required": {
			mutate: func(config *ConnectionConfig) { config.TLS.ServerName = "" },
			want:   ErrInvalidTLS,
		},
		"dial is bounded": {
			mutate: func(config *ConnectionConfig) { config.DialTimeout = 0 },
			want:   ErrInvalidBounds,
		},
		"heartbeat is bounded": {
			mutate: func(config *ConnectionConfig) { config.Heartbeat = 2 * time.Second },
			want:   ErrInvalidBounds,
		},
		"recovery attempts are bounded": {
			mutate: func(config *ConnectionConfig) { config.Recovery.MaxAttempts = MaxReconnectAttempts + 1 },
			want:   ErrInvalidBounds,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			config := valid
			config.Endpoints = append([]Endpoint(nil), valid.Endpoints...)
			test.mutate(&config)

			if err := config.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCredentialProviderFuncPropagatesProviderErrors(t *testing.T) {
	t.Parallel()

	providerErr := errors.New("credential source failed")
	provider := CredentialProviderFunc(func(context.Context) (Credentials, error) {
		return Credentials{}, providerErr
	})

	_, err := provider.Credentials(t.Context())
	if !errors.Is(err, providerErr) {
		t.Fatalf("Credentials() error = %v, want wrapped provider error", err)
	}
}
