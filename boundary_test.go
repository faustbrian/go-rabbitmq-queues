package rabbitmqqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectionConfigRejectsRemainingUnsafeBoundaries(t *testing.T) {
	t.Parallel()

	provider := CredentialProviderFunc(func(context.Context) (Credentials, error) {
		return Credentials{}, nil
	})
	base := ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
		VirtualHost: "/",
		Credentials: provider,
		TLS:         TLSConfig{ServerName: "rabbitmq.internal"},
		DialTimeout: time.Second,
		Heartbeat:   minimumHeartbeat,
		Recovery:    RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
	}

	tests := []struct {
		name   string
		mutate func(*ConnectionConfig)
		want   error
	}{
		{"too many endpoints", func(config *ConnectionConfig) {
			config.Endpoints = make([]Endpoint, MaxEndpoints+1)
		}, ErrInvalidEndpoint},
		{"endpoint port", func(config *ConnectionConfig) { config.Endpoints[0].Port = 0 }, ErrInvalidEndpoint},
		{"virtual host", func(config *ConnectionConfig) { config.VirtualHost = "" }, ErrInvalidVirtualHost},
		{"mTLS pair", func(config *ConnectionConfig) { config.TLS.ClientCertificate = []byte("certificate") }, ErrInvalidTLS},
		{"root count", func(config *ConnectionConfig) { config.TLS.RootCAs = make([][]byte, MaxRootCAs+1) }, ErrInvalidTLS},
		{"private key bytes", func(config *ConnectionConfig) {
			config.TLS.ClientCertificate = []byte("certificate")
			config.TLS.ClientPrivateKey = make([]byte, MaxTLSMaterialBytes+1)
		}, ErrInvalidTLS},
		{"maximum dial", func(config *ConnectionConfig) { config.DialTimeout = maximumDialTimeout + 1 }, ErrInvalidBounds},
		{"maximum heartbeat", func(config *ConnectionConfig) { config.Heartbeat = maximumHeartbeat + 1 }, ErrInvalidBounds},
		{"recovery delay", func(config *ConnectionConfig) { config.Recovery.MaxDelay = maximumRecoveryDelay + 1 }, ErrInvalidBounds},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := base
			config.Endpoints = append([]Endpoint(nil), base.Endpoints...)
			test.mutate(&config)
			if err := config.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestNilCredentialProviderFunctionFailsWithoutPanic(t *testing.T) {
	t.Parallel()

	var provider CredentialProviderFunc
	_, err := provider.Credentials(t.Context())
	if !errors.Is(err, ErrCredentialsRequired) {
		t.Fatalf("Credentials() error = %v, want %v", err, ErrCredentialsRequired)
	}
}

func TestPublicationCoversBoundedHeaderKindsAndInvalidPolicy(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	publication := Publication{
		Exchange:     "events",
		RoutingKey:   "orders.created",
		DeliveryMode: DeliveryPersistent,
		Message: Message{
			MessageID: "event-1",
			Headers: []Header{
				Int64Header("attempt", 2),
				BytesHeader("opaque", []byte{1, 2, 3}),
				BoolHeader("sampled", true),
			},
		},
	}
	if err := publication.Validate(limits); err != nil {
		t.Fatalf("bounded primitive headers rejected: %v", err)
	}
	publication.Message.Headers[1].Bytes[0] = 9
	original := []byte{1, 2, 3}
	owned := BytesHeader("owned", original)
	original[0] = 9
	if owned.Bytes[0] != 1 {
		t.Fatal("BytesHeader did not take an owned copy")
	}

	tests := []struct {
		name   string
		mutate func(*Publication, *Limits)
		want   error
	}{
		{"limits", func(_ *Publication, limits *Limits) { limits.MaxPayloadBytes = 0 }, ErrInvalidBounds},
		{"routing", func(publication *Publication, _ *Limits) { publication.Exchange = "" }, ErrInvalidPublication},
		{"routing controls", func(publication *Publication, _ *Limits) { publication.RoutingKey = "bad\nkey" }, ErrInvalidPublication},
		{"property controls", func(publication *Publication, _ *Limits) { publication.Message.Type = "bad\nvalue" }, ErrInvalidPublication},
		{"timestamp before epoch", func(publication *Publication, _ *Limits) { publication.Message.Timestamp = time.Unix(-1, 0) }, ErrInvalidPublication},
		{"header identity", func(publication *Publication, _ *Limits) { publication.Message.Headers[0].Key = "" }, ErrInvalidHeader},
		{"non-canonical header", func(publication *Publication, _ *Limits) { publication.Message.Headers[0].String = "also-set" }, ErrInvalidHeader},
		{"non-canonical string header", func(publication *Publication, _ *Limits) {
			publication.Message.Headers[0] = Header{Key: "bad", Kind: HeaderString, String: "value", Bool: true}
		}, ErrInvalidHeader},
		{"non-canonical bool header", func(publication *Publication, _ *Limits) {
			publication.Message.Headers[0] = Header{Key: "bad", Kind: HeaderBool, String: "also-set"}
		}, ErrInvalidHeader},
		{"non-canonical bytes header", func(publication *Publication, _ *Limits) {
			publication.Message.Headers[0] = Header{Key: "bad", Kind: HeaderBytes, String: "also-set"}
		}, ErrInvalidHeader},
		{"header kind", func(publication *Publication, _ *Limits) { publication.Message.Headers[0].Kind = 255 }, ErrInvalidHeader},
		{"header bytes", func(publication *Publication, limits *Limits) { limits.MaxHeaderBytes = 1 }, ErrHeadersTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := publication
			candidate.Message.Headers = append([]Header(nil), publication.Message.Headers...)
			candidateLimits := limits
			test.mutate(&candidate, &candidateLimits)
			if err := candidate.Validate(candidateLimits); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestInvalidPublishStateAndTopology(t *testing.T) {
	t.Parallel()

	if PublishState("unknown").Valid() {
		t.Fatal("unknown publish state accepted")
	}
	if err := (Exchange{Kind: ExchangeDirect}).Validate(); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("empty exchange error = %v, want %v", err, ErrInvalidTopology)
	}
	if err := (Queue{Type: QueueQuorum, Durable: true}).Validate(); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("server-named quorum error = %v, want %v", err, ErrInvalidTopology)
	}
	if err := (Queue{Name: "bad\nname", Type: QueueClassic}).Validate(); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("invalid queue name error = %v, want %v", err, ErrInvalidTopology)
	}
	if err := (Queue{Name: "orders", Type: QueueType("stream")}).Validate(); !errors.Is(err, ErrUnsupportedQueuePolicy) {
		t.Fatalf("unknown queue error = %v, want %v", err, ErrUnsupportedQueuePolicy)
	}
	if err := (TopologyPolicy{Mode: TopologyMode("automatic")}).Validate(); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("unknown topology mode error = %v, want %v", err, ErrInvalidTopology)
	}
}
