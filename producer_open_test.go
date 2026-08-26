package rabbitmqqueue

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestOpenProducerValidatesInputsBeforeSetup(t *testing.T) {
	t.Parallel()

	validConnection := testConnectionConfig()
	validConfig := testProducerConfig()
	tests := map[string]struct {
		ctx        context.Context
		connection ConnectionConfig
		config     ProducerConfig
		session    func() (string, error)
		dial       producerDialFunc
		want       error
	}{
		"nil context":     {nil, validConnection, validConfig, func() (string, error) { return "session", nil }, unavailableDial, ErrContextRequired},
		"connection":      {t.Context(), ConnectionConfig{}, validConfig, func() (string, error) { return "session", nil }, unavailableDial, ErrInvalidEndpoint},
		"producer config": {t.Context(), validConnection, ProducerConfig{}, func() (string, error) { return "session", nil }, unavailableDial, ErrInvalidBounds},
		"nil session":     {t.Context(), validConnection, validConfig, nil, unavailableDial, ErrProducerUnavailable},
		"nil dial":        {t.Context(), validConnection, validConfig, func() (string, error) { return "session", nil }, nil, ErrProducerUnavailable},
		"session failure": {t.Context(), validConnection, validConfig, func() (string, error) { return "", errors.New("secret") }, unavailableDial, ErrProducerUnavailable},
		"invalid session": {t.Context(), validConnection, validConfig, func() (string, error) { return "bad\nsession", nil }, unavailableDial, ErrProducerUnavailable},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			producer, err := openProducerWith(test.ctx, test.connection, test.config, test.session, test.dial)
			if producer != nil || !errors.Is(err, test.want) {
				t.Fatalf("openProducerWith() = (%#v, %v), want nil and %v", producer, err, test.want)
			}
		})
	}
}

func TestOpenProducerHonorsCancellationBeforeAttemptAndDuringBackoff(t *testing.T) {
	t.Parallel()

	t.Run("before attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connection := testConnectionConfig()
		called := false
		connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
			called = true
			return Credentials{}, nil
		})
		producer, err := openProducerWith(ctx, connection, testProducerConfig(), func() (string, error) { return "session", nil }, unavailableDial)
		if producer != nil || !errors.Is(err, context.Canceled) || called {
			t.Fatalf("openProducerWith() = (%#v, %v), provider called %t; want cancelled before attempt", producer, err, called)
		}
	})

	t.Run("during backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		connection := testConnectionConfig()
		connection.Recovery.InitialDelay = time.Second
		connection.Recovery.MaxDelay = time.Second
		dials := 0
		dial := func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			dials++
			cancel()
			return nil, nil, errors.New("unavailable")
		}
		producer, err := openProducerWith(ctx, connection, testProducerConfig(), func() (string, error) { return "session", nil }, dial)
		if producer != nil || !errors.Is(err, context.Canceled) || dials != 1 {
			t.Fatalf("openProducerWith() = (%#v, %v), dials %d; want cancelled backoff", producer, err, dials)
		}
	})
}

func TestOpenProducerCleansPartialAndFailedSetupResources(t *testing.T) {
	t.Parallel()

	for name, dial := range map[string]func(*fakeProducerChannel, *countingCloser) producerDialFunc{
		"partial channel": func(channel *fakeProducerChannel, _ *countingCloser) producerDialFunc {
			return func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
				return channel, nil, errors.New("dial failed")
			}
		},
		"partial resource": func(_ *fakeProducerChannel, resource *countingCloser) producerDialFunc {
			return func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
				return nil, resource, errors.New("dial failed")
			}
		},
		"confirm setup": func(channel *fakeProducerChannel, resource *countingCloser) producerDialFunc {
			channel.confirmErr = errors.New("confirm failed")
			return func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
				return channel, resource, nil
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			channel := newFakeProducerChannel()
			resource := &countingCloser{}
			connection := testConnectionConfig()
			connection.Recovery.MaxAttempts = 1
			producer, err := openProducerWith(t.Context(), connection, testProducerConfig(), func() (string, error) { return "session", nil }, dial(channel, resource))
			if producer != nil || !errors.Is(err, ErrProducerUnavailable) {
				t.Fatalf("openProducerWith() = (%#v, %v), want unavailable", producer, err)
			}
			if name != "partial resource" && channel.closeCount() != 1 {
				t.Fatalf("channel close calls = %d, want 1", channel.closeCount())
			}
			if name != "partial channel" && resource.calls != 1 {
				t.Fatalf("resource close calls = %d, want 1", resource.calls)
			}
		})
	}
}

func TestOpenProducerWipesCredentialSnapshotAfterAttempt(t *testing.T) {
	t.Parallel()

	password := []byte("secret")
	connection := testConnectionConfig()
	connection.Recovery.MaxAttempts = 1
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		return Credentials{Username: "publisher", Password: password}, nil
	})
	producer, err := openProducerWith(t.Context(), connection, testProducerConfig(), func() (string, error) { return "session", nil }, unavailableDial)
	if producer != nil || !errors.Is(err, ErrProducerUnavailable) {
		t.Fatalf("openProducerWith() = (%#v, %v), want unavailable", producer, err)
	}
	if string(password) != strings.Repeat("\x00", len(password)) {
		t.Fatal("credential password snapshot was not wiped")
	}
}

func TestOpenProducerRetriesCredentialAndConfirmSetupFailures(t *testing.T) {
	t.Parallel()

	t.Run("credential resolution", func(t *testing.T) {
		t.Parallel()
		connection := testConnectionConfig()
		calls := 0
		connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
			calls++
			if calls == 1 {
				return Credentials{}, errors.New("temporary credential source failure")
			}
			return Credentials{Username: "publisher", Password: []byte("rotated")}, nil
		})
		channel := newFakeProducerChannel()
		producer, err := openProducerWith(t.Context(), connection, testProducerConfig(), func() (string, error) { return "session", nil }, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			return channel, io.NopCloser(nilReader{}), nil
		})
		if err != nil || producer == nil || calls != 2 {
			t.Fatalf("openProducerWith() = (%#v, %v), credential calls %d; want retry success", producer, err, calls)
		}
		t.Cleanup(func() { closeProducerForTest(t, producer) })
	})

	t.Run("confirm setup", func(t *testing.T) {
		t.Parallel()
		connection := testConnectionConfig()
		first := newFakeProducerChannel()
		first.confirmErr = errors.New("temporary confirm setup failure")
		second := newFakeProducerChannel()
		firstResource := &countingCloser{}
		dials := 0
		producer, err := openProducerWith(t.Context(), connection, testProducerConfig(), func() (string, error) { return "session", nil }, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
			dials++
			if dials == 1 {
				return first, firstResource, nil
			}
			return second, io.NopCloser(nilReader{}), nil
		})
		if err != nil || producer == nil || dials != 2 {
			t.Fatalf("openProducerWith() = (%#v, %v), dials %d; want retry success", producer, err, dials)
		}
		if first.closeCount() != 1 || firstResource.calls != 1 {
			t.Fatalf("failed attempt cleanup = channel %d resource %d, want one each", first.closeCount(), firstResource.calls)
		}
		t.Cleanup(func() { closeProducerForTest(t, producer) })
	})
}

func TestOpenProducerCapsRecoveryDelayAndRetriesBoundedly(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 3, InitialDelay: 2 * time.Millisecond, MaxDelay: 3 * time.Millisecond}
	dials := 0
	producer, err := openProducerWith(t.Context(), connection, testProducerConfig(), func() (string, error) { return "session", nil }, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
		dials++
		return nil, nil, errors.New("unavailable")
	})
	if producer != nil || !errors.Is(err, ErrProducerUnavailable) || dials != 3 {
		t.Fatalf("openProducerWith() = (%#v, %v), dials %d; want three bounded attempts", producer, err, dials)
	}
}

func TestDialAMQPProducerBuildsVerifiedClientConfiguration(t *testing.T) {
	t.Parallel()

	connection := testConnectionConfig()
	channel := newFakeProducerChannel()
	resource := &countingCloser{}
	opened := false
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	gotChannel, gotResource, err := dialAMQPProducerWith(ctx, connection.Endpoints[0], connection, Credentials{Username: "publisher", Password: []byte("secret")}, func(address string, config amqp.Config, deadline time.Time) (producerChannel, io.Closer, error) {
		opened = true
		if address != "amqps://rabbitmq.internal:5671" || config.Vhost != "/events" || config.Heartbeat != 30*time.Second {
			t.Fatalf("client config = address %q vhost %q heartbeat %s", address, config.Vhost, config.Heartbeat)
		}
		if len(config.SASL) != 1 || config.TLSClientConfig == nil || config.TLSClientConfig.ServerName != "rabbitmq.internal" || config.TLSClientConfig.MinVersion != 0x0303 || config.TLSClientConfig.InsecureSkipVerify {
			t.Fatal("client configuration did not preserve verified TLS and credentials")
		}
		return channel, resource, nil
	})
	if err != nil || !opened || gotChannel != channel || gotResource != resource {
		t.Fatalf("dialAMQPProducerWith() = (%#v, %#v, %v), opened %t", gotChannel, gotResource, err, opened)
	}
}

func TestBoundedNetworkDialAppliesAttemptDeadlineToHandshake(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	tracked := &trackingDeadlineConn{Conn: client}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	connection, err := boundedNetworkDial(ctx, func(context.Context, string, string) (net.Conn, error) {
		return tracked, nil
	}, "tcp", "rabbitmq.internal:5671")
	if err != nil || connection != tracked {
		t.Fatalf("boundedNetworkDial() = (%#v, %v), want tracked connection", connection, err)
	}
	if !tracked.deadline.Equal(want) {
		t.Fatalf("connection deadline = %s, want %s", tracked.deadline, want)
	}
	_ = connection.Close()
}

func TestAMQPConnectionBoundaryClosesClientWhenChannelOpenFails(t *testing.T) {
	t.Parallel()

	connection := &fakeAMQPConnection{channelErr: errors.New("channel setup secret")}
	channel, resource, err := openAMQPConnectionWith("amqps://rabbitmq.internal:5671", amqp.Config{}, time.Now().Add(time.Second), func(string, amqp.Config) (amqpConnection, error) {
		return connection, nil
	})
	if channel != nil || resource != nil || !errors.Is(err, ErrProducerUnavailable) || connection.closeCalls != 1 {
		t.Fatalf("openAMQPConnectionWith() = (%#v, %#v, %v), closes %d", channel, resource, err, connection.closeCalls)
	}
}

func TestAMQPConnectionBoundaryClosesPartialClientAfterDialFailure(t *testing.T) {
	t.Parallel()

	connection := &fakeAMQPConnection{}
	deadline := time.Now().Add(time.Second)
	channel, resource, err := openAMQPConnectionWith("amqps://rabbitmq.internal:5671", amqp.Config{}, deadline, func(string, amqp.Config) (amqpConnection, error) {
		return connection, errors.New("handshake failed")
	})
	if channel != nil || resource != nil || !errors.Is(err, ErrProducerUnavailable) || connection.closeCalls != 1 {
		t.Fatalf("openAMQPConnectionWith() = (%#v, %#v, %v), closes %d", channel, resource, err, connection.closeCalls)
	}
}

func TestBuildTLSConfigRejectsInvalidMaterialAndAcceptsCustomTrustAndMTLS(t *testing.T) {
	t.Parallel()

	certificatePEM, privateKeyPEM := testCertificate(t)
	tests := map[string]struct {
		config TLSConfig
		want   error
		mTLS   bool
	}{
		"system roots": {config: TLSConfig{ServerName: "rabbitmq.internal"}},
		"invalid root": {config: TLSConfig{ServerName: "rabbitmq.internal", RootCAs: [][]byte{[]byte("not pem")}}, want: ErrInvalidTLS},
		"custom root":  {config: TLSConfig{ServerName: "rabbitmq.internal", RootCAs: [][]byte{certificatePEM}}},
		"invalid mTLS": {config: TLSConfig{ServerName: "rabbitmq.internal", ClientCertificate: []byte("bad"), ClientPrivateKey: []byte("bad")}, want: ErrInvalidTLS},
		"valid mTLS":   {config: TLSConfig{ServerName: "rabbitmq.internal", ClientCertificate: certificatePEM, ClientPrivateKey: privateKeyPEM}, mTLS: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config, err := buildTLSConfig(test.config)
			if !errors.Is(err, test.want) {
				t.Fatalf("buildTLSConfig() error = %v, want %v", err, test.want)
			}
			if test.want != nil {
				return
			}
			if config == nil || config.MinVersion != 0x0303 || config.ServerName != "rabbitmq.internal" || config.InsecureSkipVerify || config.RootCAs == nil {
				t.Fatalf("TLS config = %#v, want verified TLS 1.2 minimum", config)
			}
			if (len(config.Certificates) == 1) != test.mTLS {
				t.Fatalf("client certificates = %d, want mTLS %t", len(config.Certificates), test.mTLS)
			}
		})
	}
}

func TestRandomProducerSessionIsBoundedAndUnique(t *testing.T) {
	t.Parallel()

	first, err := randomProducerSession()
	if err != nil || len(first) != 32 || invalidIdentity(first, 128) {
		t.Fatalf("randomProducerSession() = (%q, %v), want bounded identifier", first, err)
	}
	second, err := randomProducerSession()
	if err != nil || first == second {
		t.Fatalf("second randomProducerSession() = (%q, %v), want distinct identifier", second, err)
	}
}

func unavailableDial(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
	return nil, nil, errors.New("unavailable")
}

type fakeAMQPConnection struct {
	channel    producerChannel
	channelErr error
	closeCalls int
}

type trackingDeadlineConn struct {
	net.Conn
	deadline time.Time
}

func (connection *trackingDeadlineConn) SetDeadline(deadline time.Time) error {
	connection.deadline = deadline
	return connection.Conn.SetDeadline(deadline)
}

func (connection *fakeAMQPConnection) Channel() (producerChannel, error) {
	return connection.channel, connection.channelErr
}

func (connection *fakeAMQPConnection) Close() error {
	connection.closeCalls++
	return nil
}

func (connection *fakeAMQPConnection) CloseDeadline(time.Time) error {
	return connection.Close()
}

func testCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rabbitmq.internal"},
		DNSNames:              []string{"rabbitmq.internal"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
}
