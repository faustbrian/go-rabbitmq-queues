//go:build livebroker

package rabbitmqqueue_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

const (
	liveBrokerConfigEnvironment = "RABBITMQ_QUEUE_LIVE_CONFIG"
	maximumLiveConfigBytes      = 64 << 10
	liveOperationTimeout        = 15 * time.Second
)

type liveEndpoint struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}

type liveTLS struct {
	ServerName            string `json:"server_name"`
	RootCAFile            string `json:"root_ca_file"`
	ClientCertificateFile string `json:"client_certificate_file"`
	ClientPrivateKeyFile  string `json:"client_private_key_file"`
}

type liveQueue struct {
	Name       string `json:"name"`
	RoutingKey string `json:"routing_key"`
}

type livePerformanceFixture struct {
	QueueType            rabbitmqqueue.QueueType `json:"queue_type"`
	Queues               []liveQueue             `json:"queues"`
	DailyMessages        uint64                  `json:"daily_messages"`
	WarmupSeconds        int                     `json:"warmup_seconds"`
	SampleSeconds        int                     `json:"sample_seconds"`
	Samples              int                     `json:"samples"`
	BurstMultiplier      int                     `json:"burst_multiplier"`
	BurstSeconds         int                     `json:"burst_seconds"`
	PublisherConcurrency int                     `json:"publisher_concurrency"`
	ConsumerConcurrency  int                     `json:"consumer_concurrency"`
	PayloadBytes         []int                   `json:"payload_bytes"`
	HeaderBytes          []int                   `json:"header_bytes"`
	HandlerDelayMillis   int                     `json:"handler_delay_ms"`
}

type liveBrokerFixture struct {
	Endpoints                   []liveEndpoint          `json:"endpoints"`
	VirtualHost                 string                  `json:"virtual_host"`
	Username                    string                  `json:"username"`
	Password                    string                  `json:"password"`
	TLS                         liveTLS                 `json:"tls"`
	Exchange                    string                  `json:"exchange"`
	Classic                     liveQueue               `json:"classic"`
	Quorum                      liveQueue               `json:"quorum"`
	UnroutableRoutingKey        string                  `json:"unroutable_routing_key"`
	FaultStartGateFile          string                  `json:"fault_start_gate_file"`
	FaultCompleteGateFile       string                  `json:"fault_complete_gate_file"`
	FaultWindowMessages         int                     `json:"fault_window_messages"`
	FaultQueueType              rabbitmqqueue.QueueType `json:"fault_queue_type"`
	FaultScenario               liveFaultScenario       `json:"fault_scenario"`
	FaultCycleGateFiles         []string                `json:"fault_cycle_gate_files"`
	FaultCycleCompleteGateFiles []string                `json:"fault_cycle_complete_gate_files"`
	FaultResourcePairs          int                     `json:"fault_resource_pairs"`
	Performance                 livePerformanceFixture  `json:"performance"`
	PHPInteroperability         livePHPInteropFixture   `json:"php_interoperability"`
}

func TestLiveBrokerSingleNode(t *testing.T) {
	fixture := readLiveBrokerFixture(t)
	connection := fixture.connection(t)
	verifyLiveTopology(t, connection, fixture)

	t.Run("classic round trip", func(t *testing.T) {
		assertLiveRoundTrip(t, connection, fixture.Exchange, fixture.Classic, rabbitmqqueue.QueueClassic)
	})
	t.Run("quorum round trip", func(t *testing.T) {
		assertLiveRoundTrip(t, connection, fixture.Exchange, fixture.Quorum, rabbitmqqueue.QueueQuorum)
	})
	t.Run("mandatory return", func(t *testing.T) {
		assertLiveMandatoryReturn(t, connection, fixture)
	})
	t.Run("quorum bounded requeue", func(t *testing.T) {
		assertLiveQuorumRequeueBound(t, connection, fixture)
	})
}

func readLiveBrokerFixture(t *testing.T) liveBrokerFixture {
	t.Helper()
	fixture := readLiveBrokerFixtureForEnvironment(t, liveBrokerConfigEnvironment)
	if len(fixture.Endpoints) != 1 {
		t.Fatal("single-node live-broker configuration requires exactly one endpoint")
	}
	return fixture
}

func readLiveBrokerFixtureForEnvironment(t *testing.T, environment string) liveBrokerFixture {
	t.Helper()
	fixture := decodeLiveBrokerFixture(t, environment)
	if fixture.Classic.Name == "" || fixture.Classic.RoutingKey == "" ||
		fixture.Quorum.Name == "" || fixture.Quorum.RoutingKey == "" ||
		fixture.UnroutableRoutingKey == "" ||
		fixture.UnroutableRoutingKey == fixture.Classic.RoutingKey ||
		fixture.UnroutableRoutingKey == fixture.Quorum.RoutingKey {
		t.Fatal("live-broker topology configuration is incomplete")
	}
	return fixture
}

func decodeLiveBrokerFixture(t *testing.T, environment string) liveBrokerFixture {
	t.Helper()
	configPath := os.Getenv(environment)
	if configPath == "" {
		t.Fatalf("%s must point to the live-broker fixture configuration", environment)
	}
	file, err := os.Open(configPath)
	if err != nil {
		t.Fatalf("open live-broker configuration: %v", err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maximumLiveConfigBytes+1))
	if err != nil {
		t.Fatalf("read live-broker configuration: %v", err)
	}
	if len(contents) > maximumLiveConfigBytes {
		t.Fatalf("live-broker configuration exceeds %d bytes", maximumLiveConfigBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var fixture liveBrokerFixture
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode live-broker configuration: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatal("live-broker configuration must contain exactly one JSON object")
	}
	if len(fixture.Endpoints) == 0 || fixture.Username == "" || fixture.Password == "" ||
		fixture.TLS.ServerName == "" || fixture.Exchange == "" {
		t.Fatal("live-broker connection configuration is incomplete")
	}
	return fixture
}

func (fixture liveBrokerFixture) connection(t *testing.T) rabbitmqqueue.ConnectionConfig {
	t.Helper()
	endpoints := make([]rabbitmqqueue.Endpoint, len(fixture.Endpoints))
	for index, endpoint := range fixture.Endpoints {
		endpoints[index] = rabbitmqqueue.Endpoint{Host: endpoint.Host, Port: endpoint.Port}
	}
	rootCAs := make([][]byte, 0, 1)
	if fixture.TLS.RootCAFile != "" {
		rootCAs = append(rootCAs, readBoundedSecretFile(t, fixture.TLS.RootCAFile))
	}
	var clientCertificate, clientPrivateKey []byte
	if fixture.TLS.ClientCertificateFile != "" || fixture.TLS.ClientPrivateKeyFile != "" {
		if fixture.TLS.ClientCertificateFile == "" || fixture.TLS.ClientPrivateKeyFile == "" {
			t.Fatal("client certificate and private key files must be configured together")
		}
		clientCertificate = readBoundedSecretFile(t, fixture.TLS.ClientCertificateFile)
		clientPrivateKey = readBoundedSecretFile(t, fixture.TLS.ClientPrivateKeyFile)
	}
	password := fixture.Password
	connection := rabbitmqqueue.ConnectionConfig{
		Endpoints:   endpoints,
		VirtualHost: fixture.VirtualHost,
		Credentials: rabbitmqqueue.CredentialProviderFunc(func(context.Context) (rabbitmqqueue.Credentials, error) {
			return rabbitmqqueue.Credentials{Username: fixture.Username, Password: []byte(password)}, nil
		}),
		TLS: rabbitmqqueue.TLSConfig{
			ServerName:        fixture.TLS.ServerName,
			RootCAs:           rootCAs,
			ClientCertificate: clientCertificate,
			ClientPrivateKey:  clientPrivateKey,
		},
		DialTimeout: 10 * time.Second,
		Heartbeat:   10 * time.Second,
		Recovery: rabbitmqqueue.RecoveryPolicy{
			MaxAttempts:  3,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     time.Second,
		},
	}
	if err := connection.Validate(); err != nil {
		t.Fatalf("validate live-broker connection policy: %v", err)
	}
	return connection
}

func readBoundedSecretFile(t *testing.T, filename string) []byte {
	t.Helper()
	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("open configured TLS material: %v", err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, rabbitmqqueue.MaxTLSMaterialBytes+1))
	if err != nil {
		t.Fatalf("read configured TLS material: %v", err)
	}
	if len(contents) > rabbitmqqueue.MaxTLSMaterialBytes {
		t.Fatalf("configured TLS material exceeds %d bytes", rabbitmqqueue.MaxTLSMaterialBytes)
	}
	return contents
}

func verifyLiveTopology(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	fixture liveBrokerFixture,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	result, err := rabbitmqqueue.ApplyTopology(
		ctx,
		connection,
		rabbitmqqueue.TopologyPolicy{Mode: rabbitmqqueue.TopologyPassive},
		rabbitmqqueue.Topology{
			Exchanges: []rabbitmqqueue.Exchange{{
				Name: fixture.Exchange, Kind: rabbitmqqueue.ExchangeDirect, Durable: true,
			}},
			Queues: []rabbitmqqueue.Queue{
				{Name: fixture.Classic.Name, Type: rabbitmqqueue.QueueClassic, Durable: true},
				{Name: fixture.Quorum.Name, Type: rabbitmqqueue.QueueQuorum, Durable: true},
			},
		},
	)
	if err != nil {
		t.Fatalf("passively verify live-broker topology: %v", err)
	}
	want := []string{fixture.Classic.Name, fixture.Quorum.Name}
	if !reflect.DeepEqual(result.QueueNames, want) {
		t.Fatalf("verified queue identities = %v, want %v", result.QueueNames, want)
	}
}

func assertLiveRoundTrip(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	exchange string,
	queue liveQueue,
	queueType rabbitmqqueue.QueueType,
) {
	t.Helper()
	messageID := "live-round-trip-" + randomLiveToken(t)
	received := make(chan rabbitmqqueue.Delivery, 1)
	consumer := openLiveConsumer(t, connection, queue, queueType, 1, func(
		_ context.Context,
		delivery rabbitmqqueue.Delivery,
	) (rabbitmqqueue.Settlement, error) {
		received <- delivery
		return rabbitmqqueue.Acknowledge(), nil
	})
	defer closeLiveConsumer(t, consumer)
	producer := openLiveProducer(t, connection)
	defer closeLiveProducer(t, producer)

	timestamp := time.Now().UTC().Truncate(time.Second)
	body := []byte{'l', 'i', 'v', 'e', 0, 0xff}
	expiration := time.Minute
	priority := uint16(2)
	publication := rabbitmqqueue.Publication{
		Exchange: exchange, ExchangeKind: rabbitmqqueue.ExchangeDirect,
		RoutingKey: queue.RoutingKey, Mandatory: true,
		DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{
			Body: body, MessageID: messageID, CorrelationID: "correlation-" + messageID,
			ReplyTo: "reply." + messageID, ContentType: "application/octet-stream",
			ContentEncoding: "identity", Type: "live.integration.v1", AppID: "go-rabbitmq-queues",
			Timestamp: timestamp, Expiration: &expiration, Priority: &priority,
			Headers: []rabbitmqqueue.Header{
				rabbitmqqueue.StringHeader("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
				rabbitmqqueue.BoolHeader("live", true),
				rabbitmqqueue.Int64Header("schema-version", 1),
				rabbitmqqueue.BytesHeader("opaque", []byte{0, 1, 0xff}),
			},
		},
	}
	publishContext, cancelPublish := context.WithTimeout(context.Background(), liveOperationTimeout)
	result, err := producer.Publish(publishContext, publication)
	cancelPublish()
	if err != nil || result.State != rabbitmqqueue.PublishConfirmed || result.Return != nil {
		t.Fatalf("live publish result = %#v, error = %v; want confirmed", result, err)
	}

	select {
	case delivery := <-received:
		if delivery.MessageID != messageID || delivery.CorrelationID != publication.Message.CorrelationID ||
			delivery.ReplyTo != publication.Message.ReplyTo ||
			delivery.ContentType != publication.Message.ContentType ||
			delivery.ContentEncoding != publication.Message.ContentEncoding ||
			delivery.Type != publication.Message.Type || delivery.AppID != publication.Message.AppID ||
			!delivery.Timestamp.Equal(timestamp) || !bytes.Equal(delivery.Body, body) ||
			delivery.Expiration == nil || *delivery.Expiration != expiration ||
			delivery.Priority != uint8(priority) ||
			delivery.Exchange != exchange || delivery.RoutingKey != queue.RoutingKey ||
			delivery.Redelivered || delivery.DeliveryMode != rabbitmqqueue.DeliveryPersistent {
			t.Fatalf("live delivery did not preserve the published contract")
		}
		if !reflect.DeepEqual(headersByKey(delivery.Headers), headersByKey(publication.Message.Headers)) {
			t.Fatal("live delivery did not preserve application header types and values")
		}
	case <-time.After(liveOperationTimeout):
		t.Fatal("timed out waiting for live delivery")
	}
}

func assertLiveMandatoryReturn(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	fixture liveBrokerFixture,
) {
	t.Helper()
	producer := openLiveProducer(t, connection)
	defer closeLiveProducer(t, producer)
	publication := rabbitmqqueue.Publication{
		Exchange: fixture.Exchange, ExchangeKind: rabbitmqqueue.ExchangeDirect,
		RoutingKey: fixture.UnroutableRoutingKey, Mandatory: true,
		DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{
			Body: []byte("must-return"), MessageID: "live-return-" + randomLiveToken(t),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	result, err := producer.Publish(ctx, publication)
	cancel()
	if !errors.Is(err, rabbitmqqueue.ErrPublishReturned) ||
		result.State != rabbitmqqueue.PublishReturned || result.Return == nil ||
		result.Return.Code != 312 || result.Return.Exchange != fixture.Exchange ||
		result.Return.RoutingKey != fixture.UnroutableRoutingKey {
		t.Fatalf("mandatory publish result = %#v, error = %v; want exact returned route", result, err)
	}
}

func assertLiveQuorumRequeueBound(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	fixture liveBrokerFixture,
) {
	t.Helper()
	messageID := "live-requeue-" + randomLiveToken(t)
	deliveries := make(chan rabbitmqqueue.Delivery, 4)
	consumer := openLiveConsumer(t, connection, fixture.Quorum, rabbitmqqueue.QueueQuorum, 2, func(
		_ context.Context,
		delivery rabbitmqqueue.Delivery,
	) (rabbitmqqueue.Settlement, error) {
		deliveries <- delivery
		return rabbitmqqueue.Reject(true), nil
	})
	defer closeLiveConsumer(t, consumer)
	producer := openLiveProducer(t, connection)
	defer closeLiveProducer(t, producer)
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	result, err := producer.Publish(ctx, rabbitmqqueue.Publication{
		Exchange: fixture.Exchange, ExchangeKind: rabbitmqqueue.ExchangeDirect,
		RoutingKey: fixture.Quorum.RoutingKey, Mandatory: true,
		DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message:      rabbitmqqueue.Message{Body: []byte("bounded-requeue"), MessageID: messageID},
	})
	cancel()
	if err != nil || result.State != rabbitmqqueue.PublishConfirmed {
		t.Fatalf("quorum publish result = %#v, error = %v; want confirmed", result, err)
	}

	for acquisition := uint64(0); acquisition <= 2; acquisition++ {
		select {
		case delivery := <-deliveries:
			if delivery.MessageID != messageID {
				t.Fatalf("received unexpected message %q on dedicated quorum fixture", delivery.MessageID)
			}
			if acquisition == 0 {
				if delivery.Redelivered || (delivery.AcquiredCount != nil && *delivery.AcquiredCount != 0) {
					t.Fatalf("first acquisition = %#v, redelivered = %t; want zero or omitted", delivery.AcquiredCount, delivery.Redelivered)
				}
				continue
			}
			if !delivery.Redelivered || delivery.AcquiredCount == nil || *delivery.AcquiredCount != acquisition {
				t.Fatalf("acquisition = %#v, redelivered = %t; want %d", delivery.AcquiredCount, delivery.Redelivered, acquisition)
			}
		case <-time.After(liveOperationTimeout):
			t.Fatalf("timed out waiting for quorum acquisition %d", acquisition)
		}
	}

	select {
	case delivery := <-deliveries:
		t.Fatalf("MaxRequeues allowed an unexpected fourth delivery with acquired count %#v", delivery.AcquiredCount)
	case <-time.After(2 * time.Second):
	}
}

func openLiveProducer(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
) *rabbitmqqueue.Producer {
	t.Helper()
	return openLiveProducerWithBounds(t, connection, 8, liveOperationTimeout)
}

func openLiveProducerWithBounds(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	maxOutstanding int,
	publishTimeout time.Duration,
) *rabbitmqqueue.Producer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	producer, err := rabbitmqqueue.OpenProducer(ctx, connection, rabbitmqqueue.ProducerConfig{
		Limits: rabbitmqqueue.DefaultLimits(), MaxOutstanding: maxOutstanding,
		PublishTimeout: publishTimeout,
	})
	if err != nil {
		t.Fatalf("open live producer: %v", err)
	}
	return producer
}

func openLiveConsumer(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	queue liveQueue,
	queueType rabbitmqqueue.QueueType,
	maxRequeues uint32,
	handler rabbitmqqueue.DeliveryHandler,
) *rabbitmqqueue.Consumer {
	t.Helper()
	return openLiveConsumerWithBounds(
		t, connection, queue, queueType, maxRequeues, 1, 1, liveOperationTimeout, handler,
	)
}

func openLiveConsumerWithBounds(
	t *testing.T,
	connection rabbitmqqueue.ConnectionConfig,
	queue liveQueue,
	queueType rabbitmqqueue.QueueType,
	maxRequeues uint32,
	prefetch int,
	concurrency int,
	handlerTimeout time.Duration,
	handler rabbitmqqueue.DeliveryHandler,
) *rabbitmqqueue.Consumer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	consumer, err := rabbitmqqueue.OpenConsumer(ctx, connection, rabbitmqqueue.ConsumerConfig{
		Limits: rabbitmqqueue.DefaultLimits(),
		Queue:  rabbitmqqueue.QueueReference{Name: queue.Name, Type: queueType},
		Name:   "live-consumer-" + randomLiveToken(t), Prefetch: prefetch, Concurrency: concurrency,
		HandlerTimeout: handlerTimeout, MaxRequeues: maxRequeues,
		Failure: rabbitmqqueue.Reject(false),
	}, handler)
	if err != nil {
		t.Fatalf("open live consumer: %v", err)
	}
	return consumer
}

func closeLiveProducer(t *testing.T, producer *rabbitmqqueue.Producer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	if err := producer.Close(ctx); err != nil {
		t.Errorf("close live producer: %v", err)
	}
}

func closeLiveConsumer(t *testing.T, consumer *rabbitmqqueue.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	if err := consumer.Close(ctx); err != nil {
		t.Errorf("close live consumer: %v", err)
	}
}

func randomLiveToken(t *testing.T) string {
	t.Helper()
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate live test identity: %v", err)
	}
	return hex.EncodeToString(value)
}

func headersByKey(headers []rabbitmqqueue.Header) map[string]rabbitmqqueue.Header {
	indexed := make(map[string]rabbitmqqueue.Header, len(headers))
	for _, header := range headers {
		indexed[header.Key] = header
	}
	return indexed
}
