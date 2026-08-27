//go:build livebroker

package rabbitmqqueue_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

const maximumPHPInteropOutputBytes = 4 << 10

type livePHPInteropFixture struct {
	Binary               string                  `json:"binary"`
	QueueType            rabbitmqqueue.QueueType `json:"queue_type"`
	GoToPHPQueue         string                  `json:"go_to_php_queue"`
	GoToPHPRoutingKey    string                  `json:"go_to_php_routing_key"`
	PHPToGoQueue         string                  `json:"php_to_go_queue"`
	PHPToGoRoutingKey    string                  `json:"php_to_go_routing_key"`
	UnroutableRoutingKey string                  `json:"unroutable_routing_key"`
}

type liveInteropCorpus struct {
	SchemaVersion int `json:"schema_version"`
	Routing       struct {
		Exchange   string `json:"exchange"`
		RoutingKey string `json:"routing_key"`
		Mandatory  bool   `json:"mandatory"`
	} `json:"routing"`
	Properties struct {
		DeliveryMode    string `json:"delivery_mode"`
		ContentType     string `json:"content_type"`
		ContentEncoding string `json:"content_encoding"`
		MessageID       string `json:"message_id"`
		CorrelationID   string `json:"correlation_id"`
		ReplyTo         string `json:"reply_to"`
		Timestamp       string `json:"timestamp"`
		Type            string `json:"type"`
		AppID           string `json:"app_id"`
		ExpirationMS    int64  `json:"expiration_ms"`
		Priority        uint16 `json:"priority"`
	} `json:"properties"`
	Headers []struct {
		Key    string `json:"key"`
		Type   string `json:"type"`
		String string `json:"string,omitempty"`
		Bool   bool   `json:"bool,omitempty"`
		Int64  int64  `json:"int64,omitempty"`
		Base64 string `json:"base64,omitempty"`
	} `json:"headers"`
	BodyBase64 string `json:"body_base64"`
}

type boundedPHPOutput struct {
	contents bytes.Buffer
	overflow bool
}

func (output *boundedPHPOutput) Write(value []byte) (int, error) {
	remaining := maximumPHPInteropOutputBytes - output.contents.Len()
	if remaining > 0 {
		stored := value
		if len(stored) > remaining {
			stored = stored[:remaining]
		}
		_, _ = output.contents.Write(stored)
	}
	if len(value) > remaining {
		output.overflow = true
	}
	return len(value), nil
}

func TestLiveBrokerPHPInteroperability(t *testing.T) {
	fixture := readLiveBrokerFixture(t)
	validateLivePHPInteropFixture(t, fixture)
	verifyLivePHPInteropTopology(t, fixture)
	corpus := readLiveInteropCorpus(t)

	t.Run("pinned PHP runtime", func(t *testing.T) {
		runPHPInterop(t, fixture, "self-test", "PHP_SELF_TEST_OK\n")
	})
	t.Run("Go publishes and PHP consumes", func(t *testing.T) {
		producer := openLiveProducer(t, fixture.connection(t))
		defer closeLiveProducer(t, producer)
		publication := liveInteropPublication(t, corpus, fixture.Exchange, fixture.PHPInteroperability.GoToPHPRoutingKey)
		ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		result, err := producer.Publish(ctx, publication)
		cancel()
		if err != nil || result.State != rabbitmqqueue.PublishConfirmed || result.Return != nil {
			t.Fatalf("Go interoperability publication was not confirmed")
		}
		runPHPInterop(t, fixture, "consume", "PHP_CONSUME_OK\n")
	})
	t.Run("PHP publishes and Go consumes", func(t *testing.T) {
		received := make(chan rabbitmqqueue.Delivery, 1)
		interop := fixture.PHPInteroperability
		consumer := openLiveConsumer(t, fixture.connection(t), liveQueue{
			Name: interop.PHPToGoQueue, RoutingKey: interop.PHPToGoRoutingKey,
		}, interop.QueueType, 0, func(
			_ context.Context,
			delivery rabbitmqqueue.Delivery,
		) (rabbitmqqueue.Settlement, error) {
			received <- delivery
			return rabbitmqqueue.Acknowledge(), nil
		})
		defer closeLiveConsumer(t, consumer)
		runPHPInterop(t, fixture, "publish", "PHP_PUBLISH_OK\n")
		select {
		case delivery := <-received:
			assertLiveInteropDelivery(t, delivery, corpus, fixture.Exchange, interop.PHPToGoRoutingKey)
		case <-time.After(liveOperationTimeout):
			t.Fatal("timed out waiting for PHP interoperability delivery")
		}
	})
	t.Run("PHP reconciles mandatory return", func(t *testing.T) {
		runPHPInterop(t, fixture, "return", "PHP_RETURN_OK\n")
	})
}

func validateLivePHPInteropFixture(t *testing.T, fixture liveBrokerFixture) {
	t.Helper()
	interop := fixture.PHPInteroperability
	if !filepath.IsAbs(interop.Binary) ||
		interop.GoToPHPQueue == "" || interop.GoToPHPRoutingKey == "" ||
		interop.PHPToGoQueue == "" || interop.PHPToGoRoutingKey == "" ||
		interop.UnroutableRoutingKey == "" || interop.GoToPHPQueue == interop.PHPToGoQueue ||
		interop.GoToPHPRoutingKey == interop.PHPToGoRoutingKey ||
		interop.UnroutableRoutingKey == interop.GoToPHPRoutingKey ||
		interop.UnroutableRoutingKey == interop.PHPToGoRoutingKey {
		t.Fatal("PHP interoperability configuration is incomplete or aliases dedicated routes")
	}
	if interop.QueueType != rabbitmqqueue.QueueClassic && interop.QueueType != rabbitmqqueue.QueueQuorum {
		t.Fatal("PHP interoperability queue type must be classic or quorum")
	}
	for _, queueName := range []string{fixture.Classic.Name, fixture.Quorum.Name} {
		if interop.GoToPHPQueue == queueName || interop.PHPToGoQueue == queueName {
			t.Fatal("PHP interoperability queues must be dedicated")
		}
	}
	routingKeys := []string{
		fixture.Classic.RoutingKey,
		fixture.Quorum.RoutingKey,
		fixture.UnroutableRoutingKey,
		interop.GoToPHPRoutingKey,
		interop.PHPToGoRoutingKey,
		interop.UnroutableRoutingKey,
	}
	uniqueRoutingKeys := make(map[string]struct{}, len(routingKeys))
	for _, routingKey := range routingKeys {
		uniqueRoutingKeys[routingKey] = struct{}{}
	}
	if len(uniqueRoutingKeys) != len(routingKeys) {
		t.Fatal("live-broker and PHP interoperability routing keys must be distinct")
	}
	for _, filename := range []string{interop.Binary, phpInteropAutoloadFile(t)} {
		info, err := os.Stat(filename)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal("PHP interoperability executable or autoload file is unavailable")
		}
	}
}

func verifyLivePHPInteropTopology(t *testing.T, fixture liveBrokerFixture) {
	t.Helper()
	interop := fixture.PHPInteroperability
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	result, err := rabbitmqqueue.ApplyTopology(ctx, fixture.connection(t), rabbitmqqueue.TopologyPolicy{
		Mode: rabbitmqqueue.TopologyPassive,
	}, rabbitmqqueue.Topology{
		Exchanges: []rabbitmqqueue.Exchange{{
			Name: fixture.Exchange, Kind: rabbitmqqueue.ExchangeDirect, Durable: true,
		}},
		Queues: []rabbitmqqueue.Queue{
			{Name: interop.GoToPHPQueue, Type: interop.QueueType, Durable: true},
			{Name: interop.PHPToGoQueue, Type: interop.QueueType, Durable: true},
		},
	})
	if err != nil {
		t.Fatalf("passively verify PHP interoperability topology: %v", err)
	}
	if !reflect.DeepEqual(result.QueueNames, []string{interop.GoToPHPQueue, interop.PHPToGoQueue}) {
		t.Fatal("PHP interoperability topology returned unexpected queue identities")
	}
}

func readLiveInteropCorpus(t *testing.T) liveInteropCorpus {
	t.Helper()
	file, err := os.Open("testdata/interoperability/message-v1.json")
	if err != nil {
		t.Fatalf("open interoperability corpus: %v", err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maximumLiveConfigBytes+1))
	if err != nil {
		t.Fatalf("read interoperability corpus: %v", err)
	}
	if len(contents) > maximumLiveConfigBytes {
		t.Fatalf("interoperability corpus exceeds %d bytes", maximumLiveConfigBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var corpus liveInteropCorpus
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("decode interoperability corpus: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatal("interoperability corpus must contain exactly one JSON object")
	}
	if corpus.SchemaVersion != 1 || corpus.Routing.Exchange == "" ||
		corpus.Routing.RoutingKey == "" || !corpus.Routing.Mandatory ||
		corpus.Properties.DeliveryMode != "persistent" ||
		corpus.Properties.ExpirationMS < 0 || corpus.Properties.Priority > 255 {
		t.Fatal("interoperability corpus has unsupported metadata")
	}
	return corpus
}

func liveInteropPublication(
	t *testing.T,
	corpus liveInteropCorpus,
	exchange string,
	routingKey string,
) rabbitmqqueue.Publication {
	t.Helper()
	body, err := base64.StdEncoding.DecodeString(corpus.BodyBase64)
	if err != nil {
		t.Fatal("decode interoperability body")
	}
	timestamp, err := time.Parse(time.RFC3339, corpus.Properties.Timestamp)
	if err != nil {
		t.Fatal("parse interoperability timestamp")
	}
	headers := make([]rabbitmqqueue.Header, 0, len(corpus.Headers))
	for _, header := range corpus.Headers {
		switch header.Type {
		case "string":
			headers = append(headers, rabbitmqqueue.StringHeader(header.Key, header.String))
		case "bool":
			headers = append(headers, rabbitmqqueue.BoolHeader(header.Key, header.Bool))
		case "int64":
			headers = append(headers, rabbitmqqueue.Int64Header(header.Key, header.Int64))
		case "bytes":
			value, err := base64.StdEncoding.DecodeString(header.Base64)
			if err != nil {
				t.Fatal("decode interoperability header")
			}
			headers = append(headers, rabbitmqqueue.BytesHeader(header.Key, value))
		default:
			t.Fatal("unsupported interoperability header")
		}
	}
	priority := corpus.Properties.Priority
	expiration := time.Duration(corpus.Properties.ExpirationMS) * time.Millisecond
	return rabbitmqqueue.Publication{
		Exchange: exchange, ExchangeKind: rabbitmqqueue.ExchangeDirect,
		RoutingKey: routingKey, Mandatory: true, DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{
			Body: body, ContentType: corpus.Properties.ContentType,
			ContentEncoding: corpus.Properties.ContentEncoding, MessageID: corpus.Properties.MessageID,
			CorrelationID: corpus.Properties.CorrelationID, ReplyTo: corpus.Properties.ReplyTo,
			Timestamp: timestamp, Type: corpus.Properties.Type, AppID: corpus.Properties.AppID,
			Expiration: &expiration, Priority: &priority, Headers: headers,
		},
	}
}

func assertLiveInteropDelivery(
	t *testing.T,
	delivery rabbitmqqueue.Delivery,
	corpus liveInteropCorpus,
	exchange string,
	routingKey string,
) {
	t.Helper()
	want := liveInteropPublication(t, corpus, exchange, routingKey)
	message := want.Message
	if !bytes.Equal(delivery.Body, message.Body) || delivery.ContentType != message.ContentType ||
		delivery.ContentEncoding != message.ContentEncoding || delivery.MessageID != message.MessageID ||
		delivery.CorrelationID != message.CorrelationID || delivery.ReplyTo != message.ReplyTo ||
		!delivery.Timestamp.Equal(message.Timestamp) || delivery.Type != message.Type ||
		delivery.AppID != message.AppID || delivery.Exchange != exchange || delivery.RoutingKey != routingKey ||
		delivery.DeliveryMode != rabbitmqqueue.DeliveryPersistent || delivery.Redelivered ||
		delivery.Expiration == nil || message.Expiration == nil || *delivery.Expiration != *message.Expiration ||
		delivery.Priority != uint8(*message.Priority) ||
		!reflect.DeepEqual(headersByKey(delivery.Headers), headersByKey(message.Headers)) {
		t.Fatal("PHP publication did not preserve the canonical AMQP contract")
	}
}

func runPHPInterop(t *testing.T, fixture liveBrokerFixture, mode string, expectedOutput string) {
	t.Helper()
	configurationPath, err := filepath.Abs(os.Getenv(liveBrokerConfigEnvironment))
	if err != nil {
		t.Fatal("resolve PHP interoperability configuration")
	}
	corpusPath, err := filepath.Abs("testdata/interoperability/message-v1.json")
	if err != nil {
		t.Fatal("resolve interoperability corpus")
	}
	runnerPath, err := filepath.Abs("testdata/interoperability/php/interop.php")
	if err != nil {
		t.Fatal("resolve PHP interoperability runner")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*liveOperationTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, fixture.PHPInteroperability.Binary, runnerPath, mode, corpusPath)
	environment := make([]string, 0, len(os.Environ())+2)
	for _, variable := range os.Environ() {
		if strings.HasPrefix(variable, "PHP_AMQPLIB_AUTOLOAD=") ||
			strings.HasPrefix(variable, "RABBITMQ_QUEUE_PHP_CONFIG=") {
			continue
		}
		environment = append(environment, variable)
	}
	command.Env = append(environment,
		"PHP_AMQPLIB_AUTOLOAD="+phpInteropAutoloadFile(t),
		"RABBITMQ_QUEUE_PHP_CONFIG="+configurationPath,
	)
	var stdout, stderr boundedPHPOutput
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("PHP interoperability %s failed with %T", mode, err)
	}
	if stdout.overflow || stderr.overflow {
		t.Fatal("PHP interoperability process exceeded the bounded output limit")
	}
	if stdout.contents.String() != expectedOutput || stderr.contents.Len() != 0 {
		t.Fatal("PHP interoperability process returned an unexpected status marker")
	}
}

func phpInteropAutoloadFile(t *testing.T) string {
	t.Helper()
	filename := os.Getenv("PHP_AMQPLIB_AUTOLOAD")
	if !filepath.IsAbs(filename) {
		t.Fatal("PHP_AMQPLIB_AUTOLOAD must contain an absolute path")
	}
	return filename
}
