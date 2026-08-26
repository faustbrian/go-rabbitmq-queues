package rabbitmqqueue

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type interoperabilityFixture struct {
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

func TestLanguageNeutralInteroperabilityFixturePreservesAMQPMetadataAndBytes(t *testing.T) {
	t.Parallel()

	fixture := readInteroperabilityFixture(t)
	publication := fixturePublication(t, fixture)
	if err := publication.Validate(DefaultLimits()); err != nil {
		t.Fatalf("fixture publication validation: %v", err)
	}

	published := amqpPublishing(publication.Message, publication.DeliveryMode, "fixture-token")
	delivery, err := deliveryFromAMQP(amqp.Delivery{
		Headers: published.Headers, ContentType: published.ContentType,
		ContentEncoding: published.ContentEncoding, DeliveryMode: published.DeliveryMode,
		Priority: published.Priority, CorrelationId: published.CorrelationId,
		Expiration: published.Expiration, MessageId: published.MessageId,
		Timestamp: published.Timestamp, Type: published.Type, AppId: published.AppId,
		ConsumerTag: "interop-consumer", DeliveryTag: 1,
		Exchange: publication.Exchange, RoutingKey: publication.RoutingKey, Body: published.Body,
	}, testConsumerConfig())
	if err != nil {
		t.Fatalf("fixture delivery conversion: %v", err)
	}

	message := publication.Message
	if !bytes.Equal(delivery.Body, message.Body) || delivery.MessageID != message.MessageID ||
		delivery.CorrelationID != message.CorrelationID || delivery.ContentType != message.ContentType ||
		delivery.ContentEncoding != message.ContentEncoding || delivery.Type != message.Type ||
		delivery.AppID != message.AppID || !delivery.Timestamp.Equal(message.Timestamp) ||
		delivery.Expiration != message.Expiration || delivery.Priority != uint8(*message.Priority) ||
		delivery.DeliveryMode != publication.DeliveryMode || delivery.Exchange != publication.Exchange ||
		delivery.RoutingKey != publication.RoutingKey {
		t.Fatal("fixture round trip lost stable AMQP metadata")
	}
	assertFixtureHeaders(t, delivery.Headers, message.Headers)
}

func readInteroperabilityFixture(t *testing.T) interoperabilityFixture {
	t.Helper()
	file, err := os.Open("testdata/interoperability/message-v1.json")
	if err != nil {
		t.Fatalf("open interoperability fixture: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Fatalf("close interoperability fixture: %v", err)
		}
	}()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var fixture interoperabilityFixture
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode interoperability fixture: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("interoperability fixture has trailing content: %v", err)
	}
	if fixture.SchemaVersion != 1 {
		t.Fatalf("fixture schema version = %d, want 1", fixture.SchemaVersion)
	}
	return fixture
}

func fixturePublication(t *testing.T, fixture interoperabilityFixture) Publication {
	t.Helper()
	body, err := base64.StdEncoding.DecodeString(fixture.BodyBase64)
	if err != nil {
		t.Fatalf("decode fixture body: %v", err)
	}
	if len(body) == 0 || !fixture.Routing.Mandatory {
		t.Fatal("fixture must contain opaque body bytes and mandatory routing")
	}
	timestamp, err := time.Parse(time.RFC3339, fixture.Properties.Timestamp)
	if err != nil {
		t.Fatalf("parse fixture timestamp: %v", err)
	}
	if fixture.Properties.DeliveryMode != "persistent" {
		t.Fatalf("fixture delivery mode = %q, want persistent", fixture.Properties.DeliveryMode)
	}
	if fixture.Properties.ContentType == "" || fixture.Properties.ContentEncoding == "" ||
		fixture.Properties.CorrelationID == "" || fixture.Properties.Type == "" ||
		fixture.Properties.AppID == "" || fixture.Properties.ExpirationMS < 0 {
		t.Fatal("fixture is missing required language-neutral metadata")
	}
	headers := make([]Header, 0, len(fixture.Headers))
	headerTypes := make(map[string]string, len(fixture.Headers))
	for index, header := range fixture.Headers {
		headerTypes[header.Key] = header.Type
		switch header.Type {
		case "string":
			headers = append(headers, StringHeader(header.Key, header.String))
		case "bool":
			headers = append(headers, BoolHeader(header.Key, header.Bool))
		case "int64":
			headers = append(headers, Int64Header(header.Key, header.Int64))
		case "bytes":
			value, err := base64.StdEncoding.DecodeString(header.Base64)
			if err != nil {
				t.Fatalf("decode fixture header at index %d", index)
			}
			headers = append(headers, BytesHeader(header.Key, value))
		default:
			t.Fatalf("fixture header at index %d has unsupported type", index)
		}
	}
	for key, kind := range map[string]string{
		"traceparent": "string", "tracestate": "string", "schema-version": "int64",
	} {
		if headerTypes[key] != kind {
			t.Fatal("required fixture metadata header contract mismatch")
		}
	}
	priority := fixture.Properties.Priority
	return Publication{
		Exchange: fixture.Routing.Exchange, RoutingKey: fixture.Routing.RoutingKey,
		Mandatory: fixture.Routing.Mandatory, DeliveryMode: DeliveryPersistent,
		Message: Message{
			Body: body, MessageID: fixture.Properties.MessageID,
			CorrelationID: fixture.Properties.CorrelationID,
			ContentType:   fixture.Properties.ContentType, ContentEncoding: fixture.Properties.ContentEncoding,
			Type: fixture.Properties.Type, AppID: fixture.Properties.AppID, Timestamp: timestamp,
			Expiration: time.Duration(fixture.Properties.ExpirationMS) * time.Millisecond,
			Priority:   &priority, Headers: headers,
		},
	}
}

func assertFixtureHeaders(t *testing.T, got, want []Header) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("fixture headers = %d, want %d", len(got), len(want))
	}
	byKey := make(map[string]Header, len(got))
	for _, header := range got {
		byKey[header.Key] = header
	}
	for index, header := range want {
		actual, exists := byKey[header.Key]
		if !exists || actual.Kind != header.Kind || actual.String != header.String ||
			actual.Bool != header.Bool || actual.Int64 != header.Int64 || !bytes.Equal(actual.Bytes, header.Bytes) {
			t.Fatalf("fixture header mismatch at index %d", index)
		}
	}
	if _, exists := byKey[publishTokenHeader]; exists {
		t.Fatal("package-owned publish correlation leaked into the interoperability fixture")
	}
}
