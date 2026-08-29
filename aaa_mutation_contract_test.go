package rabbitmqqueue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMutationContractInvalidIdentityConditionsRemainIndependent(t *testing.T) {
	for name, test := range map[string]struct {
		value   string
		maximum int
		want    bool
	}{
		"valid":      {value: "worker", maximum: 255, want: false},
		"empty":      {value: "", maximum: 255, want: true},
		"over limit": {value: "ab", maximum: 1, want: true},
		"whitespace": {value: " worker", maximum: 255, want: true},
		"control":    {value: "bad\nworker", maximum: 255, want: true},
	} {
		if got := invalidIdentity(test.value, test.maximum); got != test.want {
			t.Fatalf("%s invalid identity = %t, want %t", name, got, test.want)
		}
	}
}

func TestMutationContractConsumerFastFailurePredicates(t *testing.T) {
	wantErr := errors.New("failure")
	if accepted, err := acceptedConsumerConfig(nil); !accepted || err != nil {
		t.Fatalf("valid consumer config outcome = (%t, %v)", accepted, err)
	}
	if accepted, err := acceptedConsumerConfig(wantErr); accepted || !errors.Is(err, wantErr) {
		t.Fatalf("invalid consumer config outcome = (%t, %v)", accepted, err)
	}

	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	channel := newFakeConsumerChannel()
	resource := &concurrentCountingCloser{}
	if !consumerInputsPresent(handler, channel, resource) ||
		consumerInputsPresent(nil, channel, resource) ||
		consumerInputsPresent(handler, nil, resource) ||
		consumerInputsPresent(handler, channel, nil) {
		t.Fatal("consumer input presence collapsed an independent nil condition")
	}

	generation := &consumerGeneration{}
	if accepted, err := acceptedConsumerGeneration(generation, nil); !accepted || err != nil {
		t.Fatalf("valid consumer generation outcome = (%t, %v)", accepted, err)
	}
	if accepted, err := acceptedConsumerGeneration(nil, wantErr); accepted || !errors.Is(err, wantErr) {
		t.Fatalf("failed consumer generation outcome = (%t, %v)", accepted, err)
	}
	if accepted, err := acceptedConsumerGeneration(nil, nil); accepted || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("missing consumer generation outcome = (%t, %v)", accepted, err)
	}

	if got := consumerPendingCapacity(3); got != 4 {
		t.Fatalf("consumer pending capacity = %d, want 4", got)
	}
	for name, test := range map[string]struct {
		paused, draining bool
		want             bool
	}{
		"paused":        {paused: true, want: true},
		"open":          {},
		"draining":      {paused: true, draining: true},
		"open draining": {draining: true},
	} {
		if got := consumerAdmissionPaused(test.paused, test.draining); got != test.want {
			t.Fatalf("%s consumer admission paused = %t, want %t", name, got, test.want)
		}
	}

	if consumerOperationFailed(nil) || !consumerOperationFailed(wantErr) {
		t.Fatal("consumer operation failure did not distinguish nil and non-nil errors")
	}
	valid := Acknowledge()
	if invalidConsumerHandlerOutcome(nil, nil, valid) ||
		!invalidConsumerHandlerOutcome(wantErr, nil, valid) ||
		!invalidConsumerHandlerOutcome(nil, wantErr, valid) ||
		!invalidConsumerHandlerOutcome(nil, nil, Settlement{}) {
		t.Fatal("consumer handler outcome collapsed an independent failure condition")
	}
	if !delegatedSettlement(SettlementDelegate) || delegatedSettlement(SettlementAcknowledge) {
		t.Fatal("delegated settlement classification collapsed distinct methods")
	}
}

func TestMutationContractOpenAndPublishAdmissionPredicates(t *testing.T) {
	consumer := &Consumer{}
	producer := &Producer{}
	wantErr := errors.New("failure")
	var missingContext context.Context
	if contextProvided(missingContext) || !contextProvided(context.Background()) {
		t.Fatal("context presence collapsed nil and non-nil contexts")
	}
	if !consumerOpenSucceeded(consumer, nil) || consumerOpenSucceeded(nil, nil) || consumerOpenSucceeded(consumer, wantErr) {
		t.Fatal("consumer open success collapsed an independent result condition")
	}
	if !producerOpenSucceeded(producer, nil) || producerOpenSucceeded(nil, nil) || producerOpenSucceeded(producer, wantErr) {
		t.Fatal("producer open success collapsed an independent result condition")
	}
	producerChannel := newFakeProducerChannel()
	producerResource := &concurrentCountingCloser{}
	if !producerInputsPresent("session", producerChannel, producerResource) ||
		producerInputsPresent("", producerChannel, producerResource) ||
		producerInputsPresent("session", nil, producerResource) ||
		producerInputsPresent("session", producerChannel, nil) {
		t.Fatal("producer input presence collapsed an independent invalid condition")
	}

	if publishFailureRequiresRecovery(nil, false) ||
		!publishFailureRequiresRecovery(wantErr, false) ||
		publishFailureRequiresRecovery(wantErr, true) {
		t.Fatal("publish recovery classification collapsed preflight and transmitted failures")
	}
	if !isPreflightCancellation(context.Canceled, context.Canceled) ||
		isPreflightCancellation(nil, context.Canceled) ||
		isPreflightCancellation(context.Canceled, nil) ||
		isPreflightCancellation(wantErr, context.Canceled) {
		t.Fatal("preflight cancellation collapsed an independent error condition")
	}
	if !producerCapacityAvailable(0, 1) || producerCapacityAvailable(1, 1) {
		t.Fatal("producer capacity did not preserve its exact boundary")
	}
	if nextProducerActive(3) != 4 || previousProducerActive(3) != 2 {
		t.Fatal("producer active accounting did not preserve one-step transitions")
	}
	if !producerDrainComplete(true, 0) || producerDrainComplete(false, 0) || producerDrainComplete(true, 1) {
		t.Fatal("producer drain completion collapsed closed and active states")
	}
	if producerCloseStatePresent(nil) || !producerCloseStatePresent(&producerGenerationClose{}) {
		t.Fatal("producer close state presence collapsed nil and initialized state")
	}
	priority := uint16(1)
	expiration := time.Second
	if priorityPresent(nil) || !priorityPresent(&priority) || expirationPresent(nil) || !expirationPresent(&expiration) {
		t.Fatal("publication pointer presence collapsed omitted and populated values")
	}
	for _, recovered := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			want := !recovered && !closed
			if got := producerRecoveryTerminal(recovered, closed); got != want {
				t.Fatalf("producer recovery terminal(%t,%t) = %t, want %t", recovered, closed, got, want)
			}
		}
	}
	if producerBlockedChanged(false, false) || producerBlockedChanged(true, true) ||
		!producerBlockedChanged(false, true) || !producerBlockedChanged(true, false) {
		t.Fatal("producer blocked transition collapsed changed and unchanged states")
	}
	if queuePriorityPresent(0) || !queuePriorityPresent(1) ||
		queueOverflowPresent("") || !queueOverflowPresent(QueueOverflowRejectPublish) ||
		deadLetterStrategyPresent("") || !deadLetterStrategyPresent(DeadLetterAtLeastOnce) {
		t.Fatal("queue argument presence collapsed omitted and configured values")
	}
	confirms := make(chan amqp.Confirmation, 1)
	confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	producer.eventsContext = context.Background()
	if !producer.runGenerationWith(nil, confirms, nil, nil, newPublishTracker(1), nil, func(*publishTracker, <-chan amqp.Return) bool {
		return false
	}) {
		t.Fatal("failed return draining did not terminate the producer generation")
	}

	for name, test := range map[string]struct {
		maximum    int
		sequence   uint64
		token      string
		registered int
		want       error
	}{
		"valid":         {maximum: 1, sequence: 1, token: "session/1"},
		"zero maximum":  {sequence: 1, token: "session/1", want: ErrInvalidBounds},
		"zero sequence": {maximum: 1, token: "session/1", want: ErrInvalidPublishCorrelation},
		"empty token":   {maximum: 1, sequence: 1, want: ErrInvalidPublishCorrelation},
		"at capacity":   {maximum: 1, sequence: 1, token: "session/1", registered: 1, want: ErrOutstandingConfirmLimit},
	} {
		accepted, err := publishRegistrationAdmission(test.maximum, test.sequence, test.token, test.registered)
		if accepted != (test.want == nil) || !errors.Is(err, test.want) {
			t.Fatalf("%s publish admission = (%t, %v), want %v", name, accepted, err, test.want)
		}
	}
	if !abandonablePublishState(PublishNotSent) || !abandonablePublishState(PublishAmbiguous) ||
		abandonablePublishState(PublishConfirmed) {
		t.Fatal("abandonable publish state collapsed definitive and indeterminate outcomes")
	}
	if validDeliveryTag(0) || !validDeliveryTag(1) {
		t.Fatal("delivery tag validity collapsed zero and broker-assigned tags")
	}
	if _, err := deliveryFromAMQP(amqp.Delivery{}, ConsumerConfig{}); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("invalid delivery config error = %v, want invalid delivery", err)
	}
	if got := nextRecoveryAttempt(3); got != 4 {
		t.Fatalf("next topology attempt = %d, want 4", got)
	}
}

func TestConnectionConfigAcceptsEveryInclusiveBoundary(t *testing.T) {
	provider := CredentialProviderFunc(func(context.Context) (Credentials, error) {
		return Credentials{Username: "worker", Password: []byte("secret")}, nil
	})
	validEndpoint := Endpoint{Host: "rabbitmq.internal", Port: 5671}
	tests := map[string]ConnectionConfig{
		"endpoint count": {
			Endpoints: make([]Endpoint, MaxEndpoints), VirtualHost: "/", Credentials: provider,
			TLS: TLSConfig{ServerName: "rabbitmq.internal"}, DialTimeout: time.Second,
			Heartbeat: minimumHeartbeat,
			Recovery:  RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
		},
		"root count": {
			Endpoints: []Endpoint{validEndpoint}, VirtualHost: "/", Credentials: provider,
			TLS:         TLSConfig{ServerName: "rabbitmq.internal", RootCAs: make([][]byte, MaxRootCAs)},
			DialTimeout: time.Second, Heartbeat: minimumHeartbeat,
			Recovery: RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
		},
		"aggregate TLS bytes": {
			Endpoints: []Endpoint{validEndpoint}, VirtualHost: "/", Credentials: provider,
			TLS: TLSConfig{
				ServerName: "rabbitmq.internal", ClientCertificate: make([]byte, MaxTLSMaterialBytes/4),
				ClientPrivateKey: make([]byte, MaxTLSMaterialBytes/4),
				RootCAs:          [][]byte{make([]byte, MaxTLSMaterialBytes/2)},
			},
			DialTimeout: time.Second, Heartbeat: minimumHeartbeat,
			Recovery: RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
		},
		"maximum time and retry bounds": {
			Endpoints: []Endpoint{validEndpoint}, VirtualHost: "/", Credentials: provider,
			TLS: TLSConfig{ServerName: "rabbitmq.internal"}, DialTimeout: maximumDialTimeout,
			Heartbeat: maximumHeartbeat,
			Recovery: RecoveryPolicy{
				MaxAttempts: MaxReconnectAttempts, InitialDelay: maximumRecoveryDelay, MaxDelay: maximumRecoveryDelay,
			},
		},
	}
	for index := range tests["endpoint count"].Endpoints {
		config := tests["endpoint count"]
		config.Endpoints[index] = validEndpoint
		tests["endpoint count"] = config
	}
	for name, config := range tests {
		name, config := name, config
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := config.Validate(); err != nil {
				t.Fatalf("inclusive boundary rejected: %v", err)
			}
		})
	}
}

func TestConnectionConfigRejectsEveryExclusiveBoundary(t *testing.T) {
	base := testConnectionConfigForMutation()
	tests := map[string]func(*ConnectionConfig){
		"zero dial timeout":  func(config *ConnectionConfig) { config.DialTimeout = 0 },
		"below heartbeat":    func(config *ConnectionConfig) { config.Heartbeat = minimumHeartbeat - time.Nanosecond },
		"zero attempts":      func(config *ConnectionConfig) { config.Recovery.MaxAttempts = 0 },
		"zero initial delay": func(config *ConnectionConfig) { config.Recovery.InitialDelay = 0 },
		"maximum before initial": func(config *ConnectionConfig) {
			config.Recovery.MaxDelay = config.Recovery.InitialDelay - time.Nanosecond
		},
		"aggregate TLS overflow": func(config *ConnectionConfig) {
			config.TLS.ClientCertificate = make([]byte, MaxTLSMaterialBytes/2)
			config.TLS.ClientPrivateKey = make([]byte, MaxTLSMaterialBytes/2)
			config.TLS.RootCAs = [][]byte{{1}}
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := base
			mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidBounds) && name != "aggregate TLS overflow" {
				t.Fatalf("Validate() error = %v, want invalid bounds", err)
			} else if name == "aggregate TLS overflow" && !errors.Is(err, ErrInvalidTLS) {
				t.Fatalf("Validate() error = %v, want invalid TLS", err)
			}
		})
	}
}

func TestPublicationAcceptsExactLimitsAndPrimitiveHeaderAccounting(t *testing.T) {
	limits := DefaultLimits()
	priority := uint16(255)
	for name, header := range map[string]Header{
		"string": StringHeader("key", "value"),
		"bool":   BoolHeader("key", true),
		"int64":  Int64Header("key", 42),
		"bytes":  BytesHeader("key", []byte("value")),
	} {
		name, header := name, header
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			publication := testPublication()
			publication.DeliveryMode = DeliveryTransient
			publication.Message.Body = make([]byte, limits.MaxPayloadBytes)
			publication.Message.Priority = &priority
			publication.Message.Headers = []Header{header}
			local := limits
			local.MaxHeaderEntries = 1
			local.MaxHeaderBytes = len(header.Key)
			switch header.Kind {
			case HeaderString:
				local.MaxHeaderBytes += len(header.String)
			case HeaderBool:
				local.MaxHeaderBytes++
			case HeaderInt64:
				local.MaxHeaderBytes += 8
			case HeaderBytes:
				local.MaxHeaderBytes += len(header.Bytes)
			}
			if err := publication.Validate(local); err != nil {
				t.Fatalf("exact primitive boundary rejected: %v", err)
			}
		})
	}
}

func TestPublicationRejectsZeroForEveryLimit(t *testing.T) {
	tests := map[string]func(*Limits){
		"payload bytes":     func(limits *Limits) { limits.MaxPayloadBytes = 0 },
		"header entries":    func(limits *Limits) { limits.MaxHeaderEntries = 0 },
		"header bytes":      func(limits *Limits) { limits.MaxHeaderBytes = 0 },
		"name bytes":        func(limits *Limits) { limits.MaxNameBytes = 0 },
		"routing key bytes": func(limits *Limits) { limits.MaxRoutingKeyBytes = 0 },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			limits := DefaultLimits()
			mutate(&limits)
			if err := testPublication().Validate(limits); !errors.Is(err, ErrInvalidBounds) {
				t.Fatalf("Validate() error = %v, want invalid bounds", err)
			}
		})
	}
}

func TestPublicationRejectsOneBytePastEveryPrimitiveHeaderBudget(t *testing.T) {
	for name, header := range map[string]Header{
		"string": StringHeader("key", "value"),
		"bool":   BoolHeader("key", true),
		"int64":  Int64Header("key", 42),
		"bytes":  BytesHeader("key", []byte("value")),
	} {
		name, header := name, header
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			publication := testPublication()
			publication.Message.Headers = []Header{header}
			limits := DefaultLimits()
			limits.MaxHeaderBytes = len(header.Key) - 1
			switch header.Kind {
			case HeaderString:
				limits.MaxHeaderBytes += len(header.String)
			case HeaderBool:
				limits.MaxHeaderBytes++
			case HeaderInt64:
				limits.MaxHeaderBytes += 8
			case HeaderBytes:
				limits.MaxHeaderBytes += len(header.Bytes)
			}
			if err := publication.Validate(limits); !errors.Is(err, ErrHeadersTooLarge) {
				t.Fatalf("Validate() error = %v, want headers too large", err)
			}
		})
	}
}

func TestPublicationHeaderBudgetAccumulatesAcrossEntries(t *testing.T) {
	publication := testPublication()
	publication.Message.Headers = []Header{
		StringHeader("first", "one"),
		StringHeader("second", "two"),
	}
	limits := DefaultLimits()
	limits.MaxHeaderBytes = len("first") + len("one") + len("second") + len("two") - 1
	if err := publication.Validate(limits); !errors.Is(err, ErrHeadersTooLarge) {
		t.Fatalf("Validate() error = %v, want cumulative headers too large", err)
	}
}

func TestPublicationIdentityAndControlBoundaries(t *testing.T) {
	limits := DefaultLimits()
	publication := testPublication()
	publication.Exchange = strings.Repeat("e", limits.MaxNameBytes)
	publication.RoutingKey = strings.Repeat("r", limits.MaxRoutingKeyBytes)
	publication.Message.MessageID = strings.Repeat("m", limits.MaxNameBytes)
	if err := publication.Validate(limits); err != nil {
		t.Fatalf("exact identity boundaries rejected: %v", err)
	}
	for _, value := range []string{" ", "~"} {
		if containsControl(value) {
			t.Fatalf("containsControl(%q) = true", value)
		}
	}
	for _, value := range []string{"\x1f", "\x7f"} {
		if !containsControl(value) {
			t.Fatalf("containsControl(%q) = false", value)
		}
	}
}

func TestHealthDistinguishesEachStoppedAndClosedState(t *testing.T) {
	for name, producer := range map[string]*Producer{
		"stopped": {stopped: true},
		"closed":  {closed: true},
	} {
		if got := producer.DependencyHealth(); got != DependencyUnknown {
			t.Fatalf("producer %s health = %q, want unknown", name, got)
		}
	}
	for name, consumer := range map[string]*Consumer{
		"stopped":  {stopped: true},
		"stopping": {stopping: true},
	} {
		if got := consumer.DependencyHealth(); got != DependencyUnknown {
			t.Fatalf("consumer %s health = %q, want unknown", name, got)
		}
	}
}

func TestRecoveryArithmeticPreservesBoundariesAndRotation(t *testing.T) {
	maximum := 10 * time.Second
	for name, test := range map[string]struct {
		current time.Duration
		want    time.Duration
	}{
		"below half doubles":   {current: 4 * time.Second, want: 8 * time.Second},
		"half reaches maximum": {current: 5 * time.Second, want: maximum},
		"above half caps":      {current: 5*time.Second + time.Nanosecond, want: maximum},
	} {
		if got := nextRecoveryDelay(test.current, maximum); got != test.want {
			t.Fatalf("%s delay = %s, want %s", name, got, test.want)
		}
	}
	for name, test := range map[string]struct {
		start, attempt, count, want int
	}{
		"first":   {0, 0, 3, 0},
		"advance": {1, 1, 3, 2},
		"wrap":    {2, 2, 3, 1},
	} {
		if got := recoveryEndpointIndex(test.start, test.attempt, test.count); got != test.want {
			t.Fatalf("%s endpoint = %d, want %d", name, got, test.want)
		}
	}
}

func TestRecoveryDecisionHelpersPreserveEveryIndependentCondition(t *testing.T) {
	valid := Credentials{Username: "worker", Password: []byte("secret")}
	invalid := Credentials{}
	providerErr := errors.New("provider failed")
	for name, test := range map[string]struct {
		credentials Credentials
		err         error
		want        bool
	}{
		"valid":               {valid, nil, true},
		"provider error":      {valid, providerErr, false},
		"invalid credentials": {invalid, nil, false},
		"both invalid":        {invalid, providerErr, false},
	} {
		if got := usableCredentials(test.credentials, test.err); got != test.want {
			t.Fatalf("%s usable credentials = %t, want %t", name, got, test.want)
		}
	}
	consumerResourceChannel := newFakeConsumerChannel()
	producerResourceChannel := newFakeProducerChannel()
	topologyResourceChannel := &fakeTopologyChannel{}
	resource := io.Closer(&countingCloser{})
	for name, test := range map[string]struct {
		consumer consumerChannel
		producer producerChannel
		topology topologyChannel
		resource io.Closer
		err      error
		want     bool
	}{
		"valid":        {consumerResourceChannel, producerResourceChannel, topologyResourceChannel, resource, nil, true},
		"error":        {consumerResourceChannel, producerResourceChannel, topologyResourceChannel, resource, errors.New("dial"), false},
		"nil channel":  {nil, nil, nil, resource, nil, false},
		"nil resource": {consumerResourceChannel, producerResourceChannel, topologyResourceChannel, nil, nil, false},
	} {
		if got := usableConsumerResources(test.consumer, test.resource, test.err); got != test.want {
			t.Fatalf("%s consumer resources = %t, want %t", name, got, test.want)
		}
		if got := usableProducerResources(test.producer, test.resource, test.err); got != test.want {
			t.Fatalf("%s producer resources = %t, want %t", name, got, test.want)
		}
		if got := usableTopologyResources(test.topology, test.resource, test.err); got != test.want {
			t.Fatalf("%s topology resources = %t, want %t", name, got, test.want)
		}
	}
	connection := &fakeAMQPConnection{}
	if !usableAMQPConnection(connection, nil) || usableAMQPConnection(connection, errors.New("dial")) || usableAMQPConnection(nil, nil) {
		t.Fatal("AMQP connection decision collapsed an independent condition")
	}
	for name, test := range map[string]struct {
		attempt, maximum int
		final            bool
		wait             bool
	}{
		"only attempt": {0, 1, true, false},
		"first of two": {0, 2, false, false},
		"last of two":  {1, 2, true, true},
	} {
		if got := finalRecoveryAttempt(test.attempt, test.maximum); got != test.final {
			t.Fatalf("%s final = %t, want %t", name, got, test.final)
		}
		if got := shouldWaitForRecovery(test.attempt); got != test.wait {
			t.Fatalf("%s wait = %t, want %t", name, got, test.wait)
		}
	}
	for name, test := range map[string]struct {
		session string
		err     error
		want    bool
	}{
		"valid session":    {"session", nil, true},
		"generation error": {"session", errors.New("failed"), false},
		"invalid session":  {"bad\nsession", nil, false},
		"both invalid":     {"bad\nsession", errors.New("failed"), false},
	} {
		if got := usableProducerSession(test.session, test.err); got != test.want {
			t.Fatalf("%s usable producer session = %t, want %t", name, got, test.want)
		}
	}
}

func TestConsumerOpeningAttemptsEveryConfiguredEndpointExactlyOncePerRotation(t *testing.T) {
	connection := testConnectionConfig()
	connection.Endpoints = []Endpoint{{Host: "one", Port: 5671}, {Host: "two", Port: 5671}}
	connection.Recovery = RecoveryPolicy{MaxAttempts: 3, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
	var endpoints []string
	handler := DeliveryHandler(func(context.Context, Delivery) (Settlement, error) { return Acknowledge(), nil })
	consumer, err := openConsumerWith(t.Context(), connection, testConsumerConfig(), handler, func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, _ Credentials) (consumerChannel, io.Closer, error) {
		endpoints = append(endpoints, endpoint.Host)
		return nil, nil, errors.New("unavailable")
	})
	if consumer != nil || !errors.Is(err, ErrConsumerUnavailable) {
		t.Fatalf("openConsumerWith() = (%#v, %v)", consumer, err)
	}
	if got := strings.Join(endpoints, ","); got != "one,two,one" {
		t.Fatalf("endpoint rotation = %q", got)
	}
}

func TestConsumerRuntimeRecoveryExhaustsAndRotatesEveryAttempt(t *testing.T) {
	connection := testConnectionConfig()
	connection.Endpoints = []Endpoint{{Host: "one", Port: 5671}, {Host: "two", Port: 5671}}
	connection.Recovery = RecoveryPolicy{MaxAttempts: 3, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
	var endpoints []string
	consumer, stop := newMutationRecoveryConsumer(connection, 1, func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, _ Credentials) (consumerChannel, io.Closer, error) {
		endpoints = append(endpoints, endpoint.Host)
		return nil, nil, errors.New("unavailable")
	})
	defer stop()
	if generation, ok := consumer.recoverRuntime(); generation != nil || ok {
		t.Fatalf("recoverRuntime() = (%#v, %t)", generation, ok)
	}
	if got := strings.Join(endpoints, ","); got != "two,one,two" {
		t.Fatalf("runtime endpoint rotation = %q", got)
	}
}

func TestConsumerRuntimeRecoveryContinuesAfterCredentialAndSetupFailures(t *testing.T) {
	for name, configure := range map[string]func(*ConnectionConfig, *int, *int, *consumerDialFunc){
		"credentials": func(connection *ConnectionConfig, providerCalls, _ *int, _ *consumerDialFunc) {
			connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
				*providerCalls++
				if *providerCalls == 1 {
					return Credentials{}, nil
				}
				return Credentials{Username: "worker", Password: []byte("secret")}, nil
			})
		},
		"setup": func(_ *ConnectionConfig, _ *int, dialCalls *int, dial *consumerDialFunc) {
			*dial = func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
				*dialCalls++
				channel := newFakeConsumerChannel()
				if *dialCalls == 1 {
					channel.qosErr = errors.New("setup failed")
				}
				return channel, &countingCloser{}, nil
			}
		},
	} {
		name, configure := name, configure
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			connection := testConnectionConfig()
			connection.Endpoints = []Endpoint{{Host: "one", Port: 5671}, {Host: "two", Port: 5671}}
			connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
			providerCalls := 0
			dialCalls := 0
			dial := consumerDialFunc(func(context.Context, Endpoint, ConnectionConfig, Credentials) (consumerChannel, io.Closer, error) {
				dialCalls++
				return newFakeConsumerChannel(), &countingCloser{}, nil
			})
			configure(&connection, &providerCalls, &dialCalls, &dial)
			consumer, stop := newMutationRecoveryConsumer(connection, 0, dial)
			defer stop()
			generation, ok := consumer.recoverRuntime()
			if !ok || generation == nil {
				t.Fatalf("recoverRuntime() = (%#v, %t)", generation, ok)
			}
			if err := consumer.closeGeneration(generation, time.Now().Add(time.Second)); err != nil {
				t.Fatalf("close recovered generation: %v", err)
			}
			if consumer.recovery.nextEndpoint != 2 {
				t.Fatalf("next endpoint = %d, want 2", consumer.recovery.nextEndpoint)
			}
		})
	}
}

func TestAMQPConsumerDialSeparatesConfigurationFromOpening(t *testing.T) {
	connection := testConnectionConfig()
	credentials := Credentials{Username: "worker", Password: []byte("secret")}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	called := false
	channel := newFakeConsumerChannel()
	resource := &countingCloser{}
	gotChannel, gotResource, err := dialAMQPConsumerWith(ctx, connection.Endpoints[0], connection, credentials, func(string, amqp.Config, time.Time) (consumerChannel, io.Closer, error) {
		called = true
		return channel, resource, nil
	})
	if err != nil || gotChannel != channel || gotResource != resource || !called {
		t.Fatalf("valid dial boundary = (%#v, %#v, %v, called %t)", gotChannel, gotResource, err, called)
	}
	called = false
	if gotChannel, gotResource, err := dialAMQPConsumerWith(context.Background(), connection.Endpoints[0], connection, credentials, func(string, amqp.Config, time.Time) (consumerChannel, io.Closer, error) {
		called = true
		return channel, resource, nil
	}); gotChannel != nil || gotResource != nil || !errors.Is(err, ErrConsumerUnavailable) || called {
		t.Fatalf("invalid dial boundary = (%#v, %#v, %v, called %t)", gotChannel, gotResource, err, called)
	}
}

func newMutationRecoveryConsumer(connection ConnectionConfig, nextEndpoint int, dial consumerDialFunc) (*Consumer, context.CancelFunc) {
	recoveryContext, stopRecovery := context.WithCancel(context.Background())
	return &Consumer{
		config: testConsumerConfig(), recoveryContext: recoveryContext, stopRecovery: stopRecovery,
		observations: newObservationStream(ObservationConsumer, observationBufferSize),
		recovery:     &consumerRecovery{connection: connection, dial: dial, nextEndpoint: nextEndpoint},
	}, stopRecovery
}

func TestProducerRuntimeRecoveryExhaustsAndRotatesEveryAttempt(t *testing.T) {
	connection := testConnectionConfig()
	connection.Endpoints = []Endpoint{{Host: "one", Port: 5671}, {Host: "two", Port: 5671}}
	connection.Recovery = RecoveryPolicy{MaxAttempts: 3, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
	var endpoints []string
	producer, stop := newMutationRecoveryProducer(connection, 1, func() (string, error) { return "session", nil }, func(_ context.Context, endpoint Endpoint, _ ConnectionConfig, _ Credentials) (producerChannel, io.Closer, error) {
		endpoints = append(endpoints, endpoint.Host)
		return nil, nil, errors.New("unavailable")
	})
	defer stop()
	if producer.recoverRuntime() {
		t.Fatal("exhausted runtime recovery succeeded")
	}
	if got := strings.Join(endpoints, ","); got != "two,one,two" {
		t.Fatalf("runtime endpoint rotation = %q", got)
	}
}

func TestProducerRuntimeRecoveryContinuesAfterInvalidCredentials(t *testing.T) {
	connection := testConnectionConfig()
	connection.Endpoints = []Endpoint{{Host: "one", Port: 5671}, {Host: "two", Port: 5671}}
	connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
	providerCalls := 0
	connection.Credentials = CredentialProviderFunc(func(context.Context) (Credentials, error) {
		providerCalls++
		if providerCalls == 1 {
			return Credentials{}, nil
		}
		return Credentials{Username: "worker", Password: []byte("secret")}, nil
	})
	sessionCalls := 0
	dialCalls := 0
	channel := newFakeProducerChannel()
	resource := &countingCloser{}
	producer, stop := newMutationRecoveryProducer(connection, 0, func() (string, error) {
		sessionCalls++
		return "session", nil
	}, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
		dialCalls++
		return channel, resource, nil
	})
	defer stop()
	if !producer.recoverRuntime() {
		t.Fatal("recovery did not continue after invalid credentials")
	}
	if providerCalls != 2 || sessionCalls != 1 || dialCalls != 1 || producer.recovery.nextEndpoint != 2 {
		t.Fatalf("recovery calls = providers %d sessions %d dials %d next %d", providerCalls, sessionCalls, dialCalls, producer.recovery.nextEndpoint)
	}
	closeMutationProducerGeneration(t, producer)
}

func TestProducerRuntimeRecoveryRejectsEachInvalidSessionCondition(t *testing.T) {
	for name, first := range map[string]func() (string, error){
		"generation error": func() (string, error) { return "valid-session", errors.New("generation failed") },
		"invalid identity": func() (string, error) { return "bad\nsession", nil },
	} {
		name, first := name, first
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			connection := testConnectionConfig()
			connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
			sessionCalls := 0
			dialCalls := 0
			producer, stop := newMutationRecoveryProducer(connection, 0, func() (string, error) {
				sessionCalls++
				if sessionCalls == 1 {
					return first()
				}
				return "valid-session", nil
			}, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
				dialCalls++
				return newFakeProducerChannel(), &countingCloser{}, nil
			})
			defer stop()
			if !producer.recoverRuntime() {
				t.Fatal("recovery did not continue after invalid session")
			}
			if sessionCalls != 2 || dialCalls != 1 {
				t.Fatalf("recovery calls = sessions %d dials %d", sessionCalls, dialCalls)
			}
			closeMutationProducerGeneration(t, producer)
		})
	}
}

func TestProducerRuntimeRecoveryContinuesAfterSetupFailure(t *testing.T) {
	connection := testConnectionConfig()
	connection.Recovery = RecoveryPolicy{MaxAttempts: 2, InitialDelay: time.Nanosecond, MaxDelay: 2 * time.Nanosecond}
	dialCalls := 0
	producer, stop := newMutationRecoveryProducer(connection, 0, func() (string, error) { return "session", nil }, func(context.Context, Endpoint, ConnectionConfig, Credentials) (producerChannel, io.Closer, error) {
		dialCalls++
		channel := newFakeProducerChannel()
		if dialCalls == 1 {
			channel.confirmErr = errors.New("setup failed")
		}
		return channel, &countingCloser{}, nil
	})
	defer stop()
	if !producer.recoverRuntime() || dialCalls != 2 {
		t.Fatalf("runtime setup recovery = %t after %d dials", producer.channel != nil, dialCalls)
	}
	closeMutationProducerGeneration(t, producer)
}

func TestCredentialValidationPreservesEveryIndependentBoundary(t *testing.T) {
	for name, test := range map[string]struct {
		credentials Credentials
		want        bool
	}{
		"valid":             {Credentials{Username: "worker", Password: []byte("secret")}, true},
		"exact boundaries":  {Credentials{Username: strings.Repeat("u", 255), Password: make([]byte, maxCredentialBytes)}, true},
		"empty password":    {Credentials{Username: "worker"}, false},
		"oversize password": {Credentials{Username: "worker", Password: make([]byte, maxCredentialBytes+1)}, false},
		"oversize username": {Credentials{Username: strings.Repeat("u", 256), Password: []byte("secret")}, false},
		"control username":  {Credentials{Username: "bad\nuser", Password: []byte("secret")}, false},
	} {
		if got := validCredentials(test.credentials); got != test.want {
			t.Fatalf("%s credentials = %t, want %t", name, got, test.want)
		}
	}
}

func newMutationRecoveryProducer(connection ConnectionConfig, nextEndpoint int, session func() (string, error), dial producerDialFunc) (*Producer, context.CancelFunc) {
	eventsContext, stopEvents := context.WithCancel(context.Background())
	return &Producer{
		config: testProducerConfig(), eventsContext: eventsContext, stopEvents: stopEvents,
		observations: newObservationStream(ObservationProducer, observationBufferSize),
		recovery: &producerRecovery{
			connection: connection, session: session, dial: dial, nextEndpoint: nextEndpoint,
		},
	}, stopEvents
}

func closeMutationProducerGeneration(t *testing.T, producer *Producer) {
	t.Helper()
	if err := closeProducerGeneration(producer.channel, producer.resource, producer.generationClose, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("close recovered producer generation: %v", err)
	}
}

func TestLifecycleDecisionHelpersPreserveEveryIndependentCondition(t *testing.T) {
	for name, test := range map[string]struct {
		additional, used, maximum int
		want                      bool
	}{
		"below budget": {2, 2, 5, true},
		"exact budget": {3, 2, 5, true},
		"over budget":  {4, 2, 5, false},
	} {
		if got := fitsRemainingHeaderBudget(test.additional, test.used, test.maximum); got != test.want {
			t.Fatalf("%s header budget = %t, want %t", name, got, test.want)
		}
	}
	for name, test := range map[string]struct {
		closed bool
		active int
		want   bool
	}{
		"complete": {true, 0, true},
		"open":     {false, 0, false},
		"active":   {true, 1, false},
	} {
		if got := producerDrainComplete(test.closed, test.active); got != test.want {
			t.Fatalf("%s producer drain = %t, want %t", name, got, test.want)
		}
	}
	for duration, want := range map[time.Duration]bool{-time.Nanosecond: false, 0: false, time.Nanosecond: true} {
		if got := positiveDuration(duration); got != want {
			t.Fatalf("positiveDuration(%s) = %t, want %t", duration, got, want)
		}
	}

	for name, test := range map[string]struct {
		deliveriesClosed, paused bool
		pending, capacity        int
		want                     bool
	}{
		"deliveries closed": {true, false, 0, 2, true},
		"pending admission": {false, false, 1, 2, true},
		"paused pending":    {false, true, 1, 2, false},
		"capacity reached":  {false, true, 2, 2, true},
		"open and empty":    {false, false, 0, 2, false},
	} {
		if got := suspendConsumerDeliveries(test.deliveriesClosed, test.paused, test.pending, test.capacity); got != test.want {
			t.Fatalf("%s delivery suspension = %t, want %t", name, got, test.want)
		}
	}
	for name, test := range map[string]struct {
		draining, deliveriesClosed bool
		pending                    int
		want                       bool
	}{
		"complete":        {true, true, 0, true},
		"not draining":    {false, true, 0, false},
		"deliveries open": {true, false, 0, false},
		"pending":         {true, true, 1, false},
	} {
		if got := consumerDrainComplete(test.draining, test.deliveriesClosed, test.pending); got != test.want {
			t.Fatalf("%s consumer drain = %t, want %t", name, got, test.want)
		}
	}
	if hasDeadLetterHistory(nil) || !hasDeadLetterHistory([]Death{{Count: 1}}) {
		t.Fatal("dead-letter history did not distinguish empty and populated records")
	}
	for name, test := range map[string]struct {
		pending, admitted, prefetch int
		want                        bool
	}{
		"below": {1, 1, 3, false},
		"exact": {1, 2, 3, true},
		"above": {2, 2, 3, true},
	} {
		if got := consumerBacklogFull(test.pending, test.admitted, test.prefetch); got != test.want {
			t.Fatalf("%s consumer backlog = %t, want %t", name, got, test.want)
		}
	}
	if !matchingConsumerCancellation("consumer", "consumer") || matchingConsumerCancellation("other", "consumer") {
		t.Fatal("consumer cancellation matching collapsed distinct tags")
	}
	if !isAcknowledgement(SettlementAcknowledge) || isAcknowledgement(SettlementReject) || isAcknowledgement(SettlementNegativeAcknowledge) {
		t.Fatal("acknowledgement classification collapsed settlement methods")
	}
}

func TestConsumerStatePredicatesPreserveEveryIndependentCondition(t *testing.T) {
	values := []bool{false, true}
	for _, current := range values {
		for _, stopping := range values {
			for _, terminalSet := range values {
				for _, recovering := range values {
					var terminal error
					if terminalSet {
						terminal = errors.New("terminal")
					}
					want := current && !stopping && !terminalSet && !recovering
					if got := consumerRecoveryAllowed(current, stopping, terminal, recovering); got != want {
						t.Fatalf("recoveryAllowed(%t,%t,%t,%t) = %t, want %t", current, stopping, terminalSet, recovering, got, want)
					}
				}
			}
		}
	}
	for _, current := range values {
		for _, stopping := range values {
			if got, want := consumerGenerationCanSignal(current, stopping), current && !stopping; got != want {
				t.Fatalf("generationCanSignal(%t,%t) = %t, want %t", current, stopping, got, want)
			}
		}
	}
	for _, stopping := range values {
		for _, stopped := range values {
			for _, terminal := range values {
				if got, want := consumerLifecycleClosed(stopping, stopped, terminal), stopping || stopped || terminal; got != want {
					t.Fatalf("lifecycleClosed(%t,%t,%t) = %t, want %t", stopping, stopped, terminal, got, want)
				}
			}
		}
	}
	for _, closed := range values {
		if got, want := consumerGenerationCanCancel(closed), !closed; got != want {
			t.Fatalf("generationCanCancel(%t) = %t, want %t", closed, got, want)
		}
	}
	if !consumerTerminalUnset(nil) || consumerTerminalUnset(errors.New("terminal")) {
		t.Fatal("terminal error state did not distinguish unset and set errors")
	}
}

func TestPendingCancellationObservationRequiresOpenMatchingTag(t *testing.T) {
	consumer := &Consumer{
		config:       ConsumerConfig{Name: "consumer"},
		observations: newObservationStream(ObservationConsumer, observationBufferSize),
	}
	closed := make(chan string)
	close(closed)
	consumer.observePendingCancellation(closed)
	unmatched := make(chan string, 1)
	unmatched <- "other"
	consumer.observePendingCancellation(unmatched)
	select {
	case observation := <-consumer.observations.channel:
		t.Fatalf("unexpected cancellation observation: %#v", observation)
	default:
	}
	matched := make(chan string, 1)
	matched <- "consumer"
	consumer.observePendingCancellation(matched)
	select {
	case observation := <-consumer.observations.channel:
		if observation.Kind != ObservationConsumerCancellation || observation.Outcome != ObservationCancelled {
			t.Fatalf("matching cancellation observation = %#v", observation)
		}
	default:
		t.Fatal("matching cancellation was not observed")
	}
}

func TestConsumerCancellationSkipsAbsentAndClosedGenerations(t *testing.T) {
	consumer := &Consumer{}
	if err := consumer.cancelGeneration(t.Context(), nil); err != nil {
		t.Fatalf("cancel absent generation: %v", err)
	}
	generation := &consumerGeneration{channel: newFakeConsumerChannel()}
	generation.closed.Store(true)
	if err := consumer.cancelGeneration(t.Context(), generation); err != nil {
		t.Fatalf("cancel closed generation: %v", err)
	}
	if generation.channel.(*fakeConsumerChannel).cancelCount() != 0 {
		t.Fatal("closed generation was cancelled again")
	}
}

func TestConsumerGenerationFailureSignalRemainsBounded(t *testing.T) {
	generation := &consumerGeneration{failure: make(chan struct{}, 1)}
	generation.failure <- struct{}{}
	consumer := &Consumer{
		generation:   generation,
		observations: newObservationStream(ObservationConsumer, observationBufferSize),
	}
	consumer.failGeneration(generation)
	if !consumer.recovering || len(generation.failure) != 1 {
		t.Fatalf("bounded generation failure = recovering %t signals %d", consumer.recovering, len(generation.failure))
	}
}

func TestNonblockingLifecycleChannelsPreserveEmptyAndFullStates(t *testing.T) {
	failure := make(chan struct{}, 1)
	if !signalProducerFailure(failure) || signalProducerFailure(failure) || len(failure) != 1 {
		t.Fatalf("bounded producer failure signals = %d", len(failure))
	}

	blocked := make(chan ConnectionBlockedState, 1)
	state := ConnectionBlockedState{Active: true}
	if !offerBlockedState(blocked, state) || offerBlockedState(blocked, ConnectionBlockedState{}) {
		t.Fatal("blocked state offer did not distinguish empty and full channels")
	}
	if got, ok := takeBlockedState(blocked); !ok || got != state {
		t.Fatalf("take blocked state = (%#v, %t)", got, ok)
	}
	if got, ok := takeBlockedState(blocked); ok || got != (ConnectionBlockedState{}) {
		t.Fatalf("take empty blocked state = (%#v, %t)", got, ok)
	}

	observations := make(chan Observation, 1)
	want := Observation{Dropped: 3}
	observations <- want
	if got, ok := takeObservation(observations); !ok || got != want {
		t.Fatalf("take observation = (%#v, %t)", got, ok)
	}
	if got, ok := takeObservation(observations); ok || got != (Observation{}) {
		t.Fatalf("take empty observation = (%#v, %t)", got, ok)
	}
}

func TestDeliveryDeathFieldPredicatesPreserveEveryIndependentRule(t *testing.T) {
	limits := DefaultLimits()
	for name, test := range map[string]struct {
		value      any
		allowEmpty bool
		want       bool
	}{
		"valid":          {"queue", false, true},
		"exact length":   {strings.Repeat("q", limits.MaxNameBytes), false, true},
		"allowed empty":  {"", true, true},
		"required empty": {"", false, false},
		"wrong type":     {true, false, false},
		"over length":    {strings.Repeat("q", limits.MaxNameBytes+1), false, false},
		"control":        {"bad\nqueue", false, false},
	} {
		text, ok := validDeathSummaryField(test.value, test.allowEmpty, limits)
		if ok != test.want || (ok && text != test.value) {
			t.Fatalf("%s death summary field = (%q, %t), want valid %t", name, text, ok, test.want)
		}
	}

	type deathFields struct {
		reason, queue, exchange       string
		reasonOK, queueOK, exchangeOK bool
		deathTime                     time.Time
		timeOK                        bool
	}
	valid := deathFields{
		reason: "rejected", queue: "orders", exchange: "events",
		reasonOK: true, queueOK: true, exchangeOK: true,
		deathTime: time.Unix(0, 0), timeOK: true,
	}
	tests := map[string]deathFields{
		"valid":            valid,
		"missing reason":   func() deathFields { value := valid; value.reasonOK = false; return value }(),
		"invalid reason":   func() deathFields { value := valid; value.reason = ""; return value }(),
		"missing queue":    func() deathFields { value := valid; value.queueOK = false; return value }(),
		"invalid queue":    func() deathFields { value := valid; value.queue = ""; return value }(),
		"missing exchange": func() deathFields { value := valid; value.exchangeOK = false; return value }(),
		"oversize exchange": func() deathFields {
			value := valid
			value.exchange = strings.Repeat("e", limits.MaxNameBytes+1)
			return value
		}(),
		"control exchange": func() deathFields { value := valid; value.exchange = "bad\nexchange"; return value }(),
		"missing time":     func() deathFields { value := valid; value.timeOK = false; return value }(),
		"negative time":    func() deathFields { value := valid; value.deathTime = time.Unix(-1, 0); return value }(),
	}
	for name, test := range tests {
		want := name == "valid"
		if got := validDeliveryDeathFields(
			test.reason, test.reasonOK, test.queue, test.queueOK, test.exchange, test.exchangeOK,
			test.deathTime, test.timeOK, limits,
		); got != want {
			t.Fatalf("%s delivery death fields = %t, want %t", name, got, want)
		}
	}
	if got, want := deliveryDeathMetadataBytes("why", "queue", "exchange", 7), 16+3+5+8+7; got != want {
		t.Fatalf("delivery death metadata bytes = %d, want %d", got, want)
	}
}

func TestConfirmationLatencyEligibilityPreservesEveryPublishState(t *testing.T) {
	started := time.Now().Add(-time.Millisecond)
	for _, state := range []PublishState{PublishConfirmed, PublishRejected, PublishReturned} {
		if !shouldObserveConfirmationLatency(started, state) {
			t.Fatalf("eligible state %q rejected", state)
		}
		if shouldObserveConfirmationLatency(time.Time{}, state) {
			t.Fatalf("state %q accepted without a start time", state)
		}
	}
	for _, state := range []PublishState{PublishNotSent, PublishAmbiguous, PublishState("unknown")} {
		if shouldObserveConfirmationLatency(started, state) {
			t.Fatalf("ineligible state %q accepted", state)
		}
	}
}

func TestProducerAdmissionPredicatesPreserveEveryIndependentCondition(t *testing.T) {
	for size, want := range map[int]bool{
		0: false, 1: true, MaxPublishBatchSize: true, MaxPublishBatchSize + 1: false,
	} {
		if got := validPublishBatchSize(size); got != want {
			t.Fatalf("validPublishBatchSize(%d) = %t, want %t", size, got, want)
		}
	}
	cancelled := context.Canceled
	other := errors.New("other")
	for name, test := range map[string]struct {
		err, contextErr error
		want            bool
	}{
		"matching":              {cancelled, cancelled, true},
		"wrapped matching":      {errors.Join(other, cancelled), cancelled, true},
		"missing send error":    {nil, cancelled, false},
		"missing context error": {cancelled, nil, false},
		"different error":       {other, cancelled, false},
	} {
		if got := isPreflightCancellation(test.err, test.contextErr); got != test.want {
			t.Fatalf("%s preflight cancellation = %t, want %t", name, got, test.want)
		}
	}
	for _, closed := range []bool{false, true} {
		for _, recovering := range []bool{false, true} {
			want := !closed && !recovering
			if got := producerRecoveryAllowed(closed, recovering); got != want {
				t.Fatalf("producerRecoveryAllowed(%t,%t) = %t, want %t", closed, recovering, got, want)
			}
		}
	}
	for name, test := range map[string]struct {
		outstanding int
		timeout     time.Duration
		want        bool
	}{
		"minimum":          {1, time.Nanosecond, true},
		"maximum":          {MaxOutstandingConfirms, maximumDialTimeout, true},
		"zero outstanding": {0, time.Second, false},
		"too many":         {MaxOutstandingConfirms + 1, time.Second, false},
		"zero timeout":     {1, 0, false},
		"long timeout":     {1, maximumDialTimeout + time.Nanosecond, false},
	} {
		if got := validProducerBounds(test.outstanding, test.timeout); got != test.want {
			t.Fatalf("%s producer bounds = %t, want %t", name, got, test.want)
		}
	}
	for name, test := range map[string]struct {
		reason string
		want   string
	}{
		"normal":       {"returned", "returned"},
		"exact length": {strings.Repeat("r", 255), strings.Repeat("r", 255)},
		"over length":  {strings.Repeat("r", 256), ""},
		"control":      {"bad\nreason", ""},
	} {
		if got := sanitizedReturnReason(test.reason); got != test.want {
			t.Fatalf("%s sanitized return reason = %q, want %q", name, got, test.want)
		}
	}
	channel := newFakeProducerChannel()
	if !usableAMQPChannel(channel, nil) || usableAMQPChannel(nil, nil) || usableAMQPChannel(channel, errors.New("open")) {
		t.Fatal("AMQP channel usability collapsed independent channel and error states")
	}
}

func TestQuorumRejectDeliveryCountRequiresEveryCondition(t *testing.T) {
	count := uint64(0)
	base := testConsumerConfig()
	base.MaxRequeues = 1
	for name, test := range map[string]struct {
		queue      QueueType
		settlement Settlement
		delivery   Delivery
		want       bool
	}{
		"all conditions": {QueueQuorum, Reject(true), Delivery{Redelivered: true, DeliveryCount: &count}, true},
		"classic queue":  {QueueClassic, Reject(true), Delivery{Redelivered: true, DeliveryCount: &count}, false},
		"nack method":    {QueueQuorum, NegativeAcknowledge(true), Delivery{Redelivered: true, DeliveryCount: &count}, false},
		"missing count":  {QueueQuorum, Reject(true), Delivery{Redelivered: true}, false},
	} {
		config := base
		config.Queue.Type = test.queue
		if got := boundedSettlement(test.delivery, test.settlement, config).Requeue; got != test.want {
			t.Fatalf("%s requeue = %t, want %t", name, got, test.want)
		}
	}
}

func TestConsumerConfigAcceptsExactResourceBounds(t *testing.T) {
	config := testConsumerConfig()
	config.Prefetch = MaxConsumerPrefetch
	config.Concurrency = MaxConsumerConcurrency
	config.HandlerTimeout = maximumDialTimeout
	config.MaxRequeues = MaxConsumerRequeues
	if err := config.Validate(); err != nil {
		t.Fatalf("exact consumer bounds rejected: %v", err)
	}
	config.Prefetch = 1
	config.Concurrency = 1
	config.HandlerTimeout = time.Nanosecond
	if err := config.Validate(); err != nil {
		t.Fatalf("minimum consumer bounds rejected: %v", err)
	}
}

func TestOwnedConsumerConfigDetachesEveryNestedValue(t *testing.T) {
	priority := int32(7)
	bytes := []byte("value")
	config := testConsumerConfig()
	config.Priority = &priority
	config.Queue = QueueReference{Type: QueueClassic, Transient: &TransientQueue{
		Exchange:  Exchange{Name: "events", Kind: ExchangeHeaders},
		Arguments: []Header{StringHeader("first", "one"), BytesHeader("second", bytes)},
	}}
	owned := ownConsumerConfig(config)
	priority = 9
	bytes[0] = 'X'
	config.Queue.Transient.Arguments[0].String = "changed"
	config.Queue.Transient.Arguments[1].Bytes[1] = 'Y'
	if *owned.Priority != 7 || owned.Queue.Transient.Arguments[0].String != "one" || string(owned.Queue.Transient.Arguments[1].Bytes) != "value" {
		t.Fatalf("owned consumer config retained caller aliases: %#v", owned)
	}
}

func TestDeliveryAcceptsExactTransportAndMetadataBounds(t *testing.T) {
	config := testConsumerConfig()
	config.Name = "worker"
	config.Queue.Name = "orders"
	config.Limits.MaxNameBytes = 8
	config.Limits.MaxRoutingKeyBytes = 8
	config.Limits.MaxPayloadBytes = 4
	config.Limits.MaxHeaderBytes = 8
	source := testAMQPDelivery(1)
	source.ConsumerTag = "consumer"
	source.Exchange = "exchange"
	source.RoutingKey = "routing1"
	source.Body = []byte("body")
	source.MessageId = "message1"
	source.CorrelationId = ""
	source.Headers = amqp.Table{acquiredCountHeader: uint64(1)}
	delivery, err := deliveryFromAMQP(source, config)
	if err != nil {
		t.Fatalf("exact delivery bounds rejected: %v", err)
	}
	if delivery.AcquiredCount == nil || *delivery.AcquiredCount != 1 || string(delivery.Body) != "body" {
		t.Fatalf("delivery metadata = %#v", delivery)
	}
	source.Body[0] = 'X'
	if string(delivery.Body) != "body" {
		t.Fatal("delivery body retained the broker buffer")
	}
}

func TestDeliveryRejectsOneBytePastTransportAndMetadataBounds(t *testing.T) {
	baseConfig := testConsumerConfig()
	baseSource := testAMQPDelivery(1)
	tests := map[string]func(*amqp.Delivery, *ConsumerConfig){
		"routing key": func(source *amqp.Delivery, config *ConsumerConfig) {
			config.Limits.MaxRoutingKeyBytes = len(source.RoutingKey) - 1
		},
		"exchange": func(source *amqp.Delivery, config *ConsumerConfig) {
			config.Limits.MaxNameBytes = len(source.Exchange) - 1
			config.Name = strings.Repeat("n", config.Limits.MaxNameBytes)
			config.Queue.Name = strings.Repeat("q", config.Limits.MaxNameBytes)
		},
		"payload": func(source *amqp.Delivery, config *ConsumerConfig) {
			config.Limits.MaxPayloadBytes = len(source.Body) - 1
		},
		"counter metadata": func(source *amqp.Delivery, config *ConsumerConfig) {
			source.Headers = amqp.Table{acquiredCountHeader: uint64(1)}
			config.Limits.MaxHeaderBytes = 7
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := baseConfig
			source := baseSource
			mutate(&source, &config)
			if _, err := deliveryFromAMQP(source, config); !errors.Is(err, ErrInvalidDelivery) {
				t.Fatalf("deliveryFromAMQP() error = %v, want invalid delivery", err)
			}
		})
	}
}

func TestDeliveryHeaderBudgetAccumulatesAcrossEntries(t *testing.T) {
	table := amqp.Table{"first": "one", "second": "two"}
	limits := DefaultLimits()
	wantBytes := len("first") + len("one") + len("second") + len("two")
	limits.MaxHeaderBytes = wantBytes
	headers, gotBytes, err := deliveryHeaders(table, limits)
	if err != nil || len(headers) != 2 || gotBytes != wantBytes {
		t.Fatalf("deliveryHeaders() = (%#v, %d, %v), want two headers and %d bytes", headers, gotBytes, err, wantBytes)
	}
	limits.MaxHeaderBytes--
	if _, _, err := deliveryHeaders(table, limits); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("deliveryHeaders() error = %v, want cumulative overflow", err)
	}
}

func TestDeliveryMetadataBudgetCombinesHeadersSummariesAndCounters(t *testing.T) {
	config := testConsumerConfig()
	source := testAMQPDelivery(1)
	source.Headers = amqp.Table{
		"h":                   "v",
		firstDeathQueueHeader: "abc",
		deliveryCountHeader:   uint64(1),
	}
	config.Limits.MaxHeaderBytes = 2 + 3 + 8
	if _, err := deliveryFromAMQP(source, config); err != nil {
		t.Fatalf("exact combined metadata budget rejected: %v", err)
	}
	config.Limits.MaxHeaderBytes--
	if _, err := deliveryFromAMQP(source, config); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("combined metadata overflow error = %v", err)
	}
}

func TestDeliveryCounterLoopExaminesTheSecondCounter(t *testing.T) {
	config := testConsumerConfig()
	config.Limits.MaxHeaderBytes = 7
	source := testAMQPDelivery(1)
	source.Headers = amqp.Table{deliveryCountHeader: uint64(1)}
	if _, err := deliveryFromAMQP(source, config); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("second counter overflow error = %v", err)
	}
}

func TestDeliveryExpirationAcceptsTheLargestEncodableMilliseconds(t *testing.T) {
	maximum := uint64((time.Duration(1<<63 - 1)) / time.Millisecond)
	expiration, err := parseDeliveryExpiration(fmt.Sprint(maximum))
	if err != nil || expiration == nil || *expiration != time.Duration(maximum)*time.Millisecond {
		t.Fatalf("maximum expiration = (%v, %v)", expiration, err)
	}
	if expiration, err := parseDeliveryExpiration(fmt.Sprint(maximum + 1)); expiration != nil || !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("overflow expiration = (%v, %v)", expiration, err)
	}
}

func TestDeliveryDeathSummaryFieldRulesRemainIndependent(t *testing.T) {
	limits := DefaultLimits()
	for _, key := range []string{firstDeathExchangeHeader, lastDeathExchangeHeader} {
		if bytes, err := deliveryDeathSummaryBytes(amqp.Table{key: ""}, limits); err != nil || bytes != 0 {
			t.Fatalf("empty %s summary = (%d, %v)", key, bytes, err)
		}
	}
	for name, table := range map[string]amqp.Table{
		"wrong type":     {firstDeathQueueHeader: true},
		"required empty": {firstDeathQueueHeader: ""},
		"over length":    {firstDeathQueueHeader: strings.Repeat("q", limits.MaxNameBytes+1)},
		"control":        {firstDeathQueueHeader: "bad\nqueue"},
	} {
		if _, err := deliveryDeathSummaryBytes(table, limits); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatalf("%s summary error = %v", name, err)
		}
	}
	table := amqp.Table{firstDeathQueueHeader: "first", lastDeathQueueHeader: "second"}
	want := len("first") + len("second")
	limits.MaxHeaderBytes = want
	if bytes, err := deliveryDeathSummaryBytes(table, limits); err != nil || bytes != want {
		t.Fatalf("exact summary budget = (%d, %v), want %d", bytes, err, want)
	}
	limits.MaxHeaderBytes--
	if _, err := deliveryDeathSummaryBytes(table, limits); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("cumulative summary error = %v", err)
	}
}

func TestDeliveryDeathBudgetsAcceptExactInclusiveLimits(t *testing.T) {
	limits := DefaultLimits()
	death := validDeathTable()
	death["time"] = time.Unix(0, 0)
	death["routing-keys"] = make([]any, MaxDeathRoutingKeys)
	for index := 0; index < MaxDeathRoutingKeys; index++ {
		death["routing-keys"].([]any)[index] = strings.Repeat("r", limits.MaxRoutingKeyBytes)
	}
	parsed, size, err := deliveryDeath(death, limits)
	if err != nil || len(parsed.RoutingKeys) != MaxDeathRoutingKeys || size <= 0 {
		t.Fatalf("deliveryDeath() = (%#v, %d, %v)", parsed, size, err)
	}

	records := make([]any, MaxDeathRecords)
	for index := range records {
		records[index] = validDeathTable()
	}
	largeLimits := limits
	largeLimits.MaxHeaderBytes = DefaultLimits().MaxHeaderBytes
	if deaths, err := deliveryDeaths(amqp.Table{deathHeader: records}, largeLimits, 0); err != nil || len(deaths) != MaxDeathRecords {
		t.Fatalf("deliveryDeaths() = (%d, %v), want exact record limit", len(deaths), err)
	}
}

func TestDeliveryDeathBudgetAccumulatesAcrossRecords(t *testing.T) {
	first := validDeathTable()
	second := validDeathTable()
	_, firstSize, err := deliveryDeath(first, DefaultLimits())
	if err != nil {
		t.Fatalf("first deliveryDeath(): %v", err)
	}
	_, secondSize, err := deliveryDeath(second, DefaultLimits())
	if err != nil {
		t.Fatalf("second deliveryDeath(): %v", err)
	}
	third := validDeathTable()
	_, thirdSize, err := deliveryDeath(third, DefaultLimits())
	if err != nil {
		t.Fatalf("third deliveryDeath(): %v", err)
	}
	limits := DefaultLimits()
	limits.MaxHeaderBytes = firstSize + secondSize + thirdSize
	if deaths, err := deliveryDeaths(amqp.Table{deathHeader: []any{first, second, third}}, limits, 0); err != nil || len(deaths) != 3 {
		t.Fatalf("exact death budget = (%d, %v)", len(deaths), err)
	}
	limits.MaxHeaderBytes--
	if _, err := deliveryDeaths(amqp.Table{deathHeader: []any{first, second, third}}, limits, 0); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("cumulative death budget error = %v", err)
	}
	if _, err := deliveryDeaths(amqp.Table{deathHeader: "not-a-list"}, DefaultLimits(), 0); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("wrong death container error = %v", err)
	}
}

func TestDeliveryDeathRequiredFieldsFailIndependently(t *testing.T) {
	limits := DefaultLimits()
	for name, mutate := range map[string]func(amqp.Table){
		"reason":   func(fields amqp.Table) { fields["reason"] = true },
		"queue":    func(fields amqp.Table) { fields["queue"] = true },
		"exchange": func(fields amqp.Table) { fields["exchange"] = true },
		"time":     func(fields amqp.Table) { fields["time"] = true },
	} {
		fields := validDeathTable()
		mutate(fields)
		if _, _, err := deliveryDeath(fields, limits); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatalf("%s field error = %v", name, err)
		}
	}
	fields := validDeathTable()
	fields["exchange"] = strings.Repeat("e", limits.MaxNameBytes)
	fields["routing-keys"] = []any{
		strings.Repeat("a", limits.MaxRoutingKeyBytes),
		strings.Repeat("b", limits.MaxRoutingKeyBytes),
	}
	death, size, err := deliveryDeath(fields, limits)
	wantSize := 16 + len(death.Reason) + len(death.Queue) + len(death.Exchange) +
		2*limits.MaxRoutingKeyBytes
	if err != nil || len(death.RoutingKeys) != 2 || size != wantSize {
		t.Fatalf("exact death field boundaries = (%#v, %d, %v), want size %d", death, size, err, wantSize)
	}
	for name, value := range map[string]any{
		"wrong type":  true,
		"over length": strings.Repeat("r", limits.MaxRoutingKeyBytes+1),
		"control":     "bad\nrouting",
	} {
		fields := validDeathTable()
		fields["routing-keys"] = []any{value}
		if _, _, err := deliveryDeath(fields, limits); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatalf("%s routing key error = %v", name, err)
		}
	}
}

func TestUnsignedAMQPSignedIntegersAcceptZero(t *testing.T) {
	for _, value := range []any{int8(0), int16(0), int32(0), int64(0)} {
		if got, ok := unsignedAMQPInteger(value); !ok || got != 0 {
			t.Fatalf("unsignedAMQPInteger(%T) = (%d, %t)", value, got, ok)
		}
	}
}

func TestTopologyAcceptsExactCollectionAndIdentityLimits(t *testing.T) {
	exchanges := make([]Exchange, MaxTopologyExchanges)
	for index := range exchanges {
		exchanges[index] = Exchange{Name: fmt.Sprintf("exchange-%03d", index), Kind: ExchangeDirect}
	}
	if err := (Topology{Exchanges: exchanges}).Validate(TopologyPolicy{Mode: TopologyPassive}); err != nil {
		t.Fatalf("exact exchange count rejected: %v", err)
	}
	queues := make([]Queue, MaxTopologyQueues)
	for index := range queues {
		queues[index] = Queue{Name: fmt.Sprintf("queue-%03d", index), Type: QueueQuorum, Durable: true}
	}
	if err := (Topology{Queues: queues}).Validate(TopologyPolicy{Mode: TopologyPassive}); err != nil {
		t.Fatalf("exact queue count rejected: %v", err)
	}
	bindings := make([]Binding, MaxTopologyBindings)
	for index := range bindings {
		bindings[index] = Binding{Exchange: "e", Queue: "q", RoutingKey: strings.Repeat("r", DefaultLimits().MaxRoutingKeyBytes)}
	}
	topology := Topology{
		Exchanges: []Exchange{{Name: "e", Kind: ExchangeDirect}},
		Queues:    []Queue{{Name: "q", Type: QueueQuorum, Durable: true}},
		Bindings:  bindings,
	}
	if err := topology.Validate(TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}); err != nil {
		t.Fatalf("exact binding count rejected: %v", err)
	}
}

func TestTopologyValidationExaminesEveryCollectionEntry(t *testing.T) {
	policy := TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}
	tests := map[string]Topology{
		"second exchange": {
			Exchanges: []Exchange{{Name: "first", Kind: ExchangeDirect}, {Name: "bad\nexchange", Kind: ExchangeDirect}},
		},
		"second queue": {
			Queues: []Queue{{Name: "first", Type: QueueQuorum, Durable: true}, {Name: "bad\nqueue", Type: QueueQuorum, Durable: true}},
		},
		"second binding": {
			Exchanges: []Exchange{{Name: "e", Kind: ExchangeDirect}},
			Queues:    []Queue{{Name: "q", Type: QueueQuorum, Durable: true}},
			Bindings: []Binding{
				{Exchange: "e", Queue: "q", RoutingKey: "valid"},
				{Exchange: "missing", Queue: "q", RoutingKey: "invalid"},
			},
		},
	}
	for name, topology := range tests {
		if err := topology.Validate(policy); !errors.Is(err, ErrInvalidTopology) {
			t.Fatalf("%s error = %v, want invalid topology", name, err)
		}
	}
}

func TestServerNamedQueueRequiresBothClassicAndExclusive(t *testing.T) {
	for name, queue := range map[string]Queue{
		"classic non-exclusive": {Type: QueueClassic},
		"quorum exclusive":      {Type: QueueQuorum, Durable: true, Exclusive: true},
	} {
		if err := queue.Validate(); !errors.Is(err, ErrInvalidTopology) {
			t.Fatalf("%s error = %v, want invalid topology", name, err)
		}
	}
}

func TestQueuePolicyAcceptsExactDeadLetterAndNumericBounds(t *testing.T) {
	routing := strings.Repeat("r", 255)
	maximum := time.Second
	length := uint64(1<<63 - 1)
	queue := Queue{
		Name: "orders", Type: QueueQuorum, Durable: true,
		DelayedRetry: &QueueDelayedRetry{Type: DelayedRetryAll, Minimum: maximum, Maximum: &maximum},
		MaxLength:    &length, MaxLengthBytes: &length,
		Overflow: QueueOverflowRejectPublish,
		DeadLetter: &QueueDeadLetter{
			Exchange: strings.Repeat("e", 255), RoutingKey: &routing, Strategy: DeadLetterAtLeastOnce,
		},
	}
	if err := queue.Validate(); err != nil {
		t.Fatalf("exact queue policy bounds rejected: %v", err)
	}
}

func TestTopologyBindingFailuresRemainIndependent(t *testing.T) {
	policy := TopologyPolicy{Mode: TopologyDeclare, Development: PermitDevelopmentTopology()}
	tests := map[string]Topology{
		"missing exchange": {
			Exchanges: []Exchange{{Name: "e", Kind: ExchangeDirect}},
			Queues:    []Queue{{Name: "q", Type: QueueQuorum, Durable: true}},
			Bindings:  []Binding{{Exchange: "missing", Queue: "q", RoutingKey: "key"}},
		},
		"missing queue": {
			Exchanges: []Exchange{{Name: "e", Kind: ExchangeDirect}},
			Queues:    []Queue{{Name: "q", Type: QueueQuorum, Durable: true}},
			Bindings:  []Binding{{Exchange: "e", Queue: "missing", RoutingKey: "key"}},
		},
		"invalid binding": {
			Exchanges: []Exchange{{Name: "e", Kind: ExchangeFanout}},
			Queues:    []Queue{{Name: "q", Type: QueueQuorum, Durable: true}},
			Bindings:  []Binding{{Exchange: "e", Queue: "q", RoutingKey: "not-empty"}},
		},
	}
	for name, topology := range tests {
		if err := topology.Validate(policy); !errors.Is(err, ErrInvalidTopology) {
			t.Fatalf("%s error = %v, want invalid topology", name, err)
		}
	}
}

func TestExchangeBindingValidationRejectsEachIndependentInput(t *testing.T) {
	limits := DefaultLimits()
	for name, test := range map[string]struct {
		routing   string
		arguments []Header
	}{
		"routing length":  {routing: strings.Repeat("r", limits.MaxRoutingKeyBytes+1)},
		"routing control": {routing: "bad\nrouting"},
		"arguments":       {routing: "valid", arguments: []Header{{Key: "bad\nkey", Kind: HeaderString}}},
	} {
		if validExchangeBindingWithLimits(ExchangeDirect, test.routing, test.arguments, limits) {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestHeadersBindingScansPastNonMatchArguments(t *testing.T) {
	arguments := []Header{
		StringHeader("x-extension", "value"),
		StringHeader("x-match", "all-with-x"),
	}
	if !validHeadersBindingArguments(arguments) {
		t.Fatal("x-match after an extension argument was ignored")
	}
}

func TestBindingArgumentsAcceptExactCountAndRejectReservedIdentityIndependently(t *testing.T) {
	limits := DefaultLimits()
	arguments := make([]Header, limits.MaxHeaderEntries)
	for index := range arguments {
		arguments[index] = BoolHeader(fmt.Sprintf("key-%03d", index), true)
	}
	if !validBindingArgumentsWithLimits(arguments, limits) {
		t.Fatal("exact binding argument count rejected")
	}
	for name, argument := range map[string]Header{
		"invalid identity": StringHeader("bad\nkey", "value"),
		"reserved key":     StringHeader(publishTokenHeader, "value"),
	} {
		if validBindingArgumentsWithLimits([]Header{argument}, limits) {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestBindingPrimitiveBudgetsAccumulateAndCanonicalizeEveryField(t *testing.T) {
	for name, arguments := range map[string][]Header{
		"bool":  {BoolHeader("first", true), BoolHeader("second", false)},
		"int64": {Int64Header("first", 1), Int64Header("second", 2)},
		"bytes": {BytesHeader("first", []byte("one")), BytesHeader("second", []byte("two"))},
	} {
		limits := DefaultLimits()
		wantBytes := 0
		for _, argument := range arguments {
			wantBytes += len(argument.Key)
			switch argument.Kind {
			case HeaderBool:
				wantBytes++
			case HeaderInt64:
				wantBytes += 8
			case HeaderBytes:
				wantBytes += len(argument.Bytes)
			}
		}
		limits.MaxHeaderBytes = wantBytes
		if !validBindingArgumentsWithLimits(arguments, limits) {
			t.Fatalf("exact %s argument budget rejected", name)
		}
		limits.MaxHeaderBytes--
		if validBindingArgumentsWithLimits(arguments, limits) {
			t.Fatalf("cumulative %s argument overflow accepted", name)
		}
	}
	for name, argument := range map[string]Header{
		"int64 string": {Key: "key", Kind: HeaderInt64, String: "set"},
		"int64 bool":   {Key: "key", Kind: HeaderInt64, Bool: true},
		"int64 bytes":  {Key: "key", Kind: HeaderInt64, Bytes: []byte("set")},
		"bytes string": {Key: "key", Kind: HeaderBytes, String: "set"},
		"bytes bool":   {Key: "key", Kind: HeaderBytes, Bool: true},
		"bytes int64":  {Key: "key", Kind: HeaderBytes, Int64: 1},
	} {
		if validBindingArgumentsWithLimits([]Header{argument}, DefaultLimits()) {
			t.Fatalf("non-canonical %s argument accepted", name)
		}
	}
}

func TestBindingArgumentBudgetAccumulatesAcrossEntries(t *testing.T) {
	arguments := []Header{StringHeader("first", "one"), StringHeader("second", "two")}
	limits := DefaultLimits()
	wantBytes := len("first") + len("one") + len("second") + len("two")
	limits.MaxHeaderBytes = wantBytes
	if !validBindingArgumentsWithLimits(arguments, limits) {
		t.Fatal("exact binding argument budget rejected")
	}
	limits.MaxHeaderBytes--
	if validBindingArgumentsWithLimits(arguments, limits) {
		t.Fatal("cumulative binding argument overflow accepted")
	}
}

func TestHeadersBindingMatchModesRemainDistinct(t *testing.T) {
	for _, match := range []string{"all", "any"} {
		if validHeadersBindingArguments([]Header{StringHeader("x-match", match), StringHeader("x-extension", "value")}) {
			t.Fatalf("%s accepted extension-only matching", match)
		}
		if !validHeadersBindingArguments([]Header{StringHeader("x-match", match), StringHeader("tenant", "north")}) {
			t.Fatalf("%s rejected application matching", match)
		}
	}
	for _, match := range []string{"all-with-x", "any-with-x"} {
		if !validHeadersBindingArguments([]Header{StringHeader("x-match", match), StringHeader("x-extension", "value")}) {
			t.Fatalf("%s rejected extension matching", match)
		}
	}
}

func testConnectionConfigForMutation() ConnectionConfig {
	return ConnectionConfig{
		Endpoints: []Endpoint{{Host: "rabbitmq.internal", Port: 5671}}, VirtualHost: "/",
		Credentials: CredentialProviderFunc(func(context.Context) (Credentials, error) {
			return Credentials{Username: "worker", Password: []byte("secret")}, nil
		}),
		TLS: TLSConfig{ServerName: "rabbitmq.internal"}, DialTimeout: time.Second,
		Heartbeat: minimumHeartbeat,
		Recovery:  RecoveryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
	}
}
