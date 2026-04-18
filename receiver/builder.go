package receiver

import (
	"errors"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ReceiverBuilder constructs an SQSReceiver with validated configuration.
type ReceiverBuilder struct {
	client         SQSReceiveAPI
	clientName     string
	queueURL       string
	subscriber     bus.Subscriber
	wireFormat     wire.WireFormat
	waitTime       int32
	maxMessages    int32
	visTimeout     *int32
	autoDelete     bool
	concurrency    int
	topicFn        TopicFunc
	meterProvider  metric.MeterProvider
	logger         *zap.Logger
	tracerProvider trace.TracerProvider
}

// NewReceiver returns a builder with sensible defaults.
func NewReceiver() *ReceiverBuilder {
	return &ReceiverBuilder{
		waitTime:    20,
		maxMessages: 10,
		autoDelete:  true,
		concurrency: 1,
		logger:      zap.NewNop(),
	}
}

func (b *ReceiverBuilder) WithClient(c SQSReceiveAPI) *ReceiverBuilder {
	b.client = c
	return b
}

func (b *ReceiverBuilder) WithQueueURL(url string) *ReceiverBuilder {
	b.queueURL = url
	return b
}

func (b *ReceiverBuilder) WithSubscriber(s bus.Subscriber) *ReceiverBuilder {
	b.subscriber = s
	return b
}

func (b *ReceiverBuilder) WithWireFormat(wf wire.WireFormat) *ReceiverBuilder {
	b.wireFormat = wf
	return b
}

func (b *ReceiverBuilder) WithWaitTime(seconds int32) *ReceiverBuilder {
	b.waitTime = seconds
	return b
}

func (b *ReceiverBuilder) WithMaxMessages(n int32) *ReceiverBuilder {
	b.maxMessages = n
	return b
}

func (b *ReceiverBuilder) WithVisibilityTimeout(seconds int32) *ReceiverBuilder {
	b.visTimeout = &seconds
	return b
}

func (b *ReceiverBuilder) WithAutoDelete(enabled bool) *ReceiverBuilder {
	b.autoDelete = enabled
	return b
}

func (b *ReceiverBuilder) WithConcurrency(n int) *ReceiverBuilder {
	b.concurrency = n
	return b
}

func (b *ReceiverBuilder) WithTopicFunc(fn TopicFunc) *ReceiverBuilder {
	b.topicFn = fn
	return b
}

func (b *ReceiverBuilder) WithClientName(name string) *ReceiverBuilder {
	b.clientName = name
	return b
}

func (b *ReceiverBuilder) WithMeterProvider(mp metric.MeterProvider) *ReceiverBuilder {
	b.meterProvider = mp
	return b
}

func (b *ReceiverBuilder) WithLogger(l *zap.Logger) *ReceiverBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

func (b *ReceiverBuilder) WithTracerProvider(tp trace.TracerProvider) *ReceiverBuilder {
	b.tracerProvider = tp
	return b
}

// Build validates the configuration and returns a ready-to-use SQSReceiver.
func (b *ReceiverBuilder) Build() (*SQSReceiver, error) {
	if b.client == nil {
		return nil, errors.New("sqs receiver: client is required")
	}
	if b.queueURL == "" {
		return nil, errors.New("sqs receiver: queue_url is required")
	}
	if b.subscriber == nil {
		return nil, errors.New("sqs receiver: subscriber is required")
	}

	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}

	queueName := QueueNameFromURL(b.queueURL)

	topicFn := b.topicFn
	if topicFn == nil {
		// Default: use queue name as vinculum topic.
		topicFn = func(_ sqstypes.Message, _ map[string]string) string {
			return queueName
		}
	}

	return &SQSReceiver{
		client:         b.client,
		queueURL:       b.queueURL,
		queueName:      queueName,
		subscriber:     b.subscriber,
		wireFormat:     wf,
		waitTime:       b.waitTime,
		maxMessages:    b.maxMessages,
		visTimeout:     b.visTimeout,
		autoDelete:     b.autoDelete,
		concurrency:    b.concurrency,
		topicFn:        topicFn,
		metrics:        NewReceiverMetrics(b.clientName, queueName, b.meterProvider),
		logger:         b.logger,
		tracerProvider: b.tracerProvider,
	}, nil
}
