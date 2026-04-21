package sender

import (
	"errors"

	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SenderBuilder constructs an SQSSender with validated configuration.
type SenderBuilder struct {
	client         SQSSendAPI
	clientName     string
	queueURL       string
	wireFormat     wire.WireFormat
	delaySeconds   int32
	topicAttribute string
	fifo           *FIFOConfig
	meterProvider  metric.MeterProvider
	logger         *zap.Logger
	tracerProvider trace.TracerProvider
}

// NewSender returns a builder with sensible defaults.
func NewSender() *SenderBuilder {
	return &SenderBuilder{
		logger: zap.NewNop(),
	}
}

func (b *SenderBuilder) WithClient(c SQSSendAPI) *SenderBuilder {
	b.client = c
	return b
}

func (b *SenderBuilder) WithClientName(name string) *SenderBuilder {
	b.clientName = name
	return b
}

func (b *SenderBuilder) WithQueueURL(url string) *SenderBuilder {
	b.queueURL = url
	return b
}

func (b *SenderBuilder) WithWireFormat(wf wire.WireFormat) *SenderBuilder {
	b.wireFormat = wf
	return b
}

func (b *SenderBuilder) WithDelaySeconds(d int32) *SenderBuilder {
	b.delaySeconds = d
	return b
}

func (b *SenderBuilder) WithTopicAttribute(name string) *SenderBuilder {
	b.topicAttribute = name
	return b
}

func (b *SenderBuilder) WithLogger(l *zap.Logger) *SenderBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

func (b *SenderBuilder) WithTracerProvider(tp trace.TracerProvider) *SenderBuilder {
	b.tracerProvider = tp
	return b
}

func (b *SenderBuilder) WithFIFOConfig(cfg *FIFOConfig) *SenderBuilder {
	b.fifo = cfg
	return b
}

func (b *SenderBuilder) WithMeterProvider(mp metric.MeterProvider) *SenderBuilder {
	b.meterProvider = mp
	return b
}

// Build validates the configuration and returns a ready-to-use SQSSender.
func (b *SenderBuilder) Build() (*SQSSender, error) {
	if b.client == nil {
		return nil, errors.New("sqs sender: client is required")
	}
	if b.queueURL == "" {
		return nil, errors.New("sqs sender: queue_url is required")
	}

	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}

	// FIFO validation: .fifo queues require message_group_id.
	if IsFIFOQueue(b.queueURL) && (b.fifo == nil || b.fifo.GroupIDFunc == nil) {
		return nil, errors.New("sqs sender: FIFO queue requires message_group_id (queue URL ends in .fifo)")
	}

	queueName := queueNameFromURL(b.queueURL)

	return &SQSSender{
		client:         b.client,
		queueURL:       b.queueURL,
		queueName:      queueName,
		wireFormat:     wf,
		delaySeconds:   b.delaySeconds,
		topicAttribute: b.topicAttribute,
		fifo:           b.fifo,
		metrics:        NewSenderMetrics(b.clientName, queueName, b.meterProvider),
		logger:         b.logger,
		tracerProvider: b.tracerProvider,
	}, nil
}
