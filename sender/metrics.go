package sender

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// SenderMetrics instruments the SQS sender with OTel metrics.
// A nil *SenderMetrics is safe to use — all methods are no-ops.
type SenderMetrics struct {
	messagesSent      metric.Int64Counter
	operationDuration metric.Float64Histogram
	batchMessageCount metric.Float64Histogram
	baseAttrs         metric.MeasurementOption
}

// NewSenderMetrics creates sender metrics from a MeterProvider. Returns
// nil if mp is nil (metrics disabled). clientName is the vinculum client
// block name, queueName is the SQS queue name.
func NewSenderMetrics(clientName, queueName string, mp metric.MeterProvider) *SenderMetrics {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("github.com/tsarna/vinculum-sqs/sender")
	sent, _ := meter.Int64Counter("messaging.client.sent.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Messages sent by the SQS sender"),
	)
	duration, _ := meter.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of SQS send operations"),
	)
	batchSize, _ := meter.Float64Histogram("messaging.batch.message_count",
		metric.WithUnit("{message}"),
		metric.WithDescription("Messages per SQS batch send"),
	)
	return &SenderMetrics{
		messagesSent:      sent,
		operationDuration: duration,
		batchMessageCount: batchSize,
		baseAttrs: metric.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.destination.name", queueName),
			attribute.String("vinculum.client.name", clientName),
		),
	}
}

func (m *SenderMetrics) RecordSent(ctx context.Context) {
	if m == nil {
		return
	}
	m.messagesSent.Add(ctx, 1, m.baseAttrs)
}

func (m *SenderMetrics) RecordOperationDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.operationDuration.Record(ctx, d.Seconds(), m.baseAttrs)
}

func (m *SenderMetrics) RecordBatchSize(ctx context.Context, size int) {
	if m == nil {
		return
	}
	m.batchMessageCount.Record(ctx, float64(size), m.baseAttrs)
}
