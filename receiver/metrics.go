package receiver

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// ReceiverMetrics instruments the SQS receiver with OTel metrics.
// A nil *ReceiverMetrics is safe to use — all methods are no-ops.
type ReceiverMetrics struct {
	messagesConsumed metric.Int64Counter
	processDuration  metric.Float64Histogram
	errors           metric.Int64Counter
	baseAttrs        metric.MeasurementOption
}

// NewReceiverMetrics creates receiver metrics from a MeterProvider.
// Returns nil if mp is nil (metrics disabled). clientName is the
// vinculum client block name, queueName is the SQS queue name.
func NewReceiverMetrics(clientName, queueName string, mp metric.MeterProvider) *ReceiverMetrics {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("github.com/tsarna/vinculum-sqs/receiver")
	consumed, _ := meter.Int64Counter("messaging.client.consumed.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Messages received and dispatched by the SQS receiver"),
	)
	procDur, _ := meter.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of subscriber.OnEvent processing"),
	)
	errs, _ := meter.Int64Counter("vinculum.messaging.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Errors encountered while processing SQS messages"),
	)
	return &ReceiverMetrics{
		messagesConsumed: consumed,
		processDuration:  procDur,
		errors:           errs,
		baseAttrs: metric.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.destination.name", queueName),
			attribute.String("vinculum.client.name", clientName),
		),
	}
}

func (m *ReceiverMetrics) RecordConsumed(ctx context.Context) {
	if m == nil {
		return
	}
	m.messagesConsumed.Add(ctx, 1, m.baseAttrs)
}

func (m *ReceiverMetrics) RecordProcessDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.processDuration.Record(ctx, d.Seconds(), m.baseAttrs)
}

// RecordError increments the error counter. operation names the stage that
// failed (e.g. "process", "settle") and errType classifies the failure
// (e.g. "deserialize", "subscriber", "delete").
func (m *ReceiverMetrics) RecordError(ctx context.Context, operation, errType string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, m.baseAttrs, metric.WithAttributes(
		attribute.String("messaging.operation.name", operation),
		attribute.String("error.type", errType),
	))
}
