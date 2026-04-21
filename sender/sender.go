// Package sender provides SQSSender, which implements bus.Subscriber to
// forward vinculum bus events to an AWS SQS queue via SendMessage.
package sender

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	bus "github.com/tsarna/vinculum-bus"
	vsqs "github.com/tsarna/vinculum-sqs"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// maxSQSAttributes is the SQS limit on message attributes per message.
const maxSQSAttributes = 10

// traceAttributeKeys are the W3C trace context keys injected by OTel
// propagators. These take priority over user fields in the attribute budget.
var traceAttributeKeys = map[string]bool{
	"traceparent": true,
	"tracestate":  true,
	"baggage":     true,
}

// FIFOConfig holds the per-message functions for FIFO queue parameters.
// Both functions receive the vinculum topic, message, and fields, and
// return the string value to set on the SQS message.
type FIFOConfig struct {
	GroupIDFunc      func(topic string, msg any, fields map[string]string) (string, error)
	DeduplicationFunc func(topic string, msg any, fields map[string]string) (string, error) // nil = use queue's content-based dedup
}

// SQSSender receives vinculum bus events and sends them as SQS messages.
// It implements bus.Subscriber so it can be used directly as a subscription
// target.
type SQSSender struct {
	bus.BaseSubscriber

	client         SQSSendAPI
	queueURL       string
	queueName      string
	wireFormat     wire.WireFormat
	delaySeconds   int32
	topicAttribute string
	fifo           *FIFOConfig
	metrics        *SenderMetrics
	logger         *zap.Logger
	tracerProvider trace.TracerProvider
}

// SQSSendAPI is the subset of the SQS client API used by the sender.
// Defined as an interface to support testing.
type SQSSendAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

func (s *SQSSender) tracer() trace.Tracer {
	tp := s.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer("github.com/tsarna/vinculum-sqs/sender")
}

// Start is a no-op. Reserved for future use.
func (s *SQSSender) Start() {}

// Stop is a no-op. Reserved for future use.
func (s *SQSSender) Stop() {}

// OnEvent serializes the message, maps fields to SQS message attributes,
// and sends the message to the configured SQS queue.
func (s *SQSSender) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	start := time.Now()

	// Serialize payload.
	body, err := s.wireFormat.SerializeString(msg)
	if err != nil {
		return fmt.Errorf("sqs sender %s: serialize: %w", s.queueName, err)
	}

	// Build message attributes from vinculum fields.
	attrs := s.buildMessageAttributes(fields, topic)

	// Inject trace context into message attributes.
	propagator := otel.GetTextMapPropagator()
	carrier := &vsqs.MessageAttributeCarrier{Attrs: attrs}
	propagator.Inject(ctx, carrier)

	// Start tracing span.
	ctx, span := s.tracer().Start(ctx, "publish "+s.queueName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.destination.name", s.queueName),
			attribute.String("messaging.operation.type", "publish"),
		),
	)
	defer span.End()

	// Build common message parameters.
	var messageGroupID *string
	var messageDeduplicationID *string

	if s.delaySeconds > 0 {
		// delay handled per-path below
	}

	// FIFO queue parameters.
	if s.fifo != nil {
		groupID, err := s.fifo.GroupIDFunc(topic, msg, fields)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("sqs sender %s: message_group_id: %w", s.queueName, err)
		}
		messageGroupID = &groupID

		if s.fifo.DeduplicationFunc != nil {
			dedupID, err := s.fifo.DeduplicationFunc(topic, msg, fields)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return fmt.Errorf("sqs sender %s: deduplication_id: %w", s.queueName, err)
			}
			messageDeduplicationID = &dedupID
		}
	}

	input := &sqs.SendMessageInput{
		QueueUrl:               &s.queueURL,
		MessageBody:            &body,
		MessageAttributes:      attrs,
		MessageGroupId:         messageGroupID,
		MessageDeduplicationId: messageDeduplicationID,
	}
	if s.delaySeconds > 0 {
		input.DelaySeconds = s.delaySeconds
	}

	result, err := s.client.SendMessage(ctx, input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("sqs sender %s: send: %w", s.queueName, err)
	}

	if result.MessageId != nil {
		span.SetAttributes(attribute.String("messaging.message.id", *result.MessageId))
	}

	s.metrics.RecordSent(ctx)
	s.metrics.RecordOperationDuration(ctx, time.Since(start))
	return nil
}

// buildMessageAttributes converts vinculum fields to SQS message attributes.
// It handles:
//   - optional topic_attribute injection
//   - $ → _ prefix mapping for vinculum internal fields
//   - SQS attribute name validation (drop invalid names)
//   - 10-attribute limit enforcement (trace attrs > topic_attribute > user fields)
func (s *SQSSender) buildMessageAttributes(fields map[string]string, topic string) map[string]sqstypes.MessageAttributeValue {
	attrs := make(map[string]sqstypes.MessageAttributeValue, len(fields)+1)

	// Reserve slots for trace attributes (injected after this function).
	// We don't know exactly how many trace attributes will be injected,
	// so we budget 3 (traceparent + tracestate + baggage) as the worst case.
	budget := maxSQSAttributes - 3

	// Add topic attribute if configured.
	if s.topicAttribute != "" {
		attrs[s.topicAttribute] = sqstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(topic),
		}
		budget--
	}

	// Map vinculum fields to SQS message attributes.
	for k, v := range fields {
		if budget <= 0 {
			s.logger.Warn("sqs sender: dropping excess fields, attribute limit reached",
				zap.String("queue", s.queueName),
				zap.Int("limit", maxSQSAttributes),
			)
			break
		}

		sqsName := mapFieldName(k)
		if !isValidAttributeName(sqsName) {
			s.logger.Debug("sqs sender: dropping field with invalid SQS attribute name",
				zap.String("field", k),
				zap.String("mapped", sqsName),
			)
			continue
		}

		attrs[sqsName] = sqstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
		budget--
	}

	return attrs
}

// mapFieldName converts a vinculum field name to a valid SQS attribute name.
// The $ prefix used for vinculum internal fields is replaced with _.
func mapFieldName(name string) string {
	if strings.HasPrefix(name, "$") {
		return "_" + name[1:]
	}
	return name
}

// isValidAttributeName checks whether a name is valid for an SQS message
// attribute. Valid names contain only alphanumeric characters, hyphens,
// underscores, and periods, and must not start with "AWS." or "Amazon."
// (case-insensitive).
func isValidAttributeName(name string) bool {
	if name == "" {
		return false
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' && r != '.' {
			return false
		}
	}
	return true
}

// IsFIFOQueue returns true if the queue URL ends with ".fifo".
func IsFIFOQueue(queueURL string) bool {
	return strings.HasSuffix(queueURL, ".fifo")
}

// queueNameFromURL extracts the queue name from an SQS queue URL.
// Queue URLs have the form:
//
//	https://sqs.<region>.amazonaws.com/<account-id>/<queue-name>
//
// Falls back to the full URL if parsing fails.
func queueNameFromURL(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) > 0 {
		name := parts[len(parts)-1]
		if name != "" {
			return name
		}
	}
	return queueURL
}
