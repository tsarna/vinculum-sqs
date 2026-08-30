// Package receiver provides SQSReceiver, which polls an AWS SQS queue
// and dispatches received messages to a vinculum bus.Subscriber.
package receiver

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	bus "github.com/tsarna/vinculum-bus"
	vsqs "github.com/tsarna/vinculum-sqs"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// traceAttributeKeys are W3C trace context keys consumed by the OTel
// propagator and excluded from vinculum fields.
var traceAttributeKeys = map[string]bool{
	"traceparent": true,
	"tracestate":  true,
	"baggage":     true,
}

// SQSReceiveAPI is the subset of the SQS client API used by the receiver.
type SQSReceiveAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)

	// GetQueueAttributes is asked once at Start for the queue's own visibility
	// timeout, which the receiver needs to know when a message's receipt handle
	// has expired. It is only consulted when the receiver does not set a
	// visibility timeout of its own, and a failure is a warning rather than a
	// startup error.
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// TopicFunc resolves the vinculum topic for a received SQS message.
type TopicFunc func(msg sqstypes.Message, fields map[string]string) string

// SQSReceiver polls an SQS queue and dispatches received messages to a
// vinculum bus.Subscriber.
type SQSReceiver struct {
	client         SQSReceiveAPI
	queueURL       string
	queueName      string
	subscriber     bus.Subscriber
	wireFormat     wire.WireFormat
	onDecodeError  wire.DecodeErrorHook
	waitTime       int32
	maxMessages    int32
	visTimeout     *int32
	autoDelete     bool
	concurrency    int
	topicFn        TopicFunc
	metrics        *ReceiverMetrics
	logger         *zap.Logger
	tracerProvider trace.TracerProvider

	// queueVisTimeout is the queue's own visibility timeout in seconds, read
	// once at Start when visTimeout does not override it, and zero when it
	// could not be read. It bounds how long a message's receipt handle stays
	// usable, so it is what tells a settle that it has arrived too late.
	queueVisTimeout atomic.Int32

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (r *SQSReceiver) tracer() trace.Tracer {
	tp := r.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer("github.com/tsarna/vinculum-sqs/receiver")
}

// QueueURL returns the queue URL. Used by VCL functions (sqs_delete,
// sqs_extend_visibility) to issue API calls against the same queue.
func (r *SQSReceiver) QueueURL() string { return r.queueURL }

// Client returns the underlying SQS API client. Used by VCL functions
// that need to call DeleteMessage or ChangeMessageVisibility.
func (r *SQSReceiver) Client() SQSReceiveAPI { return r.client }

// Start begins the polling loop(s). Each concurrent processor runs its
// own goroutine. Returns immediately.
func (r *SQSReceiver) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cancel != nil {
		return fmt.Errorf("sqs receiver %s: already started", r.queueName)
	}

	pollCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	// Learn how long a received message stays ours before the poll loops start,
	// so the first message already knows when its receipt handle expires.
	r.learnVisibilityTimeout(ctx)

	n := r.concurrency
	if n < 1 {
		n = 1
	}

	for i := 0; i < n; i++ {
		r.wg.Add(1)
		go r.pollLoop(pollCtx)
	}

	r.logger.Info("sqs receiver started",
		zap.String("queue", r.queueName),
		zap.Int("concurrency", n),
	)

	return nil
}

// Stop signals the polling loop(s) to drain and exit, then waits for
// all goroutines to finish.
func (r *SQSReceiver) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	r.wg.Wait()

	r.logger.Info("sqs receiver stopped", zap.String("queue", r.queueName))
	return nil
}

// pollLoop is the per-goroutine receive/process/delete loop.
func (r *SQSReceiver) pollLoop(ctx context.Context) {
	defer r.wg.Done()

	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		input := &sqs.ReceiveMessageInput{
			QueueUrl:              &r.queueURL,
			WaitTimeSeconds:       r.waitTime,
			MaxNumberOfMessages:   r.maxMessages,
			MessageAttributeNames: []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
				sqstypes.MessageSystemAttributeNameAll,
			},
		}
		if r.visTimeout != nil {
			input.VisibilityTimeout = *r.visTimeout
		}

		result, err := r.client.ReceiveMessage(ctx, input)
		if err != nil {
			if ctx.Err() != nil {
				return // normal shutdown
			}
			r.logger.Error("sqs receiver: ReceiveMessage failed",
				zap.String("queue", r.queueName),
				zap.Error(err),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second // reset on success

		// A message's visibility window starts when ReceiveMessage handed it
		// over, not when this loop gets round to it — with max_messages above
		// one, those are not the same moment.
		receivedAt := time.Now()

		for _, msg := range result.Messages {
			r.processMessage(ctx, msg, receivedAt)
		}
	}
}

// processMessage deserializes, dispatches, and optionally deletes a
// single SQS message.
func (r *SQSReceiver) processMessage(ctx context.Context, msg sqstypes.Message, receivedAt time.Time) {
	// Extract message attributes → fields map.
	fields := r.extractFields(msg)

	settler := r.newSettler(msg, receivedAt)

	// Extract trace context from message attributes.
	propagator := otel.GetTextMapPropagator()
	carrier := &vsqs.MessageAttributeCarrier{Attrs: msg.MessageAttributes}
	producerCtx := propagator.Extract(ctx, carrier)

	// Carry the producer's baggage onto the processing context so it reaches
	// subscriber.OnEvent and action expressions. The consumer span below stays a
	// new root linked to the producer span — only baggage rides along, not the
	// span parent.
	if bg := baggage.FromContext(producerCtx); bg.Len() > 0 {
		ctx = baggage.ContextWithBaggage(ctx, bg)
	}

	// Start processing span as a new root linked to the producer span.
	// SQS is async — the consumer process isn't part of the producer's
	// trace tree; it's a separate operation that happens later.
	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.destination.name", r.queueName),
			attribute.String("messaging.operation.type", "process"),
		),
	}
	if psc := trace.SpanContextFromContext(producerCtx); psc.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: psc}))
	}
	ctx, span := r.tracer().Start(ctx, "process "+r.queueName, startOpts...)
	defer span.End()

	// Acknowledgement is a property of this delivery, and `fields` cannot carry
	// it — the bus rewrites those per subscription. The context can, and it is
	// preserved across the async queue's goroutine hop, so putting the settler
	// here is what lets a subscription several hops downstream settle the
	// message it handled.
	ctx = bus.WithSettler(ctx, settler)

	if msg.MessageId != nil {
		span.SetAttributes(attribute.String("messaging.message.id", *msg.MessageId))
	}

	// Deserialize body. A decode failure is fatal to the message: the
	// configured wire format is a contract, so a body that doesn't satisfy
	// it is not delivered. Use wire format "auto" for best-effort decoding.
	//
	// The message is NOT deleted, so it returns after the visibility
	// timeout. Configure an SQS redrive policy so a persistently malformed
	// message eventually lands in a dead-letter queue instead of cycling.
	var payload any
	if msg.Body != nil {
		var err error
		payload, err = r.wireFormat.Deserialize([]byte(*msg.Body))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "deserialize")
			r.logger.Error("sqs receiver: deserialize failed",
				zap.String("queue", r.queueName),
				zap.String("wire_format", r.wireFormat.Name()),
				zap.Error(err),
			)
			r.metrics.RecordError(ctx, "process", "deserialize")
			if r.onDecodeError != nil {
				attrs := map[string]string{"queue": r.queueName}
				if msg.MessageId != nil {
					attrs["message_id"] = *msg.MessageId
				}
				r.onDecodeError(ctx, wire.DecodeError{
					Raw:    []byte(*msg.Body),
					Err:    err,
					Format: r.wireFormat.Name(),
					Topic:  r.queueName,
					Fields: fields,
					Attrs:  attrs,
				})
			}
			return
		}
	}

	// Resolve vinculum topic.
	topic := r.topicFn(msg, fields)

	// Dispatch to subscriber.
	processStart := time.Now()
	if err := r.subscriber.OnEvent(ctx, topic, payload, fields); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		r.logger.Error("sqs receiver: subscriber.OnEvent failed",
			zap.String("queue", r.queueName),
			zap.String("topic", topic),
			zap.Error(err),
		)
		r.metrics.RecordError(ctx, "process", "subscriber")
		// Do NOT delete — message returns to queue after visibility timeout.
		return
	}

	r.metrics.RecordConsumed(ctx)
	r.metrics.RecordProcessDuration(ctx, time.Since(processStart))

	// Auto-delete on success, through the same settler the subscriber would
	// have used — so a subscriber that settled the message itself does not have
	// it settled twice, and "vinculum deletes for you" is one policy over one
	// mechanism rather than a second path to the queue.
	if r.autoDelete && msg.ReceiptHandle != nil {
		if _, err := settler.Ack(ctx); err != nil {
			r.logger.Error("sqs receiver: DeleteMessage failed",
				zap.String("queue", r.queueName),
				zap.Error(err),
			)
			r.metrics.RecordError(ctx, "settle", "delete")
			// Message will be redelivered after visibility timeout — at-least-once.
		}
	}
}

// extractFields builds the vinculum fields map from SQS message
// attributes and system attributes.
func (r *SQSReceiver) extractFields(msg sqstypes.Message) map[string]string {
	fields := make(map[string]string)

	// System attributes → $-prefixed fields.
	if msg.MessageId != nil {
		fields["$message_id"] = *msg.MessageId
	}
	// The receipt handle is deliberately not a field. It existed only to be
	// handed back to a manual delete, and the settler on the delivery's context
	// needs no help finding it. As a field it would be an opaque, per-receive,
	// expiring token that is meaningless to log, correlate, or compare, and its
	// one obvious use would be to save it somewhere and settle later — which is
	// storing a lease, not a value, and it expires while it sits in the
	// variable.
	for name, val := range msg.Attributes {
		switch name {
		case "ApproximateReceiveCount":
			fields["$receive_count"] = val
		case "SentTimestamp":
			fields["$sent_timestamp"] = val
		case "ApproximateFirstReceiveTimestamp":
			fields["$first_receive_timestamp"] = val
		case "MessageGroupId":
			fields["$message_group_id"] = val
		case "MessageDeduplicationId":
			fields["$deduplication_id"] = val
		case "SequenceNumber":
			fields["$sequence_number"] = val
		}
	}

	// User message attributes → fields (with _→$ reverse mapping).
	for name, attr := range msg.MessageAttributes {
		// Skip trace attributes (consumed by propagator).
		if traceAttributeKeys[name] {
			continue
		}

		fieldName := unmapFieldName(name)

		switch {
		case attr.StringValue != nil:
			fields[fieldName] = *attr.StringValue
		case attr.DataType != nil && *attr.DataType == "Number" && attr.StringValue != nil:
			fields[fieldName] = *attr.StringValue
		case attr.BinaryValue != nil:
			fields[fieldName] = base64.StdEncoding.EncodeToString(attr.BinaryValue)
		}
	}

	return fields
}

// unmapFieldName reverses the sender's $ → _ mapping.
// SQS attribute names starting with _ are converted back to $-prefixed
// vinculum field names.
func unmapFieldName(name string) string {
	if strings.HasPrefix(name, "_") {
		return "$" + name[1:]
	}
	return name
}

// DeleteMsg deletes a message by receipt handle, for a caller holding a handle
// it obtained some other way.
//
// Prefer the settler on a delivery's context, which is how a consumer of this
// package settles what it was handed: it knows the handle without being told,
// it settles once however many subscribers see the same delivery, and it
// refuses a handle whose visibility window has already lapsed.
func (r *SQSReceiver) DeleteMsg(ctx context.Context, receiptHandle string) error {
	_, err := r.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &r.queueURL,
		ReceiptHandle: &receiptHandle,
	})
	return err
}

// ExtendVisibility changes the visibility timeout for a message. It is what
// the settler's Keepalive issues, and is exported for a caller holding a
// receipt handle it obtained some other way.
func (r *SQSReceiver) ExtendVisibility(ctx context.Context, receiptHandle string, timeoutSeconds int32) error {
	_, err := r.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &r.queueURL,
		ReceiptHandle:     &receiptHandle,
		VisibilityTimeout: timeoutSeconds,
	})
	return err
}

// QueueNameFromURL extracts the queue name from an SQS queue URL.
func QueueNameFromURL(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) > 0 {
		name := parts[len(parts)-1]
		if name != "" {
			return name
		}
	}
	return queueURL
}
