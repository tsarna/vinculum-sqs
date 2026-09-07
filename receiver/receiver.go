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

	// Two cancels, because stopping is two things and a graceful shutdown wants
	// them apart. stopRead ends the poll loops and nothing else; stopWork
	// cancels the context every delivery and every settle rides on, and so is
	// the one that ends the receiver. stopRead's context is derived from
	// stopWork's, so cancelling work ends polling too.
	mu       sync.Mutex
	stopRead context.CancelFunc
	stopWork context.CancelFunc
	wg       sync.WaitGroup

	// unsettled counts deliveries handed out and not yet deleted, nacked, or
	// abandoned. See Unsettled.
	unsettled atomic.Int64

	// stillDelivering is the channel a timed-out Drain was waiting on, closed
	// when the loops finally finish. Nil until the first drain, and set by
	// every drain rather than only by one that gives up — what makes it answer
	// "no" is the channel being closed, not the field being absent.
	//
	// A channel rather than a flag because the question is asked a phase later
	// and the answer moves in between: teardown runs a whole quiesce between
	// Drain and Stop, so a delivery that overran the drain's deadline by a
	// moment has very likely finished by the time Stop looks. A flag would say
	// otherwise and put an error in the log of a shutdown where nothing went
	// wrong. See stillRunning.
	stillDelivering atomic.Pointer[chan struct{}]
}

// Unsettled reports how many deliveries this receiver has handed out that
// nothing has settled yet.
//
// It is not the number of messages in flight at the queue. A message left
// undeleted by a nack, or by a delivery that failed before reaching the
// subscriber, is SQS's business — the visibility timeout and the queue's own
// redrive policy decide what becomes of it — and nothing in this process is
// going to delete it. What this counts is the narrower thing a shutdown can
// usefully wait for: settles that are still coming.
func (r *SQSReceiver) Unsettled() int { return int(r.unsettled.Load()) }

// stillRunning reports whether a delivery a drain gave up on is running *now*,
// rather than whether one ever was.
func (r *SQSReceiver) stillRunning() bool {
	ch := r.stillDelivering.Load()
	if ch == nil {
		return false
	}
	select {
	case <-*ch:
		return false
	default:
		return true
	}
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

	if r.stopWork != nil {
		return fmt.Errorf("sqs receiver %q: already started", r.queueName)
	}

	// A loop from the last cycle still owns the WaitGroup. A Stop that gave up
	// on a delivery returns without waiting, so that goroutine never reached its
	// Done — and starting more over the top of it would leave the counter high,
	// with this cycle's Stop blocking forever on the abandoned part of it. That
	// is the unbounded wait Drain's bound exists to remove, one cycle later and
	// with nothing left to report it. Checked before the clear below, which
	// would otherwise erase the evidence.
	if r.stillRunning() {
		return fmt.Errorf("sqs receiver %q: a previous delivery is still running", r.queueName)
	}

	workCtx, stopWork := context.WithCancel(context.Background())
	readCtx, stopRead := context.WithCancel(workCtx)
	r.stopWork = stopWork
	r.stopRead = stopRead

	// stillDelivering is deliberately left as it is. The guard above has already
	// established that it is nil or closed, and a closed channel answers
	// stillRunning the same way nil does — until this cycle's own Drain
	// replaces it. Clearing it here would look like the load-bearing step and
	// would not be one.

	// Learn how long a received message stays ours before the poll loops start,
	// so the first message already knows when its receipt handle expires.
	r.learnVisibilityTimeout(ctx)

	n := r.concurrency
	if n < 1 {
		n = 1
	}

	for i := 0; i < n; i++ {
		r.wg.Add(1)
		go r.pollLoop(readCtx, workCtx)
	}

	r.logger.Info("sqs receiver started",
		zap.String("queue", r.queueName),
		zap.Int("concurrency", n),
	)

	return nil
}

// Drain stops polling for new messages and waits for the loops to finish the
// batches they are holding. It leaves everything else alone: the SQS client
// stays usable, and the settlers already handed out stay valid, so a message
// still travelling through a queue downstream is deleted normally when the work
// lands.
//
// That is the whole difference between draining and stopping, and it is what
// lets a shutdown stop consuming first and disconnect last. Between the two, a
// process is finishing work it has already accepted and taking on none.
//
// Every poll loop drains at once, under the one deadline. They stop reading
// independently, and what is being waited for is delivery — one loop's slow
// action is no reason to cut another's short.
//
// Bounded by ctx, which the caller sizes: delivery runs user-supplied work.
//
// Safe to call before Start, after Stop, or twice — though a second call after
// one that timed out reports the timeout again rather than a clean drain, since
// the delivery it gave up on is still running.
func (r *SQSReceiver) Drain(ctx context.Context) error {
	r.mu.Lock()
	stopRead := r.stopRead
	if stopRead == nil {
		r.mu.Unlock()
		if r.stillRunning() {
			return fmt.Errorf("sqs receiver %q: still delivering", r.queueName)
		}
		return nil
	}
	r.stopRead = nil
	stopRead()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	// Published before the wait, not only when the wait gives up: the waiter
	// outlives this call either way, and while it is open it is the honest
	// answer to "are the loops still delivering" — which is what a later phase
	// asks.
	//
	// Published under the same lock that cleared stopRead, because the two
	// together are what a concurrent second Drain reads. Between them it would
	// see the field already taken and no waiter yet, and report a clean drain
	// that has not happened — which is the whole defect this is here to
	// prevent, surviving in the gap. Nothing under this lock does I/O.
	r.stillDelivering.Store(&done)
	r.mu.Unlock()

	select {
	case <-done:
		r.logger.Info("sqs receiver drained", zap.String("queue", r.queueName))
		return nil
	case <-ctx.Done():
		return fmt.Errorf("sqs receiver %q: drain: %w", r.queueName, ctx.Err())
	}
}

// Stop ends the receiver: polling stops if it has not already, and the context
// every in-flight delivery and every outstanding settle rides on is cancelled.
// A deletion attempted after this has nowhere to go, which is why a graceful
// shutdown drains first and gets here only once the pipeline is empty.
//
// It waits for the loops, so a delivery still running finishes and settles
// normally — with one exception. A Drain that timed out has already given that
// delivery a bounded chance to finish, and it did not take it; waiting here
// would hand the same expression a second wait with no bound at all, and this
// time nothing would interrupt it. So Stop cancels and reports rather than
// blocking, because the one thing a stuck action must never be able to do is
// stop the process from exiting.
//
// Whether it is *still* running is checked here rather than remembered from the
// drain. A whole phase separates the two, and a delivery that overran the
// drain's deadline by a moment has usually finished by now — reporting one that
// has not, when it has, is an error in the log of a shutdown that went fine.
//
// Repeated calls repeat the answer, as Drain's do, so a caller that stops twice
// is not told the second time that everything was well. A receiver stopped this
// way cannot be started again until that delivery finishes, because the
// goroutine still owns the loops' WaitGroup; Start says so.
func (r *SQSReceiver) Stop(ctx context.Context) error {
	r.mu.Lock()
	stopWork := r.stopWork
	r.stopRead, r.stopWork = nil, nil
	r.mu.Unlock()

	if stopWork == nil {
		return r.stoppedWithDeliveryRunning()
	}
	// Cancelling work cancels polling with it: the read context is derived from
	// this one, so a Stop that was not preceded by a Drain still ends the loops.
	stopWork()

	if err := r.stoppedWithDeliveryRunning(); err != nil {
		return err
	}
	r.wg.Wait()

	r.logger.Info("sqs receiver stopped", zap.String("queue", r.queueName))
	return nil
}

func (r *SQSReceiver) stoppedWithDeliveryRunning() error {
	if !r.stillRunning() {
		return nil
	}
	return fmt.Errorf("sqs receiver %q: stopped with a delivery still running", r.queueName)
}

// pollLoop is the per-goroutine receive/process/delete loop.
//
// The two contexts are the same lifetime until a drain separates them. readCtx
// bounds the ReceiveMessage call and decides when the loop exits; workCtx is
// what every delivery and every settle runs on, and outlives readCtx by the
// length of the shutdown. Passing readCtx to a delivery would mean draining
// cancelled the work it was waiting for, and cancelled the deletion it was
// waiting for the work to produce.
//
// The batch in hand is finished either way: the exit check is at the top of the
// loop, so a drain that lands mid-batch delivers the rest of it and then stops.
// With max_messages above one that is up to ten messages, which is the whole
// reason it is worth finishing rather than abandoning.
func (r *SQSReceiver) pollLoop(readCtx, workCtx context.Context) {
	defer r.wg.Done()

	backoff := time.Second

	for {
		if readCtx.Err() != nil {
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

		result, err := r.client.ReceiveMessage(readCtx, input)
		if err != nil {
			if readCtx.Err() != nil {
				return // normal shutdown
			}
			r.logger.Error("sqs receiver: ReceiveMessage failed",
				zap.String("queue", r.queueName),
				zap.Error(err),
			)
			select {
			case <-readCtx.Done():
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
			r.processMessage(workCtx, msg, receivedAt)
		}
	}
}

// processMessage deserializes, dispatches, and optionally deletes a
// single SQS message.
func (r *SQSReceiver) processMessage(ctx context.Context, msg sqstypes.Message, receivedAt time.Time) {
	// Extract message attributes → fields map.
	fields := r.extractFields(msg)

	settler, ops := r.newSettler(msg, receivedAt)

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
			// Released, not settled. The message stays in flight at the queue —
			// that is the policy above, and the redrive policy's business — but
			// no delete is ever coming for it, so a shutdown has nothing to
			// wait for.
			ops.release()
			return
		}
	}

	// Resolve vinculum topic.
	topic := r.topicFn(msg, fields)

	// Dispatch to subscriber.
	processStart := time.Now()
	err := r.subscriber.OnEvent(ctx, topic, payload, fields)

	// The settle point. Under auto_delete this deletes a subscriber that
	// handled the message and leaves one that only queued it to settle at its
	// own completion; under manual it does nothing but report a failure,
	// because the configuration asked for the decision.
	//
	// It runs through the same settler a subscriber would have used, so a
	// subscriber that settled the message itself does not have it settled
	// twice: "vinculum deletes for you" is one policy over one mechanism
	// rather than a second path to the queue.
	bus.SettleOnReturn(ctx, r.subscriber, err)

	// An observing subscriber settles nothing and defers to nobody — it saw the
	// message go past. SettleOnReturn returns without acting, so no settle is
	// coming from anywhere and this delivery has to be released by hand or the
	// count never comes back down. It is the third of the three paths through
	// here that reach no settler; the other two are the decode failure above
	// and a message with no receipt handle, which is never counted at all.
	if bus.DispositionOf(r.subscriber) == bus.Observed {
		ops.release()
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		r.logger.Error("sqs receiver: subscriber.OnEvent failed",
			zap.String("queue", r.queueName),
			zap.String("topic", topic),
			zap.Error(err),
		)
		r.metrics.RecordError(ctx, "process", "subscriber")
		// Not deleted — the message returns to the queue after its visibility
		// timeout, which is what a nack means on SQS.
		return
	}

	r.metrics.RecordConsumed(ctx)
	r.metrics.RecordProcessDuration(ctx, time.Since(processStart))
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
