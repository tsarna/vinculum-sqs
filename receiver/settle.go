package receiver

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// messageSettleOps settles one received SQS message. The receiver builds one
// per message and puts the settler it wraps on the delivery's context, so
// anything downstream — past transforms, an async queue, and any number of bus
// hops — can settle the message without knowing it came from SQS, and without
// being handed a receipt handle it might be tempted to keep.
type messageSettleOps struct {
	receiver      *SQSReceiver
	receiptHandle string
	messageID     string

	// mu guards deadline, which Keepalive moves.
	mu sync.Mutex
	// deadline is when the receipt handle stops being usable, or the zero time
	// when the receiver could not learn the queue's visibility timeout.
	deadline time.Time
}

// Ack deletes the message, which is how SQS is told it was handled.
func (o *messageSettleOps) Ack(ctx context.Context) error {
	return o.receiver.DeleteMsg(ctx, o.receiptHandle)
}

// Nack sends nothing. An SQS message is not-acknowledged by simply not being
// deleted: it becomes visible again when its visibility timeout lapses, its
// receive count advances, and the queue's own redrive policy decides when it
// has been tried enough — which is the receiver's configured policy, and
// deliberately not the caller's choice.
//
// Returning it immediately by setting the visibility timeout to zero was
// considered and rejected: it turns a configuration that nacks into a
// redelivery loop running as fast as the queue can serve it, bounded only by
// the redrive policy. The receive count advances either way, so the only thing
// that would differ is the delay.
//
// The reason therefore reaches the log and nowhere else. Nothing in the SQS
// nack path carries a payload, so there is no header for it to become.
func (o *messageSettleOps) Nack(_ context.Context, reason string) error {
	o.receiver.logger.Info("sqs receiver: message nacked, left for the visibility timeout",
		zap.String("queue", o.receiver.queueName),
		zap.String("message_id", o.messageID),
		zap.String("reason", reason))
	return nil
}

// Keepalive gives the message another full visibility window, which is the
// lease SQS has. It reports whether anything was extended: a receiver that
// could not learn the queue's visibility timeout has no window length to ask
// for, and says so rather than inventing one — setting a shorter timeout than
// the queue's own would cut the lease rather than extend it.
func (o *messageSettleOps) Keepalive(ctx context.Context) (bool, error) {
	secs := o.receiver.visibilityTimeout()
	if secs <= 0 {
		return false, nil
	}
	if err := o.receiver.ExtendVisibility(ctx, o.receiptHandle, secs); err != nil {
		return false, err
	}
	o.mu.Lock()
	o.deadline = time.Now().Add(time.Duration(secs) * time.Second)
	o.mu.Unlock()
	return true, nil
}

// Valid reports whether the receipt handle is still inside its visibility
// window. Past it the message has gone back on the queue and may already be
// somewhere else, so deleting on this handle settles work that is being done
// again elsewhere.
//
// A receiver that could not learn the queue's visibility timeout has no
// deadline to check and reports valid: a settle attempt then gets a real error
// from SQS, which is a better answer than one this package invented.
func (o *messageSettleOps) Valid() (bool, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.deadline.IsZero() || time.Now().Before(o.deadline) {
		return true, ""
	}
	return false, "visibility timeout expired"
}

// newSettler returns the settler for one received message. receivedAt is when
// ReceiveMessage returned it, which is when its visibility window started.
//
// Under auto_delete the settler is marked as settled by the framework, which is
// the same boolean this receiver has always carried and a different thing to do
// with it. It used to mean "delete once delivery returns", which is exact only
// while delivery is synchronous — a queue or a bus hop downstream returns as
// soon as the message is enqueued. Now it means "whoever finishes the work
// settles this", and the deletion follows the work however many hops away it
// happens.
func (r *SQSReceiver) newSettler(msg sqstypes.Message, receivedAt time.Time) bus.Settler {
	ops := &messageSettleOps{receiver: r}
	if msg.ReceiptHandle != nil {
		ops.receiptHandle = *msg.ReceiptHandle
	}
	if msg.MessageId != nil {
		ops.messageID = *msg.MessageId
	}
	if secs := r.visibilityTimeout(); secs > 0 {
		ops.deadline = receivedAt.Add(time.Duration(secs) * time.Second)
	}

	// A message with no receipt handle cannot be deleted at all, so marking it
	// framework-settled would promise something this receiver cannot keep.
	if r.autoDelete && msg.ReceiptHandle != nil {
		return bus.NewSettler(ops, bus.AutoSettle())
	}
	return bus.NewSettler(ops)
}

// visibilityTimeout returns the window a received message actually gets, in
// seconds, or zero when it is not known.
func (r *SQSReceiver) visibilityTimeout() int32 {
	if r.visTimeout != nil {
		return *r.visTimeout
	}
	return r.queueVisTimeout.Load()
}

// learnVisibilityTimeout asks the queue for its default visibility timeout,
// for the case where the receiver does not override it on every receive.
//
// It is what lets a settle know it is too late, so it is worth one call at
// startup — but it is not worth failing to start over. A queue policy that
// withholds GetQueueAttributes leaves the receiver working exactly as before,
// with staleness undetectable and Keepalive unable to name a window; the
// warning says so once rather than per message.
func (r *SQSReceiver) learnVisibilityTimeout(ctx context.Context) {
	if r.visTimeout != nil {
		return
	}
	out, err := r.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &r.queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameVisibilityTimeout},
	})
	if err == nil {
		if secs, convErr := strconv.Atoi(out.Attributes[string(sqstypes.QueueAttributeNameVisibilityTimeout)]); convErr == nil && secs > 0 {
			r.queueVisTimeout.Store(int32(secs))
			return
		}
		err = errNoVisibilityAttribute
	}
	r.logger.Warn("sqs receiver: could not read the queue's visibility timeout; "+
		"a settle that arrives too late cannot be detected, and keepalive has no window to ask for",
		zap.String("queue", r.queueName),
		zap.Error(err))
}

// errNoVisibilityAttribute reports a GetQueueAttributes response that answered
// without the attribute asked for.
var errNoVisibilityAttribute = errVisibilityAttribute("queue did not report a VisibilityTimeout attribute")

type errVisibilityAttribute string

func (e errVisibilityAttribute) Error() string { return string(e) }
