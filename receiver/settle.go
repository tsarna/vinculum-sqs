package receiver

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
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

	// counted is whether this delivery was added to the receiver's unsettled
	// count. A message with no receipt handle cannot be deleted at all, so no
	// settle is ever coming for it and a shutdown has nothing to wait for.
	counted bool

	// released guards the count against being decremented twice for one
	// delivery. The settler above deduplicates settles, but it releases its
	// claim when an op returns an error, so a failed delete can be retried and
	// reach Ack a second time.
	released atomic.Bool

	// mu guards deadline, which Keepalive moves.
	mu sync.Mutex
	// deadline is when the receipt handle stops being usable, or the zero time
	// when the receiver could not learn the queue's visibility timeout.
	deadline time.Time
}

// release drops this delivery from the receiver's unsettled count, once.
//
// It runs even when the op that called it failed. A message whose delete did
// not reach SQS is genuinely still unsettled, but the count exists to tell a
// shutdown when to stop waiting, and waiting longer for a queue that is
// refusing deletes does not produce one.
func (o *messageSettleOps) release() {
	if o.counted && o.released.CompareAndSwap(false, true) {
		o.receiver.unsettled.Add(-1)
	}
}

// Ack deletes the message, which is how SQS is told it was handled.
func (o *messageSettleOps) Ack(ctx context.Context) error {
	defer o.release()
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
	defer o.release()
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
//
// Saying no is also where the delivery stops being this receiver's to settle,
// so it is where the unsettled count lets go of it. The settler asks this
// before every settle and every keepalive and abandons the delivery when the
// answer is no — never reaching Ack or Nack, and so never reaching the release
// those two carry. Without this, the count keeps a message the visibility
// window has already handed back to the queue, and keeps it forever: every
// later shutdown would then spend its whole budget waiting for an
// acknowledgement that cannot be sent, and warn about a message nobody is
// carrying.
//
// This is the one settler in the family that can go stale — a Redis entry ID
// identifies the entry itself and never expires — which is why the shape here
// needs a release the others do not.
func (o *messageSettleOps) Valid() (bool, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.deadline.IsZero() || time.Now().Before(o.deadline) {
		return true, ""
	}
	o.release()
	return false, "visibility timeout expired"
}

// newSettler returns the settler for one received message, and the ops behind
// it, so the caller can release a delivery that never reaches the subscriber
// and so is never settled through the settler at all. receivedAt is when
// ReceiveMessage returned it, which is when its visibility window started.
//
// Handing one out is what makes the delivery unsettled, so the count is
// incremented here rather than at the delivery's other end — but only for a
// message this receiver can actually settle. Teardown waits on that count: a
// message a `queue_size` queue is still carrying is deleted long after the poll
// loop that received it has stopped, and the delete has to find a usable client
// when it does.
//
// Under auto_delete the settler is marked as settled by the framework, which is
// the same boolean this receiver has always carried and a different thing to do
// with it. It used to mean "delete once delivery returns", which is exact only
// while delivery is synchronous — a queue or a bus hop downstream returns as
// soon as the message is enqueued. Now it means "whoever finishes the work
// settles this", and the deletion follows the work however many hops away it
// happens.
func (r *SQSReceiver) newSettler(msg sqstypes.Message, receivedAt time.Time) (bus.Settler, *messageSettleOps) {
	ops := &messageSettleOps{receiver: r}
	if msg.ReceiptHandle != nil {
		ops.receiptHandle = *msg.ReceiptHandle
		ops.counted = true
		r.unsettled.Add(1)
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
		return bus.NewSettler(ops, bus.AutoSettle()), ops
	}
	return bus.NewSettler(ops), ops
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
