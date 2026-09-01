package receiver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-bus/subutils"
)

// gatedSubscriber holds the goroutine delivering to it until released, so a
// test can look at the queue while the work is provably still going.
type gatedSubscriber struct {
	bus.BaseSubscriber
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	err         error
}

func newGatedSubscriber() *gatedSubscriber {
	return &gatedSubscriber{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (g *gatedSubscriber) Release() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *gatedSubscriber) OnEvent(context.Context, string, any, map[string]string) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return g.err
}

// queuedReceiver builds a receiver whose subscriber is behind an async queue,
// which is what `queue_size` on the receiver block builds. That queue is the
// reason this change exists: its OnEvent returns the moment the message is
// enqueued, so auto-delete used to fire before anything had handled it.
func queuedReceiver(t *testing.T, target bus.Subscriber, deleted *atomic.Int32) *SQSReceiver {
	t.Helper()

	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	queue := subutils.NewAsyncQueueingSubscriber(target, 10).Start()
	t.Cleanup(func() { queue.Close() })

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(queue).
		WithAutoDelete(true).
		Build()
	require.NoError(t, err)
	return r
}

// The defect, and the reason `queue_size` alongside `ack = "auto"` used to be
// refused outright. Delivery into the queue returns as soon as the message is
// enqueued, so deleting on that return told SQS the message was handled before
// anything had handled it — and a handler failure then had nothing left to
// redeliver.
func TestAutoDeleteWaitsForTheWorkBehindAQueue(t *testing.T) {
	var deleted atomic.Int32
	target := newGatedSubscriber()
	defer target.Release()

	r := queuedReceiver(t, target, &deleted)

	go r.processMessage(context.Background(), settleMessage(), time.Now())

	<-target.entered

	// Never, not once. Deleting on the enqueue's return happens within
	// microseconds of it, so a single check just after the target is entered is
	// a race the wrong behaviour can win. Holding the assertion open for as
	// long as the target is gated is what makes this deterministic.
	assert.Never(t, func() bool { return deleted.Load() > 0 },
		250*time.Millisecond, 25*time.Millisecond,
		"the subscriber is still working; the message must not be deleted yet")

	target.Release()

	assert.Eventually(t, func() bool { return deleted.Load() == 1 },
		3*time.Second, 20*time.Millisecond,
		"the delete should follow the work out of the queue")
}

// The half that matters for at-least-once. A handler that fails behind a queue
// leaves the message on the queue, so it becomes visible again and is
// redelivered — where before it was deleted at the enqueue and the failure lost
// it outright.
func TestAFailureBehindAQueueDoesNotDelete(t *testing.T) {
	var deleted atomic.Int32
	target := newGatedSubscriber()
	target.err = errors.New("the action threw")
	defer target.Release()

	r := queuedReceiver(t, target, &deleted)

	go r.processMessage(context.Background(), settleMessage(), time.Now())

	<-target.entered
	target.Release()

	assert.Never(t, func() bool { return deleted.Load() > 0 },
		500*time.Millisecond, 25*time.Millisecond,
		"a failed handler must leave the message for redelivery, not delete it")
}
