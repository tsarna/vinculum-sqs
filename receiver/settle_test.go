package receiver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// settlerSubscriber captures the settler on each delivery's context, so a test
// can settle exactly as a subscription three bus hops away would.
type settlerSubscriber struct {
	mockSubscriber
	settler bus.Settler
}

func (s *settlerSubscriber) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	s.settler = bus.SettlerFromContext(ctx)
	return s.mockSubscriber.OnEvent(ctx, topic, msg, fields)
}

func settleMessage() sqstypes.Message {
	msgID, receipt, body := "msg-1", "receipt-1", `{"k":"v"}`
	return sqstypes.Message{MessageId: &msgID, ReceiptHandle: &receipt, Body: &body}
}

// The delivery carries its own acknowledgement, and nothing in `fields` says
// how to settle it. A subscriber that never learns it is reading SQS can still
// delete the message it handled.
func TestDeliveryCarriesASettler(t *testing.T) {
	var deleted atomic.Int32
	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	sub := &settlerSubscriber{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(false).
		Build()
	require.NoError(t, err)

	r.processMessage(context.Background(), settleMessage(), time.Now())

	require.NotNil(t, sub.settler, "the delivery context should carry a settler")
	assert.Zero(t, deleted.Load(), "nothing should be deleted until something settles it")

	settled, err := sub.settler.Ack(context.Background())
	require.NoError(t, err)
	assert.True(t, settled)
	assert.EqualValues(t, 1, deleted.Load())

	settled, err = sub.settler.Ack(context.Background())
	require.NoError(t, err)
	assert.False(t, settled, "a second ack should report that it did not settle it")
	assert.EqualValues(t, 1, deleted.Load(), "and should not reach the queue again")
}

// Nacking sends nothing. The message becomes visible again when its visibility
// timeout lapses and the queue's own redrive policy decides when it has been
// tried enough — the receiver's configured policy, not the caller's choice.
func TestNackLeavesTheMessageForTheVisibilityTimeout(t *testing.T) {
	var deleted, changedVis atomic.Int32
	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
		changeVisFunc: func(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			changedVis.Add(1)
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}
	core, logs := observer.New(zap.InfoLevel)
	sub := &settlerSubscriber{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(false).
		WithLogger(zap.New(core)).
		Build()
	require.NoError(t, err)

	r.processMessage(context.Background(), settleMessage(), time.Now())

	settled, err := sub.settler.Nack(context.Background(), "schema rejected it")
	require.NoError(t, err)
	assert.True(t, settled)
	assert.Zero(t, deleted.Load(), "a nack must not delete the message")
	assert.Zero(t, changedVis.Load(), "and must not shorten its visibility timeout into a redelivery loop")

	require.Positive(t, logs.Len(), "the reason should reach the log, which is the only place it can go")
	assert.Contains(t, logs.All()[0].ContextMap()["reason"], "schema rejected it")
}

// Keepalive asks for another full visibility window, which is the lease SQS
// has, and moves the deadline that decides whether a later settle is too late.
func TestKeepaliveExtendsTheVisibilityWindow(t *testing.T) {
	var asked atomic.Int32
	var lastTimeout atomic.Int32
	mock := &mockSQSReceive{
		changeVisFunc: func(_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			asked.Add(1)
			lastTimeout.Store(params.VisibilityTimeout)
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}
	sub := &settlerSubscriber{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(false).
		WithVisibilityTimeout(45).
		Build()
	require.NoError(t, err)

	r.processMessage(context.Background(), settleMessage(), time.Now())

	extended, err := sub.settler.Keepalive(context.Background())
	require.NoError(t, err)
	assert.True(t, extended)
	assert.EqualValues(t, 1, asked.Load())
	assert.EqualValues(t, 45, lastTimeout.Load(), "it should ask for the window the receiver actually uses")
}

// A receiver that could not learn the queue's visibility timeout has no window
// length to ask for. Asking for one it invented could shorten the lease rather
// than extend it, so it reports that nothing was extended.
func TestKeepaliveWithoutAKnownWindowDoesNothing(t *testing.T) {
	var asked atomic.Int32
	mock := &mockSQSReceive{
		changeVisFunc: func(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			asked.Add(1)
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}
	sub := &settlerSubscriber{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(false).
		Build()
	require.NoError(t, err) // Start not called, so no visibility timeout is known

	r.processMessage(context.Background(), settleMessage(), time.Now())

	extended, err := sub.settler.Keepalive(context.Background())
	require.NoError(t, err)
	assert.False(t, extended)
	assert.Zero(t, asked.Load())
}

// Past its visibility window the receipt handle is no longer ours: the message
// has gone back on the queue and may already be somewhere else, so deleting on
// this handle would settle work being done again elsewhere. That is a settle
// that must reach nothing, and say why.
func TestASettleAfterTheVisibilityWindowIsRefused(t *testing.T) {
	var deleted atomic.Int32
	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	sub := &settlerSubscriber{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(false).
		WithVisibilityTimeout(30).
		Build()
	require.NoError(t, err)

	// Received 31 seconds ago, on a 30-second window.
	r.processMessage(context.Background(), settleMessage(), time.Now().Add(-31*time.Second))

	settled, err := sub.settler.Ack(context.Background())
	assert.False(t, settled)
	require.Error(t, err)
	assert.True(t, bus.IsStale(err))
	assert.Contains(t, err.Error(), "visibility timeout expired")
	assert.Zero(t, deleted.Load(), "an expired handle must not reach the queue")
}

// The queue is asked once, at Start, for the window it gives a received
// message — the fact that lets a settle know it is too late.
func TestStartLearnsTheQueuesVisibilityTimeout(t *testing.T) {
	var asked atomic.Int32
	mock := &mockSQSReceive{
		attrsFunc: func(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
			asked.Add(1)
			return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{
				string(sqstypes.QueueAttributeNameVisibilityTimeout): "120",
			}}, nil
		},
	}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()
	require.NoError(t, err)

	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	assert.EqualValues(t, 1, asked.Load(), "the queue should be asked once, not per message")
	assert.EqualValues(t, 120, r.visibilityTimeout())
}

// A receiver that sets its own visibility timeout already knows the answer, so
// it does not spend a call — nor need the permission — asking for it.
func TestAnExplicitVisibilityTimeoutIsNotLookedUp(t *testing.T) {
	var asked atomic.Int32
	mock := &mockSQSReceive{
		attrsFunc: func(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
			asked.Add(1)
			return &sqs.GetQueueAttributesOutput{}, nil
		},
	}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(&mockSubscriber{}).
		WithVisibilityTimeout(90).
		Build()
	require.NoError(t, err)

	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	assert.Zero(t, asked.Load())
	assert.EqualValues(t, 90, r.visibilityTimeout())
}

// A queue policy that withholds GetQueueAttributes must not stop a receiver
// that worked yesterday from starting. It degrades to what it had before —
// staleness undetectable, keepalive with no window to ask for — and says so
// once rather than per message.
func TestAnUnreadableVisibilityTimeoutWarnsAndStarts(t *testing.T) {
	mock := &mockSQSReceive{
		attrsFunc: func(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
			return nil, errors.New("AccessDenied")
		},
	}
	core, logs := observer.New(zap.WarnLevel)
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(&mockSubscriber{}).
		WithLogger(zap.New(core)).
		Build()
	require.NoError(t, err)

	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	assert.Zero(t, r.visibilityTimeout(), "an unknown window is zero rather than a guess")
	require.Equal(t, 1, logs.Len())
	assert.Contains(t, logs.All()[0].Message, "visibility timeout")
}

// Automatic deletion runs through the same settler, so a subscriber that
// settled the message itself does not have it settled twice. Auto is a policy
// over the one mechanism rather than a second path to the queue.
func TestAutoDeleteDoesNotSettleWhatTheSubscriberAlreadySettled(t *testing.T) {
	var deleted atomic.Int32
	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	sub := &ackingSubscriber{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/1/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(true).
		Build()
	require.NoError(t, err)

	r.processMessage(context.Background(), settleMessage(), time.Now())

	assert.True(t, sub.settled, "the subscriber should have been the one that settled it")
	assert.EqualValues(t, 1, deleted.Load(), "and the queue should have heard about it once")
}

// ackingSubscriber settles every delivery itself, the way a manual-settle
// configuration does.
type ackingSubscriber struct {
	mockSubscriber
	settled bool
}

func (s *ackingSubscriber) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	settled, err := bus.SettlerFromContext(ctx).Ack(ctx)
	if err != nil {
		return err
	}
	s.settled = settled
	return s.mockSubscriber.OnEvent(ctx, topic, msg, fields)
}
