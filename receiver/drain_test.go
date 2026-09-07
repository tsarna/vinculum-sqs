package receiver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
)

// feeder is a ReceiveMessage that serves each queued batch once and then
// answers empty, so a test can say exactly how many messages exist and when.
type feeder struct {
	batches chan []sqstypes.Message
	polls   atomic.Int64
}

func newFeeder() *feeder {
	return &feeder{batches: make(chan []sqstypes.Message, 16)}
}

func (f *feeder) offer(bodies ...string) {
	msgs := make([]sqstypes.Message, 0, len(bodies))
	for i, body := range bodies {
		id := "msg-" + body
		receipt := "receipt-" + body + "-" + string(rune('a'+i))
		b := body
		msgs = append(msgs, sqstypes.Message{MessageId: &id, ReceiptHandle: &receipt, Body: &b})
	}
	f.batches <- msgs
}

func (f *feeder) receive(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.polls.Add(1)
	select {
	case batch := <-f.batches:
		return &sqs.ReceiveMessageOutput{Messages: batch}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

// drainFixture starts a receiver over the feeder, with pollers poll loops.
// It registers no Stop cleanup beyond a best-effort one: these tests stop the
// receiver themselves, at the point in the sequence they are about.
func drainFixture(t *testing.T, autoDelete bool, sub bus.Subscriber, pollers int) (*SQSReceiver, *feeder, *atomic.Int64) {
	t.Helper()
	f := newFeeder()
	var deletes atomic.Int64

	mock := &mockSQSReceive{
		receiveFunc: f.receive,
		// Honours its context, as the real client does. A delete issued on a
		// cancelled context is the failure a drain that cancelled the work
		// would produce, and a mock that ignored ctx would hide it.
		deleteFunc: func(ctx context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			deletes.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(autoDelete).
		WithConcurrency(pollers).
		Build()
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	return r, f, &deletes
}

// counter records deliveries and settles nothing itself.
type counter struct {
	bus.BaseSubscriber
	seen atomic.Int64
}

func (c *counter) OnEvent(context.Context, string, any, map[string]string) error {
	c.seen.Add(1)
	return nil
}

func (c *counter) wait(t *testing.T, n int64) {
	t.Helper()
	require.Eventually(t, func() bool { return c.seen.Load() >= n },
		3*time.Second, 5*time.Millisecond, "the receiver never delivered %d messages", n)
}

// The point of a drain: messages already received are finished, and no more are
// taken on. Stopping does both at once, which is why a shutdown that only has
// Stop cannot stop consuming before it disconnects.
func TestDrainStopsPolling(t *testing.T) {
	c := &counter{}
	r, f, _ := drainFixture(t, true, c, 1)

	f.offer("before")
	c.wait(t, 1)

	require.NoError(t, r.Drain(context.Background()))

	f.offer("after")
	time.Sleep(200 * time.Millisecond) // several of the feeder's own poll windows

	assert.Equal(t, int64(1), c.seen.Load(), "the receiver kept polling after it was drained")
}

// Every poll loop stops, not just the one that happened to be idle. `pollers`
// is the first thing in this design to run more than one, and a drain that
// left any of them reading would leave the process consuming after it said it
// had stopped.
func TestDrainStopsEveryPoller(t *testing.T) {
	c := &counter{}
	r, f, _ := drainFixture(t, true, c, 4)

	f.offer("a")
	c.wait(t, 1)

	// Bounded, so a poller that ignores the drain fails this line with its own
	// message rather than blocking the drain forever and failing the run as a
	// timeout somewhere else.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, r.Drain(ctx), "a poll loop did not stop reading")
	before := f.polls.Load()

	f.offer("b", "c", "d")
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, int64(1), c.seen.Load(), "a poller kept reading after the drain")
	assert.Equal(t, before, f.polls.Load(), "a poller was still calling ReceiveMessage")
}

// A drained receiver can still delete, and the settler it handed out before the
// drain still settles. This is the whole reason draining and stopping are
// separate: the deletion for a message the pipeline is still carrying arrives
// after the poll loops have gone.
func TestDrainLeavesAnOutstandingSettlerAbleToDelete(t *testing.T) {
	sub := &mockSubscriber{}
	r, f, deletes := drainFixture(t, false, sub, 1)

	f.offer("hi")
	require.Eventually(t, func() bool { return len(sub.getEvents()) == 1 },
		3*time.Second, 5*time.Millisecond)

	require.NoError(t, r.Drain(context.Background()))
	require.Equal(t, 1, r.Unsettled(), "nothing settled it yet")
	require.Zero(t, deletes.Load())

	// Settled on the delivery's own context, which is what `inbound::ack(ctx)`
	// does in production — and the only way this test can tell a drain from a
	// stop. A drain that cancelled the work context would leave this delete
	// with nowhere to go.
	event := sub.getEvents()[0]
	settler := bus.SettlerFromContext(event.Ctx)
	require.NotNil(t, settler)
	settled, err := settler.Ack(event.Ctx)
	require.NoError(t, err, "the delivery's context was cancelled by the drain")
	assert.True(t, settled)

	assert.Equal(t, int64(1), deletes.Load(), "the delete did not reach SQS")
	assert.Equal(t, 0, r.Unsettled())
}

// What the shutdown phase reads. Under manual settle nothing deletes the
// message until the configuration does, so the count is what says the process
// still owes the queue an answer.
func TestUnsettledCountsWhatIsStillOwed(t *testing.T) {
	sub := &mockSubscriber{}
	r, f, _ := drainFixture(t, false, sub, 1)

	assert.Equal(t, 0, r.Unsettled())

	f.offer("one", "two")
	require.Eventually(t, func() bool { return len(sub.getEvents()) == 2 },
		3*time.Second, 5*time.Millisecond)
	assert.Equal(t, 2, r.Unsettled())

	events := sub.getEvents()
	_, err := bus.SettlerFromContext(events[0].Ctx).Ack(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, r.Unsettled())

	// A nack sends nothing to SQS — the message returns when its visibility
	// window lapses — but this process is done with it, so it stops being
	// something a shutdown waits for.
	_, err = bus.SettlerFromContext(events[1].Ctx).Nack(context.Background(), "no")
	require.NoError(t, err)
	assert.Equal(t, 0, r.Unsettled())

	require.NoError(t, r.Drain(context.Background()))
}

// Under auto the framework settles when the work finishes, so the count comes
// back to zero on its own and a shutdown waits for nothing.
func TestUnsettledReturnsToZeroWhenTheFrameworkSettles(t *testing.T) {
	c := &counter{}
	r, f, _ := drainFixture(t, true, c, 1)

	f.offer("hi")
	c.wait(t, 1)

	assert.Eventually(t, func() bool { return r.Unsettled() == 0 },
		2*time.Second, 10*time.Millisecond,
		"an automatically settled delivery stayed on the books")
}

// A message with no receipt handle cannot be deleted at all, whatever the ack
// mode: there is no token to delete it with, so the framework settle point
// leaves it alone and nothing else will ever settle it. Counting it would
// inflate the count permanently, and every later shutdown would wait out its
// whole budget for a message nobody can answer for.
func TestAMessageWithNoReceiptHandleIsNeverCounted(t *testing.T) {
	sub := &mockSubscriber{}
	r, _, _ := drainFixture(t, true, sub, 1)
	require.NoError(t, r.Drain(context.Background()))

	id, body := "msg-1", `"hi"`
	r.processMessage(context.Background(), sqstypes.Message{MessageId: &id, Body: &body}, time.Now())

	require.Len(t, sub.getEvents(), 1, "the message should still be delivered")
	assert.Equal(t, 0, r.Unsettled())
}

// An SQS receipt handle expires with its visibility window, and this is the
// only settler in the family that can go stale — a Redis entry ID identifies
// the entry itself and never expires. Past the window the message has gone back
// on the queue and may already be somewhere else, so this receiver can no
// longer settle it and must stop waiting for a settle that cannot arrive.
//
// The route matters: the settler asks Valid() *before* it runs Ack or Nack, and
// abandons the delivery when the answer is no. So a stale delivery never
// reaches the release those two carry, and a count that only released there
// would keep the message forever — with every later shutdown spending its whole
// budget on it. Under `ack = "manual"` that is not an edge case: a
// settle_timeout longer than the queue's visibility timeout makes every
// unsettled message take this path.
func TestUnsettledLetsGoOfAMessageWhoseHandleHasExpired(t *testing.T) {
	sub := &mockSubscriber{}
	r, _, _ := drainFixture(t, false, sub, 1)
	require.NoError(t, r.Drain(context.Background()))

	// Received a full visibility window ago, so the handle is already expired.
	id, receipt, body := "msg-1", "receipt-1", `"hi"`
	r.processMessage(context.Background(),
		sqstypes.Message{MessageId: &id, ReceiptHandle: &receipt, Body: &body},
		time.Now().Add(-31*time.Second))

	require.Len(t, sub.getEvents(), 1)
	settler := bus.SettlerFromContext(sub.getEvents()[0].Ctx)
	require.NotNil(t, settler)

	settled, err := settler.Ack(context.Background())
	require.Error(t, err, "an expired receipt handle should refuse the delete")
	assert.False(t, settled)

	assert.Equal(t, 0, r.Unsettled(),
		"a message this receiver can no longer settle stayed on the books")
}

// The count is decremented once per message however many times a release fires,
// and it fires from more places than the settles do: Valid() releases on its
// false branch, and the settler asks Valid() before a keepalive as well as
// before a settle. So an expired handle reaches the release twice by the
// plainest route there is.
//
// A count that went negative would *subtract* from the shutdown phase's total —
// it sums every holder — so one message could cancel another holder's real
// backlog and let the wait end a sampling interval early.
func TestAnExpiredMessageIsNotReleasedTwice(t *testing.T) {
	sub := &mockSubscriber{}
	r, _, _ := drainFixture(t, false, sub, 1)
	require.NoError(t, r.Drain(context.Background()))

	id, receipt, body := "msg-1", "receipt-1", `"hi"`
	r.processMessage(context.Background(),
		sqstypes.Message{MessageId: &id, ReceiptHandle: &receipt, Body: &body},
		time.Now().Add(-31*time.Second))

	require.Len(t, sub.getEvents(), 1)
	settler := bus.SettlerFromContext(sub.getEvents()[0].Ctx)
	require.NotNil(t, settler)
	require.Equal(t, 1, r.Unsettled(),
		"nothing has asked Valid() yet, so the handle's expiry is not yet noticed")

	// Both of these ask Valid() and get the same refusal, so both reach the
	// release. Without the guard the second would take the count negative.
	_, err := settler.Keepalive(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, r.Unsettled())

	_, err = settler.Ack(context.Background())
	require.Error(t, err)

	assert.Equal(t, 0, r.Unsettled(), "the count went negative on a second release")
}

// watcher sees deliveries go past and settles none of them, which is a
// disposition of its own rather than a subscriber that forgot.
type watcher struct {
	bus.BaseSubscriber
	seen atomic.Int64
}

func (o *watcher) OnEvent(context.Context, string, any, map[string]string) error {
	o.seen.Add(1)
	return nil
}

func (o *watcher) DeliveryDisposition() bus.Disposition { return bus.Observed }

// The third path that reaches no settler. An observing subscriber makes the
// framework settle point return without acting, so nothing is ever going to
// settle the delivery — and a count that never comes down makes every later
// shutdown wait out its whole budget and warn about a message nobody is
// carrying.
func TestUnsettledDoesNotLeakOnAnObservingSubscriber(t *testing.T) {
	o := &watcher{}
	r, f, _ := drainFixture(t, true, o, 1)

	f.offer("hi")
	require.Eventually(t, func() bool { return o.seen.Load() == 1 },
		3*time.Second, 5*time.Millisecond)

	assert.Eventually(t, func() bool { return r.Unsettled() == 0 },
		2*time.Second, 10*time.Millisecond,
		"a delivery nothing will ever settle stayed on the books")
}

// blocker holds a delivery until it is released, so a test can be sure a poll
// loop is mid-batch when it drains, and records what the delivery's own context
// looked like on the far side of the wait.
type blocker struct {
	bus.BaseSubscriber
	entered         chan struct{}
	release         chan struct{}
	errAfterRelease atomic.Pointer[error]
}

func newBlocker() *blocker {
	return &blocker{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

// entered is signalled rather than closed, so a test that restarts a receiver
// past this subscriber does not panic on a second delivery.
func (b *blocker) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	err := ctx.Err()
	b.errAfterRelease.Store(&err)
	return nil
}

// Draining must not cancel the work it is waiting for. Polling and delivering
// share one context until this splits them, and cancelling that one context to
// stop the loops would abort the delivery in flight and, with it, the deletion
// that delivery was about to produce — a shutdown breaking exactly the message
// it was trying to let finish.
func TestDrainDoesNotCancelTheDeliveryInFlight(t *testing.T) {
	b := newBlocker()
	r, f, _ := drainFixture(t, true, b, 1)

	f.offer("hi")
	<-b.entered

	drained := make(chan error, 1)
	go func() { drained <- r.Drain(context.Background()) }()

	// Long enough that a drain which cancels the delivery has already done so.
	time.Sleep(100 * time.Millisecond)
	close(b.release)

	require.NoError(t, <-drained)
	got := b.errAfterRelease.Load()
	require.NotNil(t, got)
	assert.NoError(t, *got, "the drain cancelled the context the delivery was running on")
}

// Delivery runs user-supplied work, so a drain that waited for it
// unconditionally would hand one stuck expression the power to stop a process
// from exiting. The caller's context is the bound.
func TestDrainIsBoundedByItsContext(t *testing.T) {
	b := newBlocker()
	r, f, _ := drainFixture(t, true, b, 1)
	defer close(b.release)

	f.offer("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := r.Drain(ctx)
	require.Error(t, err, "drain waited for an action that never returned")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	// And it stays reported. A second call must not answer "drained cleanly"
	// for a receiver whose delivery is still running.
	assert.Error(t, r.Drain(context.Background()))
}

// The bound above is worth nothing if the same stuck action then meets an
// unbounded wait one phase later. Stop cancels and reports instead — a delivery
// that ignored a bounded chance to finish does not get an unbounded one, and
// the process exits.
func TestStopDoesNotWaitAgainForADeliveryTheDrainGaveUpOn(t *testing.T) {
	b := newBlocker()
	r, f, _ := drainFixture(t, true, b, 1)
	defer close(b.release)

	f.offer("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx))

	stopped := make(chan error, 1)
	go func() { stopped <- r.Stop(context.Background()) }()

	select {
	case err := <-stopped:
		assert.Error(t, err, "stopping past a running delivery should say so")
		// And it keeps saying so, symmetrically with Drain.
		assert.Error(t, r.Stop(context.Background()))
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on the delivery the drain had already given up on")
	}
}

// A whole phase runs between Drain and Stop — quiesce, bounded at ten seconds
// of its own — so a delivery that overran the drain's deadline by a moment has
// very likely finished by the time Stop asks. Remembering the drain's verdict
// instead of re-checking puts an error in the log of a shutdown where nothing
// went wrong.
func TestStopReportsCleanlyWhenTheDeliveryFinishedAfterTheDrainGaveUp(t *testing.T) {
	b := newBlocker()
	r, f, _ := drainFixture(t, true, b, 1)

	f.offer("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx), "the drain should have given up")

	// The delivery finishes in the window the real shutdown spends quiescing.
	// Polling Drain is how the test sees that: after a drain has already run,
	// the call reads the answer and changes nothing.
	close(b.release)
	assert.Eventually(t, func() bool { return r.Drain(context.Background()) == nil },
		2*time.Second, 10*time.Millisecond,
		"Drain kept reporting a timeout for a delivery that had finished")

	assert.NoError(t, r.Stop(context.Background()),
		"Stop reported a delivery still running that had already finished")
}

// `wg` is one WaitGroup for the life of the receiver, and a Stop that gave up
// on a delivery leaves that delivery's goroutine still holding it. Starting
// again over the top would leave the counter high, and the next Stop would
// block forever on the abandoned part — the unbounded wait this whole
// arrangement removes, reappearing one cycle later where nothing is left to
// report it. Start refuses instead, and stops refusing once the goroutine goes.
func TestStartRefusesWhileAnAbandonedDeliveryStillOwnsTheLoop(t *testing.T) {
	b := newBlocker()
	r, f, _ := drainFixture(t, true, b, 1)

	f.offer("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx))
	require.Error(t, r.Stop(context.Background()))

	require.Error(t, r.Start(context.Background()),
		"starting again would leave the next Stop waiting on the abandoned goroutine")

	close(b.release)
	assert.Eventually(t, func() bool { return r.Start(context.Background()) == nil },
		2*time.Second, 10*time.Millisecond,
		"the refusal outlived the delivery it was about")
	require.NoError(t, r.Stop(context.Background()))
}

// A second Drain arriving while the first is still waiting must not report a
// clean drain, which is what it did while the waiter channel was published only
// on the timeout path. Teardown drains once, so this is a contract about the
// method rather than a shape the process produces.
func TestAConcurrentDrainDoesNotReportADrainThatHasNotHappened(t *testing.T) {
	b := newBlocker()
	r, f, _ := drainFixture(t, true, b, 1)
	defer close(b.release)

	f.offer("hi")
	<-b.entered

	first := make(chan error, 1)
	go func() { first <- r.Drain(context.Background()) }()

	// Long enough that the first Drain is certainly waiting.
	time.Sleep(100 * time.Millisecond)
	assert.Error(t, r.Drain(context.Background()),
		"a second drain reported success while the first was still waiting")
}

// Teardown calls both, in that order, and a receiver that was never started is
// torn down along with everything else. None of that may panic or block.
func TestDrainAndStopComposeInAnyOrder(t *testing.T) {
	c := &counter{}
	r, _, _ := drainFixture(t, true, c, 2)

	require.NoError(t, r.Drain(context.Background()))
	require.NoError(t, r.Drain(context.Background()))
	require.NoError(t, r.Stop(context.Background()))
	require.NoError(t, r.Stop(context.Background()))
	require.NoError(t, r.Drain(context.Background()))

	fresh, err := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/never-started").
		WithSubscriber(c).
		Build()
	require.NoError(t, err)
	assert.NoError(t, fresh.Drain(context.Background()))
	assert.NoError(t, fresh.Stop(context.Background()))
}
