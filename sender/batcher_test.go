package sender

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockBatchSQS struct {
	batchFunc func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

func (m *mockBatchSQS) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if m.batchFunc != nil {
		return m.batchFunc(ctx, params, optFns...)
	}
	// Default: all succeed
	successful := make([]sqstypes.SendMessageBatchResultEntry, len(params.Entries))
	for i, e := range params.Entries {
		msgID := "msg-" + *e.Id
		successful[i] = sqstypes.SendMessageBatchResultEntry{
			Id:        e.Id,
			MessageId: &msgID,
		}
	}
	return &sqs.SendMessageBatchOutput{Successful: successful}, nil
}

func TestBatcher_FlushOnMaxSize(t *testing.T) {
	var batchCount atomic.Int32
	var batchSizes []int
	var mu sync.Mutex

	mock := &mockBatchSQS{
		batchFunc: func(_ context.Context, params *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
			batchCount.Add(1)
			mu.Lock()
			batchSizes = append(batchSizes, len(params.Entries))
			mu.Unlock()

			successful := make([]sqstypes.SendMessageBatchResultEntry, len(params.Entries))
			for i, e := range params.Entries {
				msgID := "msg-" + *e.Id
				successful[i] = sqstypes.SendMessageBatchResultEntry{
					Id:        e.Id,
					MessageId: &msgID,
				}
			}
			return &sqs.SendMessageBatchOutput{Successful: successful}, nil
		},
	}

	b := NewBatcher(mock, "https://sqs.us-east-1.amazonaws.com/123/q", BatchConfig{
		MaxSize:  3,
		MaxDelay: 10 * time.Second, // long delay so only size triggers flush
	}, zap.NewNop())
	b.Start()
	defer b.Stop()

	// Submit 3 messages concurrently — should trigger flush at 3.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := "msg"
			err := b.Submit(context.Background(), sqstypes.SendMessageBatchRequestEntry{
				MessageBody: &body,
			})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	assert.GreaterOrEqual(t, batchCount.Load(), int32(1))
}

func TestBatcher_FlushOnMaxDelay(t *testing.T) {
	var batchCount atomic.Int32

	mock := &mockBatchSQS{
		batchFunc: func(_ context.Context, params *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
			batchCount.Add(1)
			successful := make([]sqstypes.SendMessageBatchResultEntry, len(params.Entries))
			for i, e := range params.Entries {
				msgID := "msg-" + *e.Id
				successful[i] = sqstypes.SendMessageBatchResultEntry{
					Id:        e.Id,
					MessageId: &msgID,
				}
			}
			return &sqs.SendMessageBatchOutput{Successful: successful}, nil
		},
	}

	b := NewBatcher(mock, "https://sqs.us-east-1.amazonaws.com/123/q", BatchConfig{
		MaxSize:  10,
		MaxDelay: 50 * time.Millisecond,
	}, zap.NewNop())
	b.Start()
	defer b.Stop()

	// Submit 1 message — should flush on timer.
	body := "msg"
	err := b.Submit(context.Background(), sqstypes.SendMessageBatchRequestEntry{
		MessageBody: &body,
	})
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, batchCount.Load(), int32(1))
}

func TestBatcher_PartialFailure(t *testing.T) {
	mock := &mockBatchSQS{
		batchFunc: func(_ context.Context, params *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
			// First entry succeeds, second fails.
			var successful []sqstypes.SendMessageBatchResultEntry
			var failed []sqstypes.BatchResultErrorEntry
			for i, e := range params.Entries {
				if i == 0 {
					msgID := "msg-ok"
					successful = append(successful, sqstypes.SendMessageBatchResultEntry{
						Id:        e.Id,
						MessageId: &msgID,
					})
				} else {
					code := "InternalError"
					msg := "something went wrong"
					failed = append(failed, sqstypes.BatchResultErrorEntry{
						Id:          e.Id,
						Code:        &code,
						Message:     &msg,
						SenderFault: false,
					})
				}
			}
			return &sqs.SendMessageBatchOutput{
				Successful: successful,
				Failed:     failed,
			}, nil
		},
	}

	b := NewBatcher(mock, "https://sqs.us-east-1.amazonaws.com/123/q", BatchConfig{
		MaxSize:  2,
		MaxDelay: 10 * time.Second,
	}, zap.NewNop())
	b.Start()
	defer b.Stop()

	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := "msg"
			results[idx] = b.Submit(context.Background(), sqstypes.SendMessageBatchRequestEntry{
				MessageBody: &body,
			})
		}(i)
	}
	wg.Wait()

	// One should succeed, one should fail.
	var successes, failures int
	for _, err := range results {
		if err == nil {
			successes++
		} else {
			failures++
		}
	}
	assert.Equal(t, 1, successes, "one entry should succeed")
	assert.Equal(t, 1, failures, "one entry should fail")
}

func TestBatcher_WholeBatchFailure(t *testing.T) {
	mock := &mockBatchSQS{
		batchFunc: func(_ context.Context, _ *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
			return nil, errors.New("network error")
		},
	}

	b := NewBatcher(mock, "https://sqs.us-east-1.amazonaws.com/123/q", BatchConfig{
		MaxSize:  2,
		MaxDelay: 10 * time.Second,
	}, zap.NewNop())
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := "msg"
			errs[idx] = b.Submit(context.Background(), sqstypes.SendMessageBatchRequestEntry{
				MessageBody: &body,
			})
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "network error")
	}
}

func TestBatcher_StopFlushesRemaining(t *testing.T) {
	var batchCount atomic.Int32

	mock := &mockBatchSQS{
		batchFunc: func(_ context.Context, params *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
			batchCount.Add(1)
			successful := make([]sqstypes.SendMessageBatchResultEntry, len(params.Entries))
			for i, e := range params.Entries {
				msgID := "msg-" + *e.Id
				successful[i] = sqstypes.SendMessageBatchResultEntry{
					Id:        e.Id,
					MessageId: &msgID,
				}
			}
			return &sqs.SendMessageBatchOutput{Successful: successful}, nil
		},
	}

	b := NewBatcher(mock, "https://sqs.us-east-1.amazonaws.com/123/q", BatchConfig{
		MaxSize:  100, // very large so timer-based flush happens
		MaxDelay: 50 * time.Millisecond,
	}, zap.NewNop())
	b.Start()

	// Submit a message — don't wait for timer.
	go func() {
		body := "msg"
		_ = b.Submit(context.Background(), sqstypes.SendMessageBatchRequestEntry{
			MessageBody: &body,
		})
	}()

	// Give it a moment to enqueue.
	time.Sleep(10 * time.Millisecond)

	// Stop should flush remaining.
	b.Stop()

	assert.GreaterOrEqual(t, batchCount.Load(), int32(1), "should have flushed on stop")
}

func TestBatcher_DefaultConfig(t *testing.T) {
	b := NewBatcher(&mockBatchSQS{}, "q", BatchConfig{}, zap.NewNop())
	assert.Equal(t, 10, b.maxSize)
	assert.Equal(t, 100*time.Millisecond, b.maxDelay)
}

func TestBatcher_ClampMaxSize(t *testing.T) {
	b := NewBatcher(&mockBatchSQS{}, "q", BatchConfig{MaxSize: 15}, zap.NewNop())
	assert.Equal(t, 10, b.maxSize, "should clamp to 10")

	b2 := NewBatcher(&mockBatchSQS{}, "q", BatchConfig{MaxSize: -1}, zap.NewNop())
	assert.Equal(t, 10, b2.maxSize, "should default to 10")
}

func TestSender_BatchedOnEvent(t *testing.T) {
	var batchCalled atomic.Bool

	mock := &mockBatchAndSendSQS{
		batchFunc: func(_ context.Context, params *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
			batchCalled.Store(true)
			successful := make([]sqstypes.SendMessageBatchResultEntry, len(params.Entries))
			for i, e := range params.Entries {
				msgID := "msg-" + *e.Id
				successful[i] = sqstypes.SendMessageBatchResultEntry{
					Id:        e.Id,
					MessageId: &msgID,
				}
			}
			return &sqs.SendMessageBatchOutput{Successful: successful}, nil
		},
	}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123/test").
		WithBatchConfig(&BatchConfig{MaxSize: 1, MaxDelay: time.Second}).
		Build()
	require.NoError(t, err)

	s.Start()
	defer s.Stop()

	err = s.OnEvent(context.Background(), "test/topic", "hello", nil)
	assert.NoError(t, err)
	assert.True(t, batchCalled.Load(), "should use batch API")
}

// mockBatchAndSendSQS implements both SQSSendAPI and SQSBatchAPI.
type mockBatchAndSendSQS struct {
	sendFunc  func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	batchFunc func(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

func (m *mockBatchAndSendSQS) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, params, optFns...)
	}
	msgID := "test-message-id"
	return &sqs.SendMessageOutput{MessageId: &msgID}, nil
}

func (m *mockBatchAndSendSQS) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if m.batchFunc != nil {
		return m.batchFunc(ctx, params, optFns...)
	}
	successful := make([]sqstypes.SendMessageBatchResultEntry, len(params.Entries))
	for i, e := range params.Entries {
		msgID := "msg-" + *e.Id
		successful[i] = sqstypes.SendMessageBatchResultEntry{
			Id:        e.Id,
			MessageId: &msgID,
		}
	}
	return &sqs.SendMessageBatchOutput{Successful: successful}, nil
}
