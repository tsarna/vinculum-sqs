package sender

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/zap"
)

// SQSBatchAPI is the subset of the SQS client API used by the batcher.
type SQSBatchAPI interface {
	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

// BatchConfig holds batching parameters.
type BatchConfig struct {
	MaxSize  int
	MaxDelay time.Duration
}

// batchEntry holds a single message and a channel to report its result.
type batchEntry struct {
	input  sqstypes.SendMessageBatchRequestEntry
	result chan error
	ctx    context.Context
}

// Batcher buffers SendMessage calls and flushes them via SendMessageBatch.
// OnEvent blocks until the batch containing its message has been sent.
type Batcher struct {
	client   SQSBatchAPI
	queueURL string
	maxSize  int
	maxDelay time.Duration
	logger   *zap.Logger

	mu      sync.Mutex
	entries []batchEntry
	timer   *time.Timer
	nextID  int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBatcher creates a new batcher. Call Start() to begin the flush loop.
func NewBatcher(client SQSBatchAPI, queueURL string, cfg BatchConfig, logger *zap.Logger) *Batcher {
	maxSize := cfg.MaxSize
	if maxSize <= 0 || maxSize > 10 {
		maxSize = 10
	}
	maxDelay := cfg.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 100 * time.Millisecond
	}
	return &Batcher{
		client:   client,
		queueURL: queueURL,
		maxSize:  maxSize,
		maxDelay: maxDelay,
		logger:   logger,
	}
}

// Start begins the background flush timer.
func (b *Batcher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(1)
	go b.flushLoop(ctx)
}

// Stop flushes any remaining entries and stops the batcher.
func (b *Batcher) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()

	// Final flush of any remaining entries.
	b.mu.Lock()
	remaining := b.entries
	b.entries = nil
	b.mu.Unlock()

	if len(remaining) > 0 {
		b.flushEntries(context.Background(), remaining)
	}
}

// Submit adds a message to the batch and blocks until it has been sent.
// Returns nil on success, or the SQS error for this specific entry on
// partial batch failure.
func (b *Batcher) Submit(ctx context.Context, entry sqstypes.SendMessageBatchRequestEntry) error {
	ch := make(chan error, 1)

	b.mu.Lock()
	b.nextID++
	entry.Id = aws.String(strconv.FormatInt(b.nextID, 10))
	b.entries = append(b.entries, batchEntry{input: entry, result: ch, ctx: ctx})
	shouldFlush := len(b.entries) >= b.maxSize
	b.mu.Unlock()

	if shouldFlush {
		b.triggerFlush()
	}

	return <-ch
}

// flushLoop runs in the background and flushes on timer ticks.
func (b *Batcher) flushLoop(ctx context.Context) {
	defer b.wg.Done()

	ticker := time.NewTicker(b.maxDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.triggerFlush()
		}
	}
}

// triggerFlush drains the current batch and sends it.
func (b *Batcher) triggerFlush() {
	b.mu.Lock()
	if len(b.entries) == 0 {
		b.mu.Unlock()
		return
	}
	entries := b.entries
	b.entries = nil
	b.mu.Unlock()

	b.flushEntries(context.Background(), entries)
}

// flushEntries sends a batch and routes per-entry results.
func (b *Batcher) flushEntries(ctx context.Context, entries []batchEntry) {
	// Build batch request.
	batchEntries := make([]sqstypes.SendMessageBatchRequestEntry, len(entries))
	for i, e := range entries {
		batchEntries[i] = e.input
	}

	result, err := b.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &b.queueURL,
		Entries:  batchEntries,
	})

	if err != nil {
		// Whole batch failed — report to all entries.
		for _, e := range entries {
			e.result <- fmt.Errorf("sqs batch send: %w", err)
		}
		return
	}

	// Build a map of failed entry IDs → errors.
	failures := make(map[string]error, len(result.Failed))
	for _, f := range result.Failed {
		var msg string
		if f.Message != nil {
			msg = *f.Message
		}
		var code string
		if f.Code != nil {
			code = *f.Code
		}
		failures[*f.Id] = fmt.Errorf("sqs batch entry %s failed: %s (%s, sender_fault=%v)", *f.Id, msg, code, f.SenderFault)
	}

	// Route results to each entry's channel.
	for _, e := range entries {
		if ferr, ok := failures[*e.input.Id]; ok {
			e.result <- ferr
		} else {
			e.result <- nil
		}
	}
}
