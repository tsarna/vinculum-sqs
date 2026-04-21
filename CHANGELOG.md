# Changelog

## Unreleased

### Removed

- **Batching (`SendMessageBatch`) removed from sender.** The blocking-submit design
  meant a single-goroutine producer (which is how vinculum's event bus dispatches)
  could never fill a batch — each `OnEvent` blocked until the batch flushed, preventing
  the next message from being enqueued. The result was batch-size-1 on timer expiry:
  added latency with no throughput benefit. Batching may return in a future version with
  a non-blocking accumulator design, pending concrete multi-producer use cases.
  - Removed `Batcher`, `BatchConfig`, `SQSBatchAPI`, `SQSBatchSendAPI` types
  - Removed `SenderBuilder.WithBatchConfig()` method
  - Removed `SenderMetrics.RecordBatchSize()` and `messaging.batch.message_count` metric
  - `Start()` and `Stop()` on `SQSSender` are now no-ops (reserved for future use)
