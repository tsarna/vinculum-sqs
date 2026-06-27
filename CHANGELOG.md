# Changelog

## Unreleased

### Fixed

- **Inbound baggage now reaches `subscriber.OnEvent`.** The receiver extracted
  W3C baggage from SQS message attributes but only used it to link the producer
  span; the baggage was not carried onto the context passed to `OnEvent`, so
  consumers could not read it. It is now copied onto the processing context
  (the consumer span remains a new root linked to the producer — only baggage
  rides along, not the span parent).

## [0.2.0] - 2026-05-27

### Changed

- Changed license to Apache-2.0

## [0.2.0] - 2026-04-21

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
