# Changelog

## Unreleased

## [0.4.1] - 2026-08-03

### Changed

- The receiver's decode-error test now checks every `Attrs` key against
  `wire.IsReservedAttr` (added in vinculum-wire v0.5.0, already required). A key that
  collides with one of `DecodeError`'s own fields is dropped by a consumer rather than
  allowed to shadow the fixed field, so its value is silently lost between the receiver
  that set it and whatever reads it — which is what happened to `vinculum-mqtt`'s
  `Attrs["topic"]`, a duplicate of `Topic` that never reached a config. This module's
  keys (`queue`, `message_id`) are and always were clean; the check is what keeps a
  future rename from quietly breaking one.

## [0.4.0] - 2026-07-20

### Changed

- **BREAKING: deserialize failures are no longer swallowed.** `SQSReceiver.processMessage`
  used to log a warning and pass the **raw body string** through as the message payload
  when the configured wire format failed to decode. That happened even when the caller
  explicitly configured `wire.JSON`, so there was no way to say "messages on this queue
  must be JSON". A decode failure is now fatal to the message: it never reaches
  `subscriber.OnEvent`, and the message is not deleted.

  **Attach an SQS redrive policy.** Because the message is not deleted it becomes visible
  again after the visibility timeout and is redelivered indefinitely. A
  [redrive policy](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
  moves a persistently malformed message to a dead-letter queue after
  `maxReceiveCount` attempts. This library cannot see the queue's redrive policy, so it
  cannot warn you when one is missing.

  Callers wanting best-effort decoding should use `wire.Auto`, which never fails (it yields
  a `string` for anything it can't parse as JSON) — which for this receiver is close to the
  old behavior, since the old fallback was also a string.

- Requires `github.com/tsarna/vinculum-wire` v0.3.0 for the `DecodeError` /
  `DecodeErrorHook` types.

### Added

- `WithDecodeErrorHook(wire.DecodeErrorHook)` on the receiver builder. The hook observes a
  decode failure — it receives the raw body, the error, the format name, the extracted
  fields, and the queue name and message ID — but cannot suppress it: the message is left
  undeleted either way. nil (the default) means no observer.

- A `vinculum.messaging.errors` counter, with `messaging.operation.name` and `error.type`
  attributes. The receiver previously had no error instrument at all. Besides deserialize
  failures (`error.type = "deserialize"`), it also covers the subscriber-failure and
  DeleteMessage-failure paths, which were silent before.

## [0.3.1] - 2026-06-27

### Fixed

- **Inbound baggage now reaches `subscriber.OnEvent`.** The receiver extracted
  W3C baggage from SQS message attributes but only used it to link the producer
  span; the baggage was not carried onto the context passed to `OnEvent`, so
  consumers could not read it. It is now copied onto the processing context
  (the consumer span remains a new root linked to the producer — only baggage
  rides along, not the span parent).

## [0.3.0] - 2026-05-27

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
