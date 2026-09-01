# Changelog

## Unreleased

### Changed

- **`auto_delete` deletes when the work finishes, not when delivery returns.**
  The two are the same thing only while delivery is synchronous. Put an async
  queue, a bus hop, or a state machine downstream and delivery returns the
  moment the message is *enqueued* — so the message was deleted before anything
  had handled it, and a handler that then failed had nothing left to redeliver.

  The receiver now marks its settler as framework-settled and lets whatever
  finishes the work settle it, however many hops away that happens. This makes
  `queue_size` alongside automatic deletion correct, where before it was a way
  to lose messages.

  A failed handler nacks, which on SQS means leaving the message to become
  visible again when its visibility timeout lapses — what used to happen
  implicitly, now as a decision the settler records.

  A message with no receipt handle cannot be deleted at all, so it is never
  marked framework-settled: promising an acknowledgement this receiver cannot
  deliver would leave a delivery that nothing will ever settle.

## [0.5.0] - 2026-08-30

### Added

- **Every delivery carries a settler.** Acknowledgement is a property of the
  delivery, not of the payload and not of the subscriber that handles it, and
  `fields` cannot carry it — the bus rewrites those per subscription. So the
  receiver now puts a `bus.Settler` (vinculum-bus v0.18.0) on the context it
  delivers with, and anything downstream — past transforms, an async queue, and
  any number of bus hops — can settle the message without knowing it came from
  SQS:

  ```go
  if s := bus.SettlerFromContext(ctx); s != nil {
      settled, err := s.Ack(ctx)
  }
  ```

  `Ack` is `DeleteMessage`. `Nack` sends nothing: an SQS message is
  not-acknowledged by simply not being deleted, and the queue's own redrive
  policy decides when it has been tried enough — the receiver's configured
  policy, deliberately not the caller's choice. Returning it immediately with a
  zero visibility timeout was considered and rejected, because it turns a
  configuration that nacks into a redelivery loop running as fast as the queue
  can serve it; the receive count advances either way, so only the delay would
  differ. The reason is logged, which on SQS is the only place it can go.
  `Keepalive` asks for another full visibility window.

- **A settle whose visibility window has lapsed is refused rather than sent**,
  with a `*StaleError` saying so. Past its window the message is back on the
  queue and may already be somewhere else, so deleting on that handle settles
  work being done again elsewhere.

- **The queue's visibility timeout is read once at `Start`**, which is what
  makes the check above possible when the receiver does not set a timeout of its
  own. This adds `GetQueueAttributes` to `SQSReceiveAPI` — a breaking change for
  anyone implementing that interface, in practice a test fake. A queue policy
  that withholds the call does not stop the receiver starting: it logs one
  warning and runs exactly as before, with staleness undetectable and
  `Keepalive` reporting that it extended nothing rather than asking for a window
  length it invented (a guess that is too short would cut the lease rather than
  extend it).

### Changed

- **`auto_delete = true` settles through that same settler**, so a subscriber
  that deleted the message itself does not have it deleted twice. Automatic
  deletion is now one policy over one mechanism rather than a second path to the
  queue. Behaviour is otherwise unchanged: the message is still deleted after
  delivery returns without error, and only then.
- `DeleteMsg` and `ExtendVisibility` remain, for a caller holding a receipt
  handle it obtained some other way; `ExtendVisibility` is what `Keepalive`
  issues.

### Removed

- **`$receipt_handle` is no longer a delivered field.** It existed only to be
  handed back to a manual delete, and the settler needs no help finding it. As a
  field it is an opaque, per-receive, expiring token that is meaningless to log,
  correlate, or compare, and its one obvious use would be to save it somewhere
  and settle later — which is storing a lease rather than a value, and it
  expires while it sits in the variable. Every other `$` system attribute stays:
  each identifies the message to a human or to correlation, which is what earns
  a field its place.

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
