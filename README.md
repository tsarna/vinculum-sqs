# vinculum-sqs

Amazon SQS client packages for [Vinculum](https://github.com/tsarna/vinculum), built on the [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2).

Provides an `SQSSender` (sink) and `SQSReceiver` (source) that integrate with the `vinculum-bus` event bus. The VCL configuration wiring lives in the main vinculum repo (`clients/sqs/`) to avoid circular imports -- this module only depends on `vinculum-bus`.

---

## Packages

### `sender` -- SQSSender

Implements `bus.Subscriber`. `OnEvent` serializes the payload, maps vinculum fields to SQS message attributes, and sends the message to a configured SQS queue.

**Features:**
- Wire-format serialization via `vinculum-wire` (`auto`, `json`, `string`, `bytes`)
- Vinculum fields mapped to SQS `MessageAttribute`s with `$` to `_` prefix mapping
- Optional `topic_attribute` to include the vinculum topic as a message attribute
- SQS attribute name validation and 10-attribute budget enforcement
- FIFO queue support: per-message `MessageGroupId` and `MessageDeduplicationId` via configurable functions
- W3C trace context propagation (inject into message attributes)
- OTel metrics instrumentation (sent count, operation duration)

**Builder example:**

```go
sender, err := sender.NewSender().
    WithClient(sqsClient).
    WithClientName("orders").
    WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/orders").
    WithWireFormat(wire.JSON).
    WithTopicAttribute("source_topic").
    WithFIFOConfig(&sender.FIFOConfig{
        GroupIDFunc: func(topic string, msg any, fields map[string]string) (string, error) {
            return topic, nil
        },
    }).
    WithMeterProvider(mp).
    WithLogger(logger).
    Build()
```

### `receiver` -- SQSReceiver

Polls an SQS queue via long-polling and dispatches received messages to a `bus.Subscriber`.

**Features:**
- Long-polling with configurable wait time (default 20s) and max messages (default 10)
- Automatic or manual message deletion (`auto_delete`)
- Per-message vinculum topic resolution via configurable `TopicFunc`
- SQS system attributes mapped to `$`-prefixed vinculum fields (`$message_id`, `$receipt_handle`, `$receive_count`, etc.)
- SQS message attributes mapped back to vinculum fields with `_` to `$` reverse mapping
- Configurable concurrency (N polling goroutines)
- W3C trace context extraction (new root span linked to producer)
- Exponential backoff on transient errors (1s to 30s)
- `DeleteMsg()` and `ExtendVisibility()` methods for VCL function support
- OTel metrics instrumentation (consumed count, process duration)

**Builder example:**

```go
receiver, err := receiver.NewReceiver().
    WithClient(sqsClient).
    WithClientName("tasks").
    WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/tasks").
    WithSubscriber(myBus).
    WithWireFormat(wire.Auto).
    WithAutoDelete(true).
    WithConcurrency(3).
    WithMeterProvider(mp).
    WithLogger(logger).
    Build()

receiver.Start(ctx)
defer receiver.Stop(ctx)
```

### Root package -- shared types

`MessageAttributeCarrier` implements `propagation.TextMapCarrier` backed by SQS message attributes, shared by both sender (inject) and receiver (extract) for W3C trace context propagation.

---

## Metrics

Both packages expose instrumentation via an OTel `metric.MeterProvider`. Pass `nil` to disable metrics (all methods are nil-safe).

### Sender metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `messaging.client.sent.messages` | Int64Counter | `{message}` | Messages sent |
| `messaging.client.operation.duration` | Float64Histogram | `s` | SendMessage latency |

### Receiver metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `messaging.client.consumed.messages` | Int64Counter | `{message}` | Messages received and dispatched |
| `messaging.process.duration` | Float64Histogram | `s` | Time for subscriber.OnEvent to return |

All metrics carry attributes: `messaging.system=aws_sqs`, `messaging.destination.name=<queue>`, `vinculum.client.name=<client>`.

---

## VCL configuration

When used via vinculum, three client types are available:

```hcl
# Shared AWS credentials
client "aws" "prod" {
    region = "us-east-1"
}

# Send vinculum bus events to an SQS queue
client "sqs_sender" "orders" {
    aws             = client.prod
    queue_url       = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
    topic_attribute = "source_topic"
}

subscription "to_sqs" {
    target     = bus.main
    topics     = ["order/#"]
    subscriber = client.orders
}

# Receive messages from an SQS queue
client "sqs_receiver" "tasks" {
    aws            = client.prod
    queue_url      = "https://sqs.us-east-1.amazonaws.com/123456789012/tasks"
    subscriber     = bus.main
    vinculum_topic = "tasks/incoming"
    wait_time      = "20s"
    max_messages   = 10
    concurrency    = 3
    auto_delete    = true
}
```

VCL functions for manual acknowledgement:

```hcl
sqs_delete(ctx, client.tasks, ctx.fields["$receipt_handle"])
sqs_extend_visibility(ctx, client.tasks, ctx.fields["$receipt_handle"], 60)
```

---

## Dependencies

- [`github.com/aws/aws-sdk-go-v2`](https://github.com/aws/aws-sdk-go-v2) -- AWS SDK for Go v2 (SQS client)
- [`github.com/tsarna/vinculum-bus`](https://github.com/tsarna/vinculum-bus) -- `Subscriber`, `EventBus` interfaces
- [`github.com/tsarna/vinculum-wire`](https://github.com/tsarna/vinculum-wire) -- `WireFormat` interface and built-in formats
- [`go.uber.org/zap`](https://pkg.go.dev/go.uber.org/zap) -- structured logging
- [`go.opentelemetry.io/otel`](https://pkg.go.dev/go.opentelemetry.io/otel) -- tracing and metrics

## License

BSD 2-Clause -- see [LICENSE](LICENSE).
