package receiver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// mockSQSReceive implements SQSReceiveAPI for testing.
type mockSQSReceive struct {
	receiveFunc   func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	deleteFunc    func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	changeVisFunc func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (m *mockSQSReceive) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.receiveFunc != nil {
		return m.receiveFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSReceive) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSReceive) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.changeVisFunc != nil {
		return m.changeVisFunc(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// mockSubscriber captures OnEvent calls for testing.
type mockSubscriber struct {
	bus.BaseSubscriber
	mu     sync.Mutex
	events []capturedEvent
	err    error
}

type capturedEvent struct {
	Ctx     context.Context
	Topic   string
	Message any
	Fields  map[string]string
}

func (s *mockSubscriber) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, capturedEvent{Ctx: ctx, Topic: topic, Message: msg, Fields: fields})
	return s.err
}

func (s *mockSubscriber) getEvents() []capturedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]capturedEvent, len(s.events))
	copy(cp, s.events)
	return cp
}

func TestProcessMessage_BasicDispatch(t *testing.T) {
	sub := &mockSubscriber{}
	var deleted atomic.Bool

	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Store(true)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		Build()
	require.NoError(t, err)

	msgID := "msg-123"
	receipt := "receipt-abc"
	body := `{"key":"value"}`
	msg := sqstypes.Message{
		MessageId:     &msgID,
		ReceiptHandle: &receipt,
		Body:          &body,
	}

	r.processMessage(context.Background(), msg)

	events := sub.getEvents()
	require.Len(t, events, 1)
	assert.Equal(t, "test-queue", events[0].Topic) // default topic = queue name
	assert.Equal(t, "msg-123", events[0].Fields["$message_id"])
	assert.Equal(t, "receipt-abc", events[0].Fields["$receipt_handle"])
	assert.True(t, deleted.Load(), "message should be auto-deleted")
}

func TestProcessMessage_CarriesInboundBaggage(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	sub := &mockSubscriber{}
	r, err := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		Build()
	require.NoError(t, err)

	body := `"hello"`
	receipt := "receipt-abc"
	msg := sqstypes.Message{
		Body:          &body,
		ReceiptHandle: &receipt,
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"baggage": {
				DataType:    aws.String("String"),
				StringValue: aws.String("tenant_id=acme,secret=x"),
			},
		},
	}

	r.processMessage(context.Background(), msg)

	events := sub.getEvents()
	require.Len(t, events, 1)
	// The producer's baggage must reach the OnEvent context (it previously did
	// not — only the trace span was linked).
	bg := baggage.FromContext(events[0].Ctx)
	assert.Equal(t, "acme", bg.Member("tenant_id").Value())
	assert.Equal(t, "x", bg.Member("secret").Value())
}

// buildStrictReceiver returns a JSON-wire-format receiver plus a flag that
// records whether the message was deleted.
func buildStrictReceiver(t *testing.T, sub bus.Subscriber, hook wire.DecodeErrorHook) (*SQSReceiver, *atomic.Bool) {
	t.Helper()
	var deleted atomic.Bool
	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Store(true)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		WithWireFormat(wire.JSON).
		WithDecodeErrorHook(hook).
		Build()
	require.NoError(t, err)
	return r, &deleted
}

// badJSONMessage returns an SQS message whose body is not valid JSON.
func badJSONMessage() sqstypes.Message {
	msgID := "msg-123"
	receipt := "receipt-abc"
	body := "not json {{"
	return sqstypes.Message{MessageId: &msgID, ReceiptHandle: &receipt, Body: &body}
}

func TestProcessMessage_DecodeErrorIsFatalAndNotDelivered(t *testing.T) {
	sub := &mockSubscriber{}
	r, deleted := buildStrictReceiver(t, sub, nil)

	r.processMessage(context.Background(), badJSONMessage())

	assert.Empty(t, sub.events, "malformed message must not be delivered")
	assert.False(t, deleted.Load(),
		"message must not be deleted, so it redelivers after the visibility timeout")
}

func TestProcessMessage_DecodeErrorInvokesHookWithoutSuppressing(t *testing.T) {
	sub := &mockSubscriber{}
	var got wire.DecodeError
	hookCalls := 0

	r, deleted := buildStrictReceiver(t, sub, func(_ context.Context, e wire.DecodeError) {
		hookCalls++
		got = e
	})

	r.processMessage(context.Background(), badJSONMessage())

	require.Equal(t, 1, hookCalls)
	assert.Equal(t, []byte("not json {{"), got.Raw)
	assert.Equal(t, "json", got.Format)
	assert.Equal(t, "test-queue", got.Attrs["queue"])
	assert.Equal(t, "msg-123", got.Attrs["message_id"])
	require.Error(t, got.Err)

	// The hook observes; it does not suppress.
	assert.Empty(t, sub.events)
	assert.False(t, deleted.Load())
}

func TestProcessMessage_AutoWireFormatToleratesNonJSON(t *testing.T) {
	sub := &mockSubscriber{}
	mock := &mockSQSReceive{}
	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		WithWireFormat(wire.Auto).
		Build()
	require.NoError(t, err)

	// "auto" is the documented migration path off the old tolerant
	// behavior: it never fails to decode, yielding a string.
	r.processMessage(context.Background(), badJSONMessage())

	require.Len(t, sub.events, 1)
	assert.Equal(t, "not json {{", sub.events[0].Message)
}

func TestProcessMessage_NoDeleteOnError(t *testing.T) {
	sub := &mockSubscriber{err: errors.New("processing failed")}
	var deleted atomic.Bool

	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Store(true)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		Build()
	require.NoError(t, err)

	msgID := "msg-123"
	receipt := "receipt-abc"
	body := `"hello"`
	msg := sqstypes.Message{
		MessageId:     &msgID,
		ReceiptHandle: &receipt,
		Body:          &body,
	}

	r.processMessage(context.Background(), msg)
	assert.False(t, deleted.Load(), "should NOT delete on OnEvent error")
}

func TestProcessMessage_AutoDeleteFalse(t *testing.T) {
	sub := &mockSubscriber{}
	var deleted atomic.Bool

	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleted.Store(true)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		WithAutoDelete(false).
		Build()
	require.NoError(t, err)

	msgID := "msg-123"
	receipt := "receipt-abc"
	body := `"hello"`
	msg := sqstypes.Message{
		MessageId:     &msgID,
		ReceiptHandle: &receipt,
		Body:          &body,
	}

	r.processMessage(context.Background(), msg)
	assert.False(t, deleted.Load(), "should NOT delete when auto_delete=false")
}

func TestProcessMessage_CustomTopicFunc(t *testing.T) {
	sub := &mockSubscriber{}

	r, err := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		WithTopicFunc(func(msg sqstypes.Message, fields map[string]string) string {
			return "custom/topic"
		}).
		Build()
	require.NoError(t, err)

	msgID := "msg-123"
	body := `"hello"`
	msg := sqstypes.Message{
		MessageId: &msgID,
		Body:      &body,
	}

	r.processMessage(context.Background(), msg)

	events := sub.getEvents()
	require.Len(t, events, 1)
	assert.Equal(t, "custom/topic", events[0].Topic)
}

func TestExtractFields_SystemAttributes(t *testing.T) {
	r, _ := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()

	msgID := "msg-123"
	receipt := "receipt-abc"
	msg := sqstypes.Message{
		MessageId:     &msgID,
		ReceiptHandle: &receipt,
		Attributes: map[string]string{
			"ApproximateReceiveCount":          "3",
			"SentTimestamp":                    "1618870000000",
			"ApproximateFirstReceiveTimestamp": "1618870001000",
			"MessageGroupId":                   "group-1",
			"MessageDeduplicationId":           "dedup-1",
			"SequenceNumber":                   "12345",
		},
	}

	fields := r.extractFields(msg)

	assert.Equal(t, "msg-123", fields["$message_id"])
	assert.Equal(t, "receipt-abc", fields["$receipt_handle"])
	assert.Equal(t, "3", fields["$receive_count"])
	assert.Equal(t, "1618870000000", fields["$sent_timestamp"])
	assert.Equal(t, "1618870001000", fields["$first_receive_timestamp"])
	assert.Equal(t, "group-1", fields["$message_group_id"])
	assert.Equal(t, "dedup-1", fields["$deduplication_id"])
	assert.Equal(t, "12345", fields["$sequence_number"])
}

func TestExtractFields_UserAttributes(t *testing.T) {
	r, _ := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()

	msg := sqstypes.Message{
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"user_id": {
				DataType:    aws.String("String"),
				StringValue: aws.String("alice"),
			},
			"_id": {
				DataType:    aws.String("String"),
				StringValue: aws.String("123"),
			},
			"source_topic": {
				DataType:    aws.String("String"),
				StringValue: aws.String("order/created"),
			},
		},
	}

	fields := r.extractFields(msg)

	assert.Equal(t, "alice", fields["user_id"])
	assert.Equal(t, "123", fields["$id"]) // _ mapped back to $
	assert.Equal(t, "order/created", fields["source_topic"])
}

func TestExtractFields_TraceAttributesFiltered(t *testing.T) {
	r, _ := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()

	msg := sqstypes.Message{
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"traceparent": {
				DataType:    aws.String("String"),
				StringValue: aws.String("00-abc-def-01"),
			},
			"tracestate": {
				DataType:    aws.String("String"),
				StringValue: aws.String("congo=t61rcWkgMzE"),
			},
			"user_field": {
				DataType:    aws.String("String"),
				StringValue: aws.String("value"),
			},
		},
	}

	fields := r.extractFields(msg)

	assert.NotContains(t, fields, "traceparent")
	assert.NotContains(t, fields, "tracestate")
	assert.Equal(t, "value", fields["user_field"])
}

func TestExtractFields_BinaryAttributeBase64(t *testing.T) {
	r, _ := NewReceiver().
		WithClient(&mockSQSReceive{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()

	msg := sqstypes.Message{
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"binary_data": {
				DataType:    aws.String("Binary"),
				BinaryValue: []byte{0x01, 0x02, 0x03},
			},
		},
	}

	fields := r.extractFields(msg)
	assert.Equal(t, "AQID", fields["binary_data"]) // base64 of [1,2,3]
}

func TestUnmapFieldName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"_id", "$id"},
		{"_message_id", "$message_id"},
		{"normal", "normal"},
		{"with.dot", "with.dot"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, unmapFieldName(tt.input))
		})
	}
}

func TestStartStop(t *testing.T) {
	var receiveCount atomic.Int32

	mock := &mockSQSReceive{
		receiveFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCount.Add(1)
			// Block until context cancelled to simulate long poll.
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()
	require.NoError(t, err)

	err = r.Start(context.Background())
	require.NoError(t, err)

	// Wait for the goroutine to enter ReceiveMessage.
	for i := 0; i < 100 && receiveCount.Load() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Greater(t, receiveCount.Load(), int32(0), "should have called ReceiveMessage")

	// Double start should error.
	err = r.Start(context.Background())
	assert.Error(t, err)

	// Stop should drain cleanly.
	err = r.Stop(context.Background())
	assert.NoError(t, err)
}

func TestPollLoop_RetriesOnError(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockSQSReceive{
		receiveFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			n := callCount.Add(1)
			if n <= 1 {
				return nil, errors.New("transient error")
			}
			// Second call: return a message.
			msgID := "msg-1"
			body := `"hello"`
			return &sqs.ReceiveMessageOutput{
				Messages: []sqstypes.Message{{
					MessageId: &msgID,
					Body:      &body,
				}},
			}, nil
		},
	}

	sub := &mockSubscriber{}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(sub).
		Build()
	require.NoError(t, err)

	err = r.Start(context.Background())
	require.NoError(t, err)

	// Wait for the message to be processed (1s backoff + processing time).
	for i := 0; i < 300 && len(sub.getEvents()) == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}

	err = r.Stop(context.Background())
	assert.NoError(t, err)

	events := sub.getEvents()
	assert.GreaterOrEqual(t, len(events), 1, "should have processed at least one message after retry")
	assert.GreaterOrEqual(t, callCount.Load(), int32(2), "should have retried after error")
}

func TestBuilder_Validation(t *testing.T) {
	t.Run("missing client", func(t *testing.T) {
		_, err := NewReceiver().
			WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test").
			WithSubscriber(&mockSubscriber{}).
			Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client is required")
	})

	t.Run("missing queue_url", func(t *testing.T) {
		_, err := NewReceiver().
			WithClient(&mockSQSReceive{}).
			WithSubscriber(&mockSubscriber{}).
			Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "queue_url is required")
	})

	t.Run("missing subscriber", func(t *testing.T) {
		_, err := NewReceiver().
			WithClient(&mockSQSReceive{}).
			WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test").
			Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "subscriber is required")
	})

	t.Run("defaults", func(t *testing.T) {
		r, err := NewReceiver().
			WithClient(&mockSQSReceive{}).
			WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test").
			WithSubscriber(&mockSubscriber{}).
			Build()
		require.NoError(t, err)
		assert.Equal(t, int32(20), r.waitTime)
		assert.Equal(t, int32(10), r.maxMessages)
		assert.True(t, r.autoDelete)
		assert.Equal(t, 1, r.concurrency)
		assert.Equal(t, wire.Auto, r.wireFormat)
	})
}

func TestDeleteMsg(t *testing.T) {
	var captured *sqs.DeleteMessageInput

	mock := &mockSQSReceive{
		deleteFunc: func(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			captured = params
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()
	require.NoError(t, err)

	err = r.DeleteMsg(context.Background(), "receipt-abc")
	require.NoError(t, err)
	assert.Equal(t, "receipt-abc", *captured.ReceiptHandle)
	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue", *captured.QueueUrl)
}

func TestExtendVisibility(t *testing.T) {
	var captured *sqs.ChangeMessageVisibilityInput

	mock := &mockSQSReceive{
		changeVisFunc: func(_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			captured = params
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	r, err := NewReceiver().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithSubscriber(&mockSubscriber{}).
		Build()
	require.NoError(t, err)

	err = r.ExtendVisibility(context.Background(), "receipt-abc", 60)
	require.NoError(t, err)
	assert.Equal(t, "receipt-abc", *captured.ReceiptHandle)
	assert.Equal(t, int32(60), captured.VisibilityTimeout)
}

func TestQueueNameFromURL(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", "my-queue"},
		{"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo", "my-queue.fifo"},
		{"http://localhost:4566/000000000000/local-queue", "local-queue"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, QueueNameFromURL(tt.url))
		})
	}
}
