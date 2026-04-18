package sender

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wire "github.com/tsarna/vinculum-wire"
)

// mockSQS implements SQSSendAPI for testing.
type mockSQS struct {
	sendFunc func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

func (m *mockSQS) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, params, optFns...)
	}
	msgID := "test-message-id"
	return &sqs.SendMessageOutput{MessageId: &msgID}, nil
}

func newTestSender(t *testing.T, client SQSSendAPI) *SQSSender {
	t.Helper()
	s, err := NewSender().
		WithClient(client).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		Build()
	require.NoError(t, err)
	return s
}

func TestOnEvent_BasicSend(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s := newTestSender(t, mock)
	err := s.OnEvent(context.Background(), "test/topic", map[string]any{"key": "value"}, nil)
	require.NoError(t, err)

	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue", *captured.QueueUrl)
	assert.Contains(t, *captured.MessageBody, "key")
}

func TestOnEvent_StringPayload(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s := newTestSender(t, mock)
	err := s.OnEvent(context.Background(), "test/topic", "hello world", nil)
	require.NoError(t, err)
	assert.Equal(t, "hello world", *captured.MessageBody)
}

func TestOnEvent_FieldsToAttributes(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s := newTestSender(t, mock)
	fields := map[string]string{
		"user_id": "alice",
		"action":  "login",
	}
	err := s.OnEvent(context.Background(), "test/topic", "msg", fields)
	require.NoError(t, err)

	assert.Equal(t, "alice", *captured.MessageAttributes["user_id"].StringValue)
	assert.Equal(t, "String", *captured.MessageAttributes["user_id"].DataType)
	assert.Equal(t, "login", *captured.MessageAttributes["action"].StringValue)
}

func TestOnEvent_DollarPrefixMapping(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s := newTestSender(t, mock)
	fields := map[string]string{
		"$id":         "123",
		"$message_id": "456",
		"normal":      "value",
	}
	err := s.OnEvent(context.Background(), "test/topic", "msg", fields)
	require.NoError(t, err)

	// $ should be mapped to _
	assert.Contains(t, captured.MessageAttributes, "_id")
	assert.Contains(t, captured.MessageAttributes, "_message_id")
	assert.Contains(t, captured.MessageAttributes, "normal")
	assert.NotContains(t, captured.MessageAttributes, "$id")
	assert.NotContains(t, captured.MessageAttributes, "$message_id")
}

func TestOnEvent_TopicAttribute(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithTopicAttribute("source_topic").
		Build()
	require.NoError(t, err)

	err = s.OnEvent(context.Background(), "order/created", "msg", nil)
	require.NoError(t, err)

	assert.Equal(t, "order/created", *captured.MessageAttributes["source_topic"].StringValue)
}

func TestOnEvent_DelaySeconds(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithDelaySeconds(30).
		Build()
	require.NoError(t, err)

	err = s.OnEvent(context.Background(), "test/topic", "msg", nil)
	require.NoError(t, err)

	assert.Equal(t, int32(30), captured.DelaySeconds)
}

func TestOnEvent_SendError(t *testing.T) {
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("access denied")
		},
	}

	s := newTestSender(t, mock)
	err := s.OnEvent(context.Background(), "test/topic", "msg", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "access denied")
	assert.Contains(t, err.Error(), "sqs sender")
}

func TestOnEvent_SerializeError(t *testing.T) {
	mock := &mockSQS{}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue").
		WithWireFormat(wire.JSON).
		Build()
	require.NoError(t, err)

	// Channels can't be JSON-serialized
	err = s.OnEvent(context.Background(), "test/topic", make(chan int), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "serialize")
}

func TestBuildMessageAttributes_InvalidNames(t *testing.T) {
	s := newTestSender(t, &mockSQS{})

	fields := map[string]string{
		"valid_name":          "ok",
		"also.valid":          "ok",
		"has spaces":          "invalid",
		"has=equals":          "invalid",
		"AWS.reserved":        "invalid",
		"Amazon.AlsoReserved": "invalid",
	}

	attrs := s.buildMessageAttributes(fields, "test/topic")

	assert.Contains(t, attrs, "valid_name")
	assert.Contains(t, attrs, "also.valid")
	assert.NotContains(t, attrs, "has spaces")
	assert.NotContains(t, attrs, "has=equals")
	assert.NotContains(t, attrs, "AWS.reserved")
	assert.NotContains(t, attrs, "Amazon.AlsoReserved")
}

func TestMapFieldName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"$id", "_id"},
		{"$message_id", "_message_id"},
		{"normal", "normal"},
		{"with.dot", "with.dot"},
		{"$", "_"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, mapFieldName(tt.input))
		})
	}
}

func TestIsValidAttributeName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"simple", true},
		{"with_underscore", true},
		{"with-hyphen", true},
		{"with.dot", true},
		{"MixedCase123", true},
		{"_leading_underscore", true},
		{"", false},
		{"has space", false},
		{"has=equals", false},
		{"has@at", false},
		{"AWS.Reserved", false},
		{"aws.reserved", false},
		{"Amazon.Reserved", false},
		{"amazon.reserved", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.valid, isValidAttributeName(tt.name))
		})
	}
}

func TestQueueNameFromURL(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", "my-queue"},
		{"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo", "my-queue.fifo"},
		{"http://localhost:4566/000000000000/local-queue", "local-queue"},
		{"simple", "simple"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, queueNameFromURL(tt.url))
		})
	}
}

func TestBuilder_Validation(t *testing.T) {
	t.Run("missing client", func(t *testing.T) {
		_, err := NewSender().
			WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test").
			Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client is required")
	})

	t.Run("missing queue_url", func(t *testing.T) {
		_, err := NewSender().
			WithClient(&mockSQS{}).
			Build()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "queue_url is required")
	})

	t.Run("defaults wire format to auto", func(t *testing.T) {
		s, err := NewSender().
			WithClient(&mockSQS{}).
			WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test").
			Build()
		require.NoError(t, err)
		assert.Equal(t, wire.Auto, s.wireFormat)
	})
}

func TestOnEvent_AttributeBudget(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	// With topic attribute, budget = 10 - 3 (trace) - 1 (topic) = 6 user fields.
	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/test").
		WithTopicAttribute("source_topic").
		Build()
	require.NoError(t, err)

	// Create 10 fields — only 6 should make it through.
	fields := make(map[string]string)
	for i := 0; i < 10; i++ {
		fields[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	err = s.OnEvent(context.Background(), "test/topic", "msg", fields)
	require.NoError(t, err)

	// Count non-trace, non-topic attributes
	userAttrCount := 0
	for k := range captured.MessageAttributes {
		if k == "source_topic" || traceAttributeKeys[k] {
			continue
		}
		userAttrCount++
	}
	assert.LessOrEqual(t, userAttrCount, 6, "should not exceed budget of 6 user field slots")
}

func TestOnEvent_NoOpSubscriberMethods(t *testing.T) {
	s := newTestSender(t, &mockSQS{})

	// BaseSubscriber methods should be no-ops.
	assert.NoError(t, s.OnSubscribe(context.Background(), "test"))
	assert.NoError(t, s.OnUnsubscribe(context.Background(), "test"))
}

func TestOnEvent_FIFO_GroupID(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo").
		WithFIFOConfig(&FIFOConfig{
			GroupIDFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return topic, nil // use topic as group ID
			},
		}).
		Build()
	require.NoError(t, err)

	err = s.OnEvent(context.Background(), "order/created", "msg", nil)
	require.NoError(t, err)

	require.NotNil(t, captured.MessageGroupId)
	assert.Equal(t, "order/created", *captured.MessageGroupId)
	assert.Nil(t, captured.MessageDeduplicationId)
}

func TestOnEvent_FIFO_GroupIDAndDedup(t *testing.T) {
	var captured *sqs.SendMessageInput
	mock := &mockSQS{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			msgID := "msg-123"
			return &sqs.SendMessageOutput{MessageId: &msgID}, nil
		},
	}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo").
		WithFIFOConfig(&FIFOConfig{
			GroupIDFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return "all", nil
			},
			DeduplicationFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return fields["$id"], nil
			},
		}).
		Build()
	require.NoError(t, err)

	err = s.OnEvent(context.Background(), "order/created", "msg", map[string]string{"$id": "evt-42"})
	require.NoError(t, err)

	assert.Equal(t, "all", *captured.MessageGroupId)
	assert.Equal(t, "evt-42", *captured.MessageDeduplicationId)
}

func TestOnEvent_FIFO_GroupIDError(t *testing.T) {
	mock := &mockSQS{}

	s, err := NewSender().
		WithClient(mock).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo").
		WithFIFOConfig(&FIFOConfig{
			GroupIDFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return "", errors.New("group ID eval failed")
			},
		}).
		Build()
	require.NoError(t, err)

	err = s.OnEvent(context.Background(), "test", "msg", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "message_group_id")
}

func TestBuilder_FIFO_RequiresGroupID(t *testing.T) {
	// .fifo queue without FIFOConfig should fail.
	_, err := NewSender().
		WithClient(&mockSQS{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo").
		Build()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FIFO")
	assert.Contains(t, err.Error(), "message_group_id")
}

func TestBuilder_NonFIFO_NoGroupIDRequired(t *testing.T) {
	// Non-.fifo queue without FIFOConfig should succeed.
	_, err := NewSender().
		WithClient(&mockSQS{}).
		WithQueueURL("https://sqs.us-east-1.amazonaws.com/123456789012/orders").
		Build()
	assert.NoError(t, err)
}

func TestIsFIFOQueue(t *testing.T) {
	assert.True(t, IsFIFOQueue("https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo"))
	assert.False(t, IsFIFOQueue("https://sqs.us-east-1.amazonaws.com/123456789012/orders"))
	assert.True(t, IsFIFOQueue("orders.fifo"))
	assert.False(t, IsFIFOQueue("orders"))
}

// Helpers for attribute inspection.
func getStringAttr(attrs map[string]sqstypes.MessageAttributeValue, key string) string {
	if v, ok := attrs[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}
