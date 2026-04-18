// Package sqs provides shared types for the vinculum SQS integration.
package sqs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// MessageAttributeCarrier implements propagation.TextMapCarrier backed
// by SQS message attributes. Used by both sender (inject) and receiver
// (extract) for W3C trace context propagation.
type MessageAttributeCarrier struct {
	Attrs map[string]sqstypes.MessageAttributeValue
}

func (c *MessageAttributeCarrier) Get(key string) string {
	if c.Attrs == nil {
		return ""
	}
	if v, ok := c.Attrs[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}

func (c *MessageAttributeCarrier) Set(key, value string) {
	if c.Attrs == nil {
		c.Attrs = make(map[string]sqstypes.MessageAttributeValue)
	}
	c.Attrs[key] = sqstypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(value),
	}
}

func (c *MessageAttributeCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Attrs))
	for k := range c.Attrs {
		keys = append(keys, k)
	}
	return keys
}
