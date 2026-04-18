package sqs

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

func TestMessageAttributeCarrier_GetSet(t *testing.T) {
	c := &MessageAttributeCarrier{
		Attrs: map[string]sqstypes.MessageAttributeValue{
			"existing": {
				DataType:    aws.String("String"),
				StringValue: aws.String("hello"),
			},
		},
	}

	assert.Equal(t, "hello", c.Get("existing"))
	assert.Equal(t, "", c.Get("missing"))

	c.Set("new_key", "new_value")
	assert.Equal(t, "new_value", c.Get("new_key"))
}

func TestMessageAttributeCarrier_Keys(t *testing.T) {
	c := &MessageAttributeCarrier{
		Attrs: map[string]sqstypes.MessageAttributeValue{
			"a": {DataType: aws.String("String"), StringValue: aws.String("1")},
			"b": {DataType: aws.String("String"), StringValue: aws.String("2")},
		},
	}

	keys := c.Keys()
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "a")
	assert.Contains(t, keys, "b")
}

func TestMessageAttributeCarrier_NilAttrs(t *testing.T) {
	c := &MessageAttributeCarrier{}

	assert.Equal(t, "", c.Get("anything"))
	assert.Empty(t, c.Keys())

	// Set should initialize the map.
	c.Set("key", "value")
	assert.Equal(t, "value", c.Get("key"))
}
