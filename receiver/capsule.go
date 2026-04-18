package receiver

import (
	"fmt"
	"reflect"

	"github.com/zclconf/go-cty/cty"
)

// ReceiverCapsuleType wraps a *SQSReceiver for HCL. It lets the
// sqs_delete() and sqs_extend_visibility() functions extract the
// receiver from an address like `client.tasks` without creating a
// dependency from vinculum/config onto the receiver runtime.
var ReceiverCapsuleType = cty.CapsuleWithOps("sqs_receiver",
	reflect.TypeOf(SQSReceiver{}),
	&cty.CapsuleOps{
		GoString: func(val interface{}) string {
			return fmt.Sprintf("sqs_receiver(%p)", val)
		},
		TypeGoString: func(_ reflect.Type) string {
			return "sqs_receiver"
		},
	})

// NewReceiverCapsule wraps a receiver for use in the HCL eval context.
func NewReceiverCapsule(r *SQSReceiver) cty.Value {
	return cty.CapsuleVal(ReceiverCapsuleType, r)
}

// GetReceiverFromCapsule extracts a *SQSReceiver from a capsule.
func GetReceiverFromCapsule(v cty.Value) (*SQSReceiver, error) {
	if v.Type() != ReceiverCapsuleType {
		return nil, fmt.Errorf("expected sqs_receiver capsule, got %s", v.Type().FriendlyName())
	}
	r, ok := v.EncapsulatedValue().(*SQSReceiver)
	if !ok {
		return nil, fmt.Errorf("encapsulated value is not an SQSReceiver, got %T", v.EncapsulatedValue())
	}
	return r, nil
}
