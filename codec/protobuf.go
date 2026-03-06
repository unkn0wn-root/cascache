package codec

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

// Protobuf is a Codec for protocol buffer messages. Requires a constructor
// for the concrete message type T so Decode can allocate a new instance.
//
// The zero value is NOT ready to use. Build with NewProtobuf.
//
// Example:
//
//	type UserPB = *mypb.User
//	pbCodec := codec.NewProtobuf(func() UserPB { return &mypb.User{} })
type Protobuf[T proto.Message] struct {
	// new returns a new zero value of T (e.g. func() *mypb.User { return &mypb.User{} }).
	new func() T
}

// NewProtobuf constructs a Protobuf codec for the given message type T.
// Provide a constructor that returns a new instance of T.
func NewProtobuf[T proto.Message](ctor func() T) Protobuf[T] {
	return Protobuf[T]{new: ctor}
}

func (c Protobuf[T]) Encode(v T) ([]byte, error) {
	return proto.Marshal(v)
}
func (c Protobuf[T]) Decode(b []byte) (T, error) {
	var zero T
	if c.new == nil {
		return zero, ErrUninitializedProtobuf
	}
	m := c.new()
	if isNilProtoMessage(m) {
		return zero, ErrUninitializedProtobuf
	}
	err := proto.Unmarshal(b, m)
	return m, err
}

func isNilProtoMessage[T proto.Message](m T) bool {
	v := reflect.ValueOf(m)
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
