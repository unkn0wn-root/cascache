package codec

import "google.golang.org/protobuf/proto"

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
	newMessage func() T
}

// NewProtobuf constructs a Protobuf codec for the given message type T.
// Provide a constructor that returns a new instance of T.
func NewProtobuf[T proto.Message](ctor func() T) Protobuf[T] {
	return Protobuf[T]{newMessage: ctor}
}

func (c Protobuf[T]) Encode(v T) ([]byte, error) {
	return proto.Marshal(v)
}

func (c Protobuf[T]) Decode(b []byte) (T, error) {
	m := c.newMessage()
	err := proto.Unmarshal(b, m)
	return m, err
}
