package codec

import "google.golang.org/protobuf/proto"

// Protobuf encodes protocol buffer messages. Create it with [NewProtobuf]; the
// zero value is not usable.
//
// Example:
//
//	type UserPB = *mypb.User
//	pbCodec := codec.NewProtobuf(func() UserPB { return &mypb.User{} })
type Protobuf[T proto.Message] struct {
	newMessage func() T
}

// NewProtobuf returns a codec that uses ctor to allocate messages for decoding.
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
