package codec

import "google.golang.org/protobuf/proto"

type ProtobufCodec[T proto.Message] struct {
	new func() T // constructor for a concrete message (e.g., func() *mypb.User { return &mypb.User{} })
}

func NewProtobufCodec[T proto.Message](ctor func() T) ProtobufCodec[T] {
	return ProtobufCodec[T]{new: ctor}
}

func (c ProtobufCodec[T]) Encode(v T) ([]byte, error) {
	return proto.Marshal(v)
}
func (c ProtobufCodec[T]) Decode(b []byte) (T, error) {
	m := c.new()
	err := proto.Unmarshal(b, m)
	return m, err
}
