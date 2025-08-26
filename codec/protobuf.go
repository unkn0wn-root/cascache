package codec

import "google.golang.org/protobuf/proto"

type Protobuf[T proto.Message] struct {
	new func() T // constructor for a concrete message (e.g., func() *mypb.User { return &mypb.User{} })
}

func NewProtobuf[T proto.Message](ctor func() T) Protobuf[T] {
	return Protobuf[T]{new: ctor}
}

func (c Protobuf[T]) Encode(v T) ([]byte, error) {
	return proto.Marshal(v)
}
func (c Protobuf[T]) Decode(b []byte) (T, error) {
	m := c.new()
	err := proto.Unmarshal(b, m)
	return m, err
}
