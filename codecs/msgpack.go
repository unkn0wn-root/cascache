package codecs

import (
	"github.com/vmihailenco/msgpack/v5"
)

type MsgpackCodec[V any] struct{}

func (MsgpackCodec[V]) Encode(v V) ([]byte, error) {
	return msgpack.Marshal(v)
}
func (MsgpackCodec[V]) Decode(b []byte) (V, error) {
	var v V
	err := msgpack.Unmarshal(b, &v)
	return v, err
}
