package codec

import "github.com/vmihailenco/msgpack/v5"

// Msgpack encodes values with vmihailenco/msgpack/v5. Its zero value is ready
// to use. Use msgpack struct tags to control field names.
type Msgpack[V any] struct{}

func (Msgpack[V]) Encode(v V) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (Msgpack[V]) Decode(b []byte) (V, error) {
	var v V
	err := msgpack.Unmarshal(b, &v)
	return v, err
}

var _ Codec[struct{}] = Msgpack[struct{}]{}
