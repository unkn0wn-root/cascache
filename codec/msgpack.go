package codec

import "github.com/vmihailenco/msgpack/v5"

// Msgpack is a Codec that serializes values using vmihailenco/msgpack/v5.
// The zero value is ready to use.
//
// Msgpack is compact and fast; be mindful of struct tag differences vs JSON.
// Use `msgpack:"fieldName"` tags if you need explicit control.
type Msgpack[V any] struct{}

func (Msgpack[V]) Encode(v V) ([]byte, error) {
	return msgpack.Marshal(v)
}
func (Msgpack[V]) Decode(b []byte) (V, error) {
	var v V
	err := msgpack.Unmarshal(b, &v)
	return v, err
}
