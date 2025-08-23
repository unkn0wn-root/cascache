package codecs

import "encoding/json"

// Codec encodes/decodes values V to []byte for storage.
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}

type JSONCodec[V any] struct{}

func (JSONCodec[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }
func (JSONCodec[V]) Decode(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}
