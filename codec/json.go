package codec

import "encoding/json"

type JSONCodec[V any] struct{}

func (JSONCodec[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }
func (JSONCodec[V]) Decode(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}
