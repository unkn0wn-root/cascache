package codec

import "encoding/json"

type JSON[V any] struct{}

func (JSON[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }
func (JSON[V]) Decode(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}
