package codec

import "encoding/json"

// JSON encodes values with encoding/json. Its zero value is ready to use.
type JSON[V any] struct{}

func (JSON[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }
func (JSON[V]) Decode(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}

var _ Codec[struct{}] = JSON[struct{}]{}
