package codec

import "encoding/json"

// JSON is a Codec that serializes values using the standard library's
// encoding/json. The zero value is ready to use and respects `json` struct tags.
//
// Notes:
//   - Interface-typed fields may decode to default concrete types (e.g. numbers
//     to float64) unless you provide custom unmarshaling.
//   - Time values use encoding/json defaults.
type JSON[V any] struct{}

func (JSON[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }
func (JSON[V]) Decode(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}
