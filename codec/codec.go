package codec

// Codec encodes and decodes values. Malformed input must return an error.
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}
