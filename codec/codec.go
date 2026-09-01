package codec

// Codec encodes and decodes values. Implementations must be safe for concurrent
// use and return an error for malformed input.
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}
