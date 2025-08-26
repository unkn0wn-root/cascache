package codec

// Codec encodes and decodes a value of type V to and from a byte slice.
// Implementations should return an error on malformed input. Encode/Decode
// should be pure (no side effects).
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}
