package codec

// Codec encodes/decodes values V to []byte for storage.
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}
