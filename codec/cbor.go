package codec

import (
	"github.com/fxamacker/cbor/v2"
)

type CBOR[V any] struct {
	enc cbor.EncMode
	dec cbor.DecMode
}

// NewCBOR creates a CBOR codec.
// * if deterministic == true, it uses Core Deterministic Encoding (RFC 8949).
// * Otherwise, it uses PreferredUnsortedEncOptions (sensible defaults).
// Also sets time encoding to RFC3339Nano.
func NewCBOR[V any](deterministic bool) (CBOR[V], error) {
	var eo cbor.EncOptions
	if deterministic {
		eo = cbor.CoreDetEncOptions()
	} else {
		eo = cbor.PreferredUnsortedEncOptions()
	}
	eo.Time = cbor.TimeRFC3339Nano

	em, err := eo.EncMode()
	if err != nil {
		return CBOR[V]{}, err
	}
	dm, err := (cbor.DecOptions{}).DecMode()
	if err != nil {
		return CBOR[V]{}, err
	}
	return CBOR[V]{enc: em, dec: dm}, nil
}

// MustCBOR is a convenience helper that panics on construction error.
func MustCBOR[V any](deterministic bool) CBOR[V] {
	c, err := NewCBOR[V](deterministic)
	if err != nil {
		panic(err)
	}
	return c
}

func (c CBOR[V]) Encode(v V) ([]byte, error) {
	return c.enc.Marshal(v)
}

func (c CBOR[V]) Decode(b []byte) (V, error) {
	var v V
	err := c.dec.Unmarshal(b, &v)
	return v, err
}
