package codec

import (
	"github.com/fxamacker/cbor/v2"
)

type CBORCodec[V any] struct {
	enc cbor.EncMode
	dec cbor.DecMode
}

// NewCBORCodec creates a CBOR codec.
// * if deterministic == true, it uses Core Deterministic Encoding (RFC 8949).
// * Otherwise, it uses PreferredUnsortedEncOptions (sensible defaults).
// Also sets time encoding to RFC3339Nano.
func NewCBORCodec[V any](deterministic bool) (CBORCodec[V], error) {
	var eo cbor.EncOptions
	if deterministic {
		eo = cbor.CoreDetEncOptions()
	} else {
		eo = cbor.PreferredUnsortedEncOptions()
	}
	eo.Time = cbor.TimeRFC3339Nano

	em, err := eo.EncMode()
	if err != nil {
		return CBORCodec[V]{}, err
	}
	dm, err := (cbor.DecOptions{}).DecMode()
	if err != nil {
		return CBORCodec[V]{}, err
	}
	return CBORCodec[V]{enc: em, dec: dm}, nil
}

// MustCBORCodec is a convenience helper that panics on construction error.
func MustCBORCodec[V any](deterministic bool) CBORCodec[V] {
	c, err := NewCBORCodec[V](deterministic)
	if err != nil {
		panic(err)
	}
	return c
}

func (c CBORCodec[V]) Encode(v V) ([]byte, error) {
	return c.enc.Marshal(v)
}

func (c CBORCodec[V]) Decode(b []byte) (V, error) {
	var v V
	err := c.dec.Unmarshal(b, &v)
	return v, err
}
