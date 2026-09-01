package codec

import (
	"github.com/fxamacker/cbor/v2"
)

// CBOR encodes values with fxamacker/cbor. Create it with [NewCBOR] or
// [MustCBOR]; the zero value is not usable.
type CBOR[V any] struct {
	enc cbor.EncMode
	dec cbor.DecMode
}

var _ Codec[struct{}] = CBOR[struct{}]{}

// NewCBOR returns a CBOR codec. When deterministic is true, it uses RFC 8949
// deterministic encoding. Otherwise it uses the library's preferred unsorted
// encoding. Times are encoded as RFC3339Nano.
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

// MustCBOR is like [NewCBOR] but panics if the codec cannot be created.
func MustCBOR[V any](deterministic bool) CBOR[V] {
	c, err := NewCBOR[V](deterministic)
	if err != nil {
		panic(err)
	}
	return c
}

// Encode encodes v as CBOR.
func (c CBOR[V]) Encode(v V) ([]byte, error) {
	return c.enc.Marshal(v)
}

// Decode decodes CBOR into a V.
func (c CBOR[V]) Decode(b []byte) (V, error) {
	var v V
	err := c.dec.Unmarshal(b, &v)
	return v, err
}
