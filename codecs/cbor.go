package codecs

import "github.com/fxamacker/cbor/v2"

type CBORCodec[V any] struct {
	enc *cbor.EncOptions
	dec *cbor.DecOptions
}

func NewCBORCodec[V any](deterministic bool) CBORCodec[V] {
	enc := cbor.EncOptions{}
	if deterministic {
		enc.Time = cbor.TimeRFC3339Nano
		enc.Deterministic = true
		enc.Canonical = true
	}
	de := cbor.DecOptions{}
	e := enc
	return CBORCodec[V]{enc: &e, dec: &de}
}

func (c CBORCodec[V]) Encode(v V) ([]byte, error) {
	enc, _ := c.enc.EncMode()
	return enc.Marshal(v)
}
func (c CBORCodec[V]) Decode(b []byte) (V, error) {
	var v V
	dec, _ := c.dec.DecMode()
	err := dec.Unmarshal(b, &v)
	return v, err
}
