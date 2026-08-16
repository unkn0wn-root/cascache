// Package codec turns cached values into bytes and back.
//
// Codecs handle values only. The cache adds framing, fences, and checksums.
//
// The package includes JSON, CBOR, msgpack, protobuf, [Bytes], and [String]
// codecs. [LimitCodec] adds a maximum decode size.
package codec
