package backend

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
)

// The storage version follows the persisted layout, not the Go module version.
const (
	ValueRoot = "cas:v4:val:"
	FenceRoot = "cas:v4:fen:"
)

// Both keys share one buffer, so fail to compile if the roots differ in length.
const (
	_ = uint(len(ValueRoot) - len(FenceRoot))
	_ = uint(len(FenceRoot) - len(ValueRoot))
)

const (
	// tagSize is the hex width of the Redis Cluster hash tag.
	tagSize = 8
	// slotSize covers "{" + tag + "}:".
	slotSize = 1 + tagSize + 2
	idAt     = len(ValueRoot) + slotSize
)

// ErrInvalidKey reports an unusable canonical key.
var ErrInvalidKey = errors.New("cascache/backend: invalid key")

var tagTable = crc32.MakeTable(crc32.Castagnoli)

// Key is the backend-independent identity of an entry. The zero value is invalid.
type Key struct {
	id string
}

// NewKey returns the canonical key for a non-empty identity.
func NewKey(id string) (Key, error) {
	if id == "" {
		return Key{}, ErrInvalidKey
	}
	return Key{id: id}, nil
}

// ID returns the canonical identity.
func (k Key) ID() string { return k.id }

// Valid reports whether k identifies an entry.
func (k Key) Valid() bool    { return k.id != "" }
func (k Key) String() string { return k.id }

// CheckKey validates k.
func CheckKey(k Key) error {
	if !k.Valid() {
		return ErrInvalidKey
	}
	return nil
}

// CheckKeyFence is [CheckKey] for the methods that also take a fence.
func CheckKeyFence(k Key, f Fence) error {
	if err := CheckKey(k); err != nil {
		return err
	}
	if !f.Valid() {
		return ErrInvalidFence
	}
	return nil
}

// StorageKeys returns the value and fence keys for k:
//
//	cas:v4:val:{tag}:<identity>
//	cas:v4:fen:{tag}:<identity>
//
// The CRC32C tag puts both keys in one Redis Cluster slot. The full identity
// remains part of each key, so tag collisions do not merge entries.
func StorageKeys(k Key) (value, fence string) {
	buf := slot(k, ValueRoot)
	value = string(buf)
	copy(buf, FenceRoot)
	return value, string(buf)
}

// ValueKey returns the storage key holding k's value.
func ValueKey(k Key) string { return string(slot(k, ValueRoot)) }

// FenceKey returns the storage key holding k's fence.
func FenceKey(k Key) string { return string(slot(k, FenceRoot)) }

func slot(k Key, root string) []byte {
	id := k.ID()
	buf := make([]byte, idAt+len(id))
	copy(buf, root)
	copy(buf[idAt:], id)

	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(buf[idAt:], tagTable))
	buf[len(root)] = '{'
	hex.Encode(buf[len(root)+1:], sum[:])
	buf[idAt-2], buf[idAt-1] = '}', ':'
	return buf
}
