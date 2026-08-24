package backend

import (
	"errors"
	"strings"
	"testing"
)

func mustKey(t testing.TB, id string) Key {
	t.Helper()
	k, err := NewKey(id)
	if err != nil {
		t.Fatalf("NewKey(%q): %v", id, err)
	}
	return k
}

func TestNewKeyRejectsEmpty(t *testing.T) {
	if _, err := NewKey(""); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("NewKey(\"\") error = %v, want ErrInvalidKey", err)
	}
	var zero Key
	if zero.Valid() {
		t.Fatal("the zero Key must be invalid")
	}
	if err := CheckKey(zero); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("CheckKey(zero) = %v, want ErrInvalidKey", err)
	}
}

func TestCheckKeyFence(t *testing.T) {
	key := mustKey(t, "entry")

	if err := CheckKeyFence(key, NewFence()); err != nil {
		t.Fatalf("CheckKeyFence(valid, valid) = %v", err)
	}
	if err := CheckKeyFence(Key{}, NewFence()); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("CheckKeyFence(invalid key) = %v, want ErrInvalidKey", err)
	}
	if err := CheckKeyFence(key, Fence{}); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("CheckKeyFence(zero fence) = %v, want ErrInvalidFence", err)
	}
}

func TestStorageKeysLayout(t *testing.T) {
	key := mustKey(t, "s:4:user:u:1")
	value, fence := StorageKeys(key)

	if !strings.HasPrefix(value, ValueRoot) {
		t.Fatalf("value key %q does not start with %q", value, ValueRoot)
	}
	if !strings.HasPrefix(fence, FenceRoot) {
		t.Fatalf("fence key %q does not start with %q", fence, FenceRoot)
	}
	if !strings.HasSuffix(value, key.ID()) || !strings.HasSuffix(fence, key.ID()) {
		t.Fatalf("storage keys must end with the identity: %q, %q", value, fence)
	}

	if value[len(ValueRoot):] != fence[len(FenceRoot):] {
		t.Fatalf("value and fence keys differ past the root:\n %q\n %q", value, fence)
	}

	tag := value[len(ValueRoot):idAt]
	if tag[0] != '{' || tag[len(tag)-2] != '}' || tag[len(tag)-1] != ':' {
		t.Fatalf("hash tag %q is not of the form {tag}:", tag)
	}

	if got := ValueKey(key); got != value {
		t.Fatalf("ValueKey = %q, StorageKeys value = %q", got, value)
	}
	if got := FenceKey(key); got != fence {
		t.Fatalf("FenceKey = %q, StorageKeys fence = %q", got, fence)
	}
}

func TestStorageKeysNeverCollide(t *testing.T) {
	long := strings.Repeat("c", 200)
	ids := []string{
		"collide", "collide:x", "collide:x:y", "collide-x",
		"", // guarded separately below
		long + "a", long + "b",
		"s:4:user:1", "s:4:use:r1", "s:5:user:1",
	}

	seen := make(map[string]string, len(ids)*2)
	for _, id := range ids {
		if id == "" {
			continue
		}
		key := mustKey(t, id)
		value, fence := StorageKeys(key)
		for _, storage := range []string{value, fence} {
			if prev, dup := seen[storage]; dup {
				t.Fatalf("identities %q and %q share storage key %q", prev, id, storage)
			}
			seen[storage] = id
		}
	}
}

func BenchmarkStorageKeys(b *testing.B) {
	key := mustKey(b, "s:4:user:some-reasonably-long-identity")
	b.ReportAllocs()
	for range b.N {
		_, _ = StorageKeys(key)
	}
}
