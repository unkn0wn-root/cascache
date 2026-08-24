package backend_test

import (
	"errors"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/backend/backendtest"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

func TestLocalConformance(t *testing.T) {
	backendtest.TestBackend(t, func(t testing.TB) backend.Backend {
		b, err := backend.NewLocal(
			memstore.New(memstore.Options{}),
			backend.LocalOptions{InvalidationTTL: backend.NoExpiration},
		)
		if err != nil {
			t.Fatalf("NewLocal: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	})
}

func TestNewLocalValidatesArguments(t *testing.T) {
	if _, err := backend.NewLocal(nil, backend.LocalOptions{}); !errors.Is(err, backend.ErrNilStore) {
		t.Fatalf("NewLocal(nil) = %v, want ErrNilStore", err)
	}
	var typedNil *memstore.Store
	if _, err := backend.NewLocal(typedNil, backend.LocalOptions{}); !errors.Is(err, backend.ErrNilStore) {
		t.Fatalf("NewLocal(typed nil) = %v, want ErrNilStore", err)
	}
	if _, err := backend.NewLocal(
		memstore.New(memstore.Options{}),
		backend.LocalOptions{InvalidationTTL: -5 * time.Second},
	); err == nil {
		t.Fatal("NewLocal accepted an invalid invalidation TTL")
	}
}

func TestLocalCloseIsIdempotent(t *testing.T) {
	b, err := backend.NewLocal(
		memstore.New(memstore.Options{}),
		backend.LocalOptions{CleanupInterval: time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	var nilBackend *backend.Local
	if err := nilBackend.Close(); err != nil {
		t.Fatalf("Close on nil Local: %v", err)
	}
	if err := new(backend.Local).Close(); err != nil {
		t.Fatalf("Close on zero Local: %v", err)
	}
}
