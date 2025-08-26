// Package provider defines the storage abstraction used by cascache.
//
// Implementations MUST be byte-for-byte transparent: a Get MUST return exactly
// the same []byte that was previously passed to Set for the same key (no
// prepended/appended metadata, no transcoding, no mutation). If the store
// performs internal transforms (e.g., compression/encryption), they must be
// fully reversed so the bytes returned by Get are identical to what Set
// received.
//
// Concurrency & atomicity:
//   - Implementations MUST be safe for concurrent use.
//   - Set must be atomic at the key level: readers must not observe partial
//     writes or torn values.
//
// Keyspace ownership:
//   - The keyspaces "single:<ns>:" and "bulk:<ns>:" are owned by cascache.
//     External code MUST NOT write under these prefixes. Strict wire-format
//     validation may treat foreign writes as corruption and delete them.
//   - Other packages in this repository may reserve additional prefixes
//     (e.g., "gen:<ns>:" used by the generation store); external code should
//     avoid those as well.
//
// Back-pressure/error:
//   - Set returns (ok=false, err=nil) if the store *intentionally* rejects the
//     write under pressure/admission policy (e.g. an in-memory cache).
//   - If the store signals refusal as an error (e.g. Redis OOM), return
//     (ok=false, err=that error).
//   - On success, return (ok=true, err=nil).
//
// TTL:
//   - ttl is the desired time-to-live for the key. If the store doesn’t support
//     TTLs, it may ignore it. If ttl<=0, treat as “no expiry” when supported.
//
// Values are treated as opaque bytes meaning cascache’s wire layer enforces integrity
// and will self-heal by deleting entries on corruption.
package provider

import (
	"context"
	"time"
)

type Provider interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (ok bool, err error)
	Del(ctx context.Context, key string) error
	Close(ctx context.Context) error
}
