// Package cascache implements a provider-agnostic cache with compare-and-swap (CAS)
// safety via per-key version fences. Single-key reads never return stale values.
// Batch results are validated on read (per member) and rejected if any member is stale.
//
// Components:
//   - Provider: byte store with TTL (e.g. Ristretto, BigCache, Redis).
//   - Codec[V]: (de)serializes V <-> []byte.
//   - VersionStore: authoritative version state per logical key. Local (in-process)
//     by default, optional Redis implementation for multi-replica / restart
//     persistence.
//     Custom implementations receive opaque version.CacheKey identities, not
//     logical user keys.
//   - KeyWriter / KeyInvalidator: optional backend-native fast paths for
//     single-key compare-and-write and invalidate. KeyMutator is the
//     alias for implementations that provide both. See the redis
//     subpackage for the built-in Redis implementation.
//
// Keys:
//
//	cas:v3:val:{<slot>}:s:<nsLen>:<ns>:<key>        - single entries
//	cas:v3:val:b:<nsLen>:<ns>:<sha256-128>          - set-shaped entries (128-bit SHA-256 over sorted keys)
//	cas:v3:ver:{<slot>}:s:<nsLen>:<ns>:<key>        - authoritative version state
//
// CAS pattern:
//
//	obs, err := cache.SnapshotVersion(ctx, k) // before DB read
//	v        := readFromDB(k)
//	if err == nil {
//		_, _ = cache.SetIfVersion(ctx, k, v, obs, 0) // write iff current version == obs
//	}
package cascache
