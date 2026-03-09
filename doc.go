// Package cascache implements a provider-agnostic cache with compare-and-swap (CAS)
// safety via per-key generations. Single-key reads never return stale values.
// Bulk results are validated on read (per member) and rejected if any member is stale.
//
// Components:
//   - Provider: byte store with TTL (e.g. Ristretto, BigCache, Redis).
//   - Codec[V]: (de)serializes V <-> []byte.
//   - GenStore: generation counter per logical key. Local (in-process) by default,
//     optional Redis implementation for multi-replica / restart persistence.
//     Custom implementations receive opaque genstore.CacheKey identities, not
//     logical user keys.
//
// Keys:
//
//	cas:v1:val:s:<nsLen>:<ns>:<key>         - single entries
//	cas:v1:val:b:<nsLen>:<ns>:<sha256-128>  - set-shaped entries (128-bit SHA-256 over sorted keys)
//	cas:v1:gen:s:<nsLen>:<ns>:<key>         - generation counters (RedisGenStore)
//
// CAS pattern:
//
//	obs, err := cache.TrySnapshotGen(ctx, k) // before DB read
//	v        := readFromDB(k)
//	if err == nil {
//		_ = cache.SetWithGen(ctx, k, v, obs, 0) // write iff current gen == obs
//	}
package cascache
