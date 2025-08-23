// Package cascache implements a provider-agnostic cache with compare-and-swap (CAS)
// safety via per-key generations. Single-key reads never return stale values;
// bulk results are validated on read (per member) and rejected if any member is stale.
//
// Components:
//   - Provider: byte store with TTL (e.g. Ristretto, BigCache, Redis).
//   - Codec[V]: (de)serializes V <-> []byte.
//   - GenStore: generation counter per logical key. Local (in-process) by default,
//     optional Redis implementation for multi-replica / restart persistence.
//
// Keys:
//
//	single:<ns>:<key>  - single entries
//	bulk:<ns>:<hash>   - set-shaped entries (hash over sorted keys)
//
// CAS pattern:
//
//	obs := cache.SnapshotGen(k) // before DB read
//	v   := readFromDB(k)
//	_   = cache.SetWithGen(ctx, k, v, obs, 0) // write iff current gen == obs
package cascache
