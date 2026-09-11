// Package redis provides cascache backends using Redis.
//
// [Backend] stores values and invalidation state atomically in Redis.
// [NewShared] keeps values in the caller's store and fences in Redis.
//
// Each entry uses two keys in the same cluster slot, so one MGET or script can
// access both. See [backend.StorageKeys] for the layout.
//
// Use a primary endpoint. Replica lag can return an old value with its old fence.
// [New] and [NewShared] return [ErrReplicaReads] for known cluster replica reads
// unless [Options.AllowReplicaReads] is set. Other replica clients are not detected.
//
// Leave Sentinel's ReplicaOnly false. For Cluster or Universal clients, leave
// ReadOnly, RouteByLatency, and RouteRandomly false. Fix [ErrReplicaReads] at startup.
//
// Redis 2.6 or later is required for EVAL and EVALSHA.
package redis
