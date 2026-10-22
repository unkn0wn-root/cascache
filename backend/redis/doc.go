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
// unless [Options.AllowReplicaReads] is set. A Sentinel client with ReplicaOnly
// set is not detected, and neither are other replica clients.
//
// Leave Sentinel's ReplicaOnly false. For Cluster or Universal clients, leave
// ReadOnly, RouteByLatency, and RouteRandomly false. Fix [ErrReplicaReads] at startup.
//
// Redis replicates asynchronously and this package does not wait for replicas,
// so an invalidation confirmed just before a failover can be lost and the old
// value served again until it expires.
//
// Redis 2.6 or later is required for EVAL and EVALSHA.
package redis
