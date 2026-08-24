// Package redis provides cascache backends using Redis.
//
// [Backend] stores values and invalidation state atomically in Redis.
// [NewShared] keeps values in a caller-owned store and only the invalidation
// state in Redis.
//
// # Layout
//
// A cache entry occupies two keys that share a hash tag, so they land in the
// same cluster slot and one MGET or one script can touch the pair:
//
//	cas:v4:val:{tag}:<identity>
//	cas:v4:fen:{tag}:<identity>
//
// [backend.StorageKeys] defines the layout for all backends.
//
// # Reads must reach the primary
//
// Replication lag can return an old value with its matching old fence. [New]
// and [NewShared] reject cluster clients configured for replica reads unless
// [Options.AllowReplicaReads] is set. Other replica-only clients cannot be
// detected. Use a primary/writer endpoint; with Sentinel leave ReplicaOnly
// false, and with Cluster or Universal clients leave ReadOnly, RouteByLatency,
// and RouteRandomly false. Treat [ErrReplicaReads] as a startup configuration
// failure.
//
// # Requirements
//
// Redis 2.6 or later is required for EVAL and EVALSHA.
package redis
