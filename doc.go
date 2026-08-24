// Package cascache caches values that an invalidation retires everywhere at
// once, including copies this process cannot reach.
//
// Every key carries invalidation state, and a stored value is served only while
// the state it was written under is still current. Invalidating a key replaces
// that state, so copies of the old value stop being served without being
// touched.
//
// # Reading
//
// [Cache.Load] returns a cached value or calls the loader after a miss.
// Concurrent callers for the same key share one loader run:
//
//	user, err := cache.Load(ctx, id, func(ctx context.Context) (User, error) {
//	    return db.FindUser(ctx, id)
//	})
//
// A completed invalidation is not undone by a later load. Changes made without
// an invalidation are governed by the TTL.
//
// # Filling by hand
//
// Manual writes take a snapshot before reading the source:
//
//	snapshot, err := cache.Snapshot(ctx, key)
//	if err != nil {
//	    return err
//	}
//	value, err := loadFromSource(ctx, key)
//	if err != nil {
//	    return err
//	}
//	_, err = cache.Set(ctx, key, value, snapshot)
//
// An invalidation between the snapshot and the write makes the snapshot stale,
// so the write is refused.
//
// # Why a token rather than a counter
//
// Invalidation state is an opaque token, which the [backend] layer calls a
// fence. It has no valid zero and is never reused, so a value whose token is
// gone cannot be proved current and becomes a miss. A counter cannot make that
// distinction once its record is lost, because "never invalidated" and "record
// gone" read the same.
//
// Redis replica reads are unsafe because lag can return an old value together
// with the matching old token. The Redis backend rejects them unless explicitly
// allowed.
//
// # Arrangements
//
// A [backend.Backend] supports three common arrangements:
//
//   - one process, values and invalidation state both in memory, with
//     backend.NewLocal;
//   - many replicas sharing values and invalidation state in Redis, atomically,
//     with redis.New;
//   - local values behind shared invalidation state, with redis.NewShared.
package cascache
