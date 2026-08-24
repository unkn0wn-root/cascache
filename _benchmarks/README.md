# Benchmarks

These benchmarks use one 256-byte value. They cover reads, sets, and
invalidations through a simple in-memory store and the CASCache Redis backend.
Plain Redis `GET`, `SET`, and `DEL` calls use the same Go client as a reference.

Run the local benchmark on its own:

```sh
go test -run '^$' -bench Local -benchmem ./_benchmarks
```

The Redis benchmarks expect a disposable Redis server. For example:

```sh
docker run --rm -p 6379:6379 redis:8-alpine
CASCACHE_BENCH_REDIS=127.0.0.1:6379 \
  go test -run '^$' -bench . -benchmem ./_benchmarks
```

The CASCache set benchmarks create their snapshot before the timer starts. The
invalidation benchmarks use the same key repeatedly, so the value is absent
after the first call. CASCache still changes the fence and attempts the delete
on every call. Plain Redis `DEL` does not provide the same guarantee and is
included only as a reference.

Use a quiet machine and run the command several times when comparing changes.
Redis results also depend on the server, network, and Docker setup.
