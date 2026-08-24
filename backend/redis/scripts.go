package redis

import goredis "github.com/redis/go-redis/v9"

// Scripts keep multi-key operations atomic. Fence writes always set or clear
// expiry so keys created under an older configuration do not keep a stale TTL.

// Return the current fence, installing candidate when the key has none. Refresh
// retention in either case.
//
//	KEYS[1] fence key
//	ARGV[1] candidate fence
//	ARGV[2] fence retention in milliseconds, 0 for none
const ensureSource = `
local current = redis.call("GET", KEYS[1])
local retention = tonumber(ARGV[2])

if not current then
    current = ARGV[1]
    if retention > 0 then
        redis.call("SET", KEYS[1], current, "PX", retention)
    else
        redis.call("SET", KEYS[1], current)
    end
elseif retention > 0 then
    redis.call("PEXPIRE", KEYS[1], retention)
else
    redis.call("PERSIST", KEYS[1])
end

return current
`

// Retain only a matching live fence; never recreate an expired one.
//
//	KEYS[1] fence key
//	ARGV[1] expected fence
//	ARGV[2] fence retention in milliseconds, 0 for none
const retainSource = `
local current = redis.call("GET", KEYS[1])
if not current or current ~= ARGV[1] then
    return 0
end

local retention = tonumber(ARGV[2])
if retention > 0 then
    redis.call("PEXPIRE", KEYS[1], retention)
else
    redis.call("PERSIST", KEYS[1])
end
return 1
`

// Write the value before refreshing the fence so the fence expires no earlier.
//
//	KEYS[1] fence key
//	KEYS[2] value key
//	ARGV[1] expected fence
//	ARGV[2] value
//	ARGV[3] value TTL in milliseconds, 0 for none
//	ARGV[4] fence retention in milliseconds, 0 for none
const compareAndStoreSource = `
local current = redis.call("GET", KEYS[1])
if not current or current ~= ARGV[1] then
    return 0
end

local ttl = tonumber(ARGV[3])
if ttl > 0 then
    redis.call("SET", KEYS[2], ARGV[2], "PX", ttl)
else
    redis.call("SET", KEYS[2], ARGV[2])
end

local retention = tonumber(ARGV[4])
if retention > 0 then
    redis.call("PEXPIRE", KEYS[1], retention)
else
    redis.call("PERSIST", KEYS[1])
end

return 1
`

// Replace the fence and remove the value atomically.
//
//	KEYS[1] fence key
//	KEYS[2] value key
//	ARGV[1] next fence
//	ARGV[2] fence retention in milliseconds, 0 for none
const invalidateSource = `
local retention = tonumber(ARGV[2])
if retention > 0 then
    redis.call("SET", KEYS[1], ARGV[1], "PX", retention)
else
    redis.call("SET", KEYS[1], ARGV[1])
end

redis.call("DEL", KEYS[2])
return 1
`

// Remove only the exact value that was rejected.
//
//	KEYS[1] value key
//	ARGV[1] the bytes the caller judged invalid
const discardSource = `
local current = redis.call("GET", KEYS[1])
if current and current == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
`

var (
	ensureScript          = goredis.NewScript(ensureSource)
	retainScript          = goredis.NewScript(retainSource)
	compareAndStoreScript = goredis.NewScript(compareAndStoreSource)
	invalidateScript      = goredis.NewScript(invalidateSource)
	discardScript         = goredis.NewScript(discardSource)
)
