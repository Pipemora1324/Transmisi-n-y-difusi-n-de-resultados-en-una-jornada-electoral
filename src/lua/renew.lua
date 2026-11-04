-- KEYS: lock, active. ARGV: token, owner, ttl, id
local value = redis.call('GET', KEYS[1])
if not value then return {'LOCK_LOST'} end
if value ~= ARGV[1] then return {'NOT_OWNER'} end
local t = redis.call('TIME')
local expires = t[1] * 1000 + math.floor(t[2] / 1000) + tonumber(ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('ZADD', KEYS[2], expires, ARGV[4])
return {'OK', tostring(expires)}
