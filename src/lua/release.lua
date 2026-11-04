-- KEYS: lock, active, claims, events. ARGV: token, owner, id
local value = redis.call('GET', KEYS[1])
if not value then return {'LOCK_LOST'} end
if value ~= ARGV[1] then return {'NOT_OWNER'} end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[3])
redis.call('HDEL', KEYS[3], ARGV[3])
redis.call('PUBLISH', KEYS[4], 'changed')
return {'OK'}
