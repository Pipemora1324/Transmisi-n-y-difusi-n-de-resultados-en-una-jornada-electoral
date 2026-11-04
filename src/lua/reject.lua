-- KEYS: lock, active, held, audit, events, queue, claims. ARGV: token, owner, id, reason
local value = redis.call('GET', KEYS[1])
if not value then return {'LOCK_LOST'} end
if value ~= ARGV[1] then return {'NOT_OWNER'} end
if redis.call('EXISTS', KEYS[3]) == 0 then return {'NOT_FOUND'} end
redis.call('XADD', KEYS[4], 'MAXLEN', '~', 10000, '*', 'action', 'rejected', 'caseId', ARGV[3], 'owner', ARGV[2], 'reason', ARGV[4])
redis.call('DEL', KEYS[1], KEYS[3])
redis.call('ZREM', KEYS[2], ARGV[3])
redis.call('ZREM', KEYS[6], ARGV[3])
redis.call('HDEL', KEYS[7], ARGV[3])
redis.call('PUBLISH', KEYS[5], 'changed')
return {'OK'}
