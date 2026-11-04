-- KEYS: active, claims, events, presence, people. ARGV: lockPrefix, batch
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[2])
local returned = 0
for _, id in ipairs(expired) do
  local ttl = redis.call('PTTL', ARGV[1] .. id)
  if ttl <= 0 then
    redis.call('ZREM', KEYS[1], id)
    redis.call('HDEL', KEYS[2], id)
    returned = returned + 1
  else redis.call('ZADD', KEYS[1], now + ttl, id) end
end
local ghosts = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, ARGV[2])
for _, session in ipairs(ghosts) do
  redis.call('ZREM', KEYS[4], session)
  redis.call('HDEL', KEYS[5], session)
end
if returned > 0 or #ghosts > 0 then redis.call('PUBLISH', KEYS[3], 'changed') end
return {returned, #ghosts}
