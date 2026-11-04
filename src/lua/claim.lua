-- KEYS: queue, claims, active, events. ARGV: lockPrefix, heldPrefix, id, token, owner, ttl
local id = ARGV[3]
if id == '' then
  local candidates = redis.call('ZREVRANGE', KEYS[1], 0, 199)
  for _, candidate in ipairs(candidates) do
    if redis.call('EXISTS', ARGV[1] .. candidate) == 0 then id = candidate; break end
  end
  if id == '' then return {'EMPTY'} end
end
if not redis.call('ZSCORE', KEYS[1], id) then return {'NOT_QUEUED'} end
local held = redis.call('HGETALL', ARGV[2] .. id)
if #held == 0 then return {'NOT_FOUND'} end
local key = ARGV[1] .. id
if redis.call('EXISTS', key) == 1 then return {'BUSY'} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local expires = now + tonumber(ARGV[6])
redis.call('SET', key, ARGV[4], 'PX', ARGV[6])
redis.call('HSET', KEYS[2], id, cjson.encode({auditor=ARGV[5], since=now}))
redis.call('ZADD', KEYS[3], expires, id)
redis.call('PUBLISH', KEYS[4], 'changed')
local payload = {id=id}
for i=1,#held,2 do payload[held[i]]=held[i+1] end
return {'OK', id, cjson.encode(payload), tostring(expires)}
