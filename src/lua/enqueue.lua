-- Fixture local. La ingesta definitiva escribe el hash held:<id> y queue:verify.
-- KEYS: queue, held, events. ARGV: id, priority, JSON de campos
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
local record = cjson.decode(ARGV[3])
for field, value in pairs(record) do redis.call('HSET', KEYS[2], field, tostring(value)) end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('PUBLISH', KEYS[3], 'changed')
return 1
