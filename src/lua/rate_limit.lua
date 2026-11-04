--[[
  rate_limit.lua — Contador por ventana fija por credencial (sección 5).
  KEYS 1 rl:<credencial>:<ventana>   ARGV 1 límite  2 ventanaSeg
  Devuelve { permitido(0/1), usados, ttlRestante }
]]
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2])) end
local ttl = redis.call('TTL', KEYS[1])
if n > tonumber(ARGV[1]) then return { 0, n, ttl } end
return { 1, n, ttl }
