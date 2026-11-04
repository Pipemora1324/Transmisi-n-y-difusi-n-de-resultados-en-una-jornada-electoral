--[[
  leader.lua — Adquirir o renovar el liderazgo del agregador (un único "ticker").
  Si el líder muere, la clave expira en leaderTtlMs y otra instancia toma el relevo;
  los tópicos sucios siguen en Redis, así que no se pierde ninguna ventana.
  KEYS 1 leader:ticker   ARGV 1 instanceId  2 ttlMs
]]
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 1
end
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
  return 1
end
return 0
