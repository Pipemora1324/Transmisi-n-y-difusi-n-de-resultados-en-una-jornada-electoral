--[[
  snapshot.lua — Fotografía consistente de uno o varios tópicos (RF-3, RF-6, RF-9).

  Lee lo PUBLICADO (pub:<tópico>) junto con el id de la última entrada del Stream en la
  misma operación atómica. Así la fotografía corresponde exactamente al estado que tenía
  un cliente que nunca se desconectó tras recibir ese id, y la continuidad posterior es
  "todo lo que venga después de ese id".

  KEYS 1 stream:updates
  ARGV tópicos (dep, mun:11, puesto:176...)
]]

local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local lastId = '0-0'
if #last > 0 then lastId = last[1][1] end

local out = {}
for _, t in ipairs(ARGV) do
  local h = redis.call('HGETALL', 'pub:' .. t)
  local m = {}
  for i = 1, #h, 2 do m[h[i]] = tonumber(h[i + 1]) end
  out[t] = m
end
return { lastId, cjson.encode(out) }
