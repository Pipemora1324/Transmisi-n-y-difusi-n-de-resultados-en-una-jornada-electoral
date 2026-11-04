--[[
  flush_window.lua — Cierre de una ventana de agregación (RF-4, RF-6).

  Lo ejecuta SOLO la instancia líder cada TICK_MS. En una única operación atómica:
    1. Toma los tópicos marcados como sucios desde la ventana anterior.
    2. Calcula el DELTA MÍNIMO por tópico: solo los campos cuyo valor cambió respecto a
       lo último publicado (pub:<tópico>). Los valores son absolutos, no incrementos,
       así que reaplicar un delta es idempotente.
    3. Agrega una única entrada al Stream de historial (con id monótono) y lo recorta.

  Restricción: las claves tot:/inf:/pub: se construyen dentro del script, lo que funciona
  en Redis de un solo nodo (o réplica). En Redis Cluster habría que declararlas en KEYS
  con un hash tag común, p. ej. {e}tot:dep, para que caigan en el mismo slot.

  KEYS 1 dirty  2 corrections:pending  3 stream:updates  4 leader:ticker  5 win:stats
  ARGV 1 instanceId  2 maxlen  3 nowMs
]]

if redis.call('GET', KEYS[4]) ~= ARGV[1] then return { 'NOT_LEADER' } end

local topics = redis.call('SMEMBERS', KEYS[1])
if #topics == 0 then return { 'EMPTY' } end
redis.call('DEL', KEYS[1])

local rawCorr = redis.call('LRANGE', KEYS[2], 0, -1)
redis.call('DEL', KEYS[2])
local corr = {}
for i, c in ipairs(rawCorr) do corr[i] = cjson.decode(c) end

local win = redis.call('HGETALL', KEYS[5])
redis.call('DEL', KEYS[5])
local w = {}
for i = 1, #win, 2 do w[win[i]] = win[i + 1] end

local out = {}
local any = false
for _, t in ipairs(topics) do
  local cur = redis.call('ZRANGE', 'tot:' .. t, 0, -1, 'WITHSCORES')
  local prev = redis.call('HGETALL', 'pub:' .. t)
  local pm = {}
  for i = 1, #prev, 2 do pm[prev[i]] = prev[i + 1] end

  local delta = {}
  local has = false
  for i = 1, #cur, 2 do
    local k, v = cur[i], cur[i + 1]
    if pm[k] ~= v then
      delta[k] = tonumber(v)
      redis.call('HSET', 'pub:' .. t, k, v)
      has = true
    end
  end
  local inf = redis.call('GET', 'inf:' .. t) or '0'
  if pm['__inf'] ~= inf then
    delta['__inf'] = tonumber(inf)
    redis.call('HSET', 'pub:' .. t, '__inf', inf)
    has = true
  end
  if has then out[t] = delta; any = true end
end

if not any then return { 'EMPTY' } end

local payload = cjson.encode({
  t = out,
  c = corr,
  n = tonumber(w.count or '0'),   -- actas aplicadas en esta ventana (para la cifra ingenua)
  t0 = tonumber(w.t0 or ARGV[3]), -- recepción de la primera acta de la ventana (latencia)
  ts = tonumber(ARGV[3]),
})
local id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', tonumber(ARGV[2]), '*', 'd', payload)
return { 'OK', id, payload }
