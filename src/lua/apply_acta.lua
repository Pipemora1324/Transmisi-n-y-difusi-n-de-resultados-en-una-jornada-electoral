--[[
  apply_acta.lua — Operación atómica compuesta (RF-1, RF-2, RF-3, RF-7).

  Redis ejecuta el script completo sin intercalar comandos de otros clientes, así que
  la reversión de la versión anterior, la suma de la nueva y la actualización de los
  cuatro niveles (mesa, puesto, municipio, departamento) son UNA sola operación
  indivisible: ningún lector ve el municipio con un acta que aún no está en el puesto.

  Modos:
    apply   -> acta válida enviada por un jurado
    hold    -> acta dudosa: se guarda aparte y entra a la cola de verificación
    approve -> un auditor dueño del bloqueo aprueba un acta retenida

  KEYS
   1 idem:<clave>          2 acta:<id>            3 tot:mesa:<m>
   4 tot:puesto:<p>        5 tot:mun:<x>          6 tot:dep
   7 inf:puesto:<p>        8 inf:mun:<x>          9 inf:dep
  10 dirty                11 queue:verify        12 held:<id>
  13 corrections:pending  14 lock:acta:<id>      15 claims
  16 win:stats
  ARGV
   1 acta  2 version  3 votosJson  4 sufragantes  5 jurado  6 mesa  7 puesto
   8 municipio  9 idemTtl  10 modo  11 nowMs  12 hashFoto  13 prioridad
  14 tokenBloqueo  15 motivo
]]

local mode = ARGV[10]
local now = ARGV[11]

-- 1) Idempotencia: un reenvío con la misma clave devuelve la MISMA respuesta original.
if mode ~= 'approve' then
  local cached = redis.call('GET', KEYS[1])
  if cached then return { 'DUPLICATE', cached } end
end

local function remember(status, payload)
  local body = cjson.encode(payload)
  if mode ~= 'approve' then
    redis.call('SET', KEYS[1], body, 'EX', tonumber(ARGV[9]))
  end
  return { status, body }
end

local acta = ARGV[1]
local version = tonumber(ARGV[2])
local votosJson = ARGV[3]
local sufragantes = ARGV[4]
local jurado = ARGV[5]
local hashFoto = ARGV[12]

-- 2) Aprobación: solo el dueño actual del bloqueo puede resolver (liberación segura).
if mode == 'approve' then
  local owner = redis.call('GET', KEYS[14])
  if owner ~= ARGV[14] then
    return { 'NOT_OWNER', cjson.encode({ acta = acta, msg = 'El bloqueo ya no te pertenece' }) }
  end
  local held = redis.call('HGETALL', KEYS[12])
  if #held == 0 then
    return { 'NOT_FOUND', cjson.encode({ acta = acta }) }
  end
  local h = {}
  for i = 1, #held, 2 do h[held[i]] = held[i + 1] end
  version = tonumber(h.version)
  votosJson = h.votos
  sufragantes = h.sufragantes
  jurado = h.jurado
  hashFoto = h.hash_foto or ''
end

-- 3) Convergencia por versión (RF-2): el estado final depende del conjunto, no del orden.
local cur = redis.call('HGET', KEYS[2], 'version')
cur = cur and tonumber(cur) or nil

local function cleanupHeld()
  redis.call('DEL', KEYS[12])
  redis.call('ZREM', KEYS[11], acta)
  redis.call('DEL', KEYS[14])
  redis.call('HDEL', KEYS[15], acta)
end

if cur and cur >= version then
  local status = (cur == version) and 'DUPLICATE_VERSION' or 'STALE'
  if mode == 'approve' then
    cleanupHeld()
    redis.call('PUBLISH', 'ch:audit', cjson.encode({ type = 'queue:resolved', acta = acta, result = 'stale' }))
  end
  return remember(status, { status = status, acta = acta, applied = cur, received = version })
end

-- 4) Acta dudosa: queda fuera del acumulado hasta que un auditor la apruebe.
if mode == 'hold' then
  local heldVersion = redis.call('HGET', KEYS[12], 'version')
  if heldVersion and tonumber(heldVersion) >= version then
    return remember('DUPLICATE_VERSION', { status = 'DUPLICATE_VERSION', acta = acta, held = tonumber(heldVersion), received = version })
  end
  redis.call('HSET', KEYS[12], 'version', version, 'votos', votosJson, 'sufragantes', sufragantes,
    'jurado', jurado, 'mesa', ARGV[6], 'puesto', ARGV[7], 'municipio', ARGV[8],
    'hash_foto', hashFoto, 'motivo', ARGV[15], 'ts', now)
  redis.call('ZADD', KEYS[11], tonumber(ARGV[13]), acta)
  redis.call('PUBLISH', 'ch:audit', cjson.encode({ type = 'queue:add', acta = acta, version = version, priority = tonumber(ARGV[13]), motivo = ARGV[15] }))
  return remember('HELD', { status = 'HELD', acta = acta, version = version, motivo = ARGV[15] })
end

-- 5) Aplicación: revertir versión anterior (si existe) y sumar la nueva en los 4 niveles.
local newVotes = cjson.decode(votosJson)
local prevVotes = nil
if cur then prevVotes = cjson.decode(redis.call('HGET', KEYS[2], 'votos')) end

local function addVotes(votes, sign)
  for opt, n in pairs(votes) do
    local d = sign * tonumber(n)
    if d ~= 0 then
      redis.call('ZINCRBY', KEYS[4], d, opt)
      redis.call('ZINCRBY', KEYS[5], d, opt)
      redis.call('ZINCRBY', KEYS[6], d, opt)
    end
  end
end

if prevVotes then
  addVotes(prevVotes, -1)
else
  -- Primera versión de esta mesa: sube el conteo de mesas informadas en cada nivel.
  redis.call('INCR', KEYS[7])
  redis.call('INCR', KEYS[8])
  redis.call('INCR', KEYS[9])
end
addVotes(newVotes, 1)

redis.call('DEL', KEYS[3])
for opt, n in pairs(newVotes) do redis.call('ZADD', KEYS[3], tonumber(n), opt) end

redis.call('HSET', KEYS[2], 'version', version, 'votos', votosJson, 'sufragantes', sufragantes,
  'jurado', jurado, 'mesa', ARGV[6], 'puesto', ARGV[7], 'municipio', ARGV[8],
  'hash_foto', hashFoto, 'ts', now)

-- Si existía una versión retenida más vieja que la que acabamos de aplicar, ya no aplica.
local heldVersion = redis.call('HGET', KEYS[12], 'version')
if mode == 'approve' or (heldVersion and tonumber(heldVersion) <= version) then
  if heldVersion then
    cleanupHeld()
    redis.call('PUBLISH', 'ch:audit', cjson.encode({ type = 'queue:resolved', acta = acta, result = (mode == 'approve') and 'approved' or 'superseded' }))
  end
end

-- 6) Marcar tópicos sucios para el agregador (RF-4) y registrar correcciones (explicación).
redis.call('SADD', KEYS[10], 'dep', 'mun:' .. ARGV[8], 'puesto:' .. ARGV[7])
redis.call('HINCRBY', KEYS[16], 'count', 1)
redis.call('HSETNX', KEYS[16], 't0', now)

local status = 'APPLIED'
if prevVotes then
  status = 'CORRECTED'
  redis.call('RPUSH', KEYS[13], cjson.encode({
    acta = acta, mesa = tonumber(ARGV[6]), puesto = tonumber(ARGV[7]), municipio = tonumber(ARGV[8]),
    from = cur, to = version, before = prevVotes, after = newVotes,
  }))
end

return remember(status, { status = status, acta = acta, version = version, previous = cur })
