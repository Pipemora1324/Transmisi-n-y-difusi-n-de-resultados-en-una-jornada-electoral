// Conexiones a Redis y carga de los scripts Lua.
// Se usan tres conexiones porque un cliente en modo SUBSCRIBE o bloqueado en XREAD no
// puede ejecutar otros comandos al mismo tiempo.
import { Redis } from 'ioredis';
import { readFileSync } from 'node:fs';
import { config } from './config.js';

const lua = (name) => readFileSync(new URL(`./lua/${name}.lua`, import.meta.url), 'utf8');

function makeClient(role) {
  const client = new Redis(config.redisUrl, {
    maxRetriesPerRequest: role === 'cmd' ? 1 : null,
    connectTimeout: 3000,
    retryStrategy: (times) => Math.min(200 * 2 ** times, 5000),
    lazyConnect: false,
  });
  client.on('error', (err) => {
    if (role === 'cmd') console.error(`[redis:${role}] error: ${err.message}`);
  });
  return client;
}

export const redis = makeClient('cmd');
export const sub = makeClient('sub');
export const streamReader = makeClient('stream');

// Registra los scripts: ioredis usa EVALSHA y reenvía el código si Redis no lo tiene en caché.
redis.defineCommand('applyActa', { numberOfKeys: 16, lua: lua('apply_acta') });
redis.defineCommand('flushWindow', { numberOfKeys: 5, lua: lua('flush_window') });
redis.defineCommand('snapshot', { numberOfKeys: 1, lua: lua('snapshot') });
redis.defineCommand('claimActa', { numberOfKeys: 3, lua: lua('claim') });
redis.defineCommand('renewLock', { numberOfKeys: 1, lua: lua('renew') });
redis.defineCommand('releaseLock', { numberOfKeys: 2, lua: lua('release') });
redis.defineCommand('rejectActa', { numberOfKeys: 4, lua: lua('reject') });
redis.defineCommand('leader', { numberOfKeys: 1, lua: lua('leader') });
redis.defineCommand('rateLimit', { numberOfKeys: 1, lua: lua('rate_limit') });

// Nombres de claves en un único lugar.
export const K = {
  idem: (k) => `idem:${k}`,
  acta: (a) => `acta:${a}`,
  held: (a) => `held:${a}`,
  totMesa: (m) => `tot:mesa:${m}`,
  totPuesto: (p) => `tot:puesto:${p}`,
  totMun: (x) => `tot:mun:${x}`,
  totDep: 'tot:dep',
  infPuesto: (p) => `inf:puesto:${p}`,
  infMun: (x) => `inf:mun:${x}`,
  infDep: 'inf:dep',
  dirty: 'dirty',
  queue: 'queue:verify',
  corrections: 'corrections:pending',
  lock: (a) => `lock:acta:${a}`,
  claims: 'claims',
  winStats: 'win:stats',
  leader: 'leader:ticker',
  presence: 'presence',
  presenceInfo: 'presence:info',
  metrics: (i) => `metrics:${i}`,
  instances: 'instances',
  rl: (cred, bucket) => `rl:${cred}:${bucket}`,
  statsEmitted: 'stats:emitted',
  statsNaive: 'stats:naive',
  actasPerMinute: (m) => `stats:actas:${m}`,
  inp: 'stats:inp',
};
