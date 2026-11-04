// Configuración central del sistema. Todo valor se puede sobrescribir con variables de entorno
// para que las 8 instancias compartan el mismo código y solo cambien su identidad.
import os from 'node:os';

const num = (name, fallback) => {
  const raw = process.env[name];
  return raw === undefined || raw === '' ? fallback : Number(raw);
};

export const config = {
  port: num('PORT', 3000),
  // Identificador visible de la instancia (RT-1): en Docker es el hostname del contenedor.
  instanceId: process.env.INSTANCE_ID || `${os.hostname()}-${process.pid}`,
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  demoMode: process.env.DEMO_MODE === '1',

  // Fan-out (RF-4): ventana de agregación y tamaño de lote antes de ceder el Event Loop.
  tickMs: num('TICK_MS', 2000),
  batchSize: num('FANOUT_BATCH', 500),

  // Historial de recuperación (RF-6): entradas del Stream retenidas (1 por tick ≈ 33 min a 2 s).
  streamKey: 'stream:updates',
  streamMaxLen: num('STREAM_MAXLEN', 1000),
  // Si al reconectar faltan más entradas que esto, es más barato enviar una fotografía.
  replayMaxEntries: num('REPLAY_MAX', 300),

  // Contrapresión SSE (RF-9).
  sse: {
    highWaterBytes: num('SSE_HIGH_WATER', 256 * 1024),
    killWaterBytes: num('SSE_KILL_WATER', 1024 * 1024),
    heartbeatMs: num('SSE_HEARTBEAT_MS', 15000),
  },

  // Bloqueos de verificación (RF-7).
  lock: {
    ttlMs: num('LOCK_TTL_MS', 30000),
    renewEveryMs: num('LOCK_RENEW_MS', 10000),
    idleMs: num('AUDITOR_IDLE_MS', 5 * 60 * 1000),
  },

  // Presencia (RF-8).
  presence: { ttlMs: num('PRESENCE_TTL_MS', 15000), beatMs: 5000 },

  // Idempotencia (RF-1).
  idemTtlSec: num('IDEM_TTL_SEC', 6 * 3600),

  // Límite de tasa por credencial (sección 5).
  rateLimit: { windowSec: 60, juror: num('RL_JUROR', 30), publicConnect: num('RL_PUBLIC', 1000) },

  // Elección de líder para el agregador y el barrido de bloqueos.
  leaderTtlMs: 5000,
};
