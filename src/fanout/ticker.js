// Agregador por ventana de tiempo (RF-4: "la trampa de escala").
//
// 6.400 actas x 400.000 clientes = 2.560 millones de mensajes si se envía uno por acta.
// En su lugar, una sola instancia líder cierra una ventana cada TICK_MS y publica UNA
// entrada con el delta mínimo de los tópicos que cambiaron. El costo deja de depender
// del número de actas y pasa a depender del número de ventanas (≈ 3.600 en 2 horas).
import { redis, K, isRedisHealthy } from '../redis.js';
import { config } from '../config.js';

let isLeader = false;
export const amLeader = () => isLeader;

export function startTicker() {
  setInterval(tick, config.tickMs).unref();
}

async function tick() {
  if (!isRedisHealthy()) { isLeader = false; return; }
  try {
    const won = await redis.leader(K.leader, config.instanceId, config.leaderTtlMs);
    if (won !== 1) {
      if (isLeader) console.log('[ticker] perdí el liderazgo');
      isLeader = false;
      return;
    }
    if (!isLeader) console.log(`[ticker] ${config.instanceId} es el líder del agregador`);
    isLeader = true;

    const res = await redis.flushWindow(
      K.dirty, K.corrections, config.streamKey, K.leader, K.winStats,
      config.instanceId, config.streamMaxLen, Date.now(),
    );
    if (res[0] === 'OK' && config.demoMode) console.log(`[ticker] ventana publicada ${res[1]}`);
  } catch (err) {
    console.error('[ticker] error', err.message);
  }
}
