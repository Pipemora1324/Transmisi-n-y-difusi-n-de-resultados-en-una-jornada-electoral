// Historial reciente para reconexión con recuperación (RF-6).
//
// El Pub/Sub clásico de Redis no tiene memoria: lo publicado mientras alguien está
// desconectado se pierde. Por eso el historial vive en un Redis STREAM:
//   - cada ventana del agregador es UNA entrada con id monótono "<ms>-<seq>";
//   - se recorta con MAXLEN ~ STREAM_MAXLEN (1000 entradas ≈ 33 min a 2 s por ventana);
//   - el id de la entrada es el id del evento SSE, así que el navegador lo guarda solo
//     y nos lo devuelve en Last-Event-ID (o en ?lastId=) al reconectar.
import { redis, K } from '../redis.js';
import { config } from '../config.js';
import { compareIds, isValidId } from './broadcaster.js';

export function parseEntry(fields) {
  // fields = ['d', '<json>']
  const idx = fields.indexOf('d');
  return idx >= 0 ? JSON.parse(fields[idx + 1]) : null;
}

// Mezcla los deltas de varias entradas en uno solo, quedándose con el valor más reciente.
export function mergeDeltas(entries, topics) {
  const delta = {};
  for (const e of entries) {
    for (const t of topics) {
      if (e.t[t]) delta[t] = { ...(delta[t] || {}), ...e.t[t] };
    }
  }
  return delta;
}

export async function takeSnapshot(topics) {
  const [lastId, json] = await redis.snapshot(config.streamKey, ...topics);
  return { id: lastId, data: JSON.parse(json) };
}

// Decide qué entregarle a un cliente que vuelve con `lastId`.
export async function recover(lastId, topics) {
  if (!isValidId(lastId)) return { mode: 'snapshot', reason: 'sin-id' };
  const oldest = await redis.xrange(config.streamKey, '-', '+', 'COUNT', 1);
  if (!oldest.length) return { mode: 'snapshot', reason: 'historial-vacio' };
  // El id que trae el cliente ya fue recortado del historial: hubo un hueco irrecuperable.
  if (compareIds(lastId, oldest[0][0]) < 0) return { mode: 'snapshot', reason: 'fuera-de-ventana' };

  const rows = await redis.xrange(config.streamKey, `(${lastId}`, '+', 'COUNT', config.replayMaxEntries + 1);
  if (rows.length > config.replayMaxEntries) return { mode: 'snapshot', reason: 'demasiado-atras' };
  if (!rows.length) return { mode: 'none', id: lastId };

  const entries = rows.map(([, f]) => parseEntry(f)).filter(Boolean);
  const delta = mergeDeltas(entries, topics);
  return { mode: 'delta', id: rows[rows.length - 1][0], delta, entries: rows.length };
}
