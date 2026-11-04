// Backplane de difusión (sección 5): cada instancia lee el Stream de Redis con XREAD
// BLOCK y reparte cada ventana a SUS clientes locales. Ninguna instancia habla con otra.
//
// Diseño del espacio de canales: tras la agregación hay ~0,5 mensajes por segundo para
// todo el departamento, y con 50.000 clientes por instancia cada instancia necesita casi
// todos los municipios. Un único flujo global es más barato que 16 + 1.842 canales.
import { streamReader } from '../redis.js';
import { config } from '../config.js';
import { registry } from './topics.js';
import { sendInBatches, enqueueFanout } from './broadcaster.js';
import { parseEntry } from './history.js';
import { metrics } from '../metrics/metrics.js';

let lastSeenId = '$';
let running = false;

export function startDispatcher() {
  if (running) return;
  running = true;
  loop();
}

async function loop() {
  while (running) {
    try {
      const res = await streamReader.xread('BLOCK', 5000, 'COUNT', 50, 'STREAMS', config.streamKey, lastSeenId);
      if (!res) continue;
      for (const [, rows] of res) {
        for (const [id, fields] of rows) {
          lastSeenId = id;
          const entry = parseEntry(fields);
          if (entry) enqueueFanout(() => dispatch(id, entry));
        }
      }
    } catch (err) {
      // Redis caído: esperamos y seguimos desde el último id visto (sin perder entradas
      // que sigan dentro del historial).
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

async function dispatch(id, entry) {
  const changed = Object.keys(entry.t);
  const groups = registry.groupsFor(changed);
  metrics.naive(entry.n * registry.size);

  let sent = 0;
  for (const g of groups) {
    const delta = {};
    for (const t of g.topics) if (entry.t[t]) delta[t] = entry.t[t];
    const payload = JSON.stringify({ id, d: delta, ts: entry.ts });
    // Mensaje serializado UNA vez por grupo y reutilizado para todos sus clientes.
    const frame = `id: ${id}\nevent: delta\ndata: ${payload}\n\n`;
    sent += await sendInBatches(g.clients, (c) => c.push(id, frame));
  }
  metrics.emitted(sent);
  metrics.publishLatency(Date.now() - (entry.t0 || entry.ts));
}

export const currentStreamId = () => lastSeenId;
