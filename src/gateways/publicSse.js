// Audiencia pública: Server-Sent Events (hasta 400.000 navegadores, una sola dirección).
//
// Por qué SSE y no WebSocket para esta audiencia (ver docs/ARQUITECTURA.md §7):
//   - El público solo recibe: no necesita canal de subida.
//   - SSE trae de fábrica el id de evento y Last-Event-ID, que es justo el mecanismo de
//     continuidad de RF-6.
//   - Es HTTP plano: pasa por proxies y firewalls y lo cachea/balancea cualquier LB.
//   - Con HTTP/2 en el balanceador desaparece el límite de 6 conexiones por navegador.
import { config } from '../config.js';
import { registry, parseView } from '../fanout/topics.js';
import { sendInBatches, compareIds } from '../fanout/broadcaster.js';
import { recover, takeSnapshot } from '../fanout/history.js';
import { metrics } from '../metrics/metrics.js';
import { redisEvents, isRedisHealthy } from '../redis.js';
import { checkRateLimit } from '../ingest/rateLimit.js';

let nextId = 1;
const SYNC_RETRY_MS = 3000;

class SseClient {
  constructor(res, view) {
    this.id = nextId++;
    this.res = res;
    this.view = view;
    this.lastId = null;
    this.replaying = true;
    this.buffer = [];
    this.closed = false;
  }

  // Entrega de una ventana del backplane. Devuelve true si se escribió.
  push(id, frame) {
    if (this.closed) return false;
    if (this.replaying) { this.buffer.push([id, frame]); return true; }
    if (this.lastId && compareIds(id, this.lastId) <= 0) return false;
    const ok = this.write(frame);
    if (ok) this.lastId = id;
    return ok;
  }

  write(frame) {
    if (this.closed) return false;
    this.res.write(frame);
    return true;
  }

  finishReplay() {
    this.replaying = false;
    for (const [id, frame] of this.buffer) {
      if (!this.lastId || compareIds(id, this.lastId) > 0) {
        this.write(frame);
        this.lastId = id;
      }
    }
    this.buffer = [];
  }
}

// Pone al día a un cliente y lo pasa al flujo en vivo. Si Redis no responde, reintenta en
// lugar de seguir sin el estado base: los deltas solo traen lo que cambió, así que sin
// fotografía o historial el cliente mostraría cifras incompletas sin saberlo.
async function syncClient(client, load) {
  let warned = false;
  while (!client.closed) {
    try {
      if (!isRedisHealthy()) throw new Error('Redis no disponible');
      await load();
      break;
    } catch {
      if (!warned) client.write(`event: status\ndata: ${JSON.stringify({ redis: false })}\n\n`);
      warned = true;
      await new Promise((r) => setTimeout(r, SYNC_RETRY_MS * (0.5 + Math.random())));
    }
  }
  if (!client.closed) client.finishReplay();
}

function snapshotFrame(snap, reason) {
  return `id: ${snap.id}\nevent: snapshot\ndata: ${JSON.stringify({ id: snap.id, s: snap.data, reason })}\n\n`;
}

export async function handlePublicSse(req, res, url) {
  // nginx agrega la IP real al final de X-Forwarded-For; lo anterior lo controla el cliente.
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',').at(-1).trim();
  const rl = await checkRateLimit(`pub:${ip}`, config.rateLimit.publicConnect, config.rateLimit.windowSec).catch(() => ({ allowed: true }));
  if (!rl.allowed) {
    res.writeHead(429, { 'Retry-After': String(rl.retryAfterSec), 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: 'Demasiadas reconexiones desde tu red. Intenta en unos segundos.' }));
  }

  // Pudo desconectarse durante la consulta del límite: 'close' ya no volvería a dispararse.
  if (res.destroyed) return;

  const view = parseView(url.searchParams.get('view'));
  const lastId = req.headers['last-event-id'] || url.searchParams.get('lastId');

  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
    'X-Instance': config.instanceId,
  });
  res.socket?.setNoDelay(true);

  const client = new SseClient(res, view);
  registry.add(client);
  metrics.conn('public', 1);
  res.on('close', () => {
    client.closed = true;
    registry.remove(client);
    metrics.conn('public', -1);
  });

  client.res.write(`event: hello\ndata: ${JSON.stringify({ instance: config.instanceId, view: view.topics, redis: isRedisHealthy() })}\n\n`);

  await syncClient(client, async () => {
    const r = await recover(lastId, view.topics);
    if (r.mode === 'snapshot') {
      const snap = await takeSnapshot(view.topics);
      client.write(snapshotFrame(snap, r.reason));
      client.lastId = snap.id;
    } else if (r.mode === 'delta') {
      const payload = JSON.stringify({ id: r.id, d: r.delta, c: r.corr, replayed: r.entries });
      client.write(`id: ${r.id}\nevent: delta\ndata: ${payload}\n\n`);
      client.lastId = r.id;
    } else {
      client.lastId = r.id;
    }
  });
}

// RF-10: el público ve explícitamente que el sistema está degradado.
redisEvents.on('status', (ok) => {
  const frame = `event: status\ndata: ${JSON.stringify({ redis: ok })}\n\n`;
  sendInBatches(registry.allClients(), (c) => c.write(frame));
});
