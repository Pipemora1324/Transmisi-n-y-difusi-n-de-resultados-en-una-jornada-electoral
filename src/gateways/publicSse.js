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
    this.lagging = false;
    this.laggingSince = 0;
    this.closed = false;
  }

  // Entrega de una ventana del backplane. Devuelve true si se escribió.
  push(id, frame) {
    if (this.closed) return false;
    if (this.replaying) { this.buffer.push([id, frame]); return true; }
    if (this.lagging) return false; // RF-9: no se acumula detalle para un cliente lento
    if (this.lastId && compareIds(id, this.lastId) <= 0) return false;
    const ok = this.write(frame);
    if (ok) this.lastId = id;
    return ok;
  }

  write(frame) {
    if (this.closed) return false;
    const res = this.res;
    if (res.writableLength > config.sse.killWaterBytes) {
      this.kill('SLOW_CLIENT');
      return false;
    }
    res.write(frame);
    if (!this.lagging && res.writableLength > config.sse.highWaterBytes) {
      // RF-9: el búfer de salida pasó el umbral. Dejamos de mandar detalle y, cuando el
      // socket se vacíe, mandamos una fotografía del estado actual.
      this.lagging = true;
      this.laggingSince = Date.now();
      metrics.lagging(1);
      res.once('drain', () => this.catchUp());
    }
    return true;
  }

  async catchUp() {
    if (this.closed) return;
    this.lagging = false;
    metrics.lagging(-1);
    // Los deltas omitidos solo se recuperan con la fotografía: no se vuelve al flujo en vivo
    // hasta enviarla, y lo que llegue mientras tanto se entrega después de ella.
    this.replaying = true;
    await syncClient(this, async () => {
      const snap = await takeSnapshot(this.view.topics);
      this.write(snapshotFrame(snap, 'contrapresion'));
      this.lastId = snap.id;
    });
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

  // Desconexión con código: el cliente sabe que debe volver con continuidad.
  kill(code) {
    if (this.closed) return;
    const frame = `event: bye\ndata: ${JSON.stringify({ code, resume: true, lastId: this.lastId })}\n\n`;
    if (code === 'SLOW_CLIENT') {
      metrics.droppedSlow();
      console.warn(`[sse] cliente ${this.id} desconectado por ${code} (pendiente ${this.res.writableLength} bytes)`);
    }
    this.closed = true;
    try { this.res.end(frame); } catch { /* ignorar */ }
    setTimeout(() => this.res.destroy(), 2000).unref();
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
    if (client.lagging) metrics.lagging(-1);
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

// Latido para que proxies y balanceadores no cierren conexiones ociosas; también vigila
// a los clientes que llevan demasiado tiempo rezagados.
setInterval(() => {
  const now = Date.now();
  sendInBatches(registry.allClients(), (c) => {
    if (c.lagging && now - c.laggingSince > 30000) { c.kill('SLOW_CLIENT'); return false; }
    if (c.lagging) return false;
    return c.write(': hb\n\n');
  });
}, config.sse.heartbeatMs).unref();

// RF-10: el público ve explícitamente que el sistema está degradado.
redisEvents.on('status', (ok) => {
  const frame = `event: status\ndata: ${JSON.stringify({ redis: ok })}\n\n`;
  sendInBatches(registry.allClients(), (c) => !c.lagging && c.write(frame));
});

// Cierre ordenado: avisamos para que cada cliente reconecte con espera aleatoria.
export async function closeAllPublic(code = 'INSTANCE_SHUTDOWN') {
  await sendInBatches(registry.allClients(), (c) => { c.kill(code); return true; });
}
