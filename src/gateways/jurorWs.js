// Audiencia de jurados: WebSocket (6.400 clientes intermitentes, tráfico bidireccional).
//
// Por qué WebSocket: el jurado ENVÍA el acta y necesita una respuesta inequívoca
// correlacionada con ese envío (ack por clave de idempotencia), además de recibir
// correcciones o rechazos. Con un solo socket full-duplex no hay que abrir una petición
// HTTP por cada reintento en una red móvil pobre.
import { WebSocketServer } from 'ws';
import { config } from '../config.js';
import { receiveActa } from '../ingest/actas.js';
import { checkRateLimit } from '../ingest/rateLimit.js';
import { metrics } from '../metrics/metrics.js';
import { redisEvents, isRedisHealthy } from '../redis.js';

export const jurorWss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });
const sockets = new Set();

jurorWss.on('connection', (ws, req, url) => {
  const jurado = (url.searchParams.get('jurado') || '').slice(0, 32);
  if (!/^JUR-\d{3,8}$/.test(jurado)) {
    ws.close(4001, 'Credencial de jurado inválida');
    return;
  }
  ws.jurado = jurado;
  ws.isAlive = true;
  sockets.add(ws);
  metrics.conn('juror', 1);
  send(ws, { type: 'hello', instance: config.instanceId, redis: isRedisHealthy(), msg: `Conectado a ${config.instanceId}` });

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('close', () => { sockets.delete(ws); metrics.conn('juror', -1); });
  ws.on('message', async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return send(ws, { type: 'error', message: 'JSON inválido' }); }
    if (msg?.type !== 'acta' || !msg.acta) return;
    const acta = { ...msg.acta, jurado };

    const rl = await checkRateLimit(`jur:${jurado}`, config.rateLimit.juror, config.rateLimit.windowSec).catch(() => ({ allowed: true }));
    if (!rl.allowed) {
      return send(ws, { type: 'ack', ref: acta.idem, status: 'RATE_LIMITED', final: false, retryAfterSec: rl.retryAfterSec,
        acta: acta.acta, message: 'Demasiados envíos en poco tiempo. Se reintentará automáticamente.' });
    }
    const result = await receiveActa(acta);
    send(ws, { ...result, ref: acta.idem, instance: config.instanceId });
  });
});

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

// Detección de conexiones muertas (portátil sin batería, red caída sin FIN).
setInterval(() => {
  for (const ws of sockets) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 20000).unref();

// RF-10: si Redis cae, los jurados lo saben al instante y su app sigue encolando.
redisEvents.on('status', (ok) => {
  for (const ws of sockets) send(ws, { type: 'status', redis: ok });
});

export function closeAllJurors() {
  for (const ws of sockets) ws.close(1012, 'Reinicio de instancia: reconecta');
}
