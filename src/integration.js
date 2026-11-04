// Une las tres partes sobre el servidor de server.js: jurados, público, agregador y aprobación.
// Va aparte para que importar server.js en las pruebas no abra conexiones a Redis.
import { config } from './config.js';
import { geoForClient } from './geo.js';
import { redis, K } from './redis.js';
import { json } from './http/routes.js';
import { approveHeld } from './ingest/actas.js';
import { jurorWss, closeAllJurors } from './gateways/jurorWs.js';
import { handlePublicSse, handleSnapshot, floodAll, closeAllPublic } from './gateways/publicSse.js';
import { startDispatcher } from './fanout/dispatcher.js';
import { startTicker } from './fanout/ticker.js';

const geoJson = JSON.stringify(geoForClient());

const readBody = (req, limit = 4096) => new Promise((resolve, reject) => {
  let data = '';
  req.on('data', (c) => { data += c; if (data.length > limit) { reject(new Error('too large')); req.destroy(); } });
  req.on('end', () => resolve(data));
  req.on('error', reject);
});

async function publicRoutes(req, res, url) {
  const path = url.pathname;
  if (req.method === 'GET' && path === '/sse/public') { await handlePublicSse(req, res, url); return true; }
  if (req.method === 'GET' && path === '/api/snapshot') { await handleSnapshot(req, res, url); return true; }
  if (req.method === 'GET' && path === '/api/geo') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=3600' });
    res.end(geoJson);
    return true;
  }
  if (req.method === 'POST' && path === '/api/inp') {
    try {
      const { value } = JSON.parse(await readBody(req));
      if (Number.isFinite(value) && value >= 0 && value < 60000) {
        await redis.pipeline().lpush(K.inp, Math.round(value)).ltrim(K.inp, 0, 999).exec();
      }
    } catch { /* ignorar muestras inválidas */ }
    res.writeHead(204);
    res.end();
    return true;
  }
  if (req.method === 'POST' && path === '/api/debug/flood') {
    if (!config.demoMode) { json(res, 403, { error: 'Solo disponible con DEMO_MODE=1' }); return true; }
    const kb = Math.min(Number(url.searchParams.get('kb') || 512), 8192);
    json(res, 200, { ok: true, kb, frames: await floodAll(kb) });
    return true;
  }
  return false;
}

export const integration = {
  approveHeld,
  upgrades: { '/ws/jurado': jurorWss },
  extensions: [publicRoutes],
  startExtensions: [async () => {
    startDispatcher();
    startTicker();
    return async () => {
      closeAllJurors();
      await closeAllPublic('INSTANCE_SHUTDOWN');
    };
  }],
};
