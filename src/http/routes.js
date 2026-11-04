// Rutas HTTP.
import { config } from '../config.js';
import { redis, K, isRedisHealthy } from '../redis.js';
import { handlePublicSse, handleSnapshot, floodAll } from '../gateways/publicSse.js';

const json = (res, code, body) => {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'X-Instance': config.instanceId, 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(body));
};

const readBody = (req, limit = 4096) => new Promise((resolve, reject) => {
  let data = '';
  req.on('data', (c) => { data += c; if (data.length > limit) { reject(new Error('too large')); req.destroy(); } });
  req.on('end', () => resolve(data));
  req.on('error', reject);
});

export async function route(req, res) {
  const url = new URL(req.url, 'http://local');
  const path = url.pathname;

  if (req.method === 'GET' && path === '/sse/public') return handlePublicSse(req, res, url);
  if (req.method === 'GET' && path === '/api/snapshot') return handleSnapshot(req, res, url);
  if (req.method === 'GET' && path === '/healthz') {
    // RF-10: una instancia sin Redis se reporta enferma para que el balanceador la evite.
    return json(res, isRedisHealthy() ? 200 : 503, { ok: isRedisHealthy(), instance: config.instanceId });
  }
  if (req.method === 'POST' && path === '/api/inp') {
    try {
      const { value } = JSON.parse(await readBody(req));
      if (Number.isFinite(value) && value >= 0 && value < 60000) {
        await redis.pipeline().lpush(K.inp, Math.round(value)).ltrim(K.inp, 0, 999).exec();
      }
    } catch { /* ignorar muestras inválidas */ }
    res.writeHead(204); return res.end();
  }
  if (req.method === 'POST' && path === '/api/debug/flood') {
    if (!config.demoMode) return json(res, 403, { error: 'Solo disponible con DEMO_MODE=1' });
    const kb = Math.min(Number(url.searchParams.get('kb') || 512), 8192);
    const sent = await floodAll(kb);
    return json(res, 200, { ok: true, kb, frames: sent });
  }
  json(res, 404, { error: 'No encontrado' });
}
