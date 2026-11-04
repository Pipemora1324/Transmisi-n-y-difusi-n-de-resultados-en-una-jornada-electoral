// Rutas HTTP.
import { config } from '../config.js';
import { isRedisHealthy } from '../redis.js';
import { handlePublicSse } from '../gateways/publicSse.js';

const json = (res, code, body) => {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'X-Instance': config.instanceId, 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(body));
};

export async function route(req, res) {
  const url = new URL(req.url, 'http://local');
  const path = url.pathname;

  if (req.method === 'GET' && path === '/sse/public') return handlePublicSse(req, res, url);
  if (req.method === 'GET' && path === '/healthz') {
    // RF-10: una instancia sin Redis se reporta enferma para que el balanceador la evite.
    return json(res, isRedisHealthy() ? 200 : 503, { ok: isRedisHealthy(), instance: config.instanceId });
  }
  json(res, 404, { error: 'No encontrado' });
}
