import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { sameSecret } from '../gateways/auditorWs.js';
const publicRoot = path.resolve(fileURLToPath(new URL('../../public/', import.meta.url)));
const types = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.svg': 'image/svg+xml' };
export function json(res, status, value) { res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(value)); }
export function createRouter({ config, redis, publisher, metrics, extensions = [], streams }) {
  return async (req, res) => {
    metrics.increment('httpRequests');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self' https://fonts.googleapis.com; style-src-attr 'unsafe-inline'; font-src https://fonts.gstatic.com; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'");
    let url;
    try { url = new URL(req.url, 'http://local'); } catch { return json(res, 400, { error: 'BAD_URL' }); }
    if (url.pathname === '/healthz') {
      const healthy = redis.status === 'ready' && await redis.ping().then(() => true, () => false);
      return json(res, healthy ? 200 : 503, { status: healthy ? 'ok' : 'degraded', redis: healthy, instanceId: config.instanceId });
    }
    if (url.pathname === '/sse/ops' || url.pathname === '/api/ops') {
      if (req.method !== 'GET') return json(res, 405, { error: 'METHOD_NOT_ALLOWED' });
      if (!sameSecret(req.headers.authorization, 'Bearer ' + config.opsToken) || !config.opsToken) return json(res, 401, { error: 'UNAUTHORIZED' });
      if (url.pathname === '/api/ops') return json(res, 200, publisher.latest || await publisher.tick());
      if (streams.size >= 200) return json(res, 503, { error: 'OPS_CAPACITY' });
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write('retry: 3000\n\n'); streams.add(res); metrics.connection('ops', 1);
      let closed = false;
      const unsubscribe = publisher.subscribe(sample => {
        if (res.writableLength > 128 * 1024) return res.destroy();
        res.write(`event: metrics\ndata: ${JSON.stringify(sample)}\n\n`);
      });
      const heartbeat = setInterval(() => { if (res.writableLength > 128 * 1024) res.destroy(); else res.write(': heartbeat\n\n'); }, 15000);
      const close = () => { if (closed) return; closed = true; clearInterval(heartbeat); unsubscribe(); streams.delete(res); metrics.connection('ops', -1); };
      res.on('close', close); res.on('error', close); return;
    }
    for (const extension of extensions) if (await extension(req, res, url)) return;
    if (!['GET', 'HEAD'].includes(req.method)) return json(res, 405, { error: 'METHOD_NOT_ALLOWED' });
    let pathname;
    try { pathname = decodeURIComponent(url.pathname); } catch { return json(res, 400, { error: 'BAD_URL' }); }
    if (pathname === '/') pathname = '/index.html';
    if (pathname === '/jurado') pathname = '/jurado.html';
    if (pathname === '/auditor') pathname = '/auditor.html';
    if (pathname === '/ops') pathname = '/ops.html';
    const filename = path.resolve(publicRoot, '.' + pathname);
    if (!filename.startsWith(publicRoot + path.sep) || pathname.includes('\0') || pathname.includes('\\')) return json(res, 403, { error: 'FORBIDDEN' });
    const info = await stat(filename).catch(() => null);
    if (!info?.isFile() || !types[path.extname(filename)]) return json(res, 404, { error: 'NOT_FOUND' });
    res.writeHead(200, { 'Content-Type': types[path.extname(filename)], 'Content-Length': info.size, 'Cache-Control': 'no-cache' });
    if (req.method === 'HEAD') return res.end();
    const source = createReadStream(filename); source.on('error', () => res.destroy()); source.pipe(res);
  };
}
