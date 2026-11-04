import http from 'node:http';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { access } from 'node:fs/promises';
import { readConfig } from './support/config.js';
import { createRedis } from './support/redis.js';
import { createQueue } from './verify/queue.js';
import { createLocks } from './verify/locks.js';
import { createPresence } from './verify/presence.js';
import { createSweeper } from './verify/sweeper.js';
import { createMetrics, useProcessMetrics } from './metrics/metrics.js';
import { createMetricsPublisher } from './metrics/publisher.js';
import { createAuditorGateway } from './gateways/auditorWs.js';
import { createRouter, json } from './http/routes.js';

export async function startServer({ config = readConfig(), approveHeld, extensions = [], upgrades = {}, startExtensions = [] } = {}) {
  if (!approveHeld) {
    const candidate = path.resolve(config.approveModule || 'src/ingest/actas.js');
    if (config.approveModule || await access(candidate).then(() => true, () => false)) {
      approveHeld = (await import(pathToFileURL(candidate).href)).approveHeld;
      if (typeof approveHeld !== 'function') throw new Error('El módulo no exporta approveHeld(actaId, token)');
    }
  }
  const connections = createRedis(config); await connections.connect();
  const { redis, subscriber } = connections;
  const metrics = createMetrics(config.instanceId);
  const releaseMetrics = useProcessMetrics(metrics);
  const queue = createQueue(redis, config); const locks = createLocks(redis, config); const presence = createPresence(redis, config);
  const sweeper = createSweeper(redis, config, metrics);
  const publisher = createMetricsPublisher(redis, config, metrics, queue);
  const gateway = createAuditorGateway({ config, redis, subscriber, queue, locks, presence, metrics, approveHeld });
  const streams = new Set();
  const route = createRouter({ config, redis, publisher, metrics, extensions, streams });
  const server = http.createServer((req, res) => route(req, res).catch(() => { if (!res.headersSent) json(res, 500, { error: 'INTERNAL_ERROR' }); else res.destroy(); }));
  server.headersTimeout = 15000; server.requestTimeout = 30000; server.keepAliveTimeout = 65000;
  server.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url, 'http://local');
    if (req.headers.origin) {
      let origin; try { origin = new URL(req.headers.origin); } catch { return socket.destroy(); }
      if (origin.host !== req.headers.host) return socket.destroy();
    }
    const wss = url.pathname === '/ws/auditor' ? gateway.wss : upgrades[url.pathname];
    if (!wss || gateway.wss.clients.size >= 500) return socket.destroy();
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req, url));
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(config.port, resolve); });
  await publisher.tick();
  const stops = [];
  for (const start of startExtensions) stops.push(await start({ server, redis, subscriber, config }));
  let closing;
  function stop() {
    return closing ||= (async () => {
      sweeper.stop(); publisher.stop(); gateway.stop(); metrics.stop(); releaseMetrics();
      for (const res of streams) res.end();
      for (const finish of stops) if (typeof finish === 'function') await finish();
      await new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); });
      connections.close();
    })();
  }
  return { server, redis, config, queue, locks, presence, sweeper, publisher, metrics, stop };
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const app = await startServer();
  console.log(`[${app.config.instanceId}] http://localhost:${app.server.address().port} | Redis: ${app.redis.status}`);
  if (!Object.keys(app.config.auditors).length || !app.config.opsToken) console.warn('Configura AUDITOR_TOKENS y OPS_TOKEN en .env para acceder a las consolas.');
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { const timeout = setTimeout(() => process.exit(1), 5000); app.stop().then(() => { clearTimeout(timeout); process.exit(0); }); });
}
