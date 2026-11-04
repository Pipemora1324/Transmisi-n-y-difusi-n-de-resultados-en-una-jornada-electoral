import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import Redis from 'ioredis';
import WebSocket from 'ws';
import { readConfig } from '../../src/support/config.js';
import { createQueue } from '../../src/verify/queue.js';
import { createLocks } from '../../src/verify/locks.js';
import { createPresence } from '../../src/verify/presence.js';
import { createSweeper } from '../../src/verify/sweeper.js';
import { verificationKeys } from '../../src/verify/keys.js';
import { startServer } from '../../src/server.js';
const url = process.env.TEST_REDIS_URL || 'redis://127.0.0.1:6379';

test('Redis real: concurrencia, TTL, fencing, cola, presencia y gateways entre dos instancias', async t => {
  const redis = new Redis(url, { lazyConnect: true, connectTimeout: 2000, retryStrategy: null, enableOfflineQueue: false });
  redis.on('error', () => {});
  await redis.connect(); // Falla explícitamente si no hay Redis: nunca simula ni omite la prueba.
  const config = { ...readConfig({ REDIS_URL: url, REDIS_PREFIX: 'test:' + randomUUID() + ':', LOCK_TTL_MS: '1000', PRESENCE_TTL_MS: '1000', AUDITOR_TOKENS: '{"one":{"id":"a","name":"Sara"},"two":{"id":"b","name":"Auditor B"}}', OPS_TOKEN: 'ops-test', METRICS_INTERVAL_MS: '200', METRICS_TTL_MS: '1000' }), port: 0 };
  const queue = createQueue(redis, config); const locks = createLocks(redis, config); const presence = createPresence(redis, config);
  const sweeper = createSweeper(redis, { ...config, sweepIntervalMs: 60000 });
  const k = verificationKeys(config.prefix);
  const apps = []; const sockets = [];
  const sample = (id, priority = 10) => ({ id, priority, version: 1, mesa: 1, votos: { C1: 12 }, sufragantes: 10 });
  try {
    await t.test('un ganador entre 40 tomas concurrentes y prioridad mayor primero', async () => {
      await queue.enqueue(sample('high', 100)); await queue.enqueue(sample('low', 1));
      const winners = await Promise.all(Array.from({ length: 40 }, (_, i) => locks.claim('aud-' + i, 'high')));
      assert.equal(winners.filter(r => r.status === 'OK').length, 1);
      const next = await locks.claim('next'); assert.equal(next.case.id, 'low');
      await locks.release('next', 'low', next.token);
      const winner = winners.find(r => r.status === 'OK');
      await locks.release('owner', 'high', winner.token);
    });
    await t.test('renovación, expiración y un dueño tardío no pueden liberar ni rechazar al nuevo dueño', async () => {
      const first = await locks.claim('a', 'high');
      await delay(600); assert.equal((await locks.renew('a', 'high', first.token)).status, 'OK');
      await delay(600); assert.ok(await redis.exists(k.lockPrefix + 'high'));
      await delay(500); await sweeper.sweep();
      const second = await locks.claim('b', 'high'); assert.equal(second.status, 'OK');
      assert.equal((await locks.release('a', 'high', first.token)).status, 'NOT_OWNER');
      assert.equal((await locks.reject('a', 'high', first.token, 'rechazo antiguo')).status, 'NOT_OWNER');
      assert.equal((await locks.renew('a', 'high', first.token)).status, 'NOT_OWNER');
      assert.equal((await locks.reject('b', 'high', second.token, 'Descuadre confirmado')).status, 'OK');
      assert.equal(await redis.exists(k.heldPrefix + 'high'), 0);
      assert.equal(await redis.xlen(k.audit), 1);
    });
    await t.test('presencia por sesión no borra al auditor que sigue en otra conexión', async () => {
      await presence.beat('s1', { id: 'a', name: 'Sara' }); await presence.beat('s2', { id: 'a', name: 'Sara' });
      await presence.leave('s1'); assert.equal((await presence.list()).length, 1);
      await delay(1100); assert.equal((await presence.list()).length, 0);
      await sweeper.sweep(); assert.equal(await redis.hlen(k.people), 0);
    });
    await t.test('dos instancias comparten Pub/Sub, cola y métricas, y una caída libera el caso', async () => {
      const a = await startServer({ config: { ...config, instanceId: 'a' } }); apps.push(a);
      const b = await startServer({ config: { ...config, instanceId: 'b' } }); apps.push(b);
      function connect(app, token) {
        const ws = new WebSocket('ws://127.0.0.1:' + app.server.address().port + '/ws/auditor'); sockets.push(ws);
        const inbox = [];
        ws.on('message', raw => inbox.push(JSON.parse(raw.toString())));
        ws.on('open', () => ws.send(JSON.stringify({ type: 'auth', token })));
        return { ws, inbox, async wait(predicate) { for (let i = 0; i < 100; i++) { const index = inbox.findIndex(predicate); if (index >= 0) return inbox.splice(index, 1)[0]; await delay(25); } throw new Error('No llegó el mensaje esperado: ' + JSON.stringify(inbox)); } };
      }
      const one = connect(a, 'one'); const two = connect(b, 'two');
      await Promise.all([one.wait(m => m.type === 'hello'), two.wait(m => m.type === 'hello')]);
      one.ws.send(JSON.stringify({ type: 'claim', id: 'low' }));
      const claim = await one.wait(m => m.type === 'result' && m.action === 'claim'); assert.equal(claim.status, 'OK'); assert.equal(claim.token, undefined);
      const state = await two.wait(m => m.type === 'state' && m.items.some(x => x.id === 'low' && x.claimedBy)); assert.equal(state.items.find(x => x.id === 'low').claimedBy.auditor, 'a');
      one.ws.send(JSON.stringify({ type: 'approve' })); assert.equal((await one.wait(m => m.type === 'result' && m.action === 'approve')).status, 'INTEGRATION_REQUIRED');
      one.ws.terminate(); await delay(150);
      two.ws.send(JSON.stringify({ type: 'claim', id: 'low' })); assert.equal((await two.wait(m => m.type === 'result' && m.action === 'claim')).status, 'OK');
      two.ws.send(JSON.stringify({ type: 'release' })); await two.wait(m => m.type === 'result' && m.action === 'release');
      await a.publisher.tick(); const metrics = await b.publisher.tick(); assert.equal(metrics.onlineInstances, 2);
      const response = await fetch('http://127.0.0.1:' + a.server.address().port + '/sse/ops', { headers: { Authorization: 'Bearer ops-test' } });
      const reader = response.body.getReader(); const chunk = await reader.read(); assert.match(new TextDecoder().decode(chunk.value), /event: metrics/); await reader.cancel();
      two.ws.close();
    });
  } finally {
    for (const ws of sockets) ws.terminate();
    for (const app of apps) await app.stop();
    sweeper.stop();
    // Solo borra las claves del prefijo aleatorio de esta prueba; jamás FLUSHDB.
    const keys = await redis.keys(config.prefix + '*'); if (keys.length) await redis.del(...keys);
    redis.disconnect();
  }
});
