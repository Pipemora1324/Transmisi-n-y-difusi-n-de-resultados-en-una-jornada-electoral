import test from 'node:test';
import assert from 'node:assert/strict';
import { readConfig } from '../src/support/config.js';
import { summarizeInstances } from '../src/metrics/publisher.js';
import { sameSecret } from '../src/gateways/auditorWs.js';
import { normalizeCase } from '../src/verify/queue.js';
import { startServer } from '../src/server.js';

test('rechaza TTL inválido y credenciales incompletas', () => {
  assert.throws(() => readConfig({ LOCK_TTL_MS: 'NaN' }), /LOCK_TTL_MS/);
  assert.throws(() => readConfig({ AUDITOR_TOKENS: '{"token":{"id":"a"}}' }), /Credencial/);
  assert.equal(readConfig({}).expectedInstances, 8);
});
test('autenticación rechaza vacíos y diferencias de longitud', () => {
  assert.equal(sameSecret('', ''), false);
  assert.equal(sameSecret('abc', 'abcd'), false);
  assert.equal(sameSecret('abc', 'abc'), true);
  assert.equal(sameSecret(null, 'abc'), false);
});
test('agrega instancias sin confundir el mayor p95 con un percentil global', () => {
  const make = (id, delay, count) => ({ instanceId: id, rssBytes: 100, eventLoop: { p95Ms: delay }, connections: { auditor: count, ops: 1 }, counters: { claimed: count } });
  const sample = summarizeInstances([make('a', 20, 3), make('b', 50, 4)], 8);
  assert.equal(sample.onlineInstances, 2);
  assert.equal(sample.totals.maxInstanceP95Ms, 50);
  assert.equal(sample.totals.auditorConnections, 7);
  assert.equal(sample.totals.counters.claimed, 7);
});
test('lee el contrato held:<acta> de la ingesta', () => {
  const item = normalizeCase('E14-1', { version: '2', mesa: '1', votos: '{"C1":7}', sufragantes: '8', ts: '100' }, '90');
  assert.deepEqual(item.votos, { C1: 7 }); assert.equal(item.version, 2); assert.equal(item.priority, 90);
});
test('servidor degradado mantiene UI, protege métricas y no expone archivos del proyecto', async () => {
  const config = { ...readConfig({ REDIS_URL: 'redis://127.0.0.1:1', OPS_TOKEN: 'test' }), port: 0 };
  const app = await startServer({ config });
  const base = 'http://127.0.0.1:' + app.server.address().port;
  try {
    const health = await fetch(base + '/healthz'); assert.equal(health.status, 503);
    const page = await fetch(base + '/auditor'); assert.equal(page.status, 200); assert.match(await page.text(), /Consola de verificación/);
    assert.equal((await fetch(base + '/.env')).status, 404);
    assert.equal((await fetch(base + '/sse/ops')).status, 401);
    const ops = await fetch(base + '/api/ops', { headers: { Authorization: 'Bearer test' } });
    assert.equal((await ops.json()).redis, false);
    assert.equal((await fetch(base + '/css/theme.css', { method: 'HEAD' })).status, 200);
  } finally { await app.stop(); }
});
