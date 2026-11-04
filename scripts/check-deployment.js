import assert from 'node:assert/strict';
import { readConfig } from '../src/support/config.js';
const config = readConfig();
const base = process.env.BASE_URL || 'http://localhost:8080';
const seen = new Set();
for (let i = 0; i < config.expectedInstances * 2; i++) {
  const response = await fetch(base + '/healthz');
  assert.equal(response.status, 200, 'Instancia no saludable');
  seen.add((await response.json()).instanceId);
}
const ops = await fetch(base + '/api/ops', { headers: { Authorization: 'Bearer ' + config.opsToken } });
assert.equal(ops.status, 200);
const sample = await ops.json();
assert.equal(sample.redis, true);
assert.equal(sample.onlineInstances, config.expectedInstances, 'No publican todas las instancias');
assert.equal(seen.size, config.expectedInstances, 'El proxy no distribuyó a todas las instancias');
assert.equal((await fetch(base + '/sse/ops')).status, 401);
for (const page of ['/auditor', '/ops', '/css/theme.css', '/js/auditor.js']) assert.equal((await fetch(base + page)).status, 200, page);
console.log(JSON.stringify({ healthy: [...seen].sort(), onlineInstances: sample.onlineInstances, queue: sample.queue, protectedOps: true }, null, 2));
