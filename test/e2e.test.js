// Prueba de extremo a extremo contra dos instancias reales y un Redis.
// Uso: levantar `npm run cluster:local` (o dos instancias en :3001 y :3002) y ejecutar
//   BASE_A=http://localhost:3001 BASE_B=http://localhost:3002 npm test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { randomBytes } from 'node:crypto';
import WebSocket from 'ws';
import { geo, actaIdForMesa } from '../src/geo.js';

const A = process.env.BASE_A || 'http://localhost:3001';
const B = process.env.BASE_B || 'http://localhost:3002';
const wsUrl = (base, path) => base.replace(/^http/, 'ws') + path;

function openSse(base, view, lastId) {
  const events = [];
  const q = new URLSearchParams({ view });
  if (lastId) q.set('lastId', lastId);
  const req = http.get(`${base}/sse/public?${q}`, (res) => {
    let buf = '';
    res.on('data', (c) => {
      buf += c;
      let i;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        const raw = buf.slice(0, i); buf = buf.slice(i + 2);
        const ev = { event: 'message' };
        for (const line of raw.split('\n')) {
          if (line.startsWith('event: ')) ev.event = line.slice(7);
          else if (line.startsWith('id: ')) ev.id = line.slice(4);
          else if (line.startsWith('data: ')) ev.data = JSON.parse(line.slice(6));
        }
        if (ev.data) events.push(ev);
      }
    });
  });
  return { events, close: () => req.destroy() };
}

function juror(base) {
  const ws = new WebSocket(wsUrl(base, '/ws/jurado?jurado=JUR-12345'));
  const pending = new Map();
  ws.on('message', (d) => {
    const m = JSON.parse(d);
    if (m.type === 'ack' && pending.has(m.ref)) { pending.get(m.ref)(m); pending.delete(m.ref); }
  });
  const ready = new Promise((r) => ws.once('open', r));
  return {
    ready, close: () => ws.close(),
    send: (acta) => new Promise((resolve) => { pending.set(acta.idem, resolve); ws.send(JSON.stringify({ type: 'acta', acta })); }),
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function makeActa(mesaId, version, votos) {
  const m = geo.mesas[mesaId - 1];
  const sufragantes = Object.values(votos).reduce((a, b) => a + b, 0);
  return { acta: actaIdForMesa(mesaId), mesa: mesaId, puesto: m.puesto, municipio: m.municipio, version,
    votos, sufragantes, hash_foto: 'test', idem: randomBytes(8).toString('hex'), ts_cierre: Date.now() / 1000 | 0 };
}

test('idempotencia, correcciones fuera de orden y fan-out entre instancias', async () => {
  const mesaId = 1000 + Math.floor(Math.random() * 5000);
  const mun = geo.mesas[mesaId - 1].municipio;
  const sse = openSse(B, `mun:${mun}`);
  const j = juror(A);
  await j.ready;
  await sleep(300);

  const v1 = makeActa(mesaId, 1, { C1: 100, C2: 50, C3: 30, BLANCO: 5, NULO: 3, NO_MARCADO: 2 });
  const r1 = await j.send(v1);
  assert.ok(['APPLIED', 'DUPLICATE_VERSION'].includes(r1.status), r1.status);

  // Reenvío con la misma clave: respuesta inequívoca y nada se suma dos veces.
  const r1b = await j.send(v1);
  assert.equal(r1b.status, 'DUPLICATE');
  assert.equal(r1b.final, true);

  // Llega v3 antes que v2.
  const v3 = makeActa(mesaId, 3, { C1: 90, C2: 60, C3: 30, BLANCO: 5, NULO: 3, NO_MARCADO: 2 });
  const v2 = makeActa(mesaId, 2, { C1: 1, C2: 1, C3: 1, BLANCO: 1, NULO: 1, NO_MARCADO: 1 });
  assert.equal((await j.send(v3)).status, 'CORRECTED');
  assert.equal((await j.send(v2)).status, 'STALE');

  // Acta que no cuadra -> verificación humana.
  const bad = makeActa(mesaId === 6400 ? 1 : mesaId + 1, 1, { C1: 10, C2: 10, C3: 10, BLANCO: 0, NULO: 0, NO_MARCADO: 0 });
  bad.sufragantes = 40;
  assert.ok(['HELD', 'DUPLICATE_VERSION'].includes((await j.send(bad)).status));

  await sleep(2500);
  const deltas = sse.events.filter((e) => e.event === 'delta');
  assert.ok(deltas.length >= 1, 'la instancia B recibió el delta publicado desde A');
  const lastId = sse.events.filter((e) => e.id).at(-1).id;
  sse.close();

  // Reconexión con continuidad: no debe llegar fotografía si el id está en ventana.
  const sse2 = openSse(A, `mun:${mun}`, lastId);
  await sleep(800);
  assert.ok(!sse2.events.some((e) => e.event === 'snapshot'), 'reconexión sin fotografía completa');
  sse2.close();
  j.close();
});
