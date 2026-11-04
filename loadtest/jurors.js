// Simulador de la jornada de transmisión (RT-8): reproduce el perfil del caso.
//   - 6.400 actas, 55 % en el primer tercio de la ventana, con ráfagas.
//   - 4 % a 11 % de reenvíos del mismo envío (misma clave de idempotencia).
//   - ~180 correcciones, algunas con versiones que llegan fuera de orden.
//   - 6 % a 9 % de actas que no cuadran (van a verificación).
//
//   node loadtest/jurors.js --url http://IP --minutes 10 --jurors 300
import WebSocket from 'ws';
import { randomBytes } from 'node:crypto';
import { geo, actaIdForMesa, TOTAL_MESAS } from '../src/geo.js';

const args = Object.fromEntries(process.argv.slice(2).map((a, i, all) => a.startsWith('--') ? [a.slice(2), all[i + 1]?.startsWith('--') || all[i + 1] === undefined ? true : all[i + 1]] : null).filter(Boolean));
const BASE = (args.url || 'http://localhost:8080').replace(/^http/, 'ws');
const MINUTES = Number(args.minutes || 10);
const JURORS = Number(args.jurors || 200);
const ACTAS = Math.min(Number(args.actas || TOTAL_MESAS), TOTAL_MESAS);
const DUP_RATE = 0.04 + Math.random() * 0.07;
const BAD_RATE = 0.06 + Math.random() * 0.03;
const CORRECTIONS = Math.round(180 * (ACTAS / TOTAL_MESAS));
const T = MINUTES * 60 * 1000;

const counts = {};
const latencies = [];

// Conexiones de jurados (cada una lleva varias mesas).
const conns = Array.from({ length: JURORS }, (_, i) => makeConn(`JUR-${String(10000 + i)}`));
function makeConn(jurado) {
  const c = { jurado, ws: null, pending: new Map(), queue: [] };
  const open = () => {
    c.ws = new WebSocket(`${BASE}/ws/jurado?jurado=${jurado}`);
    c.ws.on('open', () => { for (const m of c.queue.splice(0)) c.ws.send(m); });
    c.ws.on('message', (d) => {
      const m = JSON.parse(d);
      if (m.type !== 'ack') return;
      counts[m.status] = (counts[m.status] || 0) + 1;
      const t0 = c.pending.get(m.ref);
      if (t0) { latencies.push(Date.now() - t0); c.pending.delete(m.ref); }
    });
    c.ws.on('close', () => setTimeout(open, Math.random() * 3000));
    c.ws.on('error', () => {});
  };
  open();
  c.send = (acta) => {
    const msg = JSON.stringify({ type: 'acta', acta });
    c.pending.set(acta.idem, Date.now());
    if (c.ws.readyState === WebSocket.OPEN) c.ws.send(msg); else c.queue.push(msg);
  };
  return c;
}

function randomVotes(bad) {
  const v = { C1: rnd(40, 260), C2: rnd(30, 240), C3: rnd(20, 260), BLANCO: rnd(0, 25), NULO: rnd(0, 12), NO_MARCADO: rnd(0, 8) };
  const total = Object.values(v).reduce((a, b) => a + b, 0);
  return { votos: v, sufragantes: bad ? total + rnd(1, 15) * (Math.random() < 0.5 ? -1 : 1) : total };
}
function rnd(a, b) { return a + Math.floor(Math.random() * (b - a + 1)); }

function makeActa(mesaId, version, bad = Math.random() < BAD_RATE) {
  const m = geo.mesas[mesaId - 1];
  const { votos, sufragantes } = randomVotes(bad);
  return { acta: actaIdForMesa(mesaId), mesa: mesaId, puesto: m.puesto, municipio: m.municipio, version, votos, sufragantes,
    hash_foto: randomBytes(8).toString('hex'), idem: randomBytes(8).toString('hex'), ts_cierre: Math.floor(Date.now() / 1000) };
}

// Plan de envíos con el perfil de la jornada.
const schedule = [];
const mesas = Array.from({ length: TOTAL_MESAS }, (_, i) => i + 1).sort(() => Math.random() - 0.5).slice(0, ACTAS);
for (const mesa of mesas) {
  let t = Math.random() < 0.55 ? Math.random() * (T / 3) : T / 3 + Math.random() * (2 * T / 3);
  if (Math.random() < 0.15) t = Math.floor(t / 20000) * 20000; // ráfagas: muchas mesas a la vez
  const acta = makeActa(mesa, 1);
  schedule.push({ t, acta });
  if (Math.random() < DUP_RATE) for (let k = 0; k < rnd(1, 4); k++) schedule.push({ t: t + rnd(500, 8000), acta });
}
for (let i = 0; i < CORRECTIONS; i++) {
  const mesa = mesas[Math.floor(Math.random() * mesas.length)];
  const base = Math.random() * T * 0.8 + T * 0.15;
  const v2 = makeActa(mesa, 2, false);
  const v3 = makeActa(mesa, 3, false);
  if (Math.random() < 0.3) { schedule.push({ t: base, acta: v3 }); schedule.push({ t: base + 2000, acta: v2 }); } // fuera de orden
  else schedule.push({ t: base, acta: v2 });
}
schedule.sort((a, b) => a.t - b.t);

console.log(`[jurados] ${schedule.length} envíos (${ACTAS} actas, reenvíos ${(DUP_RATE * 100).toFixed(1)} %, dudosas ${(BAD_RATE * 100).toFixed(1)} %, ${CORRECTIONS} correcciones) en ${MINUTES} min`);
const start = Date.now();
let i = 0;
const timer = setInterval(() => {
  const now = Date.now() - start;
  while (i < schedule.length && schedule[i].t <= now) {
    const { acta } = schedule[i++];
    conns[acta.mesa % conns.length].send(acta);
  }
  if (i >= schedule.length) {
    clearInterval(timer);
    setTimeout(() => { report(); process.exit(0); }, 10000);
  }
}, 50);

function report() {
  latencies.sort((a, b) => a - b);
  const p = (q) => latencies[Math.floor(latencies.length * q)] || 0;
  console.log(`[jurados] enviados ${i} · confirmaciones ${latencies.length} · p50 ${p(0.5)} ms · p99 ${p(0.99)} ms`);
  console.log('[jurados] resultados', counts);
}
setInterval(() => console.log(`[jurados] ${i}/${schedule.length} enviados · ${JSON.stringify(counts)}`), 10000).unref();
