// Prueba de carga del público (RT-8, RF-10): levanta miles de conexiones SSE simultáneas
// repartidas en varios procesos, con la misma lógica de reconexión del navegador.
//
//   node loadtest/public-clients.js --url http://IP --clients 20000 --procs 4 --ramp 60
//   node loadtest/public-clients.js --url http://IP --clients 20000 --nojitter   (comparación)
//   node loadtest/public-clients.js --clients 4000 --csv con-jitter.csv          (gráfica)
//
// Antes de lanzarla en Linux: `ulimit -n 200000` en la máquina que genera la carga.
// Todas las conexiones salen de una IP: en el servidor sube RL_PUBLIC (p. ej. 1000000) o
// el límite por IP responderá 429 y falseará la medición.
import cluster from 'node:cluster';
import http from 'node:http';
import https from 'node:https';
import { writeFileSync } from 'node:fs';

const args = Object.fromEntries(process.argv.slice(2).map((a, i, all) => a.startsWith('--') ? [a.slice(2), all[i + 1]?.startsWith('--') || all[i + 1] === undefined ? true : all[i + 1]] : null).filter(Boolean));
const URL_BASE = args.url || 'http://localhost:8080';
const CLIENTS = Number(args.clients || 2000);
const PROCS = Number(args.procs || 1);
const RAMP_S = Number(args.ramp || 20);
const JITTER = !args.nojitter;
const CSV = typeof args.csv === 'string' ? args.csv : null;
const CSV_BUCKET_MS = 100;
const RETRY_BASE_MS = 5000; // igual que public/js/public.js
const MUNICIPIOS = 16;

if (cluster.isPrimary && PROCS > 1) {
  console.log(`[carga] ${CLIENTS} clientes en ${PROCS} procesos contra ${URL_BASE} · jitter ${JITTER ? 'activado' : 'DESACTIVADO'}`);
  const totals = new Map();
  for (let i = 0; i < PROCS; i++) {
    const w = cluster.fork({ WORKER_INDEX: String(i) });
    w.on('message', (m) => totals.set(i, m));
  }
  setInterval(() => {
    const agg = [...totals.values()].reduce((a, m) => ({ open: a.open + m.open, events: a.events + m.events, reconnects: a.reconnects + m.reconnects, snapshots: a.snapshots + m.snapshots, byes: a.byes + m.byes }), { open: 0, events: 0, reconnects: 0, snapshots: 0, byes: 0 });
    console.log(`[carga] abiertas ${agg.open} · eventos ${agg.events} · reconexiones ${agg.reconnects} · fotografías ${agg.snapshots} · bye ${agg.byes}`);
  }, 5000);
} else {
  runWorker(PROCS > 1 ? Math.ceil(CLIENTS / PROCS) : CLIENTS);
}

function runWorker(n) {
  const lib = URL_BASE.startsWith('https') ? https : http;
  const agent = new lib.Agent({ keepAlive: false, maxSockets: Infinity });
  const stats = { open: 0, events: 0, reconnects: 0, snapshots: 0, byes: 0 };
  // Línea de tiempo: cuántas reconexiones LLEGAN al servidor en cada intervalo.
  const arrivals = new Map();
  const markArrival = () => {
    const bucket = Math.floor(Date.now() / CSV_BUCKET_MS) * CSV_BUCKET_MS;
    arrivals.set(bucket, (arrivals.get(bucket) || 0) + 1);
  };
  if (CSV) {
    const file = process.env.WORKER_INDEX ? `${CSV}.${process.env.WORKER_INDEX}` : CSV;
    setInterval(() => {
      const rows = [...arrivals].sort(([a], [b]) => a - b).map(([t, n]) => `${t},${n}`);
      writeFileSync(file, `t_ms,reconexiones\n${rows.join('\n')}\n`);
    }, 1000);
  }

  function startClient(idx) {
    const view = idx % 3 === 0 ? '' : `mun:${1 + (idx % MUNICIPIOS)}`;
    const c = { lastId: null, attempt: 0, view };
    const open = () => {
      const q = new URLSearchParams({ view: c.view });
      if (c.lastId) q.set('lastId', c.lastId);
      const req = lib.get(`${URL_BASE}/sse/public?${q}`, { agent }, (res) => {
        if (res.statusCode !== 200) { res.resume(); return retry(); }
        stats.open++;
        c.attempt = 0;
        let buf = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          buf += chunk;
          let i;
          while ((i = buf.indexOf('\n\n')) >= 0) {
            const frame = buf.slice(0, i); buf = buf.slice(i + 2);
            const m = /^id: (.+)$/m.exec(frame);
            if (m) c.lastId = m[1];
            if (frame.includes('event: delta')) stats.events++;
            if (frame.includes('event: snapshot')) stats.snapshots++;
            if (frame.includes('event: bye')) stats.byes++;
          }
        });
        // 'close' llega siempre (fin normal, RST o instancia muerta); 'end' no.
        let done = false;
        res.on('close', () => { if (done) return; done = true; stats.open--; retry(); });
        res.on('error', () => {});
      });
      req.on('error', () => retry());
    };
    const retry = () => {
      stats.reconnects++;
      const ceil = Math.min(30000, RETRY_BASE_MS * 2 ** c.attempt++);
      setTimeout(() => { markArrival(); open(); }, JITTER ? Math.random() * ceil : ceil);
    };
    open();
  }

  // Rampa: los clientes entran de forma gradual durante RAMP_S segundos.
  const perTick = Math.max(1, Math.ceil(n / (RAMP_S * 10)));
  let started = 0;
  const ramp = setInterval(() => {
    for (let k = 0; k < perTick && started < n; k++) startClient(started++);
    if (started >= n) clearInterval(ramp);
  }, 100);

  setInterval(() => {
    if (process.send) process.send(stats);
    else console.log(`[carga] abiertas ${stats.open}/${n} · eventos ${stats.events} · reconexiones ${stats.reconnects} · fotografías ${stats.snapshots} · bye ${stats.byes}`);
  }, process.send ? 1000 : 5000);
}
