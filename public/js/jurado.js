// App del jurado (RT-6): cola local persistente + WebSocket con reintento automático.
// El acta se guarda en localStorage ANTES de intentar enviarla; solo sale de la cola
// cuando el servidor responde con un resultado definitivo. Cada reintento reutiliza la
// misma clave de idempotencia, así que el servidor nunca la suma dos veces (RF-1).
import { $, fmt, loadGeo, backoff, setConn, randomHex } from './common.js';

const STORE = 'preconteo:outbox:v1';
const ACK_TIMEOUT_MS = 8000;
const els = {
  conn: $('#conn'), banner: $('#banner'), form: $('#form'), code: $('#acta-code'), jurado: $('#jurado'),
  mesa: $('#mesa'), mesaInfo: $('#mesa-info'), votes: $('#votes'), suf: $('#sufragantes'), sum: $('#sum-check'),
  version: $('#version'), versionNote: $('#version-note'), fill: $('#fill'), sent: $('#sent'), net: $('#net'),
};
let geo, ws, attempt = 0;
let outbox = load();
const options = [];

init();

async function init() {
  geo = await loadGeo();
  options.push(...geo.candidates.map((c) => ({ id: c.id, label: c.name, color: c.color })),
    { id: 'BLANCO', label: 'Voto en blanco' }, { id: 'NULO', label: 'Nulos' }, { id: 'NO_MARCADO', label: 'No marcados' });
  els.votes.insertAdjacentHTML('beforeend', options.map((o) => `
    <div class="vote" style="--c:${o.color || 'var(--ink-3)'}"><label for="v-${o.id}">${o.label}</label>
      <input class="input num" id="v-${o.id}" data-opt="${o.id}" type="number" min="0" inputmode="numeric" /></div>`).join(''));

  els.jurado.value = localStorage.getItem('preconteo:jurado') || `JUR-${String(10000 + Math.floor(Math.random() * 89999))}`;
  els.jurado.addEventListener('change', () => { localStorage.setItem('preconteo:jurado', els.jurado.value.trim()); reconnectNow(); });
  els.mesa.addEventListener('input', onMesa);
  els.form.addEventListener('input', updateSum);
  els.form.addEventListener('submit', onSubmit);
  els.fill.addEventListener('click', fillTest);

  window.addEventListener('online', () => { renderNet(); reconnectNow(); });
  window.addEventListener('offline', renderNet);
  renderNet();
  renderOutbox();
  connect();
  setInterval(flush, 3000);
}

// ---------- Formulario ----------
function mesaData() {
  const id = Number(els.mesa.value);
  if (!Number.isInteger(id) || id < 1 || id > geo.totalMesas) return null;
  // Reconstruimos puesto y municipio igual que el servidor (mismo mapa determinista).
  let count = 0;
  for (const m of geo.municipios) {
    for (const [pid, mesas] of m.puestos) {
      if (id <= count + mesas) {
        const local = id - count;
        return { mesa: id, puesto: pid, municipio: m.id, munName: m.name, local,
          acta: `E14-${String(pid).padStart(3, '0')}-${String(local).padStart(4, '0')}-03` };
      }
      count += mesas;
    }
  }
  return null;
}

function onMesa() {
  const m = mesaData();
  els.code.textContent = m ? m.acta : 'E14-———-————-03';
  els.mesaInfo.textContent = m ? `${m.munName} · Puesto ${String(m.puesto).padStart(4, '0')} · Mesa local ${m.local}` : 'Escribe la mesa para ver su puesto y municipio.';
  // Si ya se transmitió esta mesa, la siguiente será una corrección.
  const prev = m && outbox.filter((o) => o.payload.acta === m.acta).reduce((max, o) => Math.max(max, o.payload.version), 0);
  els.version.value = String((prev || 0) + 1);
  els.versionNote.textContent = prev ? `Esta mesa ya fue transmitida: se enviará como corrección (versión ${prev + 1}).` : '';
}

function readVotes() {
  const votos = {};
  for (const input of els.votes.querySelectorAll('input')) votos[input.dataset.opt] = Number(input.value || 0);
  return votos;
}

function updateSum() {
  const votos = readVotes();
  const total = Object.values(votos).reduce((a, b) => a + b, 0);
  const suf = Number(els.suf.value || 0);
  if (!els.suf.value) { els.sum.dataset.state = 'idle'; els.sum.textContent = `Suma de votos: ${fmt.format(total)}`; return; }
  const ok = total === suf;
  els.sum.dataset.state = ok ? 'ok' : 'bad';
  els.sum.textContent = ok
    ? `Cuadra: ${fmt.format(total)} votos = ${fmt.format(suf)} sufragantes`
    : `No cuadra: ${fmt.format(total)} votos frente a ${fmt.format(suf)} sufragantes. Si la envías, irá a verificación humana.`;
}

function fillTest() {
  if (!els.mesa.value) els.mesa.value = String(1 + Math.floor(Math.random() * geo.totalMesas));
  onMesa();
  let total = 0;
  for (const input of els.votes.querySelectorAll('input')) {
    const special = ['BLANCO', 'NULO', 'NO_MARCADO'].includes(input.dataset.opt);
    const n = Math.floor(Math.random() * (special ? 20 : 260));
    input.value = n; total += n;
  }
  els.suf.value = String(total);
  updateSum();
}

function onSubmit(e) {
  e.preventDefault();
  const m = mesaData();
  if (!m) { els.mesaInfo.textContent = 'La mesa no existe. Revisa el número.'; els.mesa.focus(); return; }
  if (!els.suf.value) { els.suf.focus(); return; }
  const payload = {
    acta: m.acta, mesa: m.mesa, puesto: m.puesto, municipio: m.municipio,
    version: Number(els.version.value), jurado: els.jurado.value.trim(),
    ts_cierre: Math.floor(Date.now() / 1000), votos: readVotes(), sufragantes: Number(els.suf.value),
    hash_foto: randomHex(16), idem: randomHex(8),
  };
  // Primero se persiste, luego se intenta enviar.
  outbox.unshift({ payload, status: 'PENDING', message: 'Guardada en el dispositivo. Transmitiendo…', final: false, createdAt: Date.now(), tries: 0 });
  save();
  renderOutbox();
  els.form.reset();
  els.jurado.value = payload.jurado;
  onMesa(); updateSum();
  flush();
}

// ---------- Conexión ----------
function connect() {
  const jurado = els.jurado.value.trim();
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws/jurado?jurado=${encodeURIComponent(jurado)}`);
  setConn(els.conn, 'retry', 'Conectando…');
  ws.onopen = () => { attempt = 0; setConn(els.conn, 'live', 'Conectado'); flush(true); };
  ws.onmessage = (ev) => onMessage(JSON.parse(ev.data));
  ws.onclose = (ev) => {
    if (ev.code === 4001) { setConn(els.conn, 'down', 'Credencial inválida'); showBanner('Tu credencial debe tener la forma JUR-12345.'); return; }
    const wait = backoff(attempt++);
    setConn(els.conn, 'retry', `Sin conexión · reintento en ${Math.ceil(wait / 1000)} s`);
    clearTimeout(connect.timer);
    connect.timer = setTimeout(connect, wait);
  };
}
function reconnectNow() { clearTimeout(connect.timer); attempt = 0; try { ws?.close(); } catch { /* */ } }

function onMessage(m) {
  if (m.type === 'hello') { setConn(els.conn, 'live', `Conectado · ${m.instance}`); showBanner(m.redis ? '' : degradedText); }
  if (m.type === 'status') showBanner(m.redis ? '' : degradedText);
  if (m.type !== 'ack') return;
  const item = outbox.find((o) => o.payload.idem === m.ref);
  if (!item) return;
  item.status = m.status;
  item.message = m.message;
  item.final = m.final;
  item.instance = m.instance;
  item.inflightAt = 0;
  item.retryAt = m.final ? 0 : Date.now() + (m.retryAfterSec ? m.retryAfterSec * 1000 : 4000);
  save();
  renderOutbox();
}
const degradedText = 'El registro central no responde. Tus actas quedan guardadas aquí y se transmitirán solas: no las vuelvas a digitar.';

// Envía todo lo pendiente. Un acta "en vuelo" sin ack en 8 s se reenvía con la MISMA clave.
function flush(force = false) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const now = Date.now();
  for (const item of outbox) {
    if (item.final) continue;
    if (!force && item.inflightAt && now - item.inflightAt < ACK_TIMEOUT_MS) continue;
    if (!force && item.retryAt && now < item.retryAt) continue;
    item.inflightAt = now;
    item.tries++;
    ws.send(JSON.stringify({ type: 'acta', acta: item.payload }));
  }
  save();
}

// ---------- Persistencia y render ----------
function load() { try { return JSON.parse(localStorage.getItem(STORE)) || []; } catch { return []; } }
function save() { try { localStorage.setItem(STORE, JSON.stringify(outbox.slice(0, 200))); } catch { /* almacenamiento lleno */ } }

const KIND = { APPLIED: 'ok', CORRECTED: 'ok', DUPLICATE: 'ok', DUPLICATE_VERSION: 'ok', STALE: 'bad', REJECTED: 'bad', HELD: 'held' };
const LABEL = { PENDING: 'En cola', APPLIED: 'Registrada', CORRECTED: 'Corrección registrada', DUPLICATE: 'Ya registrada', DUPLICATE_VERSION: 'Ya registrada', STALE: 'Versión antigua', REJECTED: 'Rechazada', HELD: 'En verificación', UNAVAILABLE: 'En cola · sin registro central', RATE_LIMITED: 'En espera' };

function renderOutbox() {
  if (!outbox.length) return;
  els.sent.innerHTML = outbox.slice(0, 60).map((o, i) => {
    const kind = KIND[o.status] || 'pending';
    return `<li data-kind="${kind}">
      <div class="top"><span class="code">${o.payload.acta}</span><span class="chip ${kind === 'ok' ? 'ok' : kind === 'bad' ? 'bad' : kind === 'held' ? 'live' : 'warn'}">${LABEL[o.status] || o.status}</span></div>
      <div class="msg">${o.message}</div>
      <div class="meta"><span>Versión ${o.payload.version}</span><span>${new Date(o.createdAt).toLocaleTimeString('es-CO')}</span>
        <span>${o.tries} ${o.tries === 1 ? 'intento' : 'intentos'}</span>${o.instance ? `<span>${o.instance}</span>` : ''}
        ${o.final ? `<button type="button" data-fix="${i}">Corregir esta acta</button>` : ''}</div></li>`;
  }).join('');
  for (const b of els.sent.querySelectorAll('[data-fix]')) b.onclick = () => loadForCorrection(outbox[Number(b.dataset.fix)]);
}

function loadForCorrection(item) {
  els.mesa.value = item.payload.mesa;
  onMesa();
  for (const input of els.votes.querySelectorAll('input')) input.value = item.payload.votos[input.dataset.opt] ?? 0;
  els.suf.value = item.payload.sufragantes;
  updateSum();
  els.mesa.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function renderNet() {
  els.net.textContent = navigator.onLine ? 'Con red' : 'Sin red';
  els.net.className = `chip ${navigator.onLine ? 'ok' : 'bad'}`;
}
function showBanner(t) { els.banner.textContent = t; els.banner.classList.toggle('show', Boolean(t)); }
