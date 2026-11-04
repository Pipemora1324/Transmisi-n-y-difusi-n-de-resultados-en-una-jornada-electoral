// Cliente público: SSE con continuidad (RF-6), reintento con jitter (RF-10) y polling
// como último recurso. El estado es un mapa tópico -> {opción: votos, __inf: mesas}.
import { $, fmt, pct, loadGeo, backoff, setConn } from './common.js';

const POLL_AFTER_FAILURES = 3;
// Con 50.000 clientes por instancia, una base de 1 s traería a todos de vuelta en un segundo.
// Con 5 s la llegada se reparte (≈ 10.000 conexiones/s entre las 7 instancias restantes) y
// nadie pierde datos: el historial del Stream cubre muchos minutos.
const RETRY_BASE_MS = 5000;
const state = { topics: {}, lastId: null, view: 'dep', attempt: 0, es: null, polling: false, pollTimer: null, lastUpdate: 0 };
const els = {
  conn: $('#conn'), banner: $('#banner'), kicker: $('#scope-kicker'), title: $('#scope-title'),
  pct: $('#informed-pct'), count: $('#informed-count'), tally: $('#tally-fill'),
  mun: $('#sel-mun'), puesto: $('#sel-puesto'), updated: $('#updated'),
  candidates: $('#candidates'), others: $('#others'), changes: $('#changes'),
  fInstance: $('#f-instance'), fTransport: $('#f-transport'), fInp: $('#f-inp'), fId: $('#f-id'),
};
let geo;
let prevShown = {};

init();

async function init() {
  geo = await loadGeo();
  for (const m of geo.municipios) els.mun.add(new Option(`${m.name} · ${fmt.format(m.mesas)} mesas`, m.id));
  els.mun.addEventListener('change', onFilter);
  els.puesto.addEventListener('change', onFilter);
  buildCandidates();
  setInterval(renderUpdated, 1000);
  connect();
}

// ---------- Selección de vista (RF-5) ----------
function onFilter(e) {
  if (e.target === els.mun) {
    els.puesto.length = 1;
    const mun = geo.municipios.find((m) => m.id === Number(els.mun.value));
    els.puesto.disabled = !mun;
    if (mun) for (const [pid, mesas] of mun.puestos) els.puesto.add(new Option(`Puesto ${String(pid).padStart(4, '0')} · ${mesas} mesas`, pid));
  }
  const parts = [];
  if (els.mun.value) parts.push(`mun:${els.mun.value}`);
  if (els.puesto.value) parts.push(`puesto:${els.puesto.value}`);
  state.view = parts.join('/') || 'dep';
  // Cambiar de vista = nueva suscripción con fotografía completa.
  state.lastId = null;
  state.topics = {};
  prevShown = {};
  reconnectNow();
  scheduleRender();
}

const focusTopic = () => (els.puesto.value ? `puesto:${els.puesto.value}` : els.mun.value ? `mun:${els.mun.value}` : 'dep');

// ---------- Transporte ----------
function connect() {
  if (!('EventSource' in window)) return startPolling();
  const q = new URLSearchParams({ view: state.view });
  if (state.lastId) q.set('lastId', state.lastId);
  const es = new EventSource(`/sse/public?${q}`);
  state.es = es;

  es.addEventListener('hello', (e) => {
    const d = JSON.parse(e.data);
    state.attempt = 0;
    stopPolling();
    els.fInstance.textContent = `Instancia ${d.instance}`;
    setConn(els.conn, 'live', 'En vivo');
    showBanner(d.redis ? '' : 'El servidor de resultados está en modo degradado. Lo que ves puede estar desactualizado.');
  });
  es.addEventListener('snapshot', (e) => {
    const d = JSON.parse(e.data);
    for (const [t, vals] of Object.entries(d.s)) state.topics[t] = { ...vals };
    markId(d.id);
  });
  es.addEventListener('delta', (e) => {
    const d = JSON.parse(e.data);
    applyDelta(d.d);
    if (d.c?.length) addCorrections(d.c);
    markId(d.id);
  });
  es.addEventListener('status', (e) => {
    const d = JSON.parse(e.data);
    showBanner(d.redis ? '' : 'Sin conexión con el registro central. Los resultados están congelados hasta que se restablezca.');
  });
  // El servidor nos pide volver (cliente lento o instancia que se apaga).
  es.addEventListener('bye', (e) => {
    const d = JSON.parse(e.data);
    if (d.lastId) state.lastId = d.lastId;
    scheduleReconnect();
  });
  es.onerror = () => scheduleReconnect();
}

function scheduleReconnect() {
  state.es?.close();
  state.es = null;
  const wait = backoff(state.attempt++, { base: RETRY_BASE_MS });
  // Una red que corta SSE no debe dejar a la persona sin cifras mientras se reintenta.
  if (state.attempt >= POLL_AFTER_FAILURES) startPolling();
  setConn(els.conn, 'retry', `Reconectando en ${Math.ceil(wait / 1000)} s`);
  clearTimeout(state.retryTimer);
  state.retryTimer = setTimeout(connect, wait);
}

function reconnectNow() {
  state.es?.close();
  clearTimeout(state.retryTimer);
  state.attempt = 0;
  connect();
}

// Tercer transporte: polling cada 5 s si no hay EventSource o SSE falla repetidamente.
function startPolling() {
  if (state.polling) return;
  state.polling = true;
  els.fTransport.textContent = 'Transporte polling';
  poll();
}

function stopPolling() {
  state.polling = false;
  clearTimeout(state.pollTimer);
  els.fTransport.textContent = 'Transporte SSE';
}

async function poll() {
  try {
    const r = await fetch(`/api/snapshot?view=${encodeURIComponent(state.view)}`);
    const d = await r.json();
    if (!state.polling) return;
    if (d.s) { for (const [t, v] of Object.entries(d.s)) state.topics[t] = v; markId(d.id); setConn(els.conn, 'live', 'Actualizando'); }
  } catch { if (state.polling) setConn(els.conn, 'down', 'Sin conexión'); }
  if (state.polling) state.pollTimer = setTimeout(poll, 5000);
}

function markId(id) {
  if (id) state.lastId = id;
  state.lastUpdate = Date.now();
  els.fId.textContent = `Evento ${state.lastId || '—'}`;
  scheduleRender();
}

function applyDelta(d) {
  for (const [t, vals] of Object.entries(d)) state.topics[t] = { ...(state.topics[t] || {}), ...vals };
}

// ---------- Render agrupado por frame (INP bajo en gama baja) ----------
let renderQueued = false;
function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; render(); });
}

function buildCandidates() {
  els.candidates.innerHTML = geo.candidates.map((c) => `
    <li class="cand" data-id="${c.id}" style="--c:${c.color}">
      <span class="cand-swatch"></span>
      <div><div class="cand-name">${c.name}</div><div class="cand-party">${c.party}</div>
        <div class="cand-bar"><span></span></div></div>
      <div class="cand-figs"><div class="cand-pct num">0,0%</div><div class="cand-votes num">0 votos</div></div>
    </li>`).join('');
}

function render() {
  const topic = focusTopic();
  const data = state.topics[topic] || {};
  const mun = geo.municipios.find((m) => m.id === Number(els.mun.value));
  let totalMesas = geo.totalMesas;
  if (topic.startsWith('mun:')) totalMesas = mun.mesas;
  if (topic.startsWith('puesto:')) totalMesas = mun.puestos.find(([p]) => p === Number(els.puesto.value))[1];

  els.kicker.textContent = topic === 'dep' ? 'Departamento' : topic.startsWith('mun:') ? 'Municipio' : `Puesto en ${mun.name}`;
  els.title.textContent = topic === 'dep' ? 'Nariño' : topic.startsWith('mun:') ? mun.name : `Puesto ${els.puesto.value.padStart(4, '0')}`;

  const inf = data.__inf || 0;
  const p = pct(inf, totalMesas);
  els.pct.textContent = `${p.toLocaleString('es-CO', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
  els.count.textContent = `${fmt.format(inf)} de ${fmt.format(totalMesas)}`;
  els.tally.style.width = `${p}%`;

  // Porcentaje sobre votos válidos (candidatos + blanco).
  const valid = geo.candidates.reduce((a, c) => a + (data[c.id] || 0), 0) + (data.BLANCO || 0);
  const leader = geo.candidates.reduce((best, c) => ((data[c.id] || 0) > (data[best.id] || 0) ? c : best), geo.candidates[0]);
  const order = [...geo.candidates].sort((a, b) => (data[b.id] || 0) - (data[a.id] || 0));

  order.forEach((c, i) => {
    const li = els.candidates.querySelector(`[data-id="${c.id}"]`);
    const v = data[c.id] || 0;
    const share = pct(v, valid);
    li.style.order = i;
    li.querySelector('.cand-pct').textContent = `${share.toLocaleString('es-CO', { maximumFractionDigits: 1, minimumFractionDigits: 1 })}%`;
    li.querySelector('.cand-votes').textContent = `${fmt.format(v)} votos`;
    li.querySelector('.cand-bar > span').style.width = `${share}%`;
    li.classList.toggle('lead', v > 0 && c.id === leader.id);
    li.classList.toggle('down', prevShown[c.id] !== undefined && v < prevShown[c.id]);
    prevShown[c.id] = v;
  });

  els.others.innerHTML = [['BLANCO', 'Voto en blanco'], ['NULO', 'Nulos'], ['NO_MARCADO', 'No marcados']]
    .map(([k, label]) => `<span>${label}<b class="num">${fmt.format(data[k] || 0)}</b></span>`).join('');
}

function renderUpdated() {
  if (!state.lastUpdate) return;
  const s = Math.round((Date.now() - state.lastUpdate) / 1000);
  els.updated.textContent = s < 3 ? 'Actualizado ahora' : `Última actualización hace ${s} s`;
}

// ---------- Correcciones explicadas ----------
const optName = (k) => geo.candidates.find((c) => c.id === k)?.name || { BLANCO: 'Blanco', NULO: 'Nulos', NO_MARCADO: 'No marcados' }[k] || k;
function addCorrections(list) {
  els.changes.querySelector('.empty')?.remove();
  for (const c of list) {
    const li = document.createElement('li');
    const mun = geo.municipios[c.municipio - 1]?.name || '';
    li.innerHTML = `<span class="ver">v${c.from} → v${c.to}</span><span>Acta <b>${c.acta}</b> · ${mun}</span>
      <div class="diff">${Object.entries(c.diff).map(([k, d]) => `<span class="${d < 0 ? 'neg' : 'pos'}">${optName(k)} ${d > 0 ? '+' : ''}${fmt.format(d)}</span>`).join('')}</div>`;
    els.changes.prepend(li);
  }
  while (els.changes.children.length > 40) els.changes.lastChild.remove();
}

function showBanner(text) {
  els.banner.textContent = text;
  els.banner.classList.toggle('show', Boolean(text));
}
