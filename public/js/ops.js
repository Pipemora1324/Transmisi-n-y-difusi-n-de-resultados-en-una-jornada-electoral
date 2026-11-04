import { $, element, backoff, status, number } from './ui.js';
let credential = '', controller, retry, attempt = 0, lastSample = 0;
const delays = [], queues = [];
function chart(values, id, caption, unit) {
  const max = Math.max(1, ...values);
  const points = values.map((v, i) => (i ? 'L' : 'M') + (i * 600 / Math.max(1, values.length - 1)).toFixed(1) + ' ' + (170 - v * 150 / max).toFixed(1)).join(' ');
  $(id).setAttribute('d', points); $(caption).textContent = 'Escala 0–' + number(max, 1) + ' ' + unit;
}
function render(sample) {
  lastSample = Date.now();
  if (!sample.redis) { status('Redis no disponible', false); $('notice').textContent = 'Métricas no disponibles. Esperando recuperación de Redis.'; for (const id of ['online', 'auditors', 'queued', 'delay']) $(id).textContent = '—'; $('instances').replaceChildren(); return; }
  status('En línea', true); $('notice').textContent = ''; $('online').textContent = sample.onlineInstances; $('expected').textContent = 'de ' + sample.expectedInstances + ' esperadas'; $('auditors').textContent = number(sample.totals.auditorConnections); $('queued').textContent = number(sample.queue.queued); $('active').textContent = sample.queue.active + ' en revisión'; $('delay').textContent = number(sample.totals.maxInstanceP95Ms, 1);
  $('updated').textContent = 'Actualizado ' + new Date(sample.timestamp).toLocaleTimeString('es-CO');
  delays.push(sample.totals.maxInstanceP95Ms); queues.push(sample.queue.queued); if (delays.length > 60) { delays.shift(); queues.shift(); }
  chart(delays, 'delay-line', 'delay-max', 'ms'); chart(queues, 'queue-line', 'queue-max', 'actas');
  $('instances').replaceChildren();
  for (const instance of sample.instances.sort((a, b) => a.instanceId.localeCompare(b.instanceId))) {
    const row = element('tr');
    for (const [index, value] of [instance.instanceId, number(instance.uptimeSeconds) + ' s', number(instance.rssBytes / 1048576, 1) + ' MB', number(instance.eventLoop.p95Ms, 1) + ' ms', number(instance.eventLoop.utilization * 100, 1) + '%', instance.connections.auditor, instance.counters.approved || 0, instance.counters.rejected || 0].entries()) row.append(element('td', String(value), index === 0 ? 'instance-id mono' : 'mono'));
    $('instances').append(row);
  }
}
async function connect() {
  const activeController = controller = new AbortController();
  status('Conectando…', false);
  try {
    const response = await fetch('/sse/ops', { headers: { Authorization: 'Bearer ' + credential }, signal: activeController.signal });
    if (response.status === 401) { credential = ''; $('notice').textContent = 'Credencial de operación inválida.'; $('connect').disabled = false; status('Sin acceso', false); return; }
    if (!response.ok) throw new Error('HTTP ' + response.status);
    attempt = 0; $('credential').value = ''; $('connect').disabled = true; $('disconnect').hidden = false;
    const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
    let buffer = '';
    while (true) {
      const { value, done } = await reader.read(); if (done) break;
      buffer += value; let end;
      while ((end = buffer.indexOf('\n\n')) !== -1) {
        const frame = buffer.slice(0, end); buffer = buffer.slice(end + 2);
        const data = frame.split('\n').find(line => line.startsWith('data: '));
        if (data) render(JSON.parse(data.slice(6)));
      }
    }
  } catch (error) { if (error.name === 'AbortError') return; }
  if (credential && controller === activeController) { status('Reconectando…', false); retry = setTimeout(connect, backoff(attempt++)); }
}
$('login').addEventListener('submit', event => { event.preventDefault(); credential = $('credential').value.trim(); if (!credential) return; clearTimeout(retry); controller?.abort(); delays.length = 0; queues.length = 0; connect(); });
$('disconnect').addEventListener('click', () => { credential = ''; clearTimeout(retry); controller?.abort(); $('connect').disabled = false; $('disconnect').hidden = true; status('Sin conexión', false); });
setInterval(() => { if (credential && lastSample && Date.now() - lastSample > 10000) status('Datos desactualizados', false); }, 2000);
