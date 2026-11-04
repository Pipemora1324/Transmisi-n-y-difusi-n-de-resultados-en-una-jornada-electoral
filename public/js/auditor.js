import { $, element, backoff, status, number } from './ui.js';
let ws, credential = '', retry, attempt = 0, current = null, identity, approvalAvailable = false, idleMs = 300000, lastActivity = Date.now(), busy = false;
const labels = { C1: 'Valentina Rosero', C2: 'Martín Erazo', C3: 'Lucía Benavides', BLANCO: 'Voto en blanco', NULO: 'Voto nulo', NO_MARCADO: 'No marcado' };
const messages = { OK: 'Operación completada.', EMPTY: 'No hay casos disponibles en este tramo de la cola.', BUSY: 'Otro auditor tomó este caso.', NOT_QUEUED: 'El caso ya fue resuelto.', NOT_OWNER: 'La reserva pertenece a otro auditor.', LOCK_LOST: 'Tu reserva expiró. Toma el caso de nuevo.', INVALID_REASON: 'Escribe un motivo de rechazo de 5 a 500 caracteres.', INTEGRATION_REQUIRED: 'La aprobación requiere integrar el módulo de ingesta.', UNAVAILABLE: 'Redis no está disponible. No se confirmó la operación.', ALREADY_CLAIMED: 'Primero resuelve o libera tu caso actual.', APPLIED: 'Acta aprobada e incorporada al preconteo.', CORRECTED: 'Corrección aprobada.', STALE: 'Una versión más reciente reemplazó este caso.', DUPLICATE_VERSION: 'Esta versión ya estaba aplicada.', NO_CASE: 'Toma un caso primero.', NOT_FOUND: 'El caso ya no existe.' };
function notice(code) { $('notice').textContent = messages[code] || code; }
function send(data) { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
function lockControls() {
  const online = ws?.readyState === WebSocket.OPEN && Boolean(identity);
  $('claim-next').disabled = !online || Boolean(current) || busy;
  for (const id of ['approve', 'reject', 'release']) $(id).disabled = !online || !current || busy || Date.now() >= current.expiresAt || (id === 'approve' && !approvalAvailable);
}
function renderCase() {
  $('case-empty').hidden = Boolean(current); $('case-detail').hidden = !current; $('case-badge').textContent = current ? current.case.id : 'SIN CASO';
  if (!current) return lockControls();
  const item = current.case;
  $('case-meta').replaceChildren();
  for (const [label, value] of [['Mesa', item.mesa], ['Puesto', item.puesto], ['Municipio', item.municipio], ['Versión', item.version], ['Acta', item.id]]) {
    const pair = element('div'); pair.append(element('dt', label), element('dd', String(value), 'mono')); $('case-meta').append(pair);
  }
  $('case-reason').textContent = item.motivo || 'Acta retenida para revisión contable.';
  $('votes').replaceChildren();
  for (const [option, count] of Object.entries(item.votos)) { const row = element('tr'); row.append(element('td', labels[option] || option), element('td', number(count))); $('votes').append(row); }
  $('vote-total').textContent = 'Suma de votos: ' + number(Object.values(item.votos).reduce((a, b) => a + Number(b), 0));
  $('voters').textContent = 'Sufragantes: ' + number(item.sufragantes);
  $('reason').value = '';
  $('approval-note').textContent = approvalAvailable ? '' : 'Aprobación pendiente de integrar la ingesta. Puedes revisar, rechazar o liberar.';
  lockControls();
}
function renderState(data) {
  $('queue-count').textContent = data.items.length === 200 ? '200+' : String(data.items.length);
  $('queue').replaceChildren();
  if (!data.items.length) $('queue').append(element('div', 'No hay actas pendientes de verificación.', 'empty'));
  for (const item of data.items) {
    const button = element('button', undefined, 'queue-item' + (current?.case.id === item.id ? ' selected' : ''));
    const row = element('div', undefined, 'row'); row.append(element('strong', item.id), element('span', 'P ' + item.priority, 'badge'));
    button.append(row, element('small', 'Mesa ' + item.mesa + ' · Versión ' + item.version), element('p', item.motivo));
    if (item.claimedBy) button.append(element('small', 'En revisión por ' + item.claimedBy.auditor));
    button.disabled = Boolean(item.claimedBy) || Boolean(current) || busy;
    button.addEventListener('click', () => action('claim', { id: item.id })); $('queue').append(button);
  }
  $('team-count').textContent = data.people.length; $('people').replaceChildren();
  if (!data.people.length) $('people').append(element('div', 'No hay auditores activos.', 'empty'));
  for (const person of data.people) {
    const row = element('div', undefined, 'person'); const text = element('div');
    text.append(element('strong', person.name), element('small', person.caseId ? 'Revisando ' + person.caseId : 'Disponible'));
    row.append(element('span', person.name.slice(0, 1).toUpperCase(), 'avatar'), text); $('people').append(row);
  }
}
function action(type, data = {}) { if (busy || !identity) return; busy = true; lockControls(); send({ type, ...data }); }
function connect() {
  status('Conectando…', false); clearTimeout(retry); $('connect').disabled = true;
  ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws/auditor');
  ws.onopen = () => send({ type: 'auth', token: credential });
  ws.onmessage = event => {
    const data = JSON.parse(event.data);
    if (data.type === 'hello') { identity = data.identity; approvalAvailable = data.approvalAvailable; idleMs = data.idleMs; attempt = 0; busy = false; status('En línea', true); $('identity').textContent = identity.name; $('instance').textContent = 'Instancia ' + data.instanceId; $('disconnect').hidden = false; $('connect').disabled = true; $('credential').value = ''; $('notice').textContent = ''; }
    if (data.type === 'state') { status('En línea', true); renderState(data); }
    if (data.type === 'status' && !data.redis) { status('Redis no disponible', false); notice('UNAVAILABLE'); }
    if (data.type === 'result') {
      busy = false; notice(data.status);
      if (data.action === 'claim' && data.status === 'OK') current = data;
      if (data.action !== 'claim' && ['OK', 'APPLIED', 'CORRECTED', 'STALE', 'DUPLICATE_VERSION', 'NOT_OWNER', 'LOCK_LOST', 'NOT_FOUND'].includes(data.status)) current = null;
      renderCase(); send({ type: 'refresh' });
    }
    if (data.type === 'lease' && current) current.expiresAt = data.expiresAt;
    if (data.type === 'lost') { current = null; notice(data.status); renderCase(); }
    if (data.type === 'error') { busy = false; notice(data.status); }
    lockControls();
  };
  ws.onclose = event => {
    identity = null; current = null; busy = false; renderCase(); status('Sin conexión', false); $('connect').disabled = false;
    $('queue').replaceChildren(element('div', 'Sin conexión. La cola se actualizará al reconectar.', 'empty'));
    $('people').replaceChildren(element('div', 'Sin conexión', 'empty'));
    if (event.code === 4001) { credential = ''; $('notice').textContent = 'Credencial inválida. Inténtalo de nuevo.'; }
    if (credential) { status('Reconectando…', false); retry = setTimeout(connect, backoff(attempt++)); }
  };
  ws.onerror = () => status('No se pudo conectar', false);
}
$('login').addEventListener('submit', event => { event.preventDefault(); credential = $('credential').value.trim(); if (!credential) return; ws?.close(); connect(); });
$('disconnect').addEventListener('click', () => { credential = ''; clearTimeout(retry); ws?.close(1000); $('disconnect').hidden = true; });
$('claim-next').addEventListener('click', () => action('claim'));
$('release').addEventListener('click', () => action('release'));
$('approve').addEventListener('click', () => action('approve'));
$('reject').addEventListener('click', () => { const reason = $('reason').value.trim(); if (reason.length < 5) return notice('INVALID_REASON'); action('reject', { reason }); });
for (const name of ['pointerdown', 'pointermove', 'keydown', 'touchstart']) document.addEventListener(name, () => { lastActivity = Date.now(); }, { passive: true });
setInterval(() => { if (identity) send({ type: document.visibilityState === 'visible' && Date.now() - lastActivity < idleMs ? 'activity' : 'heartbeat' }); }, 5000);
setInterval(() => { if (current) { const remaining = Math.max(0, Math.ceil((current.expiresAt - Date.now()) / 1000)); $('lease').textContent = remaining ? 'Reserva vigente · ' + remaining + ' s hasta la próxima expiración' : 'Reserva expirada. Vuelve a tomar el caso.'; lockControls(); } }, 500);
