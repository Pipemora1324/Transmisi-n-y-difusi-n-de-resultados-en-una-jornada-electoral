import { randomUUID, timingSafeEqual } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import { verificationKeys } from '../verify/keys.js';

export function sameSecret(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || !b) return false;
  const left = Buffer.from(a); const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}
export function createAuditorGateway({ config, redis, subscriber, queue, locks, presence, metrics, approveHeld }) {
  const wss = new WebSocketServer({ noServer: true, maxPayload: 8192, perMessageDeflate: false });
  const channel = verificationKeys(config.prefix).events;
  let refreshing = false;
  let pending = false;
  const send = (ws, message) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (ws.bufferedAmount > 256 * 1024) return ws.close(4008, 'SLOW_CLIENT');
    ws.send(JSON.stringify(message));
  };
  async function refresh() {
    if (refreshing) { pending = true; return; }
    if (![...wss.clients].some(ws => ws.identity)) return;
    refreshing = true;
    try {
      const [items, people] = await Promise.all([queue.list(200), presence.list()]);
      for (const ws of wss.clients) if (ws.identity) send(ws, { type: 'state', items, people, redis: true });
    } catch { for (const ws of wss.clients) if (ws.identity) send(ws, { type: 'status', redis: false }); }
    finally { refreshing = false; if (pending) { pending = false; scheduleRefresh(); } }
  }
  let refreshTimer;
  function scheduleRefresh() { if (!refreshTimer) refreshTimer = setTimeout(() => { refreshTimer = null; refresh(); }, 100); }
  const onMessage = (ch) => { if (ch === channel) scheduleRefresh(); };
  subscriber.on('message', onMessage);
  const subscribe = () => subscriber.subscribe(channel).catch(() => {});
  subscriber.on('ready', subscribe);
  if (subscriber.status === 'ready') subscribe();
  const pollTimer = setInterval(refresh, 5000);
  const liveTimer = setInterval(() => {
    for (const ws of wss.clients) { if (!ws.alive) ws.terminate(); else { ws.alive = false; ws.ping(); } }
  }, 10000);
  wss.on('connection', ws => {
    ws.alive = true;
    ws.on('error', () => {});
    ws.on('pong', () => { ws.alive = true; });
    const session = randomUUID();
    let current = null;
    let lastActivity = Date.now();
    let sequence = Promise.resolve();
    let bucket = Date.now(); let messages = 0;
    const authTimer = setTimeout(() => ws.close(4001, 'AUTH_REQUIRED'), 5000);
    ws.on('message', raw => {
      if (Date.now() - bucket >= 1000) { bucket = Date.now(); messages = 0; }
      if (++messages > 20) return ws.close(4008, 'RATE_LIMITED');
      sequence = sequence.then(async () => {
        if (ws.readyState !== WebSocket.OPEN) return;
        let msg; try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', status: 'INVALID_JSON' }); }
        if (!msg || typeof msg !== 'object') return;
        metrics.increment('wsMessages');
        if (!ws.identity) {
          const entry = Object.entries(config.auditors).find(([secret]) => sameSecret(msg.token, secret));
          if (msg.type !== 'auth' || !entry) return ws.close(4001, 'UNAUTHORIZED');
          await presence.beat(session, entry[1]);
          ws.identity = entry[1]; clearTimeout(authTimer); metrics.connection('auditor', 1);
          send(ws, { type: 'hello', identity: ws.identity, instanceId: config.instanceId, lockTtlMs: config.lockTtlMs, idleMs: config.idleMs, approvalAvailable: Boolean(approveHeld) });
          return refresh();
        }
        const owner = ws.identity.id;
        if (msg.type === 'heartbeat' || msg.type === 'activity') {
          if (msg.type === 'activity') lastActivity = Date.now();
          await presence.beat(session, { ...ws.identity, caseId: current?.case.id || null });
          if (current && msg.type === 'activity' && Date.now() - lastActivity < config.idleMs) {
            const result = await locks.renew(owner, current.case.id, current.token);
            if (result.status === 'OK') { current.expiresAt = result.expiresAt; send(ws, { type: 'lease', expiresAt: result.expiresAt }); }
            else { current = null; send(ws, { type: 'lost', status: result.status }); }
          }
          return;
        }
        if (msg.type === 'refresh') return refresh();
        lastActivity = Date.now();
        if (msg.type === 'claim') {
          if (current) return send(ws, { type: 'result', action: 'claim', status: 'ALREADY_CLAIMED' });
          if (msg.id !== undefined && (typeof msg.id !== 'string' || !/^[\w:.-]{1,120}$/.test(msg.id))) return send(ws, { type: 'error', status: 'INVALID_ID' });
          const result = await locks.claim(owner, msg.id || '');
          if (result.status === 'OK') { current = result; metrics.increment('claimed'); }
          send(ws, { ...result, token: undefined, type: 'result', action: 'claim' }); return scheduleRefresh();
        }
        if (!['release', 'reject', 'approve'].includes(msg.type)) return send(ws, { type: 'error', status: 'INVALID_ACTION' });
        if (!current) return send(ws, { type: 'result', action: msg.type, status: 'NO_CASE' });
        let result;
        if (msg.type === 'approve') {
          if (!approveHeld) return send(ws, { type: 'result', action: 'approve', status: 'INTEGRATION_REQUIRED' });
          result = await approveHeld(current.case.id, current.token);
        } else if (msg.type === 'reject') result = await locks.reject(owner, current.case.id, current.token, msg.reason);
        else result = await locks.release(owner, current.case.id, current.token);
        const done = ['OK', 'APPLIED', 'CORRECTED', 'STALE', 'DUPLICATE_VERSION'].includes(result.status);
        if (done) metrics.increment({ release: 'released', reject: 'rejected', approve: 'approved' }[msg.type]);
        if (done || ['NOT_OWNER', 'LOCK_LOST', 'NOT_FOUND'].includes(result.status)) current = null;
        send(ws, { type: 'result', action: msg.type, ...result }); scheduleRefresh();
      }).catch(() => { metrics.increment('redisErrors'); send(ws, { type: 'error', status: 'UNAVAILABLE' }); if (!ws.identity) ws.close(1013, 'UNAVAILABLE'); });
    });
    ws.on('close', () => {
      clearTimeout(authTimer);
      sequence.finally(async () => {
        if (ws.identity) {
          metrics.connection('auditor', -1);
          if (current) await locks.release(ws.identity.id, current.case.id, current.token).catch(() => {});
          await presence.leave(session).catch(() => {}); scheduleRefresh();
        }
      });
    });
  });
  return {
    wss,
    stop() {
      clearInterval(pollTimer); clearInterval(liveTimer); clearTimeout(refreshTimer);
      subscriber.off('message', onMessage); subscriber.off('ready', subscribe);
      for (const ws of wss.clients) ws.close(1012, 'INSTANCE_SHUTDOWN');
      const timer = setTimeout(() => { for (const ws of wss.clients) ws.terminate(); }, 1000); timer.unref();
      wss.close();
    },
  };
}
