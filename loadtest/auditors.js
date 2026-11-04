import WebSocket from 'ws';
import { readConfig } from '../src/support/config.js';
const config = readConfig();
const url = process.env.WS_URL || 'ws://localhost:8080/ws/auditor';
const count = Number(process.env.AUDITORS || 120);
const duration = Number(process.env.DURATION_MS || 60000);
const tokens = Object.keys(config.auditors);
if (!tokens.length) throw new Error('Configura AUDITOR_TOKENS');
if (!Number.isSafeInteger(count) || count < 1 || count > 1000 || !Number.isSafeInteger(duration) || duration < 1000) throw new Error('AUDITORS o DURATION_MS inválidos');
const clients = new Set(); const timers = new Set();
const stats = { connected: 0, claims: 0, releases: 0, abruptDisconnects: 0, errors: 0 };
function later(fn, ms) { const timer = setTimeout(() => { timers.delete(timer); fn(); }, ms); timers.add(timer); }
for (let i = 0; i < count; i++) later(() => {
  const ws = new WebSocket(url); clients.add(ws); let heartbeat;
  const send = value => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(value)); };
  ws.on('open', () => send({ type: 'auth', token: tokens[i % tokens.length] }));
  ws.on('message', raw => {
    const message = JSON.parse(raw.toString());
    if (message.type === 'hello') { stats.connected++; heartbeat = setInterval(() => send({ type: 'activity' }), 5000); send({ type: 'claim' }); }
    if (message.type === 'result' && message.action === 'claim' && message.status === 'OK') {
      stats.claims++;
      later(() => { if (i % 3 === 0) { stats.abruptDisconnects++; ws.terminate(); } else send({ type: 'release' }); }, 1000 + Math.random() * 5000);
    }
    if (message.type === 'result' && message.action === 'release' && message.status === 'OK') stats.releases++;
    if (message.type === 'error') stats.errors++;
  });
  ws.on('close', () => { clearInterval(heartbeat); clients.delete(ws); });
  ws.on('error', () => stats.errors++);
}, Math.random() * Math.min(5000, duration / 4));
setTimeout(() => { for (const timer of timers) clearTimeout(timer); for (const ws of clients) ws.terminate(); console.log(JSON.stringify(stats, null, 2)); }, duration);
