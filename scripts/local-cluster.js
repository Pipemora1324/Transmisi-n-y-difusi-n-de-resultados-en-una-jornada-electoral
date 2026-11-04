import http from 'node:http';
import net from 'node:net';
import { spawn } from 'node:child_process';
const count = Number(process.env.INSTANCES || 8);
const port = Number(process.env.LB_PORT || 8080);
const firstPort = Number(process.env.FIRST_PORT || 3101);
if (!Number.isInteger(count) || count < 1 || count > 32) throw new Error('INSTANCES debe estar entre 1 y 32');
const children = [];
const live = new Map();
const sockets = new Set();
let cursor = 0;
let stopping = false;
function target() { const ports = [...live.keys()]; return ports.length ? ports[cursor++ % ports.length] : null; }
const server = http.createServer((req, res) => {
  const selected = target();
  if (!selected) { res.writeHead(503); res.end('No hay instancias disponibles'); return; }
  const upstream = http.request({ host: '127.0.0.1', port: selected, method: req.method, path: req.url, headers: req.headers }, incoming => {
    res.writeHead(incoming.statusCode, incoming.headers); incoming.pipe(res); incoming.on('error', () => res.destroy());
  });
  upstream.on('error', () => { if (!res.headersSent) { res.writeHead(502); res.end('Instancia no disponible; reintenta'); } else res.destroy(); });
  res.on('close', () => upstream.destroy()); req.pipe(upstream);
});
server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
server.on('upgrade', (req, socket, head) => {
  const selected = target(); if (!selected) return socket.destroy();
  const upstream = net.connect(selected, '127.0.0.1', () => {
    upstream.write(`${req.method} ${req.url} HTTP/${req.httpVersion}\r\n`);
    for (let i = 0; i < req.rawHeaders.length; i += 2) upstream.write(req.rawHeaders[i] + ': ' + req.rawHeaders[i + 1] + '\r\n');
    upstream.write('\r\n'); if (head.length) upstream.write(head);
    socket.pipe(upstream).pipe(socket);
  });
  upstream.on('error', () => socket.destroy()); socket.on('error', () => upstream.destroy()); socket.on('close', () => upstream.destroy());
});
for (let i = 0; i < count; i++) {
  const childPort = firstPort + i;
  const child = spawn(process.execPath, ['--env-file-if-exists=.env', 'src/server.js'], { stdio: 'inherit', windowsHide: true, env: { ...process.env, PORT: String(childPort), INSTANCE_ID: 'local-' + (i + 1), EXPECTED_INSTANCES: String(count) } });
  children.push(child);
  child.on('exit', () => { live.delete(childPort); if (!stopping) console.warn('Instancia detenida: ' + childPort); });
  child.on('error', error => console.error(error.message));
}
let probing = false;
const probe = setInterval(async () => {
  if (probing) return; probing = true;
  await Promise.all(children.map(async (child, i) => {
    if (child.exitCode !== null) return;
    const childPort = firstPort + i;
    try {
      const response = await fetch('http://127.0.0.1:' + childPort + '/healthz', { signal: AbortSignal.timeout(1500) });
      // En modo degradado se permite servir la UI para informar la caída de Redis.
      if ([200, 503].includes(response.status)) live.set(childPort, true); else live.delete(childPort);
      await response.body.cancel();
    } catch { live.delete(childPort); }
  })); probing = false;
}, 2000);
server.listen(port, () => console.log('Balanceador local: http://localhost:' + port));
function stop() { if (stopping) return; stopping = true; clearInterval(probe); server.close(); for (const socket of sockets) socket.destroy(); for (const child of children) child.kill('SIGTERM'); const timer = setTimeout(() => { for (const child of children) if (child.exitCode === null) child.kill('SIGKILL'); process.exit(0); }, 4000); timer.unref(); }
server.on('error', error => { console.error(error.message); stop(); process.exitCode = 1; });
process.once('SIGINT', stop); process.once('SIGTERM', stop);
