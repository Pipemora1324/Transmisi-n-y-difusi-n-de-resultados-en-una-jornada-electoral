// Cliente deliberadamente lento (RF-9). Abre una conexión SSE, deja de leer el socket y
// provoca ráfagas de relleno (requiere DEMO_MODE=1 en el servidor). El servidor debe
// marcarlo como rezagado al superar SSE_HIGH_WATER y cortarlo con "bye SLOW_CLIENT" al
// superar SSE_KILL_WATER, sin que su memoria crezca sin límite.
//
//   node loadtest/slow-client.js --url http://localhost:3001 --kb 4096
import net from 'node:net';
import http from 'node:http';

const args = Object.fromEntries(process.argv.slice(2).map((a, i, all) => a.startsWith('--') ? [a.slice(2), all[i + 1]] : null).filter(Boolean));
const url = new URL(args.url || 'http://localhost:3001');
const KB = Number(args.kb || 4096);
// Segundos que el cliente se queda sin leer tras la ráfaga. Con más de 30 s el servidor lo
// corta; con menos, lo pone al día con una fotografía en cuanto vuelve a leer.
const HOLD_S = Number(args.hold || 5);

const sock = net.connect(Number(url.port || 80), url.hostname, () => {
  sock.setRecvBufferSize?.(4096);
  sock.write(`GET /sse/public?view=dep HTTP/1.1\r\nHost: ${url.host}\r\nAccept: text/event-stream\r\n\r\n`);
  console.log('[lento] conectado; leo la cabecera y luego dejo de leer el socket…');
  sock.once('data', () => {
    sock.pause();
    console.log(`[lento] pausado. Pidiendo al servidor una ráfaga de ${KB} KB por cliente…`);
    const req = http.request({ method: 'POST', host: url.hostname, port: url.port, path: `/api/debug/flood?kb=${KB}` }, (res) => {
      let body = ''; res.on('data', (c) => (body += c)); res.on('end', () => {
        console.log('[lento] respuesta del servidor:', body);
        console.log(`[lento] sigo sin leer durante ${HOLD_S} s…`);
        setTimeout(() => {
          console.log('[lento] reanudo la lectura para ver cómo me dejó el servidor…');
          let tail = '';
          let bytes = 0;
          sock.on('data', (c) => { bytes += c.length; tail += c.toString(); if (tail.length > 400000) tail = tail.slice(-50000); });
          sock.resume();
          setTimeout(() => {
            const bye = /event: bye\ndata: (.+)/.exec(tail);
            const snap = /"reason":"(contrapresion)"/.exec(tail);
            console.log(`[lento] recibí ${(bytes / 1024).toFixed(0)} KB pendientes (el servidor dejó de acumular al pasar el umbral)`);
            if (bye) console.log(`[lento] el servidor me desconectó con: ${bye[1]}`);
            if (snap) console.log(`[lento] el servidor me puso al día con una fotografía (motivo: ${snap[1]})`);
            if (!bye && !snap) console.log('[lento] sin bye ni fotografía: revisa SSE_HIGH_WATER');
            process.exit(0);
          }, 4000);
        }, HOLD_S * 1000);
      });
    });
    req.end();
  });
});
sock.on('close', () => console.log('[lento] socket cerrado por el servidor'));
sock.on('error', (e) => console.log('[lento] error', e.message));
