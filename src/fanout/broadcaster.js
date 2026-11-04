// Fan-out por lotes cediendo el control al Event Loop (RF-4, RT-4).
//
// Recorrer 50.000 conexiones de un tirón bloquea el hilo único de Node durante decenas o
// cientos de milisegundos: en ese tiempo ningún jurado recibe su confirmación. Aquí se
// envía un lote de `batchSize` y luego se cede con setImmediate, que deja correr las
// fases de I/O pendientes (incluidos los mensajes de jurados) antes del siguiente lote.
import { config } from '../config.js';

export const yieldToLoop = () => new Promise((resolve) => setImmediate(resolve));

// Envía `payload` a cada cliente. `send(client)` devuelve true si se escribió.
export async function sendInBatches(clients, send, batchSize = config.batchSize) {
  let sent = 0;
  let inBatch = 0;
  for (const client of clients) {
    if (send(client)) sent++;
    if (++inBatch >= batchSize) {
      inBatch = 0;
      await yieldToLoop();
    }
  }
  return sent;
}

// Cola serial: si una ventana todavía se está repartiendo cuando llega la siguiente, la
// nueva espera. Así los deltas llegan en orden a cada cliente.
let chain = Promise.resolve();
export function enqueueFanout(task) {
  chain = chain.then(task).catch((err) => console.error('[fanout] error', err));
  return chain;
}

// Utilidades para ids de Redis Streams ("<ms>-<seq>"), que son monótonos.
export function compareIds(a, b) {
  const [am, as] = a.split('-').map(Number);
  const [bm, bs] = b.split('-').map(Number);
  return am === bm ? as - bs : am - bm;
}
export const isValidId = (id) => typeof id === 'string' && /^\d{1,15}-\d{1,10}$/.test(id);
