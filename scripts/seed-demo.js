import { readConfig } from '../src/support/config.js';
import { createRedis } from '../src/support/redis.js';
import { createQueue } from '../src/verify/queue.js';
const config = readConfig();
const clients = createRedis(config);
try {
  await clients.connect();
  const queue = createQueue(clients.redis, config);
  let inserted = 0;
  for (let i = 1; i <= 12; i++) inserted += await queue.enqueue({ id: 'DEMO-E14-' + String(i).padStart(3, '0'), version: 1, mesa: i, puesto: 1, municipio: 1, votos: { C1: 80 + i, C2: 50, C3: 30, BLANCO: 4, NULO: 2, NO_MARCADO: 1 }, sufragantes: 160, motivo: 'Demostración: diferencia de ' + (7 + i) + ' votos.', priority: (7 + i) * 10 });
  console.log(inserted + ' casos DEMO añadidos. No son actas electorales reales ni deben aprobarse con la ingesta definitiva.');
} finally { clients.close(); }
