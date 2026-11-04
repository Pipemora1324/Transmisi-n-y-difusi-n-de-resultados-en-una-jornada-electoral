import { runLua } from './lua.js';
import { verificationKeys } from './keys.js';
export function normalizeCase(id, h, priority = 0, claim = null) {
  return { id, acta: id, priority: Number(priority), version: Number(h.version || 0), mesa: Number(h.mesa || 0), puesto: Number(h.puesto || 0), municipio: Number(h.municipio || 0), votos: typeof h.votos === 'string' ? JSON.parse(h.votos) : h.votos || {}, sufragantes: Number(h.sufragantes || 0), motivo: h.motivo || '', createdAt: Number(h.ts || 0), claimedBy: claim };
}
export function createQueue(redis, config) {
  const k = verificationKeys(config.prefix);
  return {
    async enqueue(record) {
      if (!record || typeof record.id !== 'string' || !/^[\w:.-]{1,120}$/.test(record.id)) throw new Error('Id de caso inválido');
      const priority = record.priority ?? 0;
      if (!Number.isSafeInteger(priority) || priority < 0) throw new Error('Prioridad inválida');
      const { id, votos, priority: ignored, ...fields } = record;
      const payload = JSON.stringify({ ...fields, votos: JSON.stringify(votos || {}), ts: record.createdAt || Date.now() });
      if (Buffer.byteLength(payload) > 32768) throw new Error('Caso demasiado grande');
      return Boolean(await runLua(redis, 'enqueue', [k.queue, k.heldPrefix + id, k.events], [id, priority, payload]));
    },
    async list(limit = 100) {
      const rows = await redis.zrevrange(k.queue, 0, Math.max(1, Math.min(limit, 200)) - 1, 'WITHSCORES');
      if (!rows.length) return [];
      const pipeline = redis.pipeline();
      for (let i = 0; i < rows.length; i += 2) { pipeline.hgetall(k.heldPrefix + rows[i]); pipeline.hget(k.claims, rows[i]); pipeline.exists(k.lockPrefix + rows[i]); }
      const results = await pipeline.exec();
      if (results.some(([e]) => e)) throw new Error('No se pudo leer la cola');
      const cases = [];
      for (let i = 0; i < rows.length / 2; i++) {
        const h = results[i * 3][1]; if (!h.version) continue;
        const raw = results[i * 3 + 1][1];
        cases.push(normalizeCase(rows[i * 2], h, rows[i * 2 + 1], raw && results[i * 3 + 2][1] ? JSON.parse(raw) : null));
      }
      return cases;
    },
    async counts() {
      const time = await redis.time(); const now = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
      const values = await redis.multi().zcard(k.queue).zcount(k.active, now + 1, '+inf').exec();
      if (values.some(([e]) => e)) throw new Error('No se pudo leer la cola');
      return { queued: values[0][1], active: values[1][1] };
    },
  };
}
