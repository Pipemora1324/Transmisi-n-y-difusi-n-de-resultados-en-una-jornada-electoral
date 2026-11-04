import { randomUUID } from 'node:crypto';
import { runLua } from './lua.js';
import { verificationKeys } from './keys.js';
import { normalizeCase } from './queue.js';
export function createLocks(redis, config) {
  const k = verificationKeys(config.prefix);
  const key = id => k.lockPrefix + id;
  return {
    async claim(owner, id = '') {
      const token = owner + ':' + randomUUID();
      const result = await runLua(redis, 'claim', [k.queue, k.claims, k.active, k.events], [k.lockPrefix, k.heldPrefix, id, token, owner, config.lockTtlMs]);
      return result[0] === 'OK' ? { status: 'OK', case: normalizeCase(result[1], JSON.parse(result[2])), token, expiresAt: Number(result[3]) } : { status: result[0] };
    },
    async renew(owner, id, token) {
      const result = await runLua(redis, 'renew', [key(id), k.active], [token, owner, config.lockTtlMs, id]);
      return { status: result[0], expiresAt: result[1] ? Number(result[1]) : undefined };
    },
    async release(owner, id, token) {
      const result = await runLua(redis, 'release', [key(id), k.active, k.claims, k.events], [token, owner, id]);
      return { status: result[0] };
    },
    async reject(owner, id, token, reason) {
      if (typeof reason !== 'string' || reason.trim().length < 5 || reason.length > 500) return { status: 'INVALID_REASON' };
      const result = await runLua(redis, 'reject', [key(id), k.active, k.heldPrefix + id, k.audit, k.events, k.queue, k.claims], [token, owner, id, reason.trim()]);
      return { status: result[0] };
    },
  };
}
