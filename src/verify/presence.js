import { verificationKeys } from './keys.js';
const heartbeat = `local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); redis.call('ZADD',KEYS[1],now+tonumber(ARGV[3]),ARGV[1]); redis.call('HSET',KEYS[2],ARGV[1],ARGV[2]); return now`;
export function createPresence(redis, config) {
  const k = verificationKeys(config.prefix);
  return {
    async beat(session, identity) { await redis.eval(heartbeat, 2, k.presence, k.people, session, JSON.stringify({ ...identity, instanceId: config.instanceId }), config.presenceTtlMs); },
    async leave(session) { await redis.multi().zrem(k.presence, session).hdel(k.people, session).exec(); },
    async list() {
      const time = await redis.time();
      const now = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
      const sessions = await redis.zrangebyscore(k.presence, now + 1, '+inf', 'LIMIT', 0, 500);
      if (!sessions.length) return [];
      const values = await redis.hmget(k.people, ...sessions);
      const people = new Map();
      for (const value of values) if (value) { const p = JSON.parse(value); people.set(p.id, p); }
      return [...people.values()];
    },
  };
}
