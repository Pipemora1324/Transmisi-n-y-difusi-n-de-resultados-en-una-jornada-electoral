import Redis from 'ioredis';
export function createRedis(config) {
  const options = { lazyConnect: true, enableOfflineQueue: false, maxRetriesPerRequest: 1, connectTimeout: 2000, retryStrategy: n => Math.min(n * 200, 2000) };
  const redis = new Redis(config.redisUrl, options);
  const subscriber = new Redis(config.redisUrl, options);
  // ioredis emite errores al reconectar; las operaciones informan UNAVAILABLE.
  redis.on('error', () => {});
  subscriber.on('error', () => {});
  return { redis, subscriber, async connect() { await Promise.allSettled([redis.connect(), subscriber.connect()]); }, close() { redis.disconnect(); subscriber.disconnect(); } };
}
