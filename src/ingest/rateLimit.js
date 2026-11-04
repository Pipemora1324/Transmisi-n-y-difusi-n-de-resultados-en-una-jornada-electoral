// Límite de tasa por credencial con ventana fija en Redis (sección 5 del caso).
import { redis, K } from '../redis.js';

export async function checkRateLimit(credential, limit, windowSec) {
  const bucket = Math.floor(Date.now() / 1000 / windowSec);
  const [allowed, used, ttl] = await redis.rateLimit(K.rl(credential, bucket), limit, windowSec);
  return { allowed: allowed === 1, used, retryAfterSec: Math.max(ttl, 1) };
}
