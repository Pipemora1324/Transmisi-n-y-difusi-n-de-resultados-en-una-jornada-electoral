import { runLua } from './lua.js';
import { verificationKeys } from './keys.js';
export function createSweeper(redis, config, metrics) {
  const k = verificationKeys(config.prefix);
  let busy = false;
  async function sweep() {
    if (busy) return;
    busy = true;
    try {
      const [returned, ghosts] = await runLua(redis, 'sweep', [k.active, k.claims, k.events, k.presence, k.people], [k.lockPrefix, 100]);
      metrics?.increment('requeued', returned); metrics?.increment('ghostsPruned', ghosts);
      return { returned, ghosts };
    } finally { busy = false; }
  }
  const timer = setInterval(() => sweep().catch(() => metrics?.increment('redisErrors')), config.sweepIntervalMs);
  timer.unref();
  return { sweep, stop() { clearInterval(timer); } };
}
