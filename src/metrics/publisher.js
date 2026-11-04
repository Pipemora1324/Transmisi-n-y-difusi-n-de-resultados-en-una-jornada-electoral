export function summarizeInstances(instances, expectedInstances) {
  const totals = { rssBytes: 0, auditorConnections: 0, opsConnections: 0, jurorConnections: 0, publicConnections: 0, maxInstanceP95Ms: 0, counters: {} };
  for (const instance of instances) {
    totals.rssBytes += instance.rssBytes;
    totals.auditorConnections += instance.connections.auditor || 0;
    totals.opsConnections += instance.connections.ops || 0;
    totals.jurorConnections += instance.connections.juror || 0;
    totals.publicConnections += instance.connections.public || 0;
    totals.maxInstanceP95Ms = Math.max(totals.maxInstanceP95Ms, instance.eventLoop.p95Ms);
    for (const [name, count] of Object.entries(instance.counters)) totals.counters[name] = (totals.counters[name] || 0) + count;
  }
  return { instances, expectedInstances, onlineInstances: instances.length, totals };
}
export function createMetricsPublisher(redis, config, metrics, queue) {
  const registry = config.prefix + 'verify:metric-instances';
  const key = id => config.prefix + 'metrics:' + id;
  let busy = false;
  let latest = null;
  const listeners = new Set();
  async function collect() {
    const time = await redis.time(); const now = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
    await redis.zremrangebyscore(registry, '-inf', now);
    const ids = await redis.zrangebyscore(registry, now + 1, '+inf');
    const values = ids.length ? await redis.mget(...ids.map(key)) : [];
    return { timestamp: now, redis: true, ...summarizeInstances(values.filter(Boolean).map(JSON.parse), config.expectedInstances), queue: await queue.counts() };
  }
  async function tick() {
    if (busy) return;
    busy = true;
    try {
      const sample = metrics.sample();
      const script = `local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]); redis.call('ZADD',KEYS[2],now+tonumber(ARGV[2]),ARGV[3]); return 1`;
      await redis.eval(script, 2, key(config.instanceId), registry, JSON.stringify(sample), config.metricsTtlMs, config.instanceId);
      latest = await collect();
    } catch {
      metrics.increment('redisErrors');
      latest = { timestamp: Date.now(), redis: false, expectedInstances: config.expectedInstances, instances: [], onlineInstances: 0, totals: null, queue: null };
    } finally { busy = false; }
    for (const listener of listeners) { try { listener(latest); } catch { listeners.delete(listener); } }
    return latest;
  }
  const timer = setInterval(tick, config.metricsIntervalMs);
  timer.unref();
  return { tick, collect, get latest() { return latest; }, subscribe(fn) { listeners.add(fn); if (latest) fn(latest); return () => listeners.delete(fn); }, stop() { clearInterval(timer); listeners.clear(); } };
}
