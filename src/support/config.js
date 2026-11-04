function integer(env, key, fallback, min = 1) {
  const value = Number(env[key] ?? fallback);
  if (!Number.isSafeInteger(value) || value < min) throw new Error(`${key} inválido`);
  return value;
}
export function readConfig(env = process.env) {
  const auditors = JSON.parse(env.AUDITOR_TOKENS || '{}');
  if (!auditors || typeof auditors !== 'object' || Array.isArray(auditors)) throw new Error('AUDITOR_TOKENS debe ser un objeto');
  for (const [token, identity] of Object.entries(auditors)) {
    if (!token || !identity?.id || !identity?.name) throw new Error('Credencial de auditor inválida');
  }
  return {
    port: integer(env, 'PORT', 3000), instanceId: env.INSTANCE_ID || `local-${process.pid}`,
    redisUrl: env.REDIS_URL || 'redis://127.0.0.1:6379', prefix: env.REDIS_PREFIX || '',
    auditors, opsToken: env.OPS_TOKEN || '', approveModule: env.APPROVE_MODULE || '',
    lockTtlMs: integer(env, 'LOCK_TTL_MS', 30000, 1000), presenceTtlMs: integer(env, 'PRESENCE_TTL_MS', 20000, 1000),
    sweepIntervalMs: integer(env, 'SWEEP_INTERVAL_MS', 2000), metricsIntervalMs: integer(env, 'METRICS_INTERVAL_MS', 2000),
    metricsTtlMs: integer(env, 'METRICS_TTL_MS', 10000), expectedInstances: integer(env, 'EXPECTED_INSTANCES', 8),
    idleMs: integer(env, 'AUDITOR_IDLE_MS', 300000, 1000),
  };
}
