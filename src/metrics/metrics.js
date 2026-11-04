import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
export function createMetrics(instanceId) {
  const histogram = monitorEventLoopDelay({ resolution: 20 });
  histogram.enable();
  const counters = { claimed: 0, approved: 0, rejected: 0, released: 0, requeued: 0, ghostsPruned: 0, redisErrors: 0, wsMessages: 0, httpRequests: 0 };
  const connections = { auditor: 0, ops: 0, juror: 0, public: 0 };
  const gauges = { lagging: 0 };
  const latencies = [];
  let previous = performance.eventLoopUtilization();
  return {
    increment(name, value = 1) { counters[name] = (counters[name] || 0) + value; },
    connection(name, delta) { connections[name] = Math.max(0, (connections[name] || 0) + delta); },
    gauge(name, delta) { gauges[name] = Math.max(0, (gauges[name] || 0) + delta); },
    latency(ms) { if (Number.isFinite(ms) && ms >= 0) { latencies.push(ms); if (latencies.length > 1000) latencies.shift(); } },
    sample() {
      const utilization = performance.eventLoopUtilization(previous);
      previous = performance.eventLoopUtilization();
      const delay = { p95Ms: histogram.count ? histogram.percentile(95) / 1e6 : 0, maxMs: histogram.count ? histogram.max / 1e6 : 0 };
      histogram.reset();
      const sorted = latencies.splice(0).sort((a, b) => a - b);
      return { instanceId, timestamp: Date.now(), uptimeSeconds: process.uptime(), rssBytes: process.memoryUsage().rss, eventLoop: { ...delay, utilization: utilization.utilization }, publishLatencyP95Ms: sorted.length ? sorted[Math.ceil(sorted.length * .95) - 1] : null, counters: { ...counters }, connections: { ...connections }, gauges: { ...gauges } };
    },
    stop() { histogram.disable(); },
  };
}

// Compatibilidad con los puntos de instrumentación de ingesta y fan-out.
// La fachada no abre Redis ni activa un histograma al importar el módulo.
let processMetrics;
export function useProcessMetrics(collector) {
  processMetrics = collector;
  return () => { if (processMetrics === collector) processMetrics = undefined; };
}
export const metrics = {
  actaApplied() { processMetrics?.increment('actasApplied'); },
  conn(kind, delta) { processMetrics?.connection(kind, delta); },
  naive(count) { processMetrics?.increment('naiveMessages', count); },
  emitted(count) { processMetrics?.increment('emittedMessages', count); },
  publishLatency(ms) { processMetrics?.latency(ms); },
  lagging(delta) { processMetrics?.gauge('lagging', delta); },
  droppedSlow() { processMetrics?.increment('slowClientsDropped'); },
};
