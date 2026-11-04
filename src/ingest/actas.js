// Recepción de actas (RF-1, RF-2, RF-3): validación, regla contable y aplicación atómica.
import { redis, K } from '../redis.js';
import { config } from '../config.js';
import { ALL_OPTIONS, actaIdForMesa, getMesa } from '../geo.js';
import { metrics } from '../metrics/metrics.js';

// Mensajes inequívocos para el jurado, uno por cada resultado posible.
const MESSAGES = {
  APPLIED: 'Acta registrada y sumada al preconteo.',
  CORRECTED: 'Corrección registrada: se revirtió la versión anterior y se aplicó la nueva.',
  HELD: 'Acta recibida. No cuadra la regla contable: pasó a verificación humana.',
  DUPLICATE: 'Esta acta ya estaba registrada (reenvío detectado). No se sumó dos veces.',
  DUPLICATE_VERSION: 'Esta versión del acta ya estaba registrada. No se sumó dos veces.',
  STALE: 'Ya existe una versión más reciente de esta acta. Se ignoró esta versión antigua.',
  REJECTED: 'El acta tiene errores de formato y no fue registrada.',
  RATE_LIMITED: 'Demasiados envíos en poco tiempo. Se reintentará automáticamente.',
  UNAVAILABLE: 'El registro central no responde. El acta NO ha quedado registrada todavía: sigue en la cola de tu dispositivo y se reenviará sola.',
};

// Resultados definitivos: el cliente del jurado puede sacar el acta de su cola local.
export const FINAL_STATUSES = new Set(['APPLIED', 'CORRECTED', 'HELD', 'DUPLICATE', 'DUPLICATE_VERSION', 'STALE', 'REJECTED']);

export function validateActa(raw) {
  const errors = [];
  if (!raw || typeof raw !== 'object') return { errors: ['El mensaje no es un objeto JSON'] };
  const mesa = getMesa(Number(raw.mesa));
  if (!mesa) errors.push('Mesa inexistente');
  if (mesa && raw.acta !== actaIdForMesa(mesa.mesa)) errors.push('El código de acta no corresponde a la mesa');
  if (mesa && (Number(raw.puesto) !== mesa.puesto || Number(raw.municipio) !== mesa.municipio)) {
    errors.push('Puesto o municipio no corresponden a la mesa');
  }
  if (!Number.isInteger(raw.version) || raw.version < 1) errors.push('Versión inválida');
  if (typeof raw.idem !== 'string' || raw.idem.length < 8 || raw.idem.length > 64) errors.push('Clave de idempotencia inválida');
  if (typeof raw.jurado !== 'string' || !raw.jurado) errors.push('Jurado requerido');
  const votos = {};
  if (!raw.votos || typeof raw.votos !== 'object') {
    errors.push('Votos requeridos');
  } else {
    for (const opt of ALL_OPTIONS) {
      const n = raw.votos[opt];
      if (!Number.isInteger(n) || n < 0 || n > 2000) errors.push(`Valor inválido para ${opt}`);
      else votos[opt] = n;
    }
  }
  if (!Number.isInteger(raw.sufragantes) || raw.sufragantes < 0) errors.push('Sufragantes inválido');
  return { errors, mesa, votos };
}

// Regla contable: suma de votos (incluye blanco, nulo y no marcado) = sufragantes.
export function checkAccounting(votos, sufragantes) {
  const total = Object.values(votos).reduce((a, b) => a + b, 0);
  return { ok: total === sufragantes, diff: total - sufragantes };
}

function buildKeys(acta, mesa) {
  return [
    K.idem(acta.idem), K.acta(acta.acta), K.totMesa(mesa.mesa),
    K.totPuesto(mesa.puesto), K.totMun(mesa.municipio), K.totDep,
    K.infPuesto(mesa.puesto), K.infMun(mesa.municipio), K.infDep,
    K.dirty, K.queue, K.held(acta.acta), K.corrections,
    K.lock(acta.acta), K.claims, K.winStats,
  ];
}

export async function receiveActa(raw) {
  const { errors, mesa, votos } = validateActa(raw);
  if (errors.length) return reply('REJECTED', { acta: raw?.acta, errors });

  const accounting = checkAccounting(votos, raw.sufragantes);
  const mode = accounting.ok ? 'apply' : 'hold';
  // Prioridad de verificación: mayor descuadre y correcciones primero.
  const priority = Math.abs(accounting.diff) * 10 + (raw.version > 1 ? 50 : 0) + 1;
  const motivo = accounting.ok ? '' : `Descuadre de ${accounting.diff > 0 ? '+' : ''}${accounting.diff} votos frente a sufragantes`;

  try {
    const [status, body] = await redis.applyActa(
      ...buildKeys(raw, mesa),
      raw.acta, raw.version, JSON.stringify(votos), raw.sufragantes, raw.jurado,
      mesa.mesa, mesa.puesto, mesa.municipio, config.idemTtlSec, mode, Date.now(),
      raw.hash_foto || '', priority, '', motivo,
    );
    const data = JSON.parse(body);
    if (status === 'APPLIED' || status === 'CORRECTED') {
      metrics.actaApplied();
      redis.incr(K.actasPerMinute(Math.floor(Date.now() / 60000))).catch(() => {});
      redis.expire(K.actasPerMinute(Math.floor(Date.now() / 60000)), 7200).catch(() => {});
    }
    // En un DUPLICATE devolvemos la respuesta ORIGINAL guardada con la clave.
    if (status === 'DUPLICATE') return reply('DUPLICATE', { acta: raw.acta, original: data });
    return reply(status, data);
  } catch (err) {
    console.error('[actas] fallo al aplicar', err.message);
    return reply('UNAVAILABLE', { acta: raw.acta });
  }
}

function reply(status, data = {}) {
  return { type: 'ack', status, message: MESSAGES[status] || status, final: FINAL_STATUSES.has(status), ...data };
}
