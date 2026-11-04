// Recepción de actas (RF-1, RF-2, RF-3): validación y regla contable.
import { ALL_OPTIONS, actaIdForMesa, getMesa } from '../geo.js';

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
