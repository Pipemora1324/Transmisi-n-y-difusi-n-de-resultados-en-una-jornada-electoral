// División político-electoral simulada del departamento: 1.842 puestos y 6.400 mesas.
// Se genera de forma determinista (semilla fija) para que las 8 instancias, los clientes
// y la prueba de carga tengan exactamente el mismo mapa sin consultar a nadie.

export const TOTAL_PUESTOS = 1842;
export const TOTAL_MESAS = 6400;

export const CANDIDATES = [
  { id: 'C1', name: 'Valentina Rosero', party: 'Movimiento Andino', color: '#ff3d81' },
  { id: 'C2', name: 'Martín Erazo', party: 'Alianza del Pacífico Sur', color: '#2ee6c5' },
  { id: 'C3', name: 'Lucía Benavides', party: 'Coalición Galeras', color: '#ffb020' },
];
export const SPECIAL_OPTIONS = ['BLANCO', 'NULO', 'NO_MARCADO'];
export const ALL_OPTIONS = [...CANDIDATES.map((c) => c.id), ...SPECIAL_OPTIONS];

const MUNICIPIOS = [
  ['Pasto', 30], ['Ipiales', 11], ['Tumaco', 12], ['Túquerres', 5], ['La Unión', 4],
  ['Samaniego', 4], ['Barbacoas', 4], ['Sandoná', 3], ['La Cruz', 3], ['Buesaco', 3],
  ['El Charco', 3], ['Cumbal', 3], ['Ricaurte', 2], ['Yacuanquer', 2], ['Tangua', 2],
  ['Consacá', 2],
];

// PRNG determinista (mulberry32).
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Reparte `total` en partes proporcionales a `weights`, garantizando al menos `min` por parte.
function distribute(total, weights, min, rnd) {
  const sum = weights.reduce((a, b) => a + b, 0);
  const parts = weights.map((w) => Math.max(min, Math.floor((total * w) / sum)));
  let diff = total - parts.reduce((a, b) => a + b, 0);
  while (diff !== 0) {
    const i = Math.floor(rnd() * parts.length);
    if (diff > 0) { parts[i]++; diff--; } else if (parts[i] > min) { parts[i]--; diff++; }
  }
  return parts;
}

function build() {
  const rnd = mulberry32(2026);
  const municipios = [];
  const puestos = new Map();
  const mesas = new Array(TOTAL_MESAS);

  const puestosPerMun = distribute(TOTAL_PUESTOS, MUNICIPIOS.map((m) => m[1]), 3, rnd);
  let puestoId = 1;
  const allPuestos = [];
  MUNICIPIOS.forEach(([name], idx) => {
    const mun = { id: idx + 1, name, puestos: [], mesas: 0 };
    for (let i = 0; i < puestosPerMun[idx]; i++) {
      const p = { id: puestoId, municipio: mun.id, name: `Puesto ${String(puestoId).padStart(4, '0')}`, mesas: [] };
      puestos.set(puestoId, p);
      mun.puestos.push(puestoId);
      allPuestos.push(p);
      puestoId++;
    }
    municipios.push(mun);
  });

  // Puestos urbanos tienen más mesas: peso aleatorio sesgado.
  const mesaWeights = allPuestos.map(() => 1 + Math.floor(rnd() * rnd() * 12));
  const mesasPerPuesto = distribute(TOTAL_MESAS, mesaWeights, 1, rnd);
  let mesaId = 1;
  allPuestos.forEach((p, i) => {
    for (let local = 1; local <= mesasPerPuesto[i]; local++) {
      const mesa = { mesa: mesaId, puesto: p.id, municipio: p.municipio, local };
      mesas[mesaId - 1] = mesa;
      p.mesas.push(mesaId);
      municipios[p.municipio - 1].mesas++;
      mesaId++;
    }
  });
  return { municipios, puestos, mesas };
}

export const geo = build();

export const pad = (n, w) => String(n).padStart(w, '0');

// Código de acta: E14-<puesto>-<mesa local>-<corporación>.
export function actaIdForMesa(mesaId) {
  const m = geo.mesas[mesaId - 1];
  if (!m) return null;
  return `E14-${pad(m.puesto, 3)}-${pad(m.local, 4)}-03`;
}

export function getMesa(mesaId) {
  return geo.mesas[mesaId - 1] || null;
}

// Número de mesas por tópico: lo usa el cliente para calcular el % de mesas informadas.
export function mesasInTopic(topic) {
  if (topic === 'dep') return TOTAL_MESAS;
  const [kind, id] = topic.split(':');
  if (kind === 'mun') return geo.municipios[Number(id) - 1]?.mesas ?? 0;
  if (kind === 'puesto') return geo.puestos.get(Number(id))?.mesas.length ?? 0;
  return 0;
}

// Versión compacta que se envía al navegador (GET /api/geo).
export function geoForClient() {
  return {
    totalMesas: TOTAL_MESAS,
    totalPuestos: TOTAL_PUESTOS,
    candidates: CANDIDATES,
    special: SPECIAL_OPTIONS,
    municipios: geo.municipios.map((m) => ({
      id: m.id,
      name: m.name,
      mesas: m.mesas,
      puestos: m.puestos.map((pid) => [pid, geo.puestos.get(pid).mesas.length]),
    })),
  };
}
