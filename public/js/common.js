// Utilidades compartidas por las cuatro páginas.

export const fmt = new Intl.NumberFormat('es-CO');
export const pct = (a, b) => (b ? (100 * a) / b : 0);
export const $ = (sel, root = document) => root.querySelector(sel);

let geoPromise;
export const loadGeo = () => (geoPromise ||= fetch('/api/geo').then((r) => r.json()));

// Reintento con retroceso exponencial y JITTER COMPLETO (RF-10).
// Si 50.000 clientes pierden su instancia y todos reintentan a los 1 s, 2 s, 4 s exactos,
// llegan en oleadas sincronizadas que tumban a las demás. Con jitter completo cada cliente
// espera un valor aleatorio uniforme en [0, min(tope, base·2^n)], y las reconexiones se
// reparten en el tiempo. `?nojitter=1` desactiva el jitter para la comparación.
const NO_JITTER = new URLSearchParams(location.search).has('nojitter');
export function backoff(attempt, { base = 1000, cap = 30000 } = {}) {
  const ceil = Math.min(cap, base * 2 ** attempt);
  return NO_JITTER ? ceil : Math.random() * ceil;
}

export function setConn(el, state, text) {
  el.dataset.state = state;
  el.textContent = text;
}

export const randomHex = (bytes = 8) => [...crypto.getRandomValues(new Uint8Array(bytes))].map((b) => b.toString(16).padStart(2, '0')).join('');
