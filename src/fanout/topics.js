// Registro de suscripciones por tópico (RF-5).
//
// Un cliente público mira una "vista": siempre el departamento y, opcionalmente, un
// municipio y un puesto. Los clientes con la misma vista forman un GRUPO, y cada grupo
// recibe un único mensaje por ventana con el delta combinado de sus tópicos.
//
//   groups:     Map<groupKey, { topics: string[], clients: Set<Client> }>
//   topicIndex: Map<topic, Set<groupKey>>
//
// Costos: alta O(1), baja O(1), recorrido de un tópico O(grupos del tópico + clientes
// de esos grupos). Nunca se recorren todas las conexiones de la instancia.

export function parseView(view) {
  const topics = ['dep'];
  const parts = String(view || '').split('/').filter(Boolean);
  for (const p of parts) {
    const m = /^(mun|puesto):(\d{1,5})$/.exec(p);
    if (m && !topics.includes(p)) topics.push(p);
  }
  return { key: topics.join('|'), topics };
}

export class TopicRegistry {
  constructor() {
    this.groups = new Map();
    this.topicIndex = new Map();
    this.size = 0;
  }

  add(client) {
    const { key, topics } = client.view;
    let g = this.groups.get(key);
    if (!g) {
      g = { key, topics, clients: new Set() };
      this.groups.set(key, g);
      for (const t of topics) {
        if (!this.topicIndex.has(t)) this.topicIndex.set(t, new Set());
        this.topicIndex.get(t).add(key);
      }
    }
    g.clients.add(client);
    this.size++;
  }

  remove(client) {
    const g = this.groups.get(client.view.key);
    if (!g || !g.clients.delete(client)) return;
    this.size--;
    if (g.clients.size === 0) {
      this.groups.delete(g.key);
      for (const t of g.topics) {
        const s = this.topicIndex.get(t);
        s?.delete(g.key);
        if (s && s.size === 0) this.topicIndex.delete(t);
      }
    }
  }

  // Grupos afectados por un conjunto de tópicos que cambiaron en la ventana.
  groupsFor(changedTopics) {
    const keys = new Set();
    for (const t of changedTopics) {
      const s = this.topicIndex.get(t);
      if (s) for (const k of s) keys.add(k);
    }
    return [...keys].map((k) => this.groups.get(k)).filter(Boolean);
  }

  *allClients() {
    for (const g of this.groups.values()) yield* g.clients;
  }
}

export const registry = new TopicRegistry();
