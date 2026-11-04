export const $ = id => document.getElementById(id);
export function element(tag, text, className) { const node = document.createElement(tag); if (text !== undefined) node.textContent = text; if (className) node.className = className; return node; }
export function backoff(attempt) { return Math.random() * Math.min(30000, 1000 * 2 ** Math.min(attempt, 5)) + 300; }
export function status(text, online) { $('connection-text').textContent = text; $('connection').dataset.online = String(online); }
export function number(value, digits = 0) { return Number(value).toLocaleString('es-CO', { maximumFractionDigits: digits }); }
