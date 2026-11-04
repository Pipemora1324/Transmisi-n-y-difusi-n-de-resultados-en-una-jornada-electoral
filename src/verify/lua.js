import { readFile } from 'node:fs/promises';
const cache = new Map();
export async function runLua(redis, name, keys, args = []) {
  if (!cache.has(name)) cache.set(name, await readFile(new URL(`../lua/${name}.lua`, import.meta.url), 'utf8'));
  return redis.eval(cache.get(name), keys.length, ...keys, ...args.map(String));
}
