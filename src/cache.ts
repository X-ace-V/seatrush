import { Redis } from 'ioredis';

export const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const ttlMs = Number(process.env.CACHE_TTL_MS ?? 1000);

// Cache-aside: return the cached JSON string, or run `load`, cache it with a
// short TTL, and return it. We store the serialized string so a cache hit
// skips both the DB query and JSON.stringify.
export async function cached(key: string, load: () => Promise<unknown>): Promise<string> {
  const hit = await redis.get(key);
  if (hit) return hit;
  const json = JSON.stringify(await load());
  await redis.set(key, json, 'PX', ttlMs);
  return json;
}
