import { Redis } from 'ioredis';

export const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const ttlMs = Number(process.env.CACHE_TTL_MS ?? 1000);

// Loads currently running in this process, by key.
const inflight = new Map<string, Promise<string>>();

// Cache-aside: return the cached JSON string, or run `load`, cache it with a
// short TTL, and return it. We store the serialized string so a cache hit
// skips both the DB query and JSON.stringify.
//
// Single-flight: when a hot key expires, hundreds of requests miss at once.
// Instead of each querying the DB (a stampede), they all await the one load
// already in flight in this process.
export async function cached(key: string, load: () => Promise<unknown>): Promise<string> {
  const hit = await redis.get(key);
  if (hit) return hit;

  let pending = inflight.get(key);
  if (!pending) {
    pending = (async () => {
      const json = JSON.stringify(await load());
      await redis.set(key, json, 'PX', ttlMs);
      return json;
    })().finally(() => inflight.delete(key));
    inflight.set(key, pending);
  }
  return pending;
}
