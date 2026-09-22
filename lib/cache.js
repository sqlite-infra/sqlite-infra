'use strict';

// A small in-process, per-connection query-result cache, mixed onto
// Database.prototype by lib/database.js. Memoize an expensive read with a TTL
// and invalidate it by key, by tag, or by predicate when the underlying data
// changes. An extension beyond the better-sqlite3 surface.
//
// The cache lives in a module-level WeakMap keyed by the Database, so it is
// isolated per connection, is dropped when the connection is garbage-collected,
// and needs no state on the connection itself. Entries are held in a Map, whose
// insertion order gives a cheap LRU: a hit re-inserts the key (most-recent), and
// an overflow evicts the oldest.
//
// Values are stored and returned by reference — the producer's own object. A
// caller must not mutate a cached value; clone it first if it needs to.

const DEFAULT_MAX = 1000;

const caches = new WeakMap();

function storeFor(db) {
  let store = caches.get(db);
  if (!store) {
    store = { map: new Map(), max: DEFAULT_MAX, hits: 0, misses: 0, evictions: 0 };
    caches.set(db, store);
  }
  return store;
}

function expired(entry, now) {
  return entry.expiresAt !== 0 && entry.expiresAt <= now;
}

// db.cached(key, ttlMs | { ttlMs, tags, max }, producer): return the cached
// value for `key`, or compute it with producer() (synchronous), cache it and
// return it. `ttlMs <= 0` caches until invalidated. `tags` groups entries for
// invalidate({ tag }). `max` overrides this connection's entry cap.
function cached(key, ttlOrOptions, producer) {
  if (typeof key !== 'string') throw new TypeError('Expected the cache key to be a string');
  if (typeof producer !== 'function') throw new TypeError('Expected the producer to be a function');

  let ttlMs = 0;
  let tags = [];
  let max;
  if (typeof ttlOrOptions === 'number') {
    ttlMs = ttlOrOptions;
  } else if (ttlOrOptions && typeof ttlOrOptions === 'object') {
    ttlMs = Number.isFinite(ttlOrOptions.ttlMs) ? ttlOrOptions.ttlMs : 0;
    tags = Array.isArray(ttlOrOptions.tags) ? ttlOrOptions.tags : [];
    if (ttlOrOptions.max !== undefined) {
      if (!Number.isInteger(ttlOrOptions.max) || ttlOrOptions.max <= 0) throw new TypeError('Expected the "max" option to be a positive integer');
      max = ttlOrOptions.max;
    }
  } else if (ttlOrOptions !== undefined) {
    throw new TypeError('Expected the second argument to be a number of ms or an options object');
  }

  const store = storeFor(this);
  if (max !== undefined) store.max = max;
  const now = Date.now();

  const hit = store.map.get(key);
  if (hit !== undefined) {
    if (!expired(hit, now)) {
      store.hits += 1;
      store.map.delete(key);   // bump to most-recent
      store.map.set(key, hit);
      return hit.value;
    }
    store.map.delete(key);     // lazily purge the expired entry
  }

  store.misses += 1;
  const value = producer();
  store.map.set(key, { value, expiresAt: ttlMs > 0 ? now + ttlMs : 0, tags });
  while (store.map.size > store.max) {
    const oldest = store.map.keys().next().value;
    store.map.delete(oldest);
    store.evictions += 1;
  }
  return value;
}

// db.invalidate(key | { tag } | { tags } | predicate): drop matching entries.
// Returns the number removed.
function invalidate(arg) {
  // Validate the argument shape before anything else, so a misuse is reported
  // even on a connection that has cached nothing yet.
  const isKey = typeof arg === 'string';
  const isPredicate = typeof arg === 'function';
  const isTagged = arg && typeof arg === 'object';
  if (!isKey && !isPredicate && !isTagged) {
    throw new TypeError('Expected a key string, a { tag } / { tags } object, or a predicate function');
  }

  const store = caches.get(this);
  if (!store) return 0;
  const map = store.map;

  if (isKey) {
    return map.delete(arg) ? 1 : 0;
  }
  if (isPredicate) {
    let n = 0;
    for (const key of [...map.keys()]) if (arg(key)) { map.delete(key); n += 1; }
    return n;
  }
  const wanted = new Set(arg.tags !== undefined ? arg.tags : arg.tag !== undefined ? [arg.tag] : []);
  if (wanted.size === 0) return 0;
  let n = 0;
  for (const [key, entry] of [...map.entries()]) {
    if (entry.tags.some((t) => wanted.has(t))) { map.delete(key); n += 1; }
  }
  return n;
}

// db.cacheClear(): empty this connection's cache (statistics are kept).
function cacheClear() {
  const store = caches.get(this);
  if (store) store.map.clear();
  return this;
}

// db.cacheStats(): { size, hits, misses, evictions }. Expired entries are
// purged first so `size` reflects live entries.
function cacheStats() {
  const store = caches.get(this);
  if (!store) return { size: 0, hits: 0, misses: 0, evictions: 0 };
  const now = Date.now();
  for (const [key, entry] of [...store.map.entries()]) if (expired(entry, now)) store.map.delete(key);
  return { size: store.map.size, hits: store.hits, misses: store.misses, evictions: store.evictions };
}

module.exports = { cached, invalidate, cacheClear, cacheStats };
