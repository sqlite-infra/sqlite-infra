'use strict';

// Tests for lib/cache.js — the per-connection query-result cache.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/cache'));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test('cached() computes once, then returns the cached value', () => {
  const db = new Database(':memory:');
  let calls = 0;
  const produce = () => { calls += 1; return { n: 42 }; };
  const a = db.cached('k', 1000, produce);
  const b = db.cached('k', 1000, produce);
  assert.equal(calls, 1);
  assert.equal(a, b);          // same reference
  assert.equal(a.n, 42);
  db.close();
});

test('TTL expires an entry and the producer runs again', async () => {
  const db = new Database(':memory:');
  let calls = 0;
  const produce = () => { calls += 1; return calls; };
  assert.equal(db.cached('k', 10, produce), 1);
  assert.equal(db.cached('k', 10, produce), 1);   // still cached
  await sleep(20);
  assert.equal(db.cached('k', 10, produce), 2);   // expired → recomputed
  db.close();
});

test('ttlMs <= 0 caches until invalidated', async () => {
  const db = new Database(':memory:');
  let calls = 0;
  const produce = () => { calls += 1; return calls; };
  assert.equal(db.cached('k', 0, produce), 1);
  await sleep(15);
  assert.equal(db.cached('k', 0, produce), 1);     // no expiry
  assert.equal(db.invalidate('k'), 1);
  assert.equal(db.cached('k', 0, produce), 2);
  db.close();
});

test('invalidate() by exact key, by tag, and by predicate', () => {
  const db = new Database(':memory:');
  const p = (v) => () => v;
  db.cached('a', { ttlMs: 0, tags: ['t1'] }, p(1));
  db.cached('b', { ttlMs: 0, tags: ['t1', 't2'] }, p(2));
  db.cached('c', { ttlMs: 0, tags: ['t2'] }, p(3));
  db.cached('user:1', 0, p(4));
  db.cached('user:2', 0, p(5));

  assert.equal(db.invalidate('a'), 1);
  assert.equal(db.cacheStats().size, 4);
  assert.equal(db.invalidate({ tag: 't2' }), 2);   // b and c
  assert.equal(db.cacheStats().size, 2);
  assert.equal(db.invalidate((key) => key.startsWith('user:')), 2);
  assert.equal(db.cacheStats().size, 0);
  db.close();
});

test('invalidate() by { tags } drops entries carrying any listed tag', () => {
  const db = new Database(':memory:');
  db.cached('a', { ttlMs: 0, tags: ['x'] }, () => 1);
  db.cached('b', { ttlMs: 0, tags: ['y'] }, () => 2);
  db.cached('c', { ttlMs: 0, tags: ['z'] }, () => 3);
  assert.equal(db.invalidate({ tags: ['x', 'z'] }), 2);
  assert.equal(db.cacheStats().size, 1);
  db.close();
});

test('LRU eviction past the configured max, oldest first', () => {
  const db = new Database(':memory:');
  const opts = { ttlMs: 0, max: 3 };
  db.cached('k1', opts, () => 1);
  db.cached('k2', opts, () => 2);
  db.cached('k3', opts, () => 3);
  // Touch k1 so it becomes most-recent; k2 is now the oldest.
  db.cached('k1', opts, () => { throw new Error('should be a hit'); });
  db.cached('k4', opts, () => 4);   // overflow → evict k2
  const stats = db.cacheStats();
  assert.equal(stats.size, 3);
  assert.equal(stats.evictions, 1);
  // k2 was evicted → producer runs again; k1/k3/k4 still cached.
  let recomputed = false;
  db.cached('k2', opts, () => { recomputed = true; return 2; });
  assert.equal(recomputed, true);
  db.close();
});

test('cacheClear() empties entries but keeps statistics', () => {
  const db = new Database(':memory:');
  db.cached('a', 0, () => 1);
  db.cached('a', 0, () => 1);   // a hit
  assert.equal(db.cacheStats().size, 1);
  assert.equal(db.cacheClear(), db);
  const stats = db.cacheStats();
  assert.equal(stats.size, 0);
  assert.equal(stats.hits, 1);
  assert.equal(stats.misses, 1);
  db.close();
});

test('cacheStats() reports hits, misses and purges expired from size', async () => {
  const db = new Database(':memory:');
  db.cached('a', 10, () => 1);   // miss
  db.cached('a', 10, () => 1);   // hit
  db.cached('b', 0, () => 2);    // miss
  let s = db.cacheStats();
  assert.equal(s.hits, 1);
  assert.equal(s.misses, 2);
  assert.equal(s.size, 2);
  await sleep(20);
  s = db.cacheStats();
  assert.equal(s.size, 1, 'expired "a" purged, "b" remains');
  db.close();
});

test('caches are independent per connection', () => {
  const db1 = new Database(':memory:');
  const db2 = new Database(':memory:');
  db1.cached('k', 0, () => 'one');
  const fromDb2 = db2.cached('k', 0, () => 'two');
  assert.equal(fromDb2, 'two');
  assert.equal(db1.cached('k', 0, () => 'x'), 'one');
  assert.equal(db2.cacheStats().size, 1);
  db1.close();
  db2.close();
});

test('cached() validates its arguments', () => {
  const db = new Database(':memory:');
  assert.throws(() => db.cached(5, 0, () => 1), TypeError);
  assert.throws(() => db.cached('k', 0, 'notfn'), TypeError);
  assert.throws(() => db.cached('k', { max: 0 }, () => 1), TypeError);
  assert.throws(() => db.invalidate(5), TypeError);
  db.close();
});
