'use strict';

// Tests for the analytics/aggregation builder (lib/analytics.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/analytics'));

// A day in ms, and a fixed base so time buckets are deterministic.
const DAY = 86400000;
const BASE = Date.UTC(2026, 0, 1); // 2026-01-01T00:00:00Z

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, category TEXT, amount REAL, region TEXT)');
  const rows = [
    [BASE + 1 * 3600e3, 'a', 10, 'us'],
    [BASE + 2 * 3600e3, 'b', 20, 'us'],
    [BASE + 3 * 3600e3, 'a', 30, 'eu'],
    [BASE + DAY + 1 * 3600e3, 'a', 40, 'eu'],
    [BASE + DAY + 2 * 3600e3, 'b', 50, 'us'],
    [BASE + DAY + 3 * 3600e3, 'c', 60, null]
  ];
  const ins = db.prepare('INSERT INTO events (ts, category, amount, region) VALUES (?, ?, ?, ?)');
  for (const r of rows) ins.run(...r);
  return db;
}

test('agg with count/sum/avg/min/max and count-distinct', () => {
  const db = seeded();
  const row = db.analytics('events').agg({
    n: 'count',
    total: { fn: 'sum', column: 'amount' },
    mean: { fn: 'avg', column: 'amount' },
    lo: { fn: 'min', column: 'amount' },
    hi: { fn: 'max', column: 'amount' },
    cats: { fn: 'count', column: 'category', distinct: true }
  }).get();
  assert.equal(row.n, 6);
  assert.equal(row.total, 210);
  assert.equal(row.mean, 35);
  assert.equal(row.lo, 10);
  assert.equal(row.hi, 60);
  assert.equal(row.cats, 3);
  db.close();
});

test('groupBy with aggregation, ordered', () => {
  const db = seeded();
  const rows = db.analytics('events')
    .groupBy('category')
    .agg({ n: 'count', total: { fn: 'sum', column: 'amount' } })
    .orderBy('category')
    .all();
  assert.deepEqual(rows, [
    { category: 'a', n: 3, total: 80 },
    { category: 'b', n: 2, total: 70 },
    { category: 'c', n: 1, total: 60 }
  ]);
  db.close();
});

test('where filters and object/IN/IS NULL conditions', () => {
  const db = seeded();
  assert.equal(db.analytics('events').where({ region: 'us' }).agg({ n: 'count' }).get().n, 3);
  assert.equal(db.analytics('events').where({ region: ['us', 'eu'] }).agg({ n: 'count' }).get().n, 5);
  assert.equal(db.analytics('events').where({ region: null }).agg({ n: 'count' }).get().n, 1);
  assert.equal(db.analytics('events').where('amount > ?', 30).agg({ n: 'count' }).get().n, 3);
  db.close();
});

test('having filters grouped rows', () => {
  const db = seeded();
  const rows = db.analytics('events')
    .groupBy('category')
    .agg({ n: 'count' })
    .having('n >= ?', 2)
    .orderBy('category')
    .all();
  assert.deepEqual(rows.map((r) => r.category), ['a', 'b']);
  db.close();
});

test('timeBucket by day over epoch-ms', () => {
  const db = seeded();
  const rows = db.analytics('events')
    .timeBucket('ts', 'day')
    .agg({ n: 'count', total: { fn: 'sum', column: 'amount' } })
    .orderBy('bucket')
    .all();
  assert.equal(rows.length, 2);
  assert.equal(rows[0].bucket, '2026-01-01');
  assert.equal(rows[0].n, 3);
  assert.equal(rows[0].total, 60);
  assert.equal(rows[1].bucket, '2026-01-02');
  assert.equal(rows[1].total, 150);
  db.close();
});

test('pivot: category values become summed-amount columns', () => {
  const db = seeded();
  const rows = db.analytics('events')
    .pivot('region', 'category', { fn: 'sum', column: 'amount' })
    .all();
  // region 'eu': a=70; 'us': a=10,b=70; null: c=60
  const byRegion = Object.fromEntries(rows.map((r) => [r.region === null ? 'NULL' : r.region, r]));
  assert.equal(byRegion.eu.a, 70);
  assert.equal(byRegion.us.a, 10);
  assert.equal(byRegion.us.b, 70);
  assert.equal(byRegion.NULL.c, 60);
  db.close();
});

test('window column: row_number over amount', () => {
  const db = seeded();
  const rows = db.analytics('events')
    .window('row_number() OVER (ORDER BY amount DESC)', { as: 'rank' })
    .window('amount', { as: 'amount' })
    .orderBy('amount', 'desc')
    .all();
  assert.equal(rows[0].rank, 1);
  assert.equal(rows[0].amount, 60);
  assert.equal(rows[5].rank, 6);
  db.close();
});

test('limit and toSQL shape', () => {
  const db = seeded();
  const b = db.analytics('events').groupBy('category').agg({ n: 'count' }).orderBy('category').limit(2);
  const { sql, params } = b.toSQL();
  assert.match(sql, /SELECT .*count\(\*\) AS "n".*FROM "events" GROUP BY "category" ORDER BY "category" ASC LIMIT 2/s);
  assert.deepEqual(params, []);
  assert.equal(b.all().length, 2);
  db.close();
});

test('validation errors', () => {
  const db = seeded();
  assert.throws(() => db.analytics('nope'), /No such table or view/);
  assert.throws(() => db.analytics('events').agg({ x: 'median' }), /Unsupported aggregate/);
  assert.throws(() => db.analytics('events').agg({ x: { fn: 'sum' } }), /requires a "column"/);
  assert.throws(() => db.analytics('events').timeBucket('ts', 'decade'), /Unsupported interval/);
  assert.throws(() => db.analytics('events').orderBy('ts', 'sideways'), /asc.*desc/);
  db.close();
});
