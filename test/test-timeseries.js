'use strict';

// Tests for the time-series helper (lib/timeseries.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/timeseries'));

// Days as UTC epoch-ms.
const day = (s) => Date.parse(`${s}T00:00:00Z`);
const at = (s, h = 12) => Date.parse(`${s}T${String(h).padStart(2, '0')}:00:00Z`);

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, amount REAL, kind TEXT)');
  const rows = [
    // 2026-01-01: two events (10 + 5 = 15)
    [at('2026-01-01', 9), 10, 'a'], [at('2026-01-01', 15), 5, 'b'],
    // 2026-01-02: one event (20)
    [at('2026-01-02', 10), 20, 'a'],
    // 2026-01-03: GAP (no events)
    // 2026-01-04: two events (3 + 7 = 10)
    [at('2026-01-04', 8), 3, 'a'], [at('2026-01-04', 20), 7, 'a']
  ];
  const ins = db.prepare('INSERT INTO events (ts, amount, kind) VALUES (?, ?, ?)');
  for (const r of rows) ins.run(...r);
  return db;
}

test('bucket(day) + agg(sum) totals per day', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts', value: 'amount' }).bucket('day').agg('sum').all();
  assert.deepEqual(rows, [
    { bucket: '2026-01-01', value: 15 },
    { bucket: '2026-01-02', value: 20 },
    { bucket: '2026-01-04', value: 10 }
  ]);
  db.close();
});

test('agg(count) ignores the value column', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts' }).bucket('day').agg('count').all();
  assert.deepEqual(rows.map((r) => r.value), [2, 1, 2]);
  db.close();
});

test('min/max/avg aggregates', () => {
  const db = seeded();
  const max = db.timeseries('events', { time: 'ts', value: 'amount' }).bucket('day').agg('max').all();
  assert.deepEqual(max.map((r) => r.value), [10, 20, 7]);
  const avg = db.timeseries('events', { time: 'ts', value: 'amount' }).bucket('day').agg('avg').all();
  assert.equal(avg[0].value, 7.5);
  db.close();
});

test('a non-count aggregate without a value column throws', () => {
  const db = seeded();
  assert.throws(() => db.timeseries('events', { time: 'ts' }).bucket('day').agg('sum').all(), /needs a "value" column/);
  db.close();
});

test('fillGaps inserts the missing day with the fill value', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts', value: 'amount' })
    .bucket('day').agg('sum')
    .fillGaps({ from: day('2026-01-01'), to: day('2026-01-04'), fill: 0 })
    .all();
  assert.deepEqual(rows, [
    { bucket: '2026-01-01', value: 15 },
    { bucket: '2026-01-02', value: 20 },
    { bucket: '2026-01-03', value: 0 },   // the gap
    { bucket: '2026-01-04', value: 10 }
  ]);
  db.close();
});

test('movingAverage computes over current + previous buckets', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts', value: 'amount' })
    .bucket('day').agg('sum')
    .fillGaps({ from: day('2026-01-01'), to: day('2026-01-04') })
    .movingAverage(2, { as: 'ma' })
    .all();
  // values: 15, 20, 0, 10 → ma2: 15, 17.5, 10, 5
  assert.deepEqual(rows.map((r) => r.ma), [15, 17.5, 10, 5]);
  db.close();
});

test('rate gives consecutive deltas; the first is null', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts', value: 'amount' })
    .bucket('day').agg('sum')
    .fillGaps({ from: day('2026-01-01'), to: day('2026-01-04') })
    .rate({ as: 'delta' })
    .all();
  // values 15,20,0,10 → deltas null,5,-20,10
  assert.equal(rows[0].delta, null);
  assert.deepEqual(rows.slice(1).map((r) => r.delta), [5, -20, 10]);
  db.close();
});

test('where filter and orderBy/limit', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts', value: 'amount', where: 'kind = ?', params: ['a'] })
    .bucket('day').agg('sum').orderBy('desc').limit(2).all();
  // kind 'a': 01-01→10, 01-02→20, 01-04→10 ; desc, limit 2
  assert.deepEqual(rows, [{ bucket: '2026-01-04', value: 10 }, { bucket: '2026-01-02', value: 20 }]);
  db.close();
});

test('hour bucketing', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts' }).bucket('hour').agg('count').all();
  // distinct hours: 01-01 09, 01-01 15, 01-02 10, 01-04 08, 01-04 20 → 5 buckets of 1
  assert.equal(rows.length, 5);
  assert.ok(rows.every((r) => r.value === 1));
  assert.equal(rows[0].bucket, '2026-01-01 09');
  db.close();
});

test('month bucketing', () => {
  const db = seeded();
  const rows = db.timeseries('events', { time: 'ts', value: 'amount' }).bucket('month').agg('sum').all();
  assert.deepEqual(rows, [{ bucket: '2026-01', value: 45 }]);
  db.close();
});

test('unit: iso timestamps', () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE m (id INTEGER PRIMARY KEY, t TEXT, v REAL)');
  const ins = db.prepare('INSERT INTO m (t, v) VALUES (?, ?)');
  ins.run('2026-03-01T05:00:00Z', 4);
  ins.run('2026-03-01T23:00:00Z', 6);
  ins.run('2026-03-02T01:00:00Z', 9);
  const rows = db.timeseries('m', { time: 't', value: 'v', unit: 'iso' }).bucket('day').agg('sum').all();
  assert.deepEqual(rows, [{ bucket: '2026-03-01', value: 10 }, { bucket: '2026-03-02', value: 9 }]);
  db.close();
});

test('toSQL returns sql and params; validates interval and inputs', () => {
  const db = seeded();
  const q = db.timeseries('events', { time: 'ts', value: 'amount', where: 'kind = ?', params: ['a'] })
    .bucket('day').agg('sum').fillGaps({ from: day('2026-01-01'), to: day('2026-01-02'), fill: 0 });
  const { sql, params } = q.toSQL();
  assert.match(sql, /WITH RECURSIVE _series/);
  // params: alignedFrom, alignedTo, fill, then where param
  assert.equal(params.length, 4);
  assert.equal(params[2], 0);
  assert.equal(params[3], 'a');
  assert.throws(() => db.timeseries('events', { time: 'ts' }).bucket('bogus'), /interval/);
  assert.throws(() => db.timeseries('nope', { time: 'ts' }), /No such table/);
  assert.throws(() => db.timeseries('events', { time: 'ts' }).all(), /Call bucket/);
  db.close();
});
