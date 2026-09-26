'use strict';

// Tests for lib/partition.js — date/range table partitioning with a UNION ALL
// read view.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/partition'));

const JAN = Date.UTC(2026, 0, 15);   // 2026-01-15
const FEB = Date.UTC(2026, 1, 3);    // 2026-02-03

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, kind TEXT)');
  return db;
}

test('insert routes rows to month partitions and the child tables hold them', () => {
  const db = seeded();
  const p = db.partitioned('events', { column: 'ts', by: 'date', interval: 'month' });
  p.insert({ id: 1, ts: JAN, kind: 'a' });
  p.insert({ id: 2, ts: FEB, kind: 'b' });
  p.insert({ id: 3, ts: JAN + DAYS(2), kind: 'c' });

  assert.ok(db.tableExists('events__p_2026_01'));
  assert.ok(db.tableExists('events__p_2026_02'));
  assert.equal(db.prepare('SELECT count(*) FROM events__p_2026_01').pluck().get(), 2);
  assert.equal(db.prepare('SELECT count(*) FROM events__p_2026_02').pluck().get(), 1);
  db.close();
});

function DAYS(n) { return n * 86400000; }

test('keyFor / tableFor compute keys for epoch, Date and ISO', () => {
  const db = seeded();
  const month = db.partitioned('events', { column: 'ts', interval: 'month' });
  assert.equal(month.keyFor(JAN), '2026_01');
  assert.equal(month.keyFor(new Date(FEB)), '2026_02');
  assert.equal(month.tableFor(JAN), 'events__p_2026_01');

  const day = db.partitioned('events', { column: 'ts', interval: 'day' });
  assert.equal(day.keyFor(JAN), '2026_01_15');

  const year = db.partitioned('events', { column: 'ts', interval: 'year' });
  assert.equal(year.keyFor(JAN), '2026');

  const week = db.partitioned('events', { column: 'ts', interval: 'week' });
  assert.equal(week.keyFor(Date.UTC(2026, 0, 15)), '2026_01_12');   // Monday of that week

  const iso = db.partitioned('events', { column: 'ts', interval: 'month', unit: 'iso' });
  assert.equal(iso.keyFor('2026-02-03T00:00:00Z'), '2026_02');
  db.close();
});

test('insertMany groups across partitions in one transaction', () => {
  const db = seeded();
  const p = db.partitioned('events', { column: 'ts', interval: 'month' });
  const r = p.insertMany([
    { id: 1, ts: JAN, kind: 'a' },
    { id: 2, ts: FEB, kind: 'b' },
    { id: 3, ts: FEB + DAYS(1), kind: 'c' },
    { id: 4, ts: JAN + DAYS(1), kind: 'd' }
  ]);
  assert.deepEqual(r, { inserted: 4 });
  assert.equal(db.prepare('SELECT count(*) FROM events__p_2026_01').pluck().get(), 2);
  assert.equal(db.prepare('SELECT count(*) FROM events__p_2026_02').pluck().get(), 2);
  db.close();
});

test('insertMany rolls back the whole batch on a failure', () => {
  const db = seeded();
  const p = db.partitioned('events', { column: 'ts', interval: 'month' });
  p.insert({ id: 1, ts: JAN, kind: 'x' });
  // id 1 collides -> the whole insertMany rolls back
  assert.throws(() => p.insertMany([
    { id: 2, ts: FEB, kind: 'ok' },
    { id: 1, ts: JAN, kind: 'dup' }
  ]));
  assert.equal(p.count(), 1, 'only the original row remains');
  db.close();
});

test('partitions() lists children with row counts, sorted by key', () => {
  const db = seeded();
  const p = db.partitioned('events', { column: 'ts', interval: 'month' });
  p.insertMany([
    { id: 1, ts: FEB, kind: 'b' },
    { id: 2, ts: JAN, kind: 'a' },
    { id: 3, ts: JAN, kind: 'a2' }
  ]);
  assert.deepEqual(p.partitions(), [
    { key: '2026_01', table: 'events__p_2026_01', rows: 2 },
    { key: '2026_02', table: 'events__p_2026_02', rows: 1 }
  ]);
  assert.equal(p.count(), 3);
  db.close();
});

test('view() builds a UNION ALL read view across partitions', () => {
  const db = seeded();
  const p = db.partitioned('events', { column: 'ts', interval: 'month' });
  p.insertMany([
    { id: 1, ts: JAN, kind: 'a' },
    { id: 2, ts: FEB, kind: 'b' },
    { id: 3, ts: FEB, kind: 'c' }
  ]);
  const view = p.view();
  assert.equal(view, 'events_all');
  assert.equal(db.prepare('SELECT count(*) FROM events_all').pluck().get(), 3);
  assert.deepEqual(db.prepare('SELECT kind FROM events_all ORDER BY id').pluck().all(), ['a', 'b', 'c']);
  db.close();
});

test('drop() removes one partition and refreshes the view', () => {
  const db = seeded();
  const p = db.partitioned('events', { column: 'ts', interval: 'month' });
  p.insertMany([{ id: 1, ts: JAN, kind: 'a' }, { id: 2, ts: FEB, kind: 'b' }]);
  p.drop('2026_01');
  assert.ok(!db.tableExists('events__p_2026_01'));
  assert.equal(db.prepare('SELECT count(*) FROM events_all').pluck().get(), 1);
  assert.equal(p.count(), 1);
  assert.throws(() => p.drop('2026_99'), /No such partition/);
  p.drop('2026_99', { ifExists: true });   // no throw
  db.close();
});

test('range partitioning by an integer bucket width', () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE hits (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)');
  const p = db.partitioned('hits', { column: 'n', by: 'range', interval: 100 });
  assert.equal(p.keyFor(0), '0');
  assert.equal(p.keyFor(150), '100');
  assert.equal(p.keyFor(99), '0');
  assert.equal(p.keyFor(-1), '-100');
  p.insertMany([{ id: 1, n: 5 }, { id: 2, n: 105 }, { id: 3, n: 250 }, { id: 4, n: 95 }]);
  assert.deepEqual(p.partitions().map((x) => [x.key, x.rows]), [['0', 2], ['100', 1], ['200', 1]]);
  assert.equal(db.prepare('SELECT count(*) FROM hits_all').pluck().get(), 4);
  db.close();
});

test('createSql template without an existing table', () => {
  const db = new Database(':memory:');
  const p = db.partitioned('logs', { column: 'ts', interval: 'month', createSql: '(id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, msg TEXT)' });
  p.insert({ id: 1, ts: JAN, msg: 'hi' });
  assert.ok(db.tableExists('logs__p_2026_01'));
  assert.equal(db.prepare('SELECT msg FROM logs_all').pluck().get(), 'hi');
  db.close();
});

test('validates its options', () => {
  const db = seeded();
  assert.throws(() => db.partitioned('', { column: 'ts' }), TypeError);
  assert.throws(() => db.partitioned('events', {}), /"column"/);
  assert.throws(() => db.partitioned('events', { column: 'ts', by: 'bogus' }), /"by"/);
  assert.throws(() => db.partitioned('events', { column: 'ts', interval: 'decade' }), /interval/);
  assert.throws(() => db.partitioned('events', { column: 'nope' }), /No such column/);
  assert.throws(() => db.partitioned('events', { column: 'n', by: 'range', interval: 0 }), /positive integer/);
  assert.throws(() => db.partitioned('ghost', { column: 'ts' }), /template table/);
  const p = db.partitioned('events', { column: 'ts', interval: 'month' });
  assert.throws(() => p.insert({ id: 1, kind: 'no-ts' }), /missing the partition column/);
  db.close();
});
