'use strict';

// Tests for the CTE (WITH) query builder (lib/cte.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
const cte = require('../lib/cte');
Object.assign(Database.prototype, { with: cte.with, withRecursive: cte.withRecursive });

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE nums (n INTEGER PRIMARY KEY);
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER, age INTEGER);
  `);
  const ins = db.prepare('INSERT INTO nums (n) VALUES (?)');
  for (let i = 1; i <= 10; i++) ins.run(i);
  db.prepare('INSERT INTO users VALUES (?,?,?,?)').run(1, 'ada', 1, 40);
  db.prepare('INSERT INTO users VALUES (?,?,?,?)').run(2, 'bo', 0, 17);
  db.prepare('INSERT INTO users VALUES (?,?,?,?)').run(3, 'cy', 1, 22);
  return db;
}

test('single CTE then .from() selects from it, params ordered body-then-main', () => {
  const db = seeded();
  const q = db.with('big', { sql: 'SELECT n FROM nums WHERE n > ?', params: [2] })
    .from('big').where('n < ?', 8).orderBy('n');
  const { sql, params } = q.toSQL();
  assert.match(sql, /^WITH "big" AS \(SELECT n FROM nums WHERE n > \?\) SELECT \* FROM "big"/);
  assert.deepEqual(params, [2, 8]);
  assert.deepEqual(q.all().map((r) => r.n), [3, 4, 5, 6, 7]);
  db.close();
});

test('multiple CTEs compose with params in declaration order', () => {
  const db = seeded();
  const q = db.with('lo', { sql: 'SELECT n FROM nums WHERE n >= ?', params: [3] })
    .with('hi', { sql: 'SELECT n FROM nums WHERE n <= ?', params: [7] })
    .query('SELECT lo.n FROM lo JOIN hi ON lo.n = hi.n ORDER BY lo.n');
  const { sql, params } = q.toSQL();
  assert.match(sql, /^WITH "lo" AS \(.*\), "hi" AS \(.*\) SELECT/);
  assert.deepEqual(params, [3, 7]);
  assert.deepEqual(q.all().map((r) => r.n), [3, 4, 5, 6, 7]);
  db.close();
});

test('recursive CTE with a column list produces the sequence', () => {
  const db = seeded();
  const q = db.withRecursive('cnt', 'SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5', ['x'])
    .query('SELECT x FROM cnt');
  assert.match(q.toSQL().sql, /^WITH RECURSIVE "cnt" \("x"\) AS \(/);
  assert.deepEqual(q.all().map((r) => r.x), [1, 2, 3, 4, 5]);
  db.close();
});

test('a CTE whose source is a db.from(...) builder', () => {
  const db = seeded();
  const q = db.with('act', db.from('users').select('id', 'name').where({ active: 1 }))
    .from('act').orderBy('id');
  const rows = q.all();
  assert.deepEqual(rows.map((r) => r.name), ['ada', 'cy']);
  // the builder's own param (active = 1) is carried through
  assert.deepEqual(q.toSQL().params, [1]);
  db.close();
});

test('.from() chain supports where/orderBy/limit/get/count over the CTE', () => {
  const db = seeded();
  const base = () => db.with('act', db.from('users').where({ active: 1 }));
  assert.equal(base().from('act').where('age > ?', 30).get().name, 'ada');
  assert.equal(base().from('act').count(), 2);
  assert.deepEqual(base().from('act').orderBy('age').limit(1).all().map((r) => r.name), ['cy']);
  db.close();
});

test('pluck works on both .from() and .query() results', () => {
  const db = seeded();
  assert.deepEqual(
    db.with('big', { sql: 'SELECT n FROM nums WHERE n > ?', params: [8] }).from('big').select('n').pluck().orderBy('n').all(),
    [9, 10]
  );
  assert.deepEqual(
    db.withRecursive('cnt', 'SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 3', ['x']).query('SELECT x FROM cnt').pluck().all(),
    [1, 2, 3]
  );
  db.close();
});

test('validation: missing name, empty columns, bad source', () => {
  const db = seeded();
  assert.throws(() => db.with('', 'SELECT 1'), /non-empty string/);
  assert.throws(() => db.with('c', 'SELECT 1', []), /non-empty array/);
  assert.throws(() => db.with('c', 123), /SQL string/);
  db.close();
});
