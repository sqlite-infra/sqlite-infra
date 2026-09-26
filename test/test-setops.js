'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/setops'));

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT);
    CREATE TABLE b (id INTEGER PRIMARY KEY, tag TEXT);
    INSERT INTO a (id, tag) VALUES (1, 'x'), (2, 'y'), (3, 'z');
    INSERT INTO b (id, tag) VALUES (2, 'y'), (3, 'z'), (4, 'w');
  `);
  return db;
}

test('union dedups, unionAll keeps duplicates', () => {
  const db = seeded();
  const u = db.union('SELECT id FROM a', 'SELECT id FROM b').orderBy('id').all().map((r) => r.id);
  assert.deepEqual(u, [1, 2, 3, 4]);
  const ua = db.unionAll('SELECT id FROM a', 'SELECT id FROM b').all().map((r) => r.id).sort((x, y) => x - y);
  assert.deepEqual(ua, [1, 2, 2, 3, 3, 4]);
  db.close();
});

test('intersect and except semantics', () => {
  const db = seeded();
  assert.deepEqual(db.intersect('SELECT id FROM a', 'SELECT id FROM b').orderBy('id').all().map((r) => r.id), [2, 3]);
  assert.deepEqual(db.except('SELECT id FROM a', 'SELECT id FROM b').orderBy('id').all().map((r) => r.id), [1]);
  db.close();
});

test('sources as builders, {sql,params}, and raw strings — params in order', () => {
  const db = seeded();
  const q = db.union(
    db.from('a').select('id').where({ tag: 'x' }),        // builder, param 'x'
    { sql: 'SELECT id FROM b WHERE tag = ?', params: ['w'] }, // object, param 'w'
    'SELECT id FROM a WHERE id = 3'                          // raw
  ).orderBy('id');
  const { sql, params } = q.toSQL();
  assert.equal(params.length, 2);
  assert.deepEqual(params, ['x', 'w']);
  assert.equal((sql.match(/UNION/g) || []).length, 2);
  assert.match(sql, /ORDER BY "id" ASC$/);
  assert.deepEqual(q.all().map((r) => r.id), [1, 3, 4]);
  db.close();
});

test('orderBy/limit/offset apply to the combined query', () => {
  const db = seeded();
  const rows = db.union('SELECT id FROM a', 'SELECT id FROM b').orderBy('id', 'desc').limit(2).offset(1).all();
  assert.deepEqual(rows.map((r) => r.id), [3, 2]);
  db.close();
});

test('chaining a third set operation, left to right', () => {
  const db = seeded();
  // (a ∪ b) ∩ {2,3,4}
  const rows = db.union('SELECT id FROM a', 'SELECT id FROM b')
    .intersect('SELECT 2 UNION SELECT 3 UNION SELECT 4')
    .orderBy('id').all().map((r) => r.id);
  assert.deepEqual(rows, [2, 3, 4]);
  db.close();
});

test('builder-with-params on both sides keeps param order across a chain', () => {
  const db = seeded();
  const q = db.except(
    db.from('a').select('id').where('tag != ?', 'z'),   // ids 1,2  param 'z'
    db.from('b').select('id').where({ tag: 'y' })         // id 2     param 'y'
  );
  assert.deepEqual(q.toSQL().params, ['z', 'y']);
  assert.deepEqual(q.orderBy('id').all().map((r) => r.id), [1]);
  db.close();
});

test('toSQL shape and validation', () => {
  const db = seeded();
  const { sql, params } = db.union('SELECT 1').toSQL();
  assert.equal(typeof sql, 'string');
  assert.ok(Array.isArray(params));
  assert.throws(() => db.union(), TypeError);
  assert.throws(() => db.union([]), TypeError);
  assert.throws(() => db.union('SELECT 1').orderBy('id', 'sideways'), TypeError);
  assert.throws(() => db.union('SELECT 1').limit(-1), TypeError);
  assert.throws(() => db.unionAll(null), TypeError);
  db.close();
});

test('accepts an array of sources', () => {
  const db = seeded();
  const rows = db.union(['SELECT id FROM a', 'SELECT id FROM b']).orderBy('id').all().map((r) => r.id);
  assert.deepEqual(rows, [1, 2, 3, 4]);
  db.close();
});
