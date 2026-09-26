'use strict';

// Tests for lib/batch.js — bulk updateMany / upsertMany / deleteMany.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/batch'));

function db() {
  const d = new Database(':memory:');
  d.exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, city TEXT)`);
  return d;
}

test('updateMany applies per-row updates keyed by primary key', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name, age) VALUES (1,'a',10),(2,'b',20),(3,'c',30)");
  const r = d.updateMany('users', [
    { id: 1, age: 11, city: 'x' },
    { id: 3, age: 33, city: 'z' }
  ]);
  assert.deepEqual(r, { updated: 2 });
  assert.equal(d.prepare('SELECT age FROM users WHERE id=1').pluck().get(), 11);
  assert.equal(d.prepare('SELECT city FROM users WHERE id=3').pluck().get(), 'z');
  assert.equal(d.prepare('SELECT age FROM users WHERE id=2').pluck().get(), 20);
  d.close();
});

test('updateMany with composite custom keyColumns', () => {
  const d = new Database(':memory:');
  d.exec('CREATE TABLE m (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a,b))');
  d.exec("INSERT INTO m VALUES (1,1,'x'),(1,2,'y')");
  const r = d.updateMany('m', [{ a: 1, b: 2, v: 'Y' }], { keyColumns: ['a', 'b'] });
  assert.deepEqual(r, { updated: 1 });
  assert.equal(d.prepare('SELECT v FROM m WHERE a=1 AND b=2').pluck().get(), 'Y');
  d.close();
});

test('updateMany honours explicit columns and validates keys/columns', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name, age, city) VALUES (1,'a',10,'old')");
  d.updateMany('users', [{ id: 1, age: 99, city: 'ignored' }], { columns: ['age'] });
  assert.equal(d.prepare('SELECT age FROM users WHERE id=1').pluck().get(), 99);
  assert.equal(d.prepare('SELECT city FROM users WHERE id=1').pluck().get(), 'old', 'city untouched');
  assert.throws(() => d.updateMany('users', [{ age: 5 }]), /missing key column "id"/);
  assert.throws(() => d.updateMany('nope', [{ id: 1 }]), /No such table/);
  assert.throws(() => d.updateMany('users', [{ id: 1, nope: 1 }]), /No such column/);
  assert.deepEqual(d.updateMany('users', []), { updated: 0 });
  d.close();
});

test('updateMany rolls the whole batch back on a mid-batch failure', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name, age) VALUES (1,'a',10),(2,'b',20)");
  assert.throws(() => d.updateMany('users', [
    { id: 1, age: 111 },
    { id: 2, name: null }   // NOT NULL violation
  ]));
  assert.equal(d.prepare('SELECT age FROM users WHERE id=1').pluck().get(), 10, 'first update rolled back');
  d.close();
});

test('upsertMany inserts then updates on conflict', () => {
  const d = db();
  d.upsertMany('users', [{ id: 1, name: 'a', age: 10 }, { id: 2, name: 'b', age: 20 }]);
  assert.equal(d.prepare('SELECT count(*) FROM users').pluck().get(), 2);
  const r = d.upsertMany('users', [{ id: 1, name: 'a2', age: 11 }, { id: 3, name: 'c', age: 30 }]);
  assert.equal(r.inserted, 2, 'one update + one insert both count as changes');
  assert.equal(d.prepare('SELECT name FROM users WHERE id=1').pluck().get(), 'a2');
  assert.equal(d.prepare('SELECT count(*) FROM users').pluck().get(), 3);
  d.close();
});

test('upsertMany mode:ignore leaves existing rows and limited update columns', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name, age, city) VALUES (1,'a',10,'keep')");
  d.upsertMany('users', [{ id: 1, name: 'a2', age: 11, city: 'no' }], { mode: 'ignore' });
  assert.equal(d.prepare('SELECT name FROM users WHERE id=1').pluck().get(), 'a', 'ignored');

  d.upsertMany('users', [{ id: 1, name: 'a3', age: 12, city: 'no2' }], { update: ['age'] });
  const row = d.prepare('SELECT name, age, city FROM users WHERE id=1').get();
  assert.equal(row.age, 12, 'age updated');
  assert.equal(row.name, 'a', 'name not in update list');
  assert.equal(row.city, 'keep', 'city not in update list');
  d.close();
});

test('upsertMany on a custom conflict column', () => {
  const d = new Database(':memory:');
  d.exec('CREATE TABLE k (id INTEGER PRIMARY KEY, email TEXT UNIQUE, hits INTEGER)');
  d.exec("INSERT INTO k (email, hits) VALUES ('x@y', 1)");
  d.upsertMany('k', [{ email: 'x@y', hits: 5 }], { conflictColumns: 'email', columns: ['email', 'hits'] });
  assert.equal(d.prepare("SELECT hits FROM k WHERE email='x@y'").pluck().get(), 5);
  assert.equal(d.prepare('SELECT count(*) FROM k').pluck().get(), 1);
  d.close();
});

test('deleteMany by scalar keys, chunked past the variable limit', () => {
  const d = db();
  const rows = [];
  for (let i = 1; i <= 2000; i++) rows.push({ id: i, name: `n${i}` });
  d.insertMany('users', rows);
  const ids = [];
  for (let i = 1; i <= 1500; i++) ids.push(i);
  const r = d.deleteMany('users', ids);
  assert.deepEqual(r, { deleted: 1500 });
  assert.equal(d.prepare('SELECT count(*) FROM users').pluck().get(), 500);
  d.close();
});

test('deleteMany by criteria objects, including IS NULL', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name, age, city) VALUES (1,'a',10,'p'),(2,'b',NULL,'p'),(3,'c',30,'q')");
  const r = d.deleteMany('users', [{ city: 'p', age: null }, { name: 'c' }]);
  assert.deepEqual(r, { deleted: 2 });
  assert.deepEqual(d.prepare('SELECT id FROM users ORDER BY id').pluck().all(), [1]);
  d.close();
});

test('deleteMany with a custom keyColumn, empty input, and validation', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name, city) VALUES (1,'a','x'),(2,'b','x'),(3,'c','y')");
  assert.deepEqual(d.deleteMany('users', ['x'], { keyColumn: 'city' }), { deleted: 2 });
  assert.deepEqual(d.deleteMany('users', []), { deleted: 0 });
  assert.throws(() => d.deleteMany('users', [1], { keyColumn: 'nope' }), /No such column/);
  d.close();
});

test('deleteMany rolls back a failing criteria batch atomically', () => {
  const d = db();
  d.exec("INSERT INTO users (id, name) VALUES (1,'a'),(2,'b')");
  assert.throws(() => d.deleteMany('users', [{ name: 'a' }, { nope: 1 }]), /No such column/);
  assert.equal(d.prepare('SELECT count(*) FROM users').pluck().get(), 2, 'nothing deleted');
  d.close();
});
