'use strict';

// Tests for the additive schema-diff tool (lib/diff.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/diff'));

function base() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
  return db;
}

test('emits CREATE TABLE for a table missing here', () => {
  const db = base();
  const target = new Database(':memory:');
  target.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
  target.exec('CREATE TABLE orgs (id INTEGER PRIMARY KEY, title TEXT)');
  const diff = db.schemaDiff(target);
  assert.deepEqual(diff.summary.tablesToAdd, ['orgs']);
  assert.ok(diff.statements.some((s) => /CREATE TABLE .*orgs/.test(s)));
  assert.equal(diff.additive, true);
  db.close(); target.close();
});

test('emits ALTER TABLE ADD COLUMN for a missing column (array target)', () => {
  const db = base();
  const diff = db.schemaDiff([
    'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, active INTEGER NOT NULL DEFAULT 1)'
  ]);
  assert.deepEqual(diff.summary.columnsToAdd.map((c) => c.column).sort(), ['active', 'email']);
  assert.ok(diff.statements.some((s) => /ADD COLUMN "email"/.test(s)));
  assert.ok(diff.statements.some((s) => /ADD COLUMN "active" INTEGER NOT NULL DEFAULT 1/.test(s)));
  assert.equal(diff.additive, true);
  db.close();
});

test('emits CREATE INDEX for a missing index (string target)', () => {
  const db = base();
  const diff = db.schemaDiff('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL); CREATE INDEX idx_users_name ON users(name)');
  assert.deepEqual(diff.summary.indexesToAdd, ['idx_users_name']);
  assert.ok(diff.statements.some((s) => /CREATE INDEX .*idx_users_name/.test(s)));
  db.close();
});

test('{ apply: true } reconciles the schema', () => {
  const db = base();
  const target = new Database(':memory:');
  target.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)');
  target.exec('CREATE TABLE orgs (id INTEGER PRIMARY KEY, title TEXT)');
  target.exec('CREATE INDEX idx_users_email ON users(email)');
  const diff = db.schemaDiff(target, { apply: true });
  assert.ok(diff.statements.length >= 3);
  assert.ok(db.tables().includes('orgs'));
  assert.ok(db.columns('users').some((c) => c.name === 'email'));
  assert.ok(db.indexes('users').some((i) => i.name === 'idx_users_email'));
  // Re-diffing after apply is empty.
  assert.deepEqual(db.schemaDiff(target).statements, []);
  db.close(); target.close();
});

test('flags an incompatible column change and stays additive:false', () => {
  const db = base();   // users.name is TEXT NOT NULL
  const diff = db.schemaDiff(['CREATE TABLE users (id INTEGER PRIMARY KEY, name INTEGER)']);
  assert.equal(diff.additive, false);
  assert.equal(diff.summary.incompatible.length, 1);
  assert.equal(diff.summary.incompatible[0].column, 'name');
  db.close();
});

test('records extras but never drops them; identical schema is empty', () => {
  const db = base();
  db.exec('CREATE TABLE extra (id INTEGER PRIMARY KEY)');
  const diff = db.schemaDiff(['CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)']);
  assert.deepEqual(diff.statements, []);
  assert.ok(diff.summary.extra.tables.includes('extra'));

  const same = base();
  const identical = new Database(':memory:');
  identical.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
  assert.deepEqual(same.schemaDiff(identical).statements, []);
  assert.equal(same.schemaDiff(identical).additive, true);
  db.close(); same.close(); identical.close();
});

test('rejects a bad target', () => {
  const db = base();
  assert.throws(() => db.schemaDiff(42), TypeError);
  db.close();
});
