'use strict';

// Tests for the trigger-based change-tracking layer (lib/audit.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/audit'));
const dictionary = require('../lib/dictionary');

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
  return db;
}

test('trackChanges records insert, update and delete with parsed data', () => {
  const db = seeded();
  db.trackChanges('users');
  db.exec("INSERT INTO users VALUES (1, 'Ada', 30)");
  db.exec("UPDATE users SET age = 31 WHERE id = 1");
  db.exec("DELETE FROM users WHERE id = 1");

  const log = db.auditLog();               // newest-first
  assert.deepEqual(log.map((r) => r.op), ['delete', 'update', 'insert']);

  const ins = log.find((r) => r.op === 'insert');
  assert.equal(ins.old, null);
  assert.deepEqual(ins.new, { id: 1, name: 'Ada', age: 30 });
  assert.equal(ins.table, 'users');
  assert.equal(ins.rowId, 1);
  assert.equal(typeof ins.at, 'number');

  const upd = log.find((r) => r.op === 'update');
  assert.deepEqual(upd.old, { id: 1, name: 'Ada', age: 30 });
  assert.deepEqual(upd.new, { id: 1, name: 'Ada', age: 31 });

  const del = log.find((r) => r.op === 'delete');
  assert.deepEqual(del.old, { id: 1, name: 'Ada', age: 31 });
  assert.equal(del.new, null);
  db.close();
});

test('auditLog filters by table, op, since and limit', () => {
  const db = seeded();
  db.exec('CREATE TABLE orgs (id INTEGER PRIMARY KEY, title TEXT)');
  db.trackChanges('users');
  db.trackChanges('orgs');
  db.exec("INSERT INTO users VALUES (1, 'a', 1)");
  db.exec("INSERT INTO orgs VALUES (1, 'x')");
  db.exec("UPDATE users SET age = 2 WHERE id = 1");

  assert.equal(db.auditLog({ table: 'orgs' }).length, 1);
  assert.equal(db.auditLog({ op: 'update' }).length, 1);
  assert.equal(db.auditLog({ limit: 1 }).length, 1);
  const all = db.auditLog();
  const midAt = all[1].at;
  assert.ok(db.auditLog({ since: midAt }).every((r) => r.at >= midAt));
  db.close();
});

test('the audit table is internal and hidden from db.tables()', () => {
  const db = seeded();
  db.trackChanges('users');
  assert.ok(!db.tables().includes(dictionary.tables.audit));
  assert.ok(db.tableExists(dictionary.tables.audit));   // it does exist
  db.close();
});

test('events subset tracks only the chosen operations', () => {
  const db = seeded();
  db.trackChanges('users', { events: ['delete'] });
  db.exec("INSERT INTO users VALUES (1, 'a', 1)");
  db.exec("DELETE FROM users WHERE id = 1");
  const log = db.auditLog();
  assert.deepEqual(log.map((r) => r.op), ['delete']);
  db.close();
});

test('columns projection limits captured fields', () => {
  const db = seeded();
  db.trackChanges('users', { columns: ['id', 'name'] });
  db.exec("INSERT INTO users VALUES (1, 'Ada', 30)");
  assert.deepEqual(db.auditLog()[0].new, { id: 1, name: 'Ada' });
  db.close();
});

test('stopTracking removes the triggers', () => {
  const db = seeded();
  db.trackChanges('users');
  db.exec("INSERT INTO users VALUES (1, 'a', 1)");
  db.stopTracking('users');
  db.exec("INSERT INTO users VALUES (2, 'b', 2)");
  assert.equal(db.auditLog().length, 1, 'only the pre-stop insert is logged');
  // no audit triggers remain
  const trigs = db.prepare("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name LIKE '_bsq_audit_%'").pluck().get();
  assert.equal(trigs, 0);
  db.close();
});

test('clearAudit deletes rows older than a timestamp', async () => {
  const db = seeded();
  db.trackChanges('users');
  db.exec("INSERT INTO users VALUES (1, 'a', 1)");
  const cutoff = db.auditLog()[0].at + 1;
  // The audit timestamp has millisecond precision (unixepoch('subsec')), so two
  // inserts in the same millisecond would share a timestamp and both fall before
  // the cutoff. Wait past the cutoff so the second row is strictly newer.
  await new Promise((r) => setTimeout(r, 5));
  db.exec("INSERT INTO users VALUES (2, 'b', 2)");
  const removed = db.clearAudit({ before: cutoff });
  assert.equal(removed, 1);
  assert.deepEqual(db.auditLog().map((r) => r.new.id), [2]);
  db.close();
});

test('a custom "into" table keeps a separate log and independent triggers', () => {
  const db = seeded();
  db.trackChanges('users', { into: 'my_audit' });
  db.exec("INSERT INTO users VALUES (1, 'a', 1)");
  assert.equal(db.auditLog({ into: 'my_audit' }).length, 1);
  assert.equal(db.auditLog().length, 0, 'default audit table is untouched');
  db.stopTracking('users', { into: 'my_audit' });
  db.exec("INSERT INTO users VALUES (2, 'b', 2)");
  assert.equal(db.auditLog({ into: 'my_audit' }).length, 1);
  db.close();
});

test('trackChanges validates input', () => {
  const db = seeded();
  assert.throws(() => db.trackChanges('ghost'), /No such table: ghost/);
  assert.throws(() => db.trackChanges(''), TypeError);
  assert.throws(() => db.trackChanges('users', { events: ['bogus'] }), /Unknown event/);
  assert.throws(() => db.trackChanges('users', { columns: ['nope'] }), /No such column/);
  db.close();
});
