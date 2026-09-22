'use strict';

// Tests for lib/scope.js — db.scoped(): soft-delete filtering and named scopes.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/scope'));

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      status TEXT NOT NULL,
      org_id INTEGER,
      deleted_at INTEGER
    );
    INSERT INTO users (id, name, status, org_id) VALUES
      (1, 'a', 'active',   1),
      (2, 'b', 'inactive', 1),
      (3, 'c', 'active',   2),
      (4, 'd', 'active',   2);
  `);
  return db;
}

function users(db) {
  return db.scoped('users', {
    softDelete: 'deleted_at',
    scopes: {
      active: { status: 'active' },
      inOrg: (q, org) => q.where({ org_id: org }),
      named: { where: 'name = ?', params: ['a'] }
    }
  });
}

test('default reads exclude soft-deleted rows; withDeleted/onlyDeleted reveal them', () => {
  const db = seeded();
  const u = users(db);
  assert.equal(u.count(), 4);
  assert.equal(u.softDelete(2), 1);
  assert.equal(u.count(), 3);
  assert.deepEqual(u.all().map((r) => r.id), [1, 3, 4]);
  assert.equal(u.withDeleted().count(), 4);
  assert.deepEqual(u.onlyDeleted().all().map((r) => r.id), [2]);
  db.close();
});

test('restore brings a row back; hardDelete removes it permanently', () => {
  const db = seeded();
  const u = users(db);
  u.softDelete(2);
  assert.equal(u.restore(2), 1);
  assert.equal(u.count(), 4);
  assert.equal(u.hardDelete(2), 1);
  assert.equal(u.withDeleted().count(), 3);
  db.close();
});

test('re-softDelete does not re-count an already-deleted row', () => {
  const db = seeded();
  const u = users(db);
  assert.equal(u.softDelete(2), 1);
  assert.equal(u.softDelete(2), 0, 'already soft-deleted, nothing changed');
  db.close();
});

test('named scopes: criteria, function-with-args, and fragment', () => {
  const db = seeded();
  const u = users(db);
  assert.deepEqual(u.scope('active').pluck('id').all(), [1, 3, 4]);
  assert.deepEqual(u.scope('inOrg', 2).pluck('id').all(), [3, 4]);
  assert.deepEqual(u.scope('named').pluck('id').all(), [1]);
  db.close();
});

test('where + scope combine with AND, and honour soft-delete', () => {
  const db = seeded();
  const u = users(db);
  u.softDelete(3);
  assert.deepEqual(u.scope('active').where({ org_id: 2 }).pluck('id').all(), [4], 'excludes soft-deleted 3');
  assert.deepEqual(u.scope('active').where({ org_id: 2 }).withDeleted().pluck('id').all(), [3, 4]);
  db.close();
});

test('orderBy, limit, offset, first, get', () => {
  const db = seeded();
  const u = users(db);
  assert.deepEqual(u.orderBy('id', 'desc').pluck('id').all(), [4, 3, 2, 1]);
  assert.deepEqual(u.orderBy('id').limit(2).pluck('id').all(), [1, 2]);
  assert.deepEqual(u.orderBy('id').limit(2).offset(2).pluck('id').all(), [3, 4]);
  assert.deepEqual(u.orderBy('id').offset(3).pluck('id').all(), [4], 'offset without limit');
  assert.equal(u.orderBy('id').first().id, 1);
  assert.equal(u.get(3).name, 'c');
  u.softDelete(3);
  assert.equal(u.get(3), undefined, 'get respects the soft-delete filter');
  assert.ok(u.withDeleted().get(3), 'withDeleted lifts it');
  db.close();
});

test('the base scoped object is reusable and immutable across chains', () => {
  const db = seeded();
  const u = users(db);
  const a = u.scope('active').where({ org_id: 1 });
  const b = u.scope('inOrg', 2);
  assert.deepEqual(a.pluck('id').all(), [1]);
  assert.deepEqual(b.pluck('id').all(), [3, 4]);
  assert.equal(u.count(), 4, 'base query unchanged by the branches');
  db.close();
});

test('softDelete without a configured column throws', () => {
  const db = seeded();
  const plain = db.scoped('users');
  assert.throws(() => plain.softDelete(1), /needs a "softDelete" column/);
  assert.throws(() => db.scoped('ghost'), /No such table or view/);
  assert.throws(() => plain.scope('nope'), /No such scope: nope/);
  db.close();
});
