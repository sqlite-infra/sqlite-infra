'use strict';

// Tests for the model/repository (data-mapper) layer, lib/model.js.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/model'));

function makeDb() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE users (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT UNIQUE,
    prefs      TEXT,
    active     INTEGER,
    seen_at    TEXT,
    created_at INTEGER,
    updated_at INTEGER
  )`);
  return db;
}

function userModel(db, extra = {}) {
  return db.model('users', Object.assign({
    columns: { id: 'integer', name: 'text', email: 'text', prefs: 'json', active: 'boolean', seen_at: 'date' },
    timestamps: true
  }, extra));
}

test('model() validates its input', () => {
  const db = makeDb();
  assert.throws(() => db.model(''), /non-empty/);
  assert.throws(() => db.model('ghost'), /No such table: ghost/);
  assert.throws(() => db.model('users', { columns: { x: 'nope' } }), /Unknown column type/);
  const m = db.model('users');
  assert.equal(m.table, 'users');
  assert.equal(m.primaryKey, 'id');
  db.close();
});

test('insert() returns the row with its generated id and decodes types', () => {
  const db = makeDb();
  const Users = userModel(db);
  const row = Users.insert({ name: 'Ada', email: 'ada@x', prefs: { theme: 'dark' }, active: true, seen_at: new Date('2020-01-02T03:04:05.000Z') });
  assert.equal(typeof row.id, 'number');
  assert.equal(row.name, 'Ada');
  assert.deepEqual(row.prefs, { theme: 'dark' });      // json decoded to object
  assert.equal(row.active, true);                       // boolean decoded
  assert.ok(row.seen_at instanceof Date);               // date decoded
  assert.equal(row.seen_at.toISOString(), '2020-01-02T03:04:05.000Z');
  db.close();
});

test('typed columns round-trip through storage as scalars', () => {
  const db = makeDb();
  const Users = userModel(db);
  const { id } = Users.insert({ name: 'B', active: false, prefs: { a: [1, 2, 3] } });
  // Stored as scalars, not JS objects.
  const raw = db.prepare('SELECT active, prefs FROM users WHERE id = ?').get(id);
  assert.equal(raw.active, 0);
  assert.equal(raw.prefs, '{"a":[1,2,3]}');
  const back = Users.find(id);
  assert.equal(back.active, false);
  assert.deepEqual(back.prefs, { a: [1, 2, 3] });
  db.close();
});

test('timestamps are set on insert and bumped on update', async () => {
  const db = makeDb();
  const Users = userModel(db);
  const created = Users.insert({ name: 'T' });
  assert.equal(typeof created.created_at, 'number');
  assert.equal(created.updated_at, created.created_at);
  await new Promise((r) => setTimeout(r, 2));
  const updated = Users.update(created.id, { name: 'T2' });
  assert.equal(updated.created_at, created.created_at, 'created_at is preserved');
  assert.ok(updated.updated_at >= created.updated_at, 'updated_at is bumped');
  db.close();
});

test('validate() rejects, by throw and by returned message', () => {
  const db = makeDb();
  const throwing = userModel(db, { validate: (r) => { if (!r.name) throw new Error('name required'); } });
  assert.throws(() => throwing.insert({ email: 'x' }), /name required/);

  const returning = userModel(db, { validate: (r) => (r.name === 'bad' ? 'bad name' : undefined) });
  assert.throws(() => returning.insert({ name: 'bad' }), (e) => e.name === 'ValidationError' && /bad name/.test(e.message));
  assert.ok(returning.insert({ name: 'ok' }).id);

  const arr = userModel(db, { validate: () => ['e1', 'e2'] });
  assert.throws(() => arr.insert({ name: 'x' }), /e1; e2/);
  db.close();
});

test('hooks fire in order around writes', () => {
  const db = makeDb();
  const events = [];
  const Users = userModel(db, {
    hooks: {
      beforeInsert: (r) => events.push(['beforeInsert', r.name]),
      afterInsert: (r) => events.push(['afterInsert', r.id != null]),
      beforeUpdate: (p, pk) => events.push(['beforeUpdate', pk]),
      afterUpdate: (r) => events.push(['afterUpdate', r.name]),
      beforeDelete: (pk) => events.push(['beforeDelete', pk]),
      afterDelete: (pk) => events.push(['afterDelete', pk])
    }
  });
  const row = Users.insert({ name: 'H' });
  Users.update(row.id, { name: 'H2' });
  Users.delete(row.id);
  assert.deepEqual(events, [
    ['beforeInsert', 'H'], ['afterInsert', true],
    ['beforeUpdate', row.id], ['afterUpdate', 'H2'],
    ['beforeDelete', row.id], ['afterDelete', row.id]
  ]);
  db.close();
});

test('find / findBy / first / all / where with IN and null', () => {
  const db = makeDb();
  const Users = userModel(db);
  const a = Users.insert({ name: 'A', email: 'a@x', active: true });
  const b = Users.insert({ name: 'B', email: 'b@x', active: false });
  const c = Users.insert({ name: 'C', email: null, active: true });

  assert.equal(Users.find(a.id).name, 'A');
  assert.equal(Users.find(999), undefined);
  assert.equal(Users.findBy({ email: 'b@x' }).name, 'B');
  assert.equal(Users.first({ active: true }).name, 'A');
  assert.equal(Users.all().length, 3);
  assert.deepEqual(Users.where({ active: true }).map((r) => r.name).sort(), ['A', 'C']);
  assert.deepEqual(Users.where({ name: ['A', 'B'] }).map((r) => r.name).sort(), ['A', 'B']);
  assert.deepEqual(Users.where({ email: null }).map((r) => r.name), ['C']);
  assert.deepEqual(Users.where({ name: [] }), []);   // IN () matches nothing
  db.close();
});

test('update / updateWhere', () => {
  const db = makeDb();
  const Users = userModel(db);
  const a = Users.insert({ name: 'A', active: true });
  Users.insert({ name: 'B', active: true });
  assert.equal(Users.update(a.id, { name: 'AA' }).name, 'AA');
  assert.equal(Users.update(999, { name: 'x' }), undefined);
  assert.equal(Users.update(a.id, {}).id, a.id);   // empty patch is a no-op read
  assert.equal(Users.updateWhere({ active: true }, { active: false }), 2);
  assert.equal(Users.count({ active: false }), 2);
  db.close();
});

test('upsert inserts then updates on conflict', () => {
  const db = makeDb();
  const Users = userModel(db);
  const first = Users.upsert({ id: 1, name: 'One', active: true });
  assert.equal(first.name, 'One');
  const second = Users.upsert({ id: 1, name: 'Uno', active: false });
  assert.equal(second.name, 'Uno');
  assert.equal(second.active, false);
  assert.equal(Users.count(), 1);

  // conflict on a non-primary unique column
  const byEmail = db.model('users', { columns: { active: 'boolean' } });
  byEmail.insert({ name: 'E', email: 'e@x' });
  const up = byEmail.upsert({ name: 'E2', email: 'e@x' }, { onConflict: 'email' });
  assert.equal(up.name, 'E2');
  db.close();
});

test('delete / deleteWhere', () => {
  const db = makeDb();
  const Users = userModel(db);
  const a = Users.insert({ name: 'A' });
  Users.insert({ name: 'B' });
  Users.insert({ name: 'B' });
  assert.equal(Users.delete(a.id), true);
  assert.equal(Users.delete(a.id), false);
  assert.equal(Users.deleteWhere({ name: 'B' }), 2);
  assert.equal(Users.count(), 0);
  db.close();
});

test('count / exists', () => {
  const db = makeDb();
  const Users = userModel(db);
  Users.insert({ name: 'A', active: true });
  Users.insert({ name: 'B', active: false });
  assert.equal(Users.count(), 2);
  assert.equal(Users.count({ active: true }), 1);
  assert.equal(Users.exists({ name: 'A' }), true);
  assert.equal(Users.exists({ name: 'Z' }), false);
  db.close();
});

test('insertMany and paginate', () => {
  const db = makeDb();
  const Users = userModel(db);
  const rows = [];
  for (let i = 1; i <= 25; i++) rows.push({ name: `u${i}`, active: i % 2 === 0 });
  assert.deepEqual(Users.insertMany(rows), { inserted: 25 });

  const p1 = Users.paginate({ page: 1, perPage: 10, orderBy: 'id' });
  assert.equal(p1.total, 25);
  assert.equal(p1.pages, 3);
  assert.equal(p1.rows.length, 10);
  assert.equal(p1.rows[0].name, 'u1');

  const p3 = Users.paginate({ page: 3, perPage: 10, orderBy: [{ column: 'id', direction: 'desc' }] });
  assert.equal(p3.rows.length, 5);
  assert.equal(p3.rows[0].name, 'u5');   // descending 25..1, last page holds ids 5..1
  assert.equal(p3.rows[4].name, 'u1');

  const filtered = Users.paginate({ where: { active: true }, perPage: 100 });
  assert.equal(filtered.total, 12);
  db.close();
});
