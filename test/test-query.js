'use strict';

// Tests for the fluent query builder (lib/query.js). Self-contained: it mixes
// the builder onto Database.prototype the same way lib/database.js does for the
// shipped build, so this file passes whether or not the wiring has landed yet.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/query'));

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE orgs (id INTEGER PRIMARY KEY, title TEXT);
    CREATE TABLE users (
      id     INTEGER PRIMARY KEY,
      name   TEXT NOT NULL UNIQUE,
      email  TEXT,
      org_id INTEGER REFERENCES orgs(id)
    );
    INSERT INTO orgs (id, title) VALUES (1, 'Platform'), (2, 'Finance');
    INSERT INTO users (id, name, email, org_id) VALUES
      (1, 'ada',  'ada@x',  1),
      (2, 'bo',   NULL,     1),
      (3, 'cy',   'cy@x',   2),
      (4, 'dot',  'dot@x',  2);
  `);
  return db;
}

// ---- SELECT ---------------------------------------------------------------

test('from().where(object) with =, IN and IS NULL', () => {
  const db = seeded();
  assert.deepEqual(db.from('users').where({ org_id: 1 }).pluck().select('name').all(), ['ada', 'bo']);
  assert.deepEqual(db.from('users').where({ id: [1, 3] }).pluck().select('name').all(), ['ada', 'cy']);
  assert.deepEqual(db.from('users').where({ email: null }).pluck().select('name').all(), ['bo']);
  assert.deepEqual(db.from('users').where({ id: [] }).all(), []);   // empty IN
  db.close();
});

test('from().where(fragment) and andWhere / orWhere', () => {
  const db = seeded();
  assert.deepEqual(
    db.from('users').where('id > ?', 1).andWhere({ org_id: 2 }).pluck().select('name').all(),
    ['cy', 'dot']
  );
  assert.deepEqual(
    db.from('users').where({ id: 1 }).orWhere({ id: 4 }).pluck().select('name').orderBy('id').all(),
    ['ada', 'dot']
  );
  db.close();
});

test('joins, groupBy/having, orderBy/limit/offset', () => {
  const db = seeded();
  const rows = db.from('users')
    .select({ expr: 'orgs.title', as: 'org' }, { expr: 'count(*)', as: 'n' })
    .join('orgs', 'orgs.id = users.org_id')
    .groupBy('orgs.title')
    .having('count(*) > ?', 1)
    .orderBy('org', 'asc')
    .all();
  assert.deepEqual(rows, [{ org: 'Finance', n: 2 }, { org: 'Platform', n: 2 }]);

  const page = db.from('users').select('name').pluck().orderBy('id', 'desc').limit(2).offset(1).all();
  assert.deepEqual(page, ['cy', 'bo']);

  assert.throws(() => db.from('users').orderBy('id', 'sideways'), /asc.*desc/);
  db.close();
});

test('get, iterate, count and leftJoin', () => {
  const db = seeded();
  assert.deepEqual(db.from('users').where({ id: 1 }).get(), { id: 1, name: 'ada', email: 'ada@x', org_id: 1 });
  assert.equal(db.from('users').where({ id: 999 }).get(), undefined);
  const names = [];
  for (const r of db.from('users').select('name').pluck().orderBy('id').iterate()) names.push(r);
  assert.deepEqual(names, ['ada', 'bo', 'cy', 'dot']);
  assert.equal(db.from('users').count(), 4);
  assert.equal(db.from('users').where({ org_id: 2 }).count(), 2);
  // leftJoin keeps the unmatched left row (org_id NULL matches no org)
  db.exec("INSERT INTO users (id, name) VALUES (5, 'ez')");
  const left = db.from('users').select('users.name', { expr: 'orgs.title', as: 't' })
    .leftJoin('orgs', 'orgs.id = users.org_id').where({ 'users.id': 5 }).get();
  assert.equal(left.t, null);
  db.close();
});

test('toSQL exposes the compiled SQL and params', () => {
  const db = seeded();
  const { sql, params } = db.from('users').select('name').where({ org_id: 1 }).orderBy('name').limit(5).toSQL();
  assert.match(sql, /^SELECT "name" FROM "users" WHERE \("org_id" = \?\) ORDER BY "name" ASC LIMIT 5$/);
  assert.deepEqual(params, [1]);
  db.close();
});

// ---- INSERT ---------------------------------------------------------------

test('insertInto single, bulk, returning', () => {
  const db = seeded();
  const r = db.insertInto('orgs').values({ id: 3, title: 'Ops' }).run();
  assert.equal(r.changes, 1);
  assert.equal(r.lastInsertRowid, 3);

  const rows = db.insertInto('orgs').values([{ id: 4, title: 'A' }, { id: 5, title: 'B' }]).returning('id', 'title').run();
  assert.deepEqual(rows, [{ id: 4, title: 'A' }, { id: 5, title: 'B' }]);
  assert.equal(db.from('orgs').count(), 5);
  db.close();
});

test('insertInto onConflict doNothing / doUpdate', () => {
  const db = seeded();
  // id 1 already exists
  const nothing = db.insertInto('orgs').values({ id: 1, title: 'dup' }).onConflict('id').doNothing().run();
  assert.equal(nothing.changes, 0);
  assert.equal(db.from('orgs').where({ id: 1 }).pluck().select('title').get(), 'Platform');

  const updated = db.insertInto('orgs').values({ id: 1, title: 'ignored' })
    .onConflict('id').doUpdate({ title: 'Platform!' }).returning('title').run();
  assert.deepEqual(updated, [{ title: 'Platform!' }]);
  db.close();
});

// ---- UPDATE / DELETE ------------------------------------------------------

test('updateTable set object and fragment, with where and returning', () => {
  const db = seeded();
  const r = db.updateTable('users').set({ email: 'new@x' }).where({ id: 1 }).run();
  assert.equal(r.changes, 1);
  assert.equal(db.from('users').where({ id: 1 }).pluck().select('email').get(), 'new@x');

  const out = db.updateTable('orgs').set('title = title || ?', '!').where({ id: 1 }).returning('id', 'title').run();
  assert.deepEqual(out, [{ id: 1, title: 'Platform!' }]);
  db.close();
});

test('deleteFrom with where and returning', () => {
  const db = seeded();
  const out = db.deleteFrom('users').where('id > ?', 2).returning('id').run();
  assert.deepEqual(out.map((r) => r.id), [3, 4]);
  assert.equal(db.from('users').count(), 2);
  db.close();
});

// ---- safety ---------------------------------------------------------------

test('values are always parameterized (SQL injection is stored as data)', () => {
  const db = seeded();
  const evil = "x'); DROP TABLE users;--";
  db.insertInto('orgs').values({ id: 10, title: evil }).run();
  // The table still exists and the string round-trips untouched.
  assert.equal(db.from('orgs').where({ id: 10 }).pluck().select('title').get(), evil);
  assert.equal(db.from('users').count(), 4);
  // Same through where().
  assert.equal(db.from('orgs').where({ title: evil }).pluck().select('id').get(), 10);
  db.close();
});

test('builder validation', () => {
  const db = seeded();
  assert.throws(() => db.from(''), TypeError);
  assert.throws(() => db.insertInto('orgs').run(), /requires values/);
  assert.throws(() => db.insertInto('orgs').values([]), /at least one row/);
  assert.throws(() => db.updateTable('orgs').run(), /requires set/);
  assert.throws(() => db.from('users').limit(-1), TypeError);
  assert.throws(() => db.from('users').select(123).all(), /string or/);
  db.close();
});
