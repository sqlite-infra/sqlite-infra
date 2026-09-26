'use strict';

// Tests for the JSON-column helpers (lib/json.js): db.json(table, column).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/json'));

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)');
  db.prepare('INSERT INTO docs (id, data) VALUES (?, ?)').run(1, JSON.stringify({
    name: 'Ada', age: 36, active: true, tags: ['a', 'b'], meta: { role: 'admin', level: 3 }
  }));
  return db;
}

test('get returns the whole document and values at a path, typed', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  assert.deepEqual(j.get(1), { name: 'Ada', age: 36, active: true, tags: ['a', 'b'], meta: { role: 'admin', level: 3 } });
  assert.equal(j.get(1, '$.name'), 'Ada');
  assert.equal(j.get(1, '$.age'), 36);
  assert.equal(j.get(1, '$.active'), true);
  assert.deepEqual(j.get(1, '$.tags'), ['a', 'b']);
  assert.equal(j.get(1, '$.tags[1]'), 'b');
  assert.deepEqual(j.get(1, '$.meta'), { role: 'admin', level: 3 });
  assert.equal(j.get(1, '$.missing'), undefined);   // unresolved path
  assert.equal(j.get(99), undefined);               // missing row
  db.close();
});

test('set writes scalars and objects, creating structure', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  j.set(1, '$.age', 37);
  assert.equal(j.get(1, '$.age'), 37);
  j.set(1, '$.meta.level', 5);
  assert.equal(j.get(1, '$.meta.level'), 5);
  j.set(1, '$.address', { city: 'Lovelace', zip: '00001' });
  assert.deepEqual(j.get(1, '$.address'), { city: 'Lovelace', zip: '00001' });
  // set into a NULL column creates the document
  db.prepare('INSERT INTO docs (id, data) VALUES (2, NULL)').run();
  j.set(2, '$.hello', 'world');
  assert.deepEqual(j.get(2), { hello: 'world' });
  db.close();
});

test('merge patches keys; remove drops a path', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  j.merge(1, { age: 40, city: 'X' });
  assert.equal(j.get(1, '$.age'), 40);
  assert.equal(j.get(1, '$.city'), 'X');
  assert.equal(j.get(1, '$.name'), 'Ada');   // untouched
  j.remove(1, '$.city');
  assert.equal(j.get(1, '$.city'), undefined);
  db.close();
});

test('where matches rows by an extracted scalar and by a nested value', () => {
  const db = seeded();
  db.prepare('INSERT INTO docs (id, data) VALUES (?, ?)').run(2, JSON.stringify({ name: 'Bob', meta: { role: 'user' } }));
  const j = db.json('docs', 'data');
  assert.deepEqual(j.where('$.name', 'Ada').map((r) => r.id), [1]);
  assert.deepEqual(j.where('$.meta.role', 'user').map((r) => r.id), [2]);
  assert.deepEqual(j.where('$.name', 'nobody'), []);
  db.close();
});

test('each enumerates entries with parsed values', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  const tags = j.each(1, '$.tags');
  assert.deepEqual(tags.map((e) => e.value), ['a', 'b']);
  const meta = j.each(1, '$.meta');
  assert.deepEqual(meta.map((e) => e.key).sort(), ['level', 'role']);
  const top = j.each(1);
  assert.ok(top.find((e) => e.key === 'meta' && typeof e.value === 'object'));
  db.close();
});

test('valid reflects json_valid', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  assert.equal(j.valid(1), true);
  db.prepare('INSERT INTO docs (id, data) VALUES (3, ?)').run('not json {');
  assert.equal(j.valid(3), false);
  db.close();
});

test('values are parameterized (injection-safe)', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  const nasty = "x'); DROP TABLE docs;--";
  j.set(1, '$.note', nasty);
  assert.equal(j.get(1, '$.note'), nasty);
  assert.equal(db.prepare('SELECT count(*) FROM docs').pluck().get(), 1);   // table survives
  db.close();
});

test('validation errors', () => {
  const db = seeded();
  const j = db.json('docs', 'data');
  assert.throws(() => j.get(1, 'name'), /JSON path/);        // path must start with $
  assert.throws(() => j.set(1, 'bad', 1), /JSON path/);
  assert.throws(() => db.json('nope', 'data'), /No such table/);
  assert.throws(() => db.json('docs', 'ghost'), /No such column/);
  db.close();
});
