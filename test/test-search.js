'use strict';

// Tests for the full-text search shim (lib/search.js): a JS-tokenized inverted
// index in a shadow table with a lazy dirty-queue, since this node:sqlite has no
// FTS5.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/search'));

function seeded() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)`);
  const ins = db.prepare('INSERT INTO docs (id, title, body) VALUES (?, ?, ?)');
  ins.run(1, 'SQLite tutorial', 'Learn about databases and indexes');
  ins.run(2, 'Database design', 'Normalization and index strategy for a database');
  ins.run(3, 'Cooking pasta', 'Boil water, add salt, cook the pasta');
  return db;
}

test('createSearchIndex + search finds rows by term, with a _score', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  const hits = db.search('docs', 'database');
  assert.deepEqual(hits.map((r) => r.id).sort(), [1, 2]);
  assert.ok(hits.every((r) => typeof r._score === 'number' && r._score >= 1));
  assert.equal(db.search('docs', 'pasta')[0].id, 3);
  assert.deepEqual(db.search('docs', 'nonexistentword'), []);
  db.close();
});

test('prefix matching on the last token', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  assert.deepEqual(db.search('docs', 'index').map((r) => r.id).sort(), [1, 2]);   // matches "indexes"/"index"
  assert.deepEqual(db.search('docs', 'norm').map((r) => r.id), [2]);              // prefix of "normalization"
  assert.deepEqual(db.search('docs', 'norm', { prefix: false }), []);            // exact: no bare "norm"
  db.close();
});

test('ranking orders rows by number of distinct query terms matched', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  // doc 2 has both "database" and "index"; doc 1 has both too ("databases"→? no).
  const hits = db.search('docs', 'database index', { match: 'any' });
  assert.equal(hits[0].id, 2, 'the row containing both terms ranks first');
  assert.ok(hits[0]._score >= hits[hits.length - 1]._score);
  db.close();
});

test('match "all" vs "any"', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  // "salt water" both only in doc 3.
  assert.deepEqual(db.search('docs', 'salt water', { match: 'all' }).map((r) => r.id), [3]);
  // "pasta database": all → none; any → docs 3,1,2 (containing either)
  assert.deepEqual(db.search('docs', 'pasta database', { match: 'all' }), []);
  assert.deepEqual(db.search('docs', 'pasta database', { match: 'any' }).map((r) => r.id).sort(), [1, 2, 3]);
  db.close();
});

test('INSERT/UPDATE/DELETE after indexing are reflected via the dirty queue', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  db.exec("INSERT INTO docs (id, title, body) VALUES (4, 'Zebra facts', 'stripes and savanna')");
  assert.deepEqual(db.search('docs', 'zebra').map((r) => r.id), [4]);
  db.exec("UPDATE docs SET body = 'penguins live in antarctica' WHERE id = 4");
  assert.deepEqual(db.search('docs', 'stripes'), [], 'old body term gone after update');
  assert.deepEqual(db.search('docs', 'penguins').map((r) => r.id), [4], 'new body term indexed');
  assert.deepEqual(db.search('docs', 'zebra').map((r) => r.id), [4], 'unchanged title term still matches');
  db.exec('DELETE FROM docs WHERE id = 4');
  assert.deepEqual(db.search('docs', 'penguins'), [], 'deleted row drops out');
  assert.deepEqual(db.search('docs', 'zebra'), [], 'deleted row gone from all columns');
  db.close();
});

test('reindex rebuilds from the base table', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title']);
  // Insert bypassing... triggers still fire, so force a stale state by dropping
  // the index rows directly then reindexing.
  db.exec(`DELETE FROM _better_sqlite_search_docs`);
  assert.deepEqual(db.search('docs', 'sqlite'), [], 'index emptied');
  db.reindex('docs');
  assert.deepEqual(db.search('docs', 'sqlite').map((r) => r.id), [1]);
  db.close();
});

test('multi-column indexing and a column filter', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  assert.deepEqual(db.search('docs', 'tutorial').map((r) => r.id), [1]);           // in title
  assert.deepEqual(db.search('docs', 'tutorial', { columns: ['body'] }), []);      // not in body
  assert.deepEqual(db.search('docs', 'tutorial', { columns: ['title'] }).map((r) => r.id), [1]);
  db.close();
});

test('minToken and stopWords', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['body'], { minToken: 4, stopWords: ['database', 'databases'] });
  assert.deepEqual(db.search('docs', 'and'), [], 'short token dropped');
  assert.deepEqual(db.search('docs', 'database'), [], 'stop word not indexed');
  assert.deepEqual(db.search('docs', 'normalization').map((r) => r.id), [2]);
  db.close();
});

test('shadow tables use the internal prefix and are removed by dropSearchIndex', () => {
  const db = seeded();
  db.createSearchIndex('docs', ['title', 'body']);
  // Internal prefix → excluded from db.tables()
  assert.deepEqual(db.tables(), ['docs']);
  const shadowCount = () => db.prepare("SELECT count(*) FROM sqlite_master WHERE name LIKE '_better_sqlite_search_docs%'").pluck().get();
  assert.ok(shadowCount() > 0);
  db.dropSearchIndex('docs');
  assert.equal(shadowCount(), 0, 'all shadow tables, indexes and triggers gone');
  // base table intact
  assert.equal(db.prepare('SELECT count(*) FROM docs').pluck().get(), 3);
  db.close();
});

test('search without an index throws', () => {
  const db = seeded();
  assert.throws(() => db.search('docs', 'x'), /No search index for table: docs/);
  assert.throws(() => db.createSearchIndex('nope', ['a']), /No such table: nope/);
  assert.throws(() => db.createSearchIndex('docs', ['ghost']), /No such column/);
  db.close();
});
