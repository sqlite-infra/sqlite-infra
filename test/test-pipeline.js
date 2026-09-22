'use strict';

// Tests for the streaming ETL pipeline (lib/pipeline.js): sources (array,
// { query }, { file }, iterables), lazy transforms and terminal operations,
// against a real database and real temp files.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/pipeline'));

const tmp = (ext) => path.join(os.tmpdir(), `better-sqlite-pipeline-${process.pid}-${Math.random().toString(16).slice(2)}${ext}`);

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE raw (id INTEGER PRIMARY KEY, amount REAL, tag TEXT);
    CREATE TABLE clean (id INTEGER PRIMARY KEY, cents INTEGER, tag TEXT);
  `);
  db.exec("INSERT INTO raw (id, amount, tag) VALUES (1, 1.5, 'a'), (2, -3, 'b'), (3, 4.25, 'a')");
  return db;
}

test('array source -> map/filter -> into() with each mode', async () => {
  const db = seeded();
  const rows = [
    { id: 1, cents: 100, tag: 'x' },
    { id: 2, cents: 200, tag: 'y' },
    { id: 3, cents: 300, tag: 'x' }
  ];
  const r = await db.pipeline(rows).filter((x) => x.tag === 'x').map((x) => ({ ...x, cents: x.cents + 1 })).into('clean');
  assert.deepEqual(r, { inserted: 2 });
  assert.deepEqual(db.prepare('SELECT id, cents FROM clean ORDER BY id').all(), [{ id: 1, cents: 101 }, { id: 3, cents: 301 }]);

  // replace overwrites, ignore skips conflicts.
  await db.pipeline([{ id: 1, cents: 999, tag: 'x' }]).into('clean', { mode: 'replace' });
  assert.equal(db.prepare('SELECT cents FROM clean WHERE id = 1').pluck().get(), 999);
  const ig = await db.pipeline([{ id: 1, cents: 5, tag: 'x' }, { id: 7, cents: 7, tag: 'z' }]).into('clean', { mode: 'ignore' });
  assert.deepEqual(ig, { inserted: 1 });
  assert.equal(db.prepare('SELECT cents FROM clean WHERE id = 1').pluck().get(), 999, 'ignored, not overwritten');

  await assert.rejects(db.pipeline([{ id: 9 }]).into('ghost'), /No such table: ghost/);
  await assert.rejects(db.pipeline([{ id: 9 }]).into('clean', { mode: 'bogus' }), /"mode"/);
  db.close();
});

test('{ query } source -> transform -> toArray', async () => {
  const db = seeded();
  const out = await db.pipeline({ query: 'SELECT * FROM raw WHERE amount > ?', params: [0] })
    .map((r) => ({ id: r.id, cents: Math.round(r.amount * 100) }))
    .toArray();
  assert.deepEqual(out, [{ id: 1, cents: 150 }, { id: 3, cents: 425 }]);
  db.close();
});

test('a Statement source works too', async () => {
  const db = seeded();
  const stmt = db.prepare('SELECT id FROM raw ORDER BY id');
  const ids = await db.pipeline(stmt).map((r) => r.id).toArray();
  assert.deepEqual(ids, [1, 2, 3]);
  db.close();
});

test('CSV file -> into(table), with quoted fields, commas, quotes and newlines', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, note TEXT)');
  const file = tmp('.csv');
  fs.writeFileSync(file,
    'id,name,note\n' +
    '1,"Doe, John","line1\nline2"\n' +
    '2,"She said ""hi""",plain\n' +
    '3,simple,\n');
  const r = await db.pipeline({ file }).map((row) => ({ id: Number(row.id), name: row.name, note: row.note })).into('t');
  assert.deepEqual(r, { inserted: 3 });
  const rows = db.prepare('SELECT * FROM t ORDER BY id').all();
  assert.equal(rows[0].name, 'Doe, John');
  assert.equal(rows[0].note, 'line1\nline2');
  assert.equal(rows[1].name, 'She said "hi"');
  assert.equal(rows[2].note, '');
  fs.rmSync(file, { force: true });
  db.close();
});

test('table via { query } -> toFile NDJSON, then read back', async () => {
  const db = seeded();
  const file = tmp('.ndjson');
  const res = await db.pipeline({ query: 'SELECT id, tag FROM raw ORDER BY id' }).toFile(file);
  assert.equal(res.rows, 3);
  const back = await db.pipeline({ file }).toArray();
  assert.deepEqual(back, [{ id: 1, tag: 'a' }, { id: 2, tag: 'b' }, { id: 3, tag: 'a' }]);
  fs.rmSync(file, { force: true });
  db.close();
});

test('toFile CSV round-trips through a CSV read', async () => {
  const db = new Database(':memory:');
  const file = tmp('.csv');
  await db.pipeline([{ a: 'x,y', b: 'he said "hi"' }, { a: 'plain', b: 'multi\nline' }]).toFile(file);
  const back = await db.pipeline({ file }).toArray();
  assert.deepEqual(back, [{ a: 'x,y', b: 'he said "hi"' }, { a: 'plain', b: 'multi\nline' }]);
  fs.rmSync(file, { force: true });
  db.close();
});

test('batch(n) groups items; flatMap flattens', async () => {
  const db = new Database(':memory:');
  const grouped = await db.pipeline([1, 2, 3, 4, 5]).batch(2).toArray();
  assert.deepEqual(grouped, [[1, 2], [3, 4], [5]]);
  const flat = await db.pipeline([1, 2, 3]).flatMap((n) => [n, n * 10]).toArray();
  assert.deepEqual(flat, [1, 10, 2, 20, 3, 30]);
  assert.throws(() => db.pipeline([]).batch(0), /positive integer/);
  db.close();
});

test('take/drop bound the stream', async () => {
  const db = new Database(':memory:');
  assert.deepEqual(await db.pipeline([1, 2, 3, 4, 5]).take(2).toArray(), [1, 2]);
  assert.deepEqual(await db.pipeline([1, 2, 3, 4, 5]).drop(3).toArray(), [4, 5]);
  assert.deepEqual(await db.pipeline([1, 2, 3]).take(0).toArray(), []);
  db.close();
});

test('reduce, count, forEach and tap', async () => {
  const db = new Database(':memory:');
  assert.equal(await db.pipeline([1, 2, 3, 4]).reduce((a, b) => a + b, 0), 10);
  assert.equal(await db.pipeline([1, 2, 3, 4]).filter((n) => n % 2 === 0).count(), 2);
  const seen = [];
  await db.pipeline([1, 2, 3]).tap((n) => seen.push(n)).forEach(() => {});
  assert.deepEqual(seen, [1, 2, 3]);
  db.close();
});

test('async transform functions are awaited', async () => {
  const db = new Database(':memory:');
  const out = await db.pipeline([1, 2, 3])
    .map(async (n) => { await new Promise((r) => setTimeout(r, 1)); return n * 2; })
    .filter(async (n) => n > 2)
    .toArray();
  assert.deepEqual(out, [4, 6]);
  db.close();
});

test('large input commits in batches without buffering everything', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE big (id INTEGER PRIMARY KEY, v INTEGER)');
  function* gen() { for (let i = 1; i <= 2000; i++) yield { id: i, v: i * 2 }; }
  const r = await db.pipeline(gen()).into('big', { batchSize: 250 });
  assert.deepEqual(r, { inserted: 2000 });
  assert.equal(db.prepare('SELECT count(*) AS n FROM big').pluck().get(), 2000);
  assert.equal(db.prepare('SELECT v FROM big WHERE id = 2000').pluck().get(), 4000);
  db.close();
});

test('into() honours explicit columns and ignores surplus keys', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (a INTEGER, b INTEGER)');
  const r = await db.pipeline([{ a: 1, b: 2, extra: 'x' }]).into('t', { columns: ['a', 'b'] });
  assert.deepEqual(r, { inserted: 1 });
  assert.deepEqual(db.prepare('SELECT a, b FROM t').get(), { a: 1, b: 2 });
  db.close();
});
