'use strict';

// Tests for lib/importer.js load(): NDJSON and CSV bulk loads from an
// ObjectStore, value decoding that reverses the sync's encoding, column
// validation before any write, insert/replace/upsert modes, and resumable
// progress.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const zlib = require('node:zlib');

const Database = require('../');
const { Importer, Sync } = Database;
const { MemoryObjectStore } = Database.interfaces;
const importer = require('../lib/importer');

function target() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, avatar BLOB, big INTEGER, score REAL)');
  return db;
}

const ndjson = (rows) => Buffer.from(rows.map((r) => JSON.stringify(r)).join('\n') + '\n');

test('validates its arguments before touching the store', async () => {
  const db = target();
  const store = new MemoryObjectStore();
  await assert.rejects(Importer.load({}, { store, key: 'k', table: 'users' }), /Expected first argument to be a Database/);
  await assert.rejects(Importer.load(db, { store: {}, key: 'k', table: 'users' }), /ObjectStore/);
  await assert.rejects(Importer.load(db, { store, key: 'k', table: 'nope' }), /No such table/);
  await assert.rejects(Importer.load(db, { store, key: 'k', table: 'users', mode: 'merge' }), /"mode"/);
  await assert.rejects(Importer.load(db, { store, key: 'k', table: 'users', batch: 0 }), /"batch"/);
  await assert.rejects(Importer.load(db, { store, key: 'k', table: 'users' }), (err) => err.code === 'ENOENT');
  db.close();
});

test('loads NDJSON, decoding base64 BLOBs and big integers, in batches with progress', async () => {
  const db = target();
  const store = new MemoryObjectStore();
  const rows = [];
  for (let i = 1; i <= 1234; i += 1) rows.push({ id: i, name: `u${i}`, avatar: Buffer.from([i & 255]).toString('base64'), big: i === 1 ? '9007199254740993' : i, score: i / 2 });
  await store.put('users.ndjson.gz', zlib.gzipSync(ndjson(rows)));

  const result = await Importer.load(db, { store, key: 'users.ndjson.gz', table: 'users', batch: 100 });
  assert.deepEqual(result, { rows: 1234, skipped: 0, lines: 1234, done: true });
  assert.equal(db.prepare('SELECT count(*) n FROM users').get().n, 1234);
  const first = db.prepare('SELECT id, name, avatar, CAST(big AS TEXT) big, score, typeof(big) t FROM users WHERE id = 1').get();
  assert.deepEqual(first, { id: 1, name: 'u1', avatar: Buffer.from([1]), big: '9007199254740993', score: 0.5, t: 'integer' });
  const progress = db.prepare(`SELECT lines, rows, done_at IS NOT NULL done FROM ${Importer.TABLE} WHERE key = ?`).get('users.ndjson.gz');
  assert.deepEqual(progress, { lines: 1234, rows: 1234, done: 1 });

  // Loading the same key again is a no-op by default, a full reload with resume: false.
  assert.deepEqual(await Importer.load(db, { store, key: 'users.ndjson.gz', table: 'users' }), { rows: 0, skipped: 1234, lines: 1234, done: true, resumed: true });
  const again = await Importer.load(db, { store, key: 'users.ndjson.gz', table: 'users', resume: false, mode: 'replace' });
  assert.equal(again.rows, 1234);
  assert.equal(db.prepare('SELECT count(*) n FROM users').get().n, 1234);
  db.close();
});

test('an unknown column, invalid JSON or a missing upsert key fails before any row is written', async () => {
  const db = target();
  const store = new MemoryObjectStore();
  await store.put('bad-col.ndjson', ndjson([{ id: 1, name: 'a' }, { id: 2, nom: 'b' }]));
  await assert.rejects(Importer.load(db, { store, key: 'bad-col.ndjson', table: 'users' }), /bad-col\.ndjson:2: unknown column "nom"/);
  assert.equal(db.prepare('SELECT count(*) n FROM users').get().n, 0, 'the batch before the bad line is not committed either');

  await store.put('bad-json.ndjson', Buffer.from('{"id":1,"name":"a"}\n{oops\n'));
  await assert.rejects(Importer.load(db, { store, key: 'bad-json.ndjson', table: 'users' }), /bad-json\.ndjson:2: invalid JSON/);

  await store.put('no-pk.ndjson', ndjson([{ name: 'x' }]));
  await assert.rejects(Importer.load(db, { store, key: 'no-pk.ndjson', table: 'users', mode: 'upsert' }), /missing primary key column id/);
  assert.equal(db.prepare('SELECT count(*) n FROM users').get().n, 0);
  db.close();
});

test('modes: insert conflicts throw, replace overwrites, upsert updates only the provided columns', async () => {
  const db = target();
  const store = new MemoryObjectStore();
  db.prepare("INSERT INTO users (id, name, score) VALUES (1, 'old', 1.5)").run();
  await store.put('one.ndjson', ndjson([{ id: 1, name: 'new' }]));

  await assert.rejects(Importer.load(db, { store, key: 'one.ndjson', table: 'users' }), (err) => err.code === 'SQLITE_CONSTRAINT_PRIMARYKEY');
  await Importer.load(db, { store, key: 'one.ndjson', table: 'users', mode: 'upsert', resume: false });
  assert.deepEqual(db.prepare('SELECT name, score FROM users WHERE id = 1').get(), { name: 'new', score: 1.5 });
  await Importer.load(db, { store, key: 'one.ndjson', table: 'users', mode: 'replace', resume: false });
  assert.deepEqual(db.prepare('SELECT name, score FROM users WHERE id = 1').get(), { name: 'new', score: null });
  db.close();
});

test('resumes at the first uncommitted line after a failure mid-file', async () => {
  const db = target();
  const store = new MemoryObjectStore();
  const rows = [];
  for (let i = 1; i <= 50; i += 1) rows.push({ id: i, name: `n${i}` });
  rows[29].name = null;   // NOT NULL violation on line 30
  await store.put('resume.ndjson', ndjson(rows));

  await assert.rejects(Importer.load(db, { store, key: 'resume.ndjson', table: 'users', batch: 10 }), (err) => err.code === 'SQLITE_CONSTRAINT_NOTNULL');
  assert.equal(db.prepare('SELECT count(*) n FROM users').get().n, 20, 'two full batches committed');

  rows[29].name = 'fixed';
  await store.put('resume.ndjson', ndjson(rows));
  const result = await Importer.load(db, { store, key: 'resume.ndjson', table: 'users', batch: 10 });
  assert.deepEqual(result, { rows: 30, skipped: 20, lines: 50, done: true });
  assert.equal(db.prepare('SELECT count(*) n FROM users').get().n, 50);
  db.close();
});

test('loads CSV with a header, quoted fields, embedded newlines and NULLs', async () => {
  const db = target();
  const store = new MemoryObjectStore();
  const csv = 'id,name,score\n1,"Smith, ""Ann""",2.5\n2,"multi\nline",\n3,plain,7\n';
  await store.put('people.csv', Buffer.from(csv));
  const result = await Importer.load(db, { store, key: 'people.csv', table: 'users' });
  assert.equal(result.rows, 3);
  assert.deepEqual(db.prepare('SELECT id, name, score FROM users ORDER BY id').all(), [
    { id: 1, name: 'Smith, "Ann"', score: 2.5 },
    { id: 2, name: 'multi\nline', score: null },
    { id: 3, name: 'plain', score: 7 }
  ]);

  await store.put('headless.csv', Buffer.from('9,nine\n'));
  await Importer.load(db, { store, key: 'headless.csv', table: 'users', columns: ['id', 'name'] });
  assert.equal(db.prepare('SELECT name FROM users WHERE id = 9').pluck().get(), 'nine');

  await store.put('broken.csv', Buffer.from('id,name\n1,"open\n'));
  await assert.rejects(Importer.load(db, { store, key: 'broken.csv', table: 'users' }), /unterminated quoted field/);

  assert.deepEqual(importer.parseCsvRecord('a,"b,c",,"d""e"'), ['a', 'b,c', '', 'd"e']);
  assert.equal(importer.parseCsvRecord('"unterminated'), null);
  db.close();
});

test('exportRows() then load() round-trips a table exactly', async () => {
  const src = new Database(':memory:');
  src.exec(`
    CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT, data BLOB, n INTEGER, f REAL, note TEXT);
    INSERT INTO things VALUES (1, 'a', x'deadbeef', 9223372036854775807, 1.25, NULL), (2, 'quote "x"', x'', -5, -0.5, 'z');
  `);
  const store = new MemoryObjectStore();
  const out = new Sync(src, { store, prefix: 'rt' });
  const exported = await out.exportRows('things');
  assert.equal(exported.rows, 2);

  const dst = new Database(':memory:');
  dst.exec('CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT, data BLOB, n INTEGER, f REAL, note TEXT)');
  const loaded = await Importer.load(dst, { store, key: exported.key, table: 'things' });
  assert.equal(loaded.rows, 2);
  const view = (db) => db.prepare('SELECT id, label, hex(data) data, CAST(n AS TEXT) n, typeof(n) tn, f, note FROM things ORDER BY id').all();
  assert.deepEqual(view(dst), view(src));
  out.close();
  src.close();
  dst.close();
});

test('decodeValue reverses the sync encoding per column affinity', () => {
  const blob = { declared: 'BLOB', affinity: 'BLOB' };
  const text = { declared: 'TEXT', affinity: 'TEXT' };
  const int = { declared: 'INTEGER', affinity: 'INTEGER' };
  const untyped = { declared: '', affinity: 'BLOB' };
  assert.deepEqual(importer.decodeValue('AAH/', blob), Buffer.from([0, 1, 255]));
  assert.equal(importer.decodeValue('AAH/', text), 'AAH/');
  assert.equal(importer.decodeValue('AAH/', untyped), 'AAH/', 'a typeless column keeps its text');
  assert.equal(importer.decodeValue('42', int), 42);
  assert.equal(importer.decodeValue('9007199254740993', int), 9007199254740993n);
  assert.equal(importer.decodeValue('abc', int), 'abc');
  assert.equal(importer.decodeValue(true, int), 1);
  assert.equal(importer.decodeValue(null, int), null);
  assert.equal(importer.decodeValue({ a: 1 }, text), '{"a":1}');
  assert.equal(importer.affinity('VARCHAR(20)'), 'TEXT');
  assert.equal(importer.affinity('BIGINT'), 'INTEGER');
  assert.equal(importer.affinity('DOUBLE PRECISION'), 'REAL');
  assert.equal(importer.affinity('DECIMAL(10,2)'), 'NUMERIC');
});
