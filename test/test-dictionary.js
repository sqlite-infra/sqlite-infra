'use strict';

// Tests for lib/dictionary.js, the one place the library spells its strings.
//
// Two kinds of check. The lint test proves the dictionary is the source: no
// module under lib/, scripts/ or adapters/ may spell one of its literals
// itself (the .d.ts files and lib/contract.js are exempt: typings must show
// the literal, and the contract suite is a test fixture that asserts wire
// values on purpose). The behavioural tests prove the values are the ones
// that reach the wire, so a dictionary edit that changes a table name, an
// event or a code is caught here rather than by a consumer.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const zlib = require('node:zlib');

const Database = require('../index.js');
const dictionary = require('../lib/dictionary');
const codes = require('../lib/codes');
const changeset = require('../lib/changeset');
const collisionCheck = require('../lib/collision');
const { MemoryObjectStore, MemoryQueue, outcomeForStatus } = Database.interfaces;

const d = dictionary;
const read = (file) => fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
const sorted = (iterable) => [...iterable].sort();
const leaves = (node, out = []) => {
  for (const value of Object.values(node)) {
    if (value && typeof value === 'object') leaves(value, out);
    else out.push(value);
  }
  return out;
};
const until = async (predicate, ms = 4000, label = 'condition') => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 10));
  }
};

test('is deeply frozen, with only scalar leaves', () => {
  const walk = (node, at) => {
    assert.ok(Object.isFrozen(node), `${at} is not frozen`);
    for (const [key, value] of Object.entries(node)) {
      if (value && typeof value === 'object') walk(value, `${at}.${key}`);
    }
  };
  walk(dictionary, 'dictionary');
  assert.throws(() => { dictionary.tables.queue = 'x'; }, TypeError);
  assert.throws(() => { dictionary.env.extra = 'x'; }, TypeError);
  for (const value of leaves(dictionary)) {
    assert.ok(['string', 'number', 'boolean', 'function'].includes(typeof value), `unexpected leaf ${value}`);
  }
});

test('stays a leaf module: imports only lib/codes.js', () => {
  const requires = [...read('lib/dictionary.js').matchAll(/^const .*require\('([^']+)'\)/gm)].map((m) => m[1]);
  assert.deepEqual(requires, ['./codes']);
  assert.ok(!fs.existsSync(path.join(__dirname, '..', 'lib/defaults.js')), 'lib/defaults.js was folded into the dictionary');
});

test('events.consumer: handled and duplicate', async () => {
  const db = new Database(':memory:');
  const queue = new MemoryQueue();
  const seen = { handled: 0, duplicate: 0 };
  const consumer = new Database.Consumer(db, { source: queue, pollMs: 20, handler() {} });
  consumer.on(d.events.consumer.handled, () => { seen.handled += 1; });
  consumer.on(d.events.consumer.duplicate, () => { seen.duplicate += 1; });
  const envelope = { id: 'm1', kind: 'k', attempt: 1, body: '{"x":1}', attributes: {}, orderingKey: 'k' };
  await queue.publish([envelope]);
  await until(() => seen.handled === 1, 4000, 'handled');
  await queue.publish([envelope]);
  await until(() => seen.duplicate === 1, 4000, 'duplicate');
  consumer.stop();
  await consumer.drain();
  db.close();
});

test('events.sync and keys: snapshot, export, changeset land under the documented layout', async () => {
  const db = new Database(':memory:');
  db.exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL); INSERT INTO orders VALUES (1, 9.5)");
  const store = new MemoryObjectStore();
  const out = new Database.Sync(db, { store, prefix: 'p' });
  const fired = [];
  for (const name of Object.values(d.events.sync)) out.on(name, () => fired.push(name));

  const base = `p/${out.dbId}/`;
  const layout = out.keys();
  assert.equal(layout.manifest, base + d.keys.manifest);
  assert.equal(layout.snapshots, base + d.keys.snapshots);
  assert.equal(layout.changesets, base + d.keys.changesets);
  assert.equal(layout.exports, base + d.keys.exports);

  const snap = await out.snapshot();
  assert.ok(snap.key.startsWith(layout.snapshots), snap.key);
  assert.ok(snap.key.endsWith(d.keys.extensions.sqlite + d.keys.extensions.gzip), snap.key);
  assert.equal(zlib.gunzipSync(store.bytes(snap.key)).subarray(0, 15).toString(), 'SQLite format 3');

  const rows = await out.exportRows('orders');
  assert.ok(rows.key.startsWith(`${layout.exports}orders/${d.keys.partition}`), rows.key);
  assert.ok(rows.key.endsWith(d.keys.extensions.ndjson + d.keys.extensions.gzip), rows.key);
  assert.ok(await store.head(`${layout.exports}orders/${d.keys.schema}`), 'schema sidecar');

  const expected = [d.events.sync.snapshot, d.events.sync.export];
  if (Database.engine.sessions) {
    db.exec('INSERT INTO orders VALUES (2, 1)');
    const cs = await out.changeset();
    assert.ok(cs.key.startsWith(layout.changesets) && cs.key.endsWith(d.keys.extensions.changeset), cs.key);
    expected.push(d.events.sync.changeset);
  }
  assert.deepEqual(fired, expected);
  db.close();
});

test('contentTypes and contentEncodings: what the sync records on each object', async () => {
  const db = new Database(':memory:');
  db.exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL); INSERT INTO orders VALUES (1, 9.5)");
  const store = new MemoryObjectStore();
  const out = new Database.Sync(db, { store, prefix: 'p' });
  const snap = await out.snapshot();
  assert.equal((await store.head(snap.key)).meta.contentType, d.contentTypes.gzip);
  assert.equal((await store.head(snap.key)).meta.contentEncoding, d.contentEncodings.gzip);
  const rows = await out.exportRows('orders');
  assert.equal((await store.head(rows.key)).meta.contentType, d.contentTypes.ndjson);
  assert.equal((await store.head(out.keys().manifest)).meta.contentType, d.contentTypes.json);
  const raw = new Database.Sync(db, { store, prefix: 'raw', gzip: false });
  const rawSnap = await raw.snapshot();
  assert.equal((await store.head(rawSnap.key)).meta.contentType, d.contentTypes.sqlite);
  assert.ok(rawSnap.key.endsWith(d.keys.extensions.sqlite), rawSnap.key);
  if (Database.engine.sessions) {
    db.exec('INSERT INTO orders VALUES (2, 1)');
    const cs = await out.changeset();
    assert.equal((await store.head(cs.key)).meta.contentType, d.contentTypes.binary);
  }
  db.close();
});

test('importer: modes and formats are what load() accepts; onConflict values; integrity code', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
  const store = new MemoryObjectStore();
  await store.put('rows.ndjson', Buffer.from('{"id":1,"v":"a"}\n'));
  await store.put('rows.csv', Buffer.from('id,v\n2,b\n'));

  await assert.rejects(Database.Importer.load(db, { store, key: 'rows.ndjson', table: 't', mode: 'bogus' }),
    { message: 'Expected "mode" to be "insert", "replace" or "upsert"' });
  await assert.rejects(Database.Importer.load(db, { store, key: 'rows.ndjson', table: 't', format: 'bogus' }),
    { message: 'Expected "format" to be "ndjson" or "csv"' });
  for (const mode of Object.values(d.importer.modes)) {
    const result = await Database.Importer.load(db, { store, key: 'rows.ndjson', table: 't', mode, resume: false });
    assert.equal(result.lines, 1, mode);
  }
  const loaded = await Database.Importer.load(db, { store, key: 'rows.csv', table: 't', format: d.importer.formats.csv, mode: d.importer.modes.upsert });
  assert.equal(loaded.rows, 1);

  const typed = read('lib/importer.d.ts').match(/type ConflictResolution = ((?:'[a-z]+' \| )+)/)[1].match(/[a-z]+/g);
  assert.deepEqual(sorted(typed), sorted(Object.values(d.importer.onConflict)));
  await assert.rejects(Database.Importer.restore({ store, prefix: 'x', dbId: 'y', to: 'z', onConflict: 'bogus' }),
    { message: 'Expected "onConflict" to be "abort", "replace", "omit" or a function' });

  // A snapshot whose bytes do not match the manifest's hash fails with the integrity code.
  db.exec("CREATE TABLE orders (id INTEGER PRIMARY KEY); INSERT INTO orders VALUES (1)");
  const out = new Database.Sync(db, { store, prefix: 'p', changesets: false });
  const snap = await out.snapshot();
  await store.put(snap.key, zlib.gzipSync(Buffer.from('not a database')));
  const to = path.join(os.tmpdir(), `better-sqlite-dictionary-${process.pid}-${Date.now()}.db`);
  await assert.rejects(Database.Importer.restore({ store, prefix: 'p', dbId: out.dbId, to }), (err) => err.code === d.importer.errors.integrity);
  assert.ok(!fs.existsSync(to));
  db.close();
});

test('store: a missing key rejects with the dictionary code', async () => {
  const store = new MemoryObjectStore();
  await assert.rejects(store.get('absent'), (err) => err.code === d.store.errors.notFound);
});

test('changeset: framing magic', () => {
  assert.equal(changeset.MAGIC.toString(), d.changeset.magic);
  assert.equal(changeset.pack([]).subarray(0, 4).toString(), d.changeset.magic);
});

test('collision: the check honours the dictionary modes, variable and package.json fields', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'better-sqlite-dictionary-'));
  const pkg = { name: 'consumer', dependencies: { sqlite3: '5.0.0' } };
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify(pkg));
  assert.equal(collisionCheck.check(root, {}).mode, d.defaults.collision);
  assert.equal(collisionCheck.check(root, { [d.env.collision]: d.collision.modes.warn }).mode, d.collision.modes.warn);
  assert.equal(collisionCheck.check(root, { [d.env.collision]: d.collision.modes.off }).skipped, true);

  pkg['better-sqlite3'] = { [d.collision.fields.mode]: d.collision.modes.off, [d.collision.fields.allow]: ['sqlite3'] };
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify(pkg));
  assert.equal(collisionCheck.check(root, {}).skipped, true);
  const text = collisionCheck.format({ root, findings: [{ name: 'sqlite3', why: 'x', sources: ['s'] }] });
  assert.match(text, new RegExp(`${d.env.collision}=${d.collision.modes.off} `));
  assert.match(text, new RegExp(`"${d.collision.fields.mode}": "${d.collision.modes.off}"`));
  assert.match(text, new RegExp(`"${d.collision.fields.allow}": \\["<name>"\\]`));
  fs.rmSync(root, { recursive: true, force: true });
});

test('errors: every name is a real result code, and the ones a data path hits are observed', () => {
  const known = new Set(Object.values(codes.NAMES));
  for (const [name, code] of Object.entries(d.errors)) {
    if (name === 'unknown') assert.equal(codes.nameOf('x'), code);
    else assert.ok(known.has(code), `${name}: ${code} is not in lib/codes.js`);
  }
  assert.deepEqual(d.sqliteCodes, Object.values(codes.NAMES));
  assert.ok(Object.isFrozen(d.sqliteCodes));

  const db = new Database(':memory:');
  db.exec(`CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, n INTEGER CHECK (n > 0));
           CREATE TABLE c (pid INTEGER REFERENCES p(id));
           INSERT INTO p VALUES (1, 'a', 1)`);
  const code = (fn) => { try { fn(); } catch (err) { return err.code; } return undefined; };
  assert.equal(code(() => db.exec("INSERT INTO p VALUES (2, 'a', 1)")), d.errors.unique);
  assert.equal(code(() => db.exec("INSERT INTO p VALUES (1, 'b', 1)")), d.errors.primaryKey);
  assert.equal(code(() => db.exec("INSERT INTO p VALUES (3, NULL, 1)")), d.errors.notNull);
  assert.equal(code(() => db.exec("INSERT INTO p VALUES (3, 'c', 0)")), d.errors.check);
  assert.equal(code(() => db.exec('INSERT INTO c VALUES (99)')), d.errors.foreignKey);
  assert.equal(code(() => db.exec('SELECT * FROM nope')), d.errors.error);
  db.close();
  // A missing directory is a TypeError (better-sqlite3 parity); a missing file is SQLITE_CANTOPEN.
  const missing = path.join(os.tmpdir(), `better-sqlite-dictionary-${process.pid}-${Date.now()}.db`);
  assert.equal(code(() => new Database(missing, { fileMustExist: true })), d.errors.cantOpen);
});

test('find: reverse lookup by exact value', () => {
  assert.deepEqual(d.find('retry'), ['events.queue.retry', 'events.consumer.retry', 'outcomes.retry']);
  assert.deepEqual(d.find('_better_sqlite_outbox'), ['tables.queue']);
  assert.deepEqual(d.find('SQLITE_CONSTRAINT_UNIQUE'), ['errors.unique']);
  assert.deepEqual(d.find('nothing like this'), []);
});
