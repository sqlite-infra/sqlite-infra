'use strict';

// Tests for lib/sync.js with lib/importer.js's restore: snapshot and
// changeset generations round-trip through an ObjectStore, integrity checks
// fail closed, row exports produce NDJSON with a schema sidecar, pending
// changesets survive a crash, and scheduling takes a lease.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');
const crypto = require('node:crypto');

const Database = require('../');
const { Importer } = Database;
const Sync = require('../lib/sync'); // internal; not on the package export
const { MemoryObjectStore } = require('../lib/interfaces');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sync-'));
const tmpFile = (name) => path.join(tmp, name);
const sessions = Database.engine.sessions;

function seeded(file) {
  const db = new Database(file);
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, avatar BLOB, big INTEGER);
    CREATE TABLE tags (user_id INTEGER, tag TEXT, PRIMARY KEY (user_id, tag)) WITHOUT ROWID;
    INSERT INTO users (id, name, avatar, big) VALUES (1, 'ann', x'0001ff', 9007199254740993), (2, 'bob', NULL, 5);
    INSERT INTO tags VALUES (1, 'admin'), (2, 'guest');
  `);
  return db;
}

function rowsOf(db) {
  return {
    users: db.prepare('SELECT id, name, hex(avatar) avatar, CAST(big AS TEXT) big FROM users ORDER BY id').all(),
    tags: db.prepare('SELECT * FROM tags ORDER BY user_id, tag').all()
  };
}

test('validates its arguments', () => {
  const db = new Database(':memory:');
  const store = new MemoryObjectStore();
  assert.throws(() => new Sync({}, { store }), /Expected first argument to be a Database/);
  assert.throws(() => new Sync(db, { store: {} }), /ObjectStore/);
  assert.throws(() => new Sync(db, { store, prefix: 3 }), /"prefix"/);
  assert.throws(() => new Sync(db, { store, everyMs: -1 }), /"everyMs"/);
  const out = new Sync(db, { store });
  assert.throws(() => out.start(), /"everyMs"/);
  assert.equal(out.generation, 0);
  assert.equal(out.seq, 0);
  assert.equal(out.pending(), 0);
  out.close();
  db.close();
});

test('snapshot() uploads a verified, gzipped copy and restore() reproduces it', async () => {
  const db = seeded(tmpFile('snap.db'));
  const store = new MemoryObjectStore();
  const out = new Sync(db, { store, prefix: 'bk/' });
  const events = [];
  out.on('snapshot', (info) => events.push(info));

  const info = await out.snapshot();
  assert.equal(info.generation, 1);
  assert.match(info.key, /^bk\/[0-9a-f]{16}\/snapshots\/1\/\d{4}-\d{2}-\d{2}T[\d-]+\.\d{3}Z\.sqlite\.gz$/);
  assert.equal(events.length, 1);

  const stored = store.bytes(info.key);
  assert.equal(stored.length, info.size);
  assert.equal(crypto.createHash('sha256').update(stored).digest('hex'), info.sha256);
  assert.deepEqual((await store.head(info.key)).meta.sha256, info.sha256);
  const unzipped = zlib.gunzipSync(stored);
  assert.equal(unzipped.subarray(0, 15).toString(), 'SQLite format 3');

  const manifest = await out.manifest();
  assert.equal(manifest.version, 1);
  assert.equal(manifest.generation, 1);
  assert.equal(manifest.seq, 0);
  assert.equal(manifest.snapshot.key, info.key);
  assert.equal(manifest.dbId, out.dbId);

  const to = tmpFile('restored.db');
  const result = await Importer.restore({ store, prefix: 'bk', dbId: out.dbId, to });
  assert.equal(result.generation, 1);
  assert.deepEqual(result.applied, []);
  const copy = new Database(to, { readonly: true });
  assert.deepEqual(rowsOf(copy), rowsOf(db));
  copy.close();

  // No temp files left in the database's directory.
  assert.deepEqual(fs.readdirSync(tmp).filter((f) => f.startsWith('.snap.db.')), []);
  out.close();
  db.close();
});

test('a tampered snapshot or manifest fails restore and leaves no file behind', async () => {
  const db = seeded(tmpFile('tamper.db'));
  const store = new MemoryObjectStore();
  const out = new Sync(db, { store, prefix: 'p' });
  const info = await out.snapshot();

  const key = info.key;
  const entry = store.objects.get(key);
  entry.body = Buffer.concat([entry.body, Buffer.from([0])]);
  const to = tmpFile('tampered.db');
  await assert.rejects(Importer.restore({ store, prefix: 'p', dbId: out.dbId, to }), (err) => err.code === 'EINTEGRITY');
  assert.equal(fs.existsSync(to), false);
  assert.deepEqual(fs.readdirSync(tmp).filter((f) => f.startsWith('tampered.db.restore-')), []);

  await assert.rejects(Importer.restore({ store, prefix: 'p', dbId: 'nope', to }), (err) => err.code === 'ENOENT');
  await assert.rejects(Importer.restore({ store, prefix: 'p', dbId: out.dbId, to, onConflict: 'maybe' }), /"onConflict"/);
  out.close();
  db.close();
});

test('changesets: writes after a snapshot are shipped in order and restore applies them', { skip: !sessions && 'node:sqlite has no sessions here' }, async () => {
  const db = seeded(tmpFile('cs.db'));
  const store = new MemoryObjectStore();
  const out = new Sync(db, { store, prefix: 'cs' });
  assert.deepEqual(out.unsupportedTables(), []);

  assert.equal(await out.changeset(), null, 'nothing changed yet');
  await out.snapshot();

  db.prepare("INSERT INTO users (id, name) VALUES (3, 'cy')").run();
  db.prepare("UPDATE users SET name = 'anne' WHERE id = 1").run();
  const one = await out.changeset();
  assert.equal(one.generation, 1);
  assert.equal(one.seq, 1);
  assert.match(one.key, /\/changesets\/1\/000000000001\.changeset$/);
  assert.equal((await store.head(one.key)).meta.sha256, one.sha256);

  db.prepare('DELETE FROM tags WHERE user_id = 2').run();
  db.prepare("INSERT INTO tags VALUES (3, 'new')").run();
  const two = await out.changeset();
  assert.equal(two.seq, 2);
  assert.equal(await out.changeset(), null);
  assert.equal(out.pending(), 0);

  const manifest = await out.manifest();
  assert.equal(manifest.seq, 2);

  const to = tmpFile('cs-restored.db');
  const result = await Importer.restore({ store, prefix: 'cs', dbId: out.dbId, to });
  assert.equal(result.seq, 2);
  assert.equal(result.applied.length, 2);
  const copy = new Database(to, { readonly: true });
  assert.deepEqual(rowsOf(copy), rowsOf(db));
  copy.close();

  // Point in time: only the first changeset.
  const pit = tmpFile('cs-pit.db');
  await Importer.restore({ store, prefix: 'cs', dbId: out.dbId, to: pit, upToSeq: 1 });
  const pitDb = new Database(pit, { readonly: true });
  assert.equal(pitDb.prepare('SELECT count(*) n FROM tags').get().n, 2);
  assert.equal(pitDb.prepare('SELECT name FROM users WHERE id = 1').pluck().get(), 'anne');
  pitDb.close();

  // A changeset whose stored hash is missing is refused.
  const head = store.objects.get(two.key);
  delete head.meta.sha256;
  await assert.rejects(Importer.restore({ store, prefix: 'cs', dbId: out.dbId, to: tmpFile('cs-nohash.db') }), /no sha256 metadata/);
  out.close();
  db.close();
});

test('a new snapshot starts a new generation and restore uses only that generation', { skip: !sessions && 'node:sqlite has no sessions here' }, async () => {
  const db = seeded(tmpFile('gen.db'));
  const store = new MemoryObjectStore();
  const out = new Sync(db, { store, prefix: 'g' });
  await out.snapshot();
  db.prepare("INSERT INTO users (id, name) VALUES (10, 'ten')").run();
  await out.changeset();
  db.prepare("INSERT INTO users (id, name) VALUES (11, 'eleven')").run();
  // Written before the second snapshot but never cut as a changeset: must be
  // in the copy.
  const second = await out.snapshot();
  assert.equal(second.generation, 2);
  assert.equal(out.seq, 0);
  db.prepare("INSERT INTO users (id, name) VALUES (12, 'twelve')").run();
  const cs = await out.changeset();
  assert.equal(cs.generation, 2);
  assert.equal(cs.seq, 1);

  const to = tmpFile('gen-restored.db');
  const result = await Importer.restore({ store, prefix: 'g', dbId: out.dbId, to });
  assert.equal(result.generation, 2);
  assert.equal(result.applied.length, 1);
  const copy = new Database(to, { readonly: true });
  assert.deepEqual(copy.prepare('SELECT id FROM users ORDER BY id').pluck().all(), [1, 2, 10, 11, 12]);
  copy.close();
  out.close();
  db.close();
});

test('pending changesets survive a failed upload and go out on the next attempt or the next Sync', { skip: !sessions && 'node:sqlite has no sessions here' }, async () => {
  const file = tmpFile('pending.db');
  const db = seeded(file);
  const store = new MemoryObjectStore();
  let failPuts = 0;
  const flaky = Object.create(store);
  flaky.put = (key, body, meta) => (failPuts-- > 0 ? Promise.reject(new Error('bucket unavailable')) : store.put(key, body, meta));
  const out = new Sync(db, { store: flaky, prefix: 'pd' });
  await out.snapshot();

  db.prepare("INSERT INTO users (id, name) VALUES (20, 'x')").run();
  failPuts = 1;
  await assert.rejects(out.changeset(), /bucket unavailable/);
  assert.equal(out.pending(), 1, 'the cut changeset is stored locally');
  assert.equal(out.seq, 1);

  // Writes made while the upload was failing land in the next seq, not lost.
  db.prepare("INSERT INTO users (id, name) VALUES (21, 'y')").run();
  const last = await out.changeset();
  assert.equal(last.seq, 2);
  assert.equal(out.pending(), 0);
  assert.equal((await store.list(`pd/${out.dbId}/changesets/1/`)).length, 2);

  // Crash simulation: pending row left behind, a new Sync uploads it.
  db.prepare("INSERT INTO users (id, name) VALUES (22, 'z')").run();
  failPuts = 1;
  await assert.rejects(out.changeset(), /bucket unavailable/);
  out.close();
  const again = new Sync(db, { store, prefix: 'pd' });
  assert.equal(again.pending(), 1);
  const shipped = await again.changeset();
  assert.equal(shipped.seq, 3);
  assert.equal(again.pending(), 0);

  const to = tmpFile('pending-restored.db');
  await Importer.restore({ store, prefix: 'pd', dbId: again.dbId, to });
  const copy = new Database(to, { readonly: true });
  assert.deepEqual(copy.prepare('SELECT id FROM users WHERE id >= 20 ORDER BY id').pluck().all(), [20, 21, 22]);
  copy.close();
  again.close();
  db.close();
});

test('exportRows() streams NDJSON with encoded BLOB/BigInt values and writes a schema sidecar', async () => {
  const db = seeded(tmpFile('rows.db'));
  const store = new MemoryObjectStore();
  const out = new Sync(db, { store, prefix: 'ex' });
  assert.throws(() => out.exportRows(''), /non-empty string/);
  await assert.rejects(out.exportRows('missing'), /No such table or view/);
  await assert.rejects(out.exportRows('users', { columns: ['nope'] }), /No such column/);

  const result = await out.exportRows('users', {
    where: 'id >= ?', params: [1],
    transform: (row) => (row.name === 'bob' ? null : Object.assign(row, { name: row.name.toUpperCase() }))
  });
  assert.equal(result.rows, 1);
  assert.match(result.key, /^ex\/[0-9a-f]{16}\/exports\/users\/dt=\d{4}-\d{2}-\d{2}\/.*\.ndjson\.gz$/);
  const lines = zlib.gunzipSync(store.bytes(result.key)).toString().trim().split('\n').map(JSON.parse);
  assert.deepEqual(lines, [{ id: 1, name: 'ANN', avatar: Buffer.from('0001ff', 'hex').toString('base64'), big: '9007199254740993' }]);

  const schema = JSON.parse(store.bytes(result.schemaKey).toString());
  assert.equal(schema.table, 'users');
  assert.deepEqual(schema.columns.map((c) => [c.name, c.type, c.pk]), [['id', 'INTEGER', 1], ['name', 'TEXT', 0], ['avatar', 'BLOB', 0], ['big', 'INTEGER', 0]]);

  // Small integers stay numbers; partition: false drops the dt= segment.
  const exact = await out.exportRows('users', { columns: ['id', 'big'], partition: false, where: 'id = 2' });
  assert.match(exact.key, /exports\/users\/[^/]+\.ndjson\.gz$/);
  assert.deepEqual(JSON.parse(zlib.gunzipSync(store.bytes(exact.key)).toString().trim()), { id: 2, big: 5 });

  // An in-memory database exports through the same connection.
  const mem = new Database(':memory:');
  mem.exec("CREATE TABLE t (a); INSERT INTO t VALUES (1), (2), (3)");
  const memOut = new Sync(mem, { store, gzip: false });
  const r = await memOut.exportRows('t');
  assert.equal(r.rows, 3);
  assert.equal(store.bytes(r.key).toString(), '{"a":1}\n{"a":2}\n{"a":3}\n');
  memOut.close(); mem.close();
  out.close();
  db.close();
});

test('start() ticks under a lease: first a snapshot, then changesets, then a snapshot again on schema change', { skip: !sessions && 'node:sqlite has no sessions here' }, async () => {
  const file = tmpFile('sched.db');
  const db = seeded(file);
  const store = new MemoryObjectStore();
  const out = new Sync(db, { store, prefix: 's', everyMs: 30, snapshotEvery: 100 });
  const seen = [];
  out.on('snapshot', (i) => seen.push(`snapshot:${i.generation}`));
  out.on('changeset', (i) => seen.push(`changeset:${i.generation}.${i.seq}`));
  out.on('error', (err) => seen.push(`error:${err.message}`));
  out.start();
  const until = async (pred, label) => {
    const deadline = Date.now() + 5000;
    while (!pred()) { if (Date.now() > deadline) throw new Error(`timeout: ${label} (${seen})`); await new Promise((r) => setTimeout(r, 10)); }
  };
  await until(() => seen.includes('snapshot:1'), 'first snapshot');
  db.prepare("INSERT INTO users (id, name) VALUES (30, 'a')").run();
  await until(() => seen.includes('changeset:1.1'), 'first changeset');
  db.exec('CREATE TABLE extra (id INTEGER PRIMARY KEY, v TEXT)');
  assert.equal(out.schemaChanged, true);
  await until(() => seen.includes('snapshot:2'), 'snapshot after schema change');
  db.prepare("INSERT INTO extra VALUES (1, 'v')").run();
  await until(() => seen.includes('changeset:2.1'), 'changeset in generation 2');
  await out.drain();
  assert.equal(out.running, false);
  assert.ok(!seen.some((s) => s.startsWith('error:')), seen.join(','));

  // A second process holding the lease keeps this one from ticking.
  const other = new Database(file);
  other.prepare(`UPDATE ${Sync.TABLES.SYNC} SET locked_until = ?, lock_owner = 'someone-else' WHERE id = 1`).run(Date.now() + 60000);
  const before = seen.length;
  out.start();
  db.prepare("INSERT INTO extra VALUES (2, 'w')").run();
  await new Promise((r) => setTimeout(r, 120));
  assert.equal(seen.length, before, 'no tick while another owner holds the lease');
  other.prepare(`UPDATE ${Sync.TABLES.SYNC} SET locked_until = NULL, lock_owner = NULL WHERE id = 1`).run();
  await until(() => seen.includes('changeset:2.2'), 'tick after lease released');
  await out.drain();
  out.close();
  other.close();
  db.close();
});
