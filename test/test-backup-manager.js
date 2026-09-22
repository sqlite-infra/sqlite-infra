'use strict';

// Tests for lib/backup-manager.js — scheduled backups + retention over an
// ObjectStore, driven directly (backupNow/_tick/prune) rather than by timers.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
const { MemoryObjectStore } = Database.interfaces;
const { BackupManager } = require('../lib/backup-manager');

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT NOT NULL)');
  db.exec("INSERT INTO items VALUES (1, 'a'), (2, 'b')");
  return db;
}

test('backupNow({ snapshot: true }) writes a snapshot object and fires events', async () => {
  const db = seeded();
  const store = new MemoryObjectStore();
  const mgr = new BackupManager(db, { store, prefix: 'bk', autoStart: false });
  const seen = [];
  mgr.on('snapshot', (i) => seen.push(['snapshot', i.generation]));
  mgr.on('backup', (i) => seen.push(['backup', i.generation]));

  const info = await mgr.backupNow({ snapshot: true });
  assert.equal(info.generation, 1);
  const keys = mgr.sync.keys();
  const listed = await store.list(keys.snapshots);
  assert.ok(listed.some((o) => o.key.startsWith(`${keys.snapshots}1/`)), 'a gen-1 snapshot object exists');
  assert.deepEqual(seen, [['snapshot', 1], ['backup', 1]]);

  const manifest = await mgr.manifest();
  assert.equal(manifest.generation, 1);
  assert.ok(manifest.snapshot && manifest.snapshot.generation === 1);
  mgr.close();
  db.close();
});

test('backupNow() takes a changeset after a snapshot when the engine has sessions', async () => {
  if (!Database.engine.sessions) return;
  const db = seeded();
  const store = new MemoryObjectStore();
  const mgr = new BackupManager(db, { store, prefix: 'bk', changesets: true, autoStart: false });

  await mgr.backupNow({ snapshot: true });        // generation 1
  db.exec("INSERT INTO items VALUES (3, 'c')");
  const info = await mgr.backupNow();              // should be a changeset, not a snapshot
  assert.ok(info, 'a changeset was produced for the new row');
  assert.equal(info.generation, 1);
  assert.equal(info.seq, 1);
  const keys = mgr.sync.keys();
  const cs = await store.list(keys.changesets);
  assert.ok(cs.some((o) => o.key.startsWith(`${keys.changesets}1/`)), 'a changeset object exists');
  mgr.close();
  db.close();
});

test('backupNow() with no changes and changesets on resolves null', async () => {
  if (!Database.engine.sessions) return;
  const db = seeded();
  const mgr = new BackupManager(db, { store: new MemoryObjectStore(), changesets: true, autoStart: false });
  await mgr.backupNow({ snapshot: true });
  const info = await mgr.backupNow();   // nothing changed
  assert.equal(info, null);
  mgr.close();
  db.close();
});

test('prune() keeps only the newest `retain` snapshot generations', async () => {
  const db = seeded();
  const store = new MemoryObjectStore();
  const mgr = new BackupManager(db, { store, prefix: 'bk', changesets: false, retain: 2, autoStart: false });

  for (let i = 0; i < 4; i++) await mgr.backupNow({ snapshot: true });   // generations 1..4
  const keys = mgr.sync.keys();
  const before = new Set((await store.list(keys.snapshots))
    .map((o) => o.key.slice(keys.snapshots.length).split('/')[0]));
  assert.deepEqual([...before].sort(), ['1', '2', '3', '4']);

  let pruned = 0;
  mgr.on('pruned', (n) => { pruned = n; });
  const removed = await mgr.prune();
  assert.equal(removed, 2);
  assert.equal(pruned, 2);
  const after = new Set((await store.list(keys.snapshots))
    .map((o) => o.key.slice(keys.snapshots.length).split('/')[0]));
  assert.deepEqual([...after].sort(), ['3', '4'], 'only the two newest generations remain');

  assert.equal(await mgr.prune(), 0, 'pruning again removes nothing');
  mgr.close();
  db.close();
});

test('only one backup runs at a time (serialized, sequential generations)', async () => {
  const db = seeded();
  const mgr = new BackupManager(db, { store: new MemoryObjectStore(), changesets: false, autoStart: false });
  const [a, b] = await Promise.all([mgr.backupNow({ snapshot: true }), mgr.backupNow({ snapshot: true })]);
  assert.equal(a.generation, 1);
  assert.equal(b.generation, 2, 'the second backup waited for the first');
  mgr.close();
  db.close();
});

test('_tick emits "error" on a broken store instead of throwing', async () => {
  const db = seeded();
  const store = new MemoryObjectStore();
  store.put = async () => { throw new Error('store is down'); };
  const mgr = new BackupManager(db, { store, changesets: false, autoStart: false });
  const errors = [];
  mgr.on('error', (e) => errors.push(e));
  mgr.start();
  await assert.doesNotReject(mgr._tick());
  assert.equal(errors.length, 1);
  assert.match(String(errors[0].message), /store is down/);
  mgr.close();
  db.close();
});

test('close() stops the manager and closes the sync', async () => {
  const db = seeded();
  const mgr = new BackupManager(db, { store: new MemoryObjectStore(), autoStart: false });
  mgr.start();
  assert.equal(mgr.running, true);
  mgr.close();
  assert.equal(mgr.running, false);
  db.close();
});

test('constructor validates its arguments', () => {
  const db = seeded();
  assert.throws(() => new BackupManager(db, {}), /needs a "store"/);
  assert.throws(() => new BackupManager({}, { store: new MemoryObjectStore() }), /to be a Database/);
  db.close();
});
