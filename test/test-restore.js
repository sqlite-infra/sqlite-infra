'use strict';

// Round-trip coverage for Database.Importer.restore(): the verified-restore
// half of lib/importer.js, which the rest of the suite exercised only through
// its integrity-failure path (test-dictionary.js). Here a database is synced to
// an object store as a snapshot (and, where the engine has sessions, a
// changeset generation on top), then restored to a fresh file and checked.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const Database = require('../index.js');
const Sync = require('../lib/sync'); // internal; not on the package export
const { MemoryObjectStore } = require('../lib/interfaces');

const tmp = () => path.join(os.tmpdir(), `better-sqlite-restore-${process.pid}-${Math.random().toString(16).slice(2)}.db`);

test('restore() rebuilds the newest generation into a fresh file, verified', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');
  db.exec("INSERT INTO items VALUES (1, 'a'), (2, 'b')");

  const store = new MemoryObjectStore();
  const sync = new Sync(db, { store, prefix: 'p' });
  const snap = await sync.snapshot();
  assert.ok(snap.key);

  let expectedRows = 2;
  if (Database.engine.sessions) {
    db.exec("INSERT INTO items VALUES (3, 'c')");
    const cs = await sync.changeset();
    assert.ok(cs, 'a changeset should be produced when the engine has sessions');
    expectedRows = 3;
  }

  const to = tmp();
  const result = await Database.Importer.restore({ store, prefix: 'p', dbId: sync.dbId, to });
  assert.equal(result.to, to);
  assert.equal(result.snapshotKey, snap.key);
  assert.ok(Array.isArray(result.applied));
  assert.ok(fs.existsSync(to));

  const restored = new Database(to, { readonly: true });
  assert.equal(restored.prepare('SELECT count(*) AS n FROM items').pluck().get(), expectedRows);
  assert.equal(restored.prepare('SELECT label FROM items WHERE id = 1').pluck().get(), 'a');
  restored.close();

  sync.close();
  db.close();
  fs.rmSync(to, { force: true });
});

test('restore() honours upToSeq, stopping at an earlier generation', async () => {
  if (!Database.engine.sessions) return;   // needs changeset generations to have a seq to stop at

  const db = new Database(':memory:');
  db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');
  db.exec("INSERT INTO items VALUES (1, 'a')");

  const store = new MemoryObjectStore();
  const sync = new Sync(db, { store, prefix: 'q' });
  await sync.snapshot();
  db.exec("INSERT INTO items VALUES (2, 'b')");
  const cs1 = await sync.changeset();
  db.exec("INSERT INTO items VALUES (3, 'c')");
  await sync.changeset();

  const to = tmp();
  const result = await Database.Importer.restore({ store, prefix: 'q', dbId: sync.dbId, to, upToSeq: cs1.seq });
  assert.ok(result.seq <= cs1.seq);

  const restored = new Database(to, { readonly: true });
  const n = restored.prepare('SELECT count(*) AS n FROM items').pluck().get();
  assert.ok(n <= 2, `stopped at seq ${cs1.seq}, expected <= 2 rows, got ${n}`);
  restored.close();

  sync.close();
  db.close();
  fs.rmSync(to, { force: true });
});
