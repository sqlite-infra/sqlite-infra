'use strict';

// Tests for the advisory-lock layer (lib/lock.js). Locks coordinate across
// connections/processes on one file, so these tests open two connections on a
// shared temp database.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/lock'));

const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

function withFile(fn) {
  const file = path.join(os.tmpdir(), `better-sqlite-lock-${process.pid}-${Math.random().toString(16).slice(2)}.db`);
  const a = new Database(file);
  const b = new Database(file);
  try {
    fn(a, b, file);
  } finally {
    a.close();
    b.close();
    for (const s of ['', '-wal', '-shm']) fs.rmSync(`${file}${s}`, { force: true });
  }
}

test('a lock is exclusive across connections; unlock frees it', () => {
  withFile((a, b) => {
    assert.equal(a.lock('job', { owner: 'A' }), true);
    assert.equal(b.lock('job', { owner: 'B' }), false, 'held by A');
    assert.equal(a.unlock('job'), true);
    assert.equal(b.lock('job', { owner: 'B' }), true, 'free after unlock');
    assert.equal(b.unlock('job'), true);
  });
});

test('owner-scoped unlock only releases for the matching owner', () => {
  withFile((a) => {
    a.lock('x', { owner: 'A' });
    assert.equal(a.unlock('x', { owner: 'B' }), false, 'wrong owner');
    assert.equal(a.unlock('x', { owner: 'A' }), true);
  });
});

test('a same-owner re-acquire refreshes and keeps the lock', () => {
  withFile((a, b) => {
    assert.equal(a.lock('r', { owner: 'A', ttlMs: 100000 }), true);
    assert.equal(a.lock('r', { owner: 'A', ttlMs: 100000 }), true, 're-acquire by same owner');
    assert.equal(b.lock('r', { owner: 'B' }), false);
  });
});

test('an expired lock is reclaimable by another owner', () => {
  withFile((a, b) => {
    assert.equal(a.lock('t', { owner: 'A', ttlMs: 5 }), true);
    assert.equal(b.lock('t', { owner: 'B' }), false, 'not yet expired');
    sleep(20);
    assert.equal(b.lock('t', { owner: 'B' }), true, 'reclaimed after expiry');
    assert.equal(b.unlock('t'), true);
  });
});

test('withLock runs fn and releases, even when fn throws', () => {
  withFile((a, b) => {
    const result = a.withLock('w', () => 42, { owner: 'A' });
    assert.equal(result, 42);
    assert.equal(b.lock('w', { owner: 'B' }), true, 'released after withLock');
    b.unlock('w');

    assert.throws(() => a.withLock('w', () => { throw new Error('boom'); }, { owner: 'A' }), /boom/);
    assert.equal(b.lock('w', { owner: 'B' }), true, 'released even after a throw');
    b.unlock('w');
  });
});

test('withLock waits and acquires after the holder releases', () => {
  withFile((a, b) => {
    assert.equal(a.lock('q', { owner: 'A', ttlMs: 15 }), true);
    // A holds with a 15ms ttl; b waits up to 500ms and should get it once it expires.
    const got = b.withLock('q', () => 'ok', { owner: 'B', wait: 500 });
    assert.equal(got, 'ok');
  });
});

test('withLock throws when it cannot acquire within wait', () => {
  withFile((a, b) => {
    assert.equal(a.lock('z', { owner: 'A', ttlMs: 100000 }), true);
    assert.throws(() => b.withLock('z', () => 1, { owner: 'B', wait: 30 }), /Could not acquire lock "z"/);
    a.unlock('z');
  });
});

test('locks() lists held locks and omits expired ones', () => {
  withFile((a) => {
    a.lock('a1', { owner: 'A' });
    a.lock('a2', { owner: 'A', ttlMs: 5 });
    let held = a.locks();
    assert.ok(held.some((l) => l.name === 'a1'));
    assert.equal(held.find((l) => l.name === 'a1').owner, 'A');
    sleep(20);
    held = a.locks();
    assert.ok(held.some((l) => l.name === 'a1'));
    assert.ok(!held.some((l) => l.name === 'a2'), 'expired lock purged');
  });
});

test('lock validates its arguments', () => {
  withFile((a) => {
    assert.throws(() => a.lock(''), TypeError);
    assert.throws(() => a.lock('n', { ttlMs: -1 }), TypeError);
    assert.throws(() => a.withLock('n', 'not a fn'), TypeError);
  });
});
