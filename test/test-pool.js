'use strict';

// Tests for lib/pool.js, the WAL connection pool: a dedicated writer plus
// round-robin read-only readers over one file, with WAL cross-connection
// visibility.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const Database = require('../index.js');
const { createPool, Pool } = require('../lib/pool');

const tmp = () => path.join(os.tmpdir(), `better-sqlite-pool-${process.pid}-${Math.random().toString(16).slice(2)}.db`);
function cleanup(file) {
  for (const suffix of ['', '-wal', '-shm', '-journal']) fs.rmSync(`${file}${suffix}`, { force: true });
}

test('a pool writes on the writer and reads the committed data on a reader (WAL)', () => {
  const file = tmp();
  const pool = createPool(file, { readers: 3 });
  try {
    assert.ok(pool instanceof Pool);
    assert.equal(pool.size, 3);
    assert.equal(pool.open, true);
    assert.equal(pool.writer().pragma('journal_mode', { simple: true }), 'wal');

    pool.write((w) => w.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)'));
    pool.write((w) => w.prepare('INSERT INTO t (v) VALUES (?)').run('hello'));

    // A separate reader connection sees the committed write under WAL.
    const n = pool.read((r) => r.prepare('SELECT count(*) AS n FROM t').pluck().get());
    assert.equal(n, 1);
    const v = pool.read((r) => r.prepare('SELECT v FROM t WHERE id = 1').pluck().get());
    assert.equal(v, 'hello');
  } finally {
    pool.close();
    cleanup(file);
  }
});

test('readers are read-only', () => {
  const file = tmp();
  const pool = createPool(file, { readers: 1 });
  try {
    pool.write((w) => w.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)'));
    assert.throws(() => pool.read((r) => r.exec("INSERT INTO t VALUES (1)")), (err) => err.code === 'SQLITE_READONLY');
    assert.equal(pool.reader().readonly, true);
  } finally {
    pool.close();
    cleanup(file);
  }
});

test('reader() is round-robin across the pool', () => {
  const file = tmp();
  const pool = createPool(file, { readers: 2 });
  try {
    const a = pool.reader();
    const b = pool.reader();
    const c = pool.reader();
    assert.notEqual(a, b, 'two consecutive readers differ');
    assert.equal(a, c, 'wraps around after size readers');
  } finally {
    pool.close();
    cleanup(file);
  }
});

test('transaction() commits on success and rolls back on throw', () => {
  const file = tmp();
  const pool = createPool(file, { readers: 1 });
  try {
    pool.write((w) => w.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)'));

    const inserted = pool.transaction((w) => {
      w.prepare('INSERT INTO t (v) VALUES (?)').run('a');
      w.prepare('INSERT INTO t (v) VALUES (?)').run('b');
      return 2;
    });
    assert.equal(inserted, 2);
    assert.equal(pool.read((r) => r.prepare('SELECT count(*) AS n FROM t').pluck().get()), 2);

    assert.throws(() => pool.transaction((w) => {
      w.prepare('INSERT INTO t (v) VALUES (?)').run('c');
      throw new Error('boom');
    }), /boom/);
    assert.equal(pool.read((r) => r.prepare('SELECT count(*) AS n FROM t').pluck().get()), 2, 'the failed transaction rolled back');
  } finally {
    pool.close();
    cleanup(file);
  }
});

test('close() closes every connection and is idempotent; use after close throws', () => {
  const file = tmp();
  const pool = createPool(file, { readers: 2 });
  const writer = pool.writer();
  const reader = pool.reader();
  pool.write((w) => w.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)'));

  assert.equal(pool.close(), pool);
  assert.equal(pool.open, false);
  assert.equal(writer.open, false);
  assert.equal(reader.open, false);
  assert.doesNotThrow(() => pool.close());   // idempotent
  assert.throws(() => pool.writer(), /pool is closed/);
  assert.throws(() => pool.reader(), /pool is closed/);
  cleanup(file);
});

test('a pool requires a real file path and validates options', () => {
  assert.throws(() => createPool(':memory:'), /file path/);
  assert.throws(() => createPool(''), /file path/);
  assert.throws(() => createPool(), /file path/);
  const file = tmp();
  assert.throws(() => createPool(file, { readers: 0 }), /positive integer/);
  assert.throws(() => createPool(file, { readers: 1.5 }), /positive integer/);
  cleanup(file);
});

test('dbOptions pass through and the writer stays read-write even if readonly is requested', () => {
  const file = tmp();
  const pool = createPool(file, { readers: 1, dbOptions: { readonly: true, timeout: 1000 } });
  try {
    // Writer ignores the readonly dbOption.
    assert.equal(pool.writer().readonly, false);
    pool.write((w) => w.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)'));
    assert.equal(pool.reader().readonly, true);
  } finally {
    pool.close();
    cleanup(file);
  }
});
