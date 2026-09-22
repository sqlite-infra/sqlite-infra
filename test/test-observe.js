'use strict';

// Tests for lib/observe.js — per-connection query timing/observability.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/observe'));

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
  db.exec("INSERT INTO t (v) VALUES ('a'), ('b'), ('c')");
  return db;
}

test('observe() collects per-sql stats for run/get/all, sorted by totalMs', () => {
  const db = seeded();
  db.observe();
  db.prepare('SELECT * FROM t').all();
  db.prepare('SELECT * FROM t').all();
  db.prepare('SELECT * FROM t WHERE id = ?').get(1);
  db.prepare("INSERT INTO t (v) VALUES (?)").run('d');

  const stats = db.queryStats();
  const bySql = new Map(stats.map((s) => [s.sql, s]));
  assert.equal(bySql.get('SELECT * FROM t').count, 2);
  assert.equal(bySql.get('SELECT * FROM t').rows, 6);          // 3 rows × 2 calls
  assert.equal(bySql.get('SELECT * FROM t WHERE id = ?').count, 1);
  assert.equal(bySql.get('SELECT * FROM t WHERE id = ?').rows, 1);
  assert.equal(bySql.get("INSERT INTO t (v) VALUES (?)").count, 1);
  assert.equal(bySql.get("INSERT INTO t (v) VALUES (?)").rows, 0);
  for (const s of stats) {
    assert.ok(s.totalMs >= 0 && s.maxMs >= 0);
    assert.equal(s.avgMs, s.totalMs / s.count);
  }
  // sorted by totalMs desc
  for (let i = 1; i < stats.length; i++) assert.ok(stats[i - 1].totalMs >= stats[i].totalMs);
  db.close();
});

test('onQuery fires for every call; onSlow fires at/over slowMs; a throwing callback is contained', () => {
  const db = seeded();
  const seen = [];
  let slow = 0;
  db.observe({
    slowMs: 0,
    onQuery: (sql) => { seen.push(sql); throw new Error('callback boom'); },   // must not break the query
    onSlow: () => { slow += 1; }
  });
  const rows = db.prepare('SELECT * FROM t').all();
  assert.equal(rows.length, 3, 'query still returns despite the throwing onQuery');
  db.prepare('SELECT * FROM t WHERE id = ?').get(2);
  assert.equal(seen.length, 2);
  assert.equal(slow, 2, 'slowMs:0 makes every call slow');
  db.close();
});

test('onSlow does not fire below the threshold', () => {
  const db = seeded();
  let slow = 0;
  db.observe({ slowMs: 60000, onSlow: () => { slow += 1; } });   // no in-memory query takes a minute
  db.prepare('SELECT * FROM t').all();
  assert.equal(slow, 0);
  db.close();
});

test('resetStats clears counters but keeps observing on', () => {
  const db = seeded();
  db.observe();
  db.prepare('SELECT * FROM t').all();
  assert.equal(db.queryStats().length, 1);
  db.resetStats();
  assert.deepEqual(db.queryStats(), []);
  db.prepare('SELECT * FROM t').all();
  assert.equal(db.queryStats()[0].count, 1, 'still observing after reset');
  db.close();
});

test('unobserve restores the prototype prepare and stops collecting', () => {
  const db = seeded();
  db.observe();
  db.prepare('SELECT * FROM t').all();
  const before = db.queryStats()[0].count;
  db.unobserve();
  assert.equal(db.prepare, Database.prototype.prepare, 'prepare identity restored to the prototype');
  db.prepare('SELECT * FROM t').all();
  db.prepare('SELECT * FROM t').all();
  const after = db.queryStats().find((s) => s.sql === 'SELECT * FROM t');
  assert.equal(after.count, before, 'no new stats after unobserve');
  db.close();
});

test('two connections observe independently', () => {
  const a = seeded();
  const b = seeded();
  a.observe();
  b.observe();
  a.prepare('SELECT * FROM t').all();
  b.prepare('SELECT * FROM t WHERE id = ?').get(1);
  b.prepare('SELECT * FROM t WHERE id = ?').get(2);
  assert.equal(a.queryStats().length, 1);
  assert.equal(a.queryStats()[0].sql, 'SELECT * FROM t');
  assert.equal(b.queryStats()[0].sql, 'SELECT * FROM t WHERE id = ?');
  assert.equal(b.queryStats()[0].count, 2);
  a.close();
  b.close();
});

test('observe validates its argument', () => {
  const db = seeded();
  assert.throws(() => db.observe(null), TypeError);
  db.close();
});
