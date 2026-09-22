'use strict';

// Tests for lib/expr.js — the safe SQL-composition DSL (sql tag + expr helpers).

const test = require('node:test');
const assert = require('node:assert/strict');

const { sql, expr, Fragment } = require('../lib/expr');
const Database = require('../index.js');

test('sql tag: values become ordered ? params', () => {
  const f = sql`SELECT * FROM users WHERE id = ${7} AND name = ${'ada'}`;
  assert.equal(f.sql, 'SELECT * FROM users WHERE id = ? AND name = ?');
  assert.deepEqual(f.params, [7, 'ada']);
  assert.ok(f instanceof Fragment);
});

test('expr.list expands to placeholders with params; empty throws', () => {
  const f = sql`status IN ${expr.list(['a', 'b', 'c'])}`;
  assert.equal(f.sql, 'status IN (?, ?, ?)');
  assert.deepEqual(f.params, ['a', 'b', 'c']);
  assert.throws(() => expr.list([]), /non-empty/);
});

test('nested fragments keep positional param order', () => {
  const inner = sql`age > ${18}`;
  const f = sql`SELECT * FROM u WHERE name = ${'x'} AND ${inner} AND city = ${'y'}`;
  assert.equal(f.sql, 'SELECT * FROM u WHERE name = ? AND age > ? AND city = ?');
  assert.deepEqual(f.params, ['x', 18, 'y']);
});

test('expr.id quotes and qualifies, adds no param; expr.raw is trusted, no param', () => {
  assert.equal(expr.id('name').sql, '"name"');
  assert.equal(expr.id('t.col').sql, '"t"."col"');
  assert.deepEqual(expr.id('t.col').params, []);
  const f = sql`SELECT ${expr.id('users.id')}, ${expr.raw('count(*)')} FROM users`;
  assert.equal(f.sql, 'SELECT "users"."id", count(*) FROM users');
  assert.deepEqual(f.params, []);
});

test('and/or skip null args, parenthesize, and concatenate params', () => {
  const f = expr.and(expr.eq('a', 1), null, undefined, expr.gt('b', 2));
  assert.equal(f.sql, '("a" = ? AND "b" > ?)');
  assert.deepEqual(f.params, [1, 2]);
  assert.equal(expr.or(expr.eq('a', 1), expr.eq('a', 2)).sql, '("a" = ? OR "a" = ?)');
  assert.equal(expr.and().sql, '', 'no meaningful parts -> empty');
  assert.equal(expr.and(null, undefined).sql, '');
});

test('comparison, in, between, like, isNull helpers', () => {
  assert.deepEqual([expr.eq('a', 1).sql, expr.eq('a', 1).params], ['"a" = ?', [1]]);
  assert.equal(expr.ne('a', 1).sql, '"a" <> ?');
  assert.equal(expr.gte('a', 1).sql, '"a" >= ?');
  assert.equal(expr.lte('a', 1).sql, '"a" <= ?');
  const inf = expr.in('a', [1, 2]);
  assert.equal(inf.sql, '"a" IN (?, ?)');
  assert.deepEqual(inf.params, [1, 2]);
  const bt = expr.between('a', 1, 9);
  assert.equal(bt.sql, '"a" BETWEEN ? AND ?');
  assert.deepEqual(bt.params, [1, 9]);
  assert.equal(expr.like('a', '%x%').sql, '"a" LIKE ?');
  assert.equal(expr.isNull('a').sql, '"a" IS NULL');
  assert.equal(expr.notNull('a').sql, '"a" IS NOT NULL');
});

test('Fragment.append builds incrementally', () => {
  const f = new Fragment('SELECT 1');
  f.append(sql` WHERE x = ${5}`).append(' -- done');
  assert.equal(f.sql, 'SELECT 1 WHERE x = ? -- done');
  assert.deepEqual(f.params, [5]);
  assert.equal(String(f), f.sql);
});

test('end-to-end: composed query is injection-safe against a real db', () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  const evil = "x'); DROP TABLE t;--";
  db.prepare('INSERT INTO t (name) VALUES (?)').run(evil);
  db.prepare('INSERT INTO t (name) VALUES (?)').run('safe');

  const frag = sql`SELECT ${expr.id('id')}, ${expr.id('name')} FROM ${expr.raw('t')} WHERE ${expr.and(expr.eq('name', evil))}`;
  assert.equal(frag.sql, 'SELECT "id", "name" FROM t WHERE ("name" = ?)');
  const rows = db.prepare(frag.sql).all(...frag.params);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].name, evil, 'value round-trips as data, not SQL');
  // The table still exists (nothing was dropped) and still has both rows.
  assert.equal(db.prepare('SELECT count(*) c FROM t').get().c, 2);
  db.close();
});
