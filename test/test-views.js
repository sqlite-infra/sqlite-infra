'use strict';

// Tests for lib/views.js — SQL views and table-backed materialized views.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/views'));

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT NOT NULL, amount INTEGER NOT NULL);
    INSERT INTO sales (region, amount) VALUES
      ('us', 10), ('us', 20), ('eu', 5), ('eu', 15), ('eu', 30);
  `);
  return db;
}

test('createView from a raw SELECT and from a builder; then query and drop', () => {
  const db = seeded();
  db.createView('big_sales', "SELECT id, region, amount FROM sales WHERE amount >= 15");
  assert.deepEqual(db.from('big_sales').select('amount').orderBy('amount').pluck().all(), [15, 20, 30]);
  assert.ok(db.views().includes('big_sales'));

  // From a query builder (its params must be empty).
  db.createView('us_sales', db.from('sales').where("region = 'us'"), { ifNotExists: true });
  assert.equal(db.from('us_sales').count(), 2);

  db.dropView('big_sales');
  assert.ok(!db.views().includes('big_sales'));
  db.dropView('missing', { ifExists: true });   // no throw
  db.close();
});

test('createView rejects a source with bound parameters', () => {
  const db = seeded();
  assert.throws(() => db.createView('bad', { sql: 'SELECT * FROM sales WHERE amount > ?', params: [10] }),
    /cannot use bound parameters/);
  assert.throws(() => db.createView('bad', db.from('sales').where({ region: 'us' })), /cannot use bound parameters/);
  db.close();
});

test('materialize a GROUP BY summary into a real table', () => {
  const db = seeded();
  const res = db.materialize('sales_by_region',
    'SELECT region, sum(amount) AS total, count(*) AS n FROM sales GROUP BY region');
  assert.equal(res.name, 'sales_by_region');
  assert.equal(res.rows, 2);
  assert.ok(db.tables().includes('sales_by_region'), 'it is a real table');
  const rows = db.from('sales_by_region').orderBy('region').all();
  assert.deepEqual(rows, [{ region: 'eu', total: 50, n: 3 }, { region: 'us', total: 30, n: 2 }]);
  db.close();
});

test('refreshView recomputes after base data changes', () => {
  const db = seeded();
  db.materialize('totals', 'SELECT region, sum(amount) AS total FROM sales GROUP BY region');
  assert.equal(db.prepare("SELECT total FROM totals WHERE region='us'").pluck().get(), 30);

  db.exec("INSERT INTO sales (region, amount) VALUES ('us', 100)");
  // Stale until refreshed.
  assert.equal(db.prepare("SELECT total FROM totals WHERE region='us'").pluck().get(), 30);
  const res = db.refreshView('totals');
  assert.equal(res.rows, 2);
  assert.equal(db.prepare("SELECT total FROM totals WHERE region='us'").pluck().get(), 130);

  assert.throws(() => db.refreshView('never_materialized'), /was not materialized/);
  db.close();
});

test('materialize with a parameterised source, refresh, and indexes', () => {
  const db = seeded();
  const res = db.materialize('filtered',
    { sql: 'SELECT id, region, amount FROM sales WHERE amount >= ? ORDER BY id', params: [15] },
    { indexes: ['region'], unique: 'id' });
  assert.equal(res.rows, 3);
  assert.deepEqual(db.prepare('SELECT amount FROM filtered ORDER BY amount').pluck().all(), [15, 20, 30]);

  // Index and unique index exist on the materialized table.
  const idx = db.indexes('filtered');
  assert.ok(idx.some((i) => !i.unique && i.columns.includes('region')), 'non-unique region index');
  assert.ok(idx.some((i) => i.unique && i.columns.includes('id')), 'unique id index');

  // Refresh re-runs the parameterised definition.
  db.exec("INSERT INTO sales (region, amount) VALUES ('eu', 40)");
  assert.equal(db.refreshView('filtered').rows, 4);
  db.close();
});

test('dropMaterialized removes the table and forgets the definition', () => {
  const db = seeded();
  db.materialize('m', 'SELECT region, sum(amount) AS total FROM sales GROUP BY region');
  assert.ok(db.tables().includes('m'));
  db.dropMaterialized('m');
  assert.ok(!db.tables().includes('m'));
  assert.throws(() => db.refreshView('m'), /was not materialized/);
  db.dropMaterialized('m', { ifExists: true });   // no throw
  db.close();
});

test('validation', () => {
  const db = seeded();
  assert.throws(() => db.createView('', 'SELECT 1'), TypeError);
  assert.throws(() => db.materialize('x', 123), /source/);
  assert.throws(() => db.materialize('x', 'SELECT 1', { indexes: [[]] }), /non-empty/);
  db.close();
});
