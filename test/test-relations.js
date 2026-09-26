'use strict';

// Tests for the batch eager-loader (lib/relations.js): relations are attached
// with one query each, with no N+1.

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/relations'));

const test = require('node:test');
const assert = require('node:assert/strict');

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, org_id INTEGER);
    CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, created_at INTEGER);
    INSERT INTO orgs VALUES (1,'Platform'),(2,'Finance');
    INSERT INTO users VALUES (1,'Ada',1),(2,'Grace',1),(3,'Lin',2),(4,'Solo',NULL);
    INSERT INTO orders VALUES
      (10,1,9.5,3),(11,1,4.0,1),(12,2,1.0,2),(13,3,7.0,1);
  `);
  return db;
}

test('hasMany attaches arrays, including empty for a parent with none', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users ORDER BY id').all();
  db.loadRelated(users, { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id' });
  assert.deepEqual(users[0].orders.map((o) => o.id).sort(), [10, 11]);
  assert.deepEqual(users[1].orders.map((o) => o.id), [12]);
  assert.deepEqual(users[3].orders, []);   // Solo has no orders
  db.close();
});

test('belongsTo attaches a single object or null', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users ORDER BY id').all();
  db.loadRelated(users, { as: 'org', type: 'belongsTo', table: 'orgs', localKey: 'org_id' });
  assert.equal(users[0].org.name, 'Platform');
  assert.equal(users[2].org.name, 'Finance');
  assert.equal(users[3].org, null);   // org_id NULL
  db.close();
});

test('hasOne attaches one row or null', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users WHERE id IN (1,4) ORDER BY id').all();
  db.loadRelated(users, { as: 'anOrder', type: 'hasOne', table: 'orders', foreignKey: 'user_id', orderBy: 'total DESC' });
  assert.equal(users[0].anOrder.id, 10);   // highest total for Ada
  assert.equal(users[1].anOrder, null);
  db.close();
});

test('runs a single query per relation (no N+1)', () => {
  const db = seeded();
  // 100 parents, one order each
  db.exec('DELETE FROM users; DELETE FROM orders');
  const insU = db.prepare('INSERT INTO users (id, name, org_id) VALUES (?, ?, 1)');
  const insO = db.prepare('INSERT INTO orders (id, user_id, total, created_at) VALUES (?, ?, ?, 0)');
  for (let i = 1; i <= 100; i++) { insU.run(i, `u${i}`); insO.run(1000 + i, i, i); }

  const users = db.prepare('SELECT * FROM users').all();
  let childSelects = 0;
  const realPrepare = db.prepare.bind(db);
  db.prepare = (sql) => { if (/FROM "orders"/.test(sql)) childSelects += 1; return realPrepare(sql); };
  db.loadRelated(users, { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id' });
  db.prepare = realPrepare;

  assert.equal(childSelects, 1, 'exactly one SELECT against the child table');
  assert.equal(users.length, 100);
  assert.equal(users.every((u) => u.orders.length === 1), true);
  db.close();
});

test('columns projection fetches the join key internally but drops it when not requested', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users WHERE id = 1').all();
  db.loadRelated(users, { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id', columns: ['id', 'total'] });
  const o = users[0].orders[0];
  assert.deepEqual(Object.keys(o).sort(), ['id', 'total']);   // user_id was added internally, then dropped
  // When the caller DOES ask for the join key, it stays.
  const users2 = db.prepare('SELECT * FROM users WHERE id = 1').all();
  db.loadRelated(users2, { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id', columns: ['id', 'user_id'] });
  assert.ok('user_id' in users2[0].orders[0]);
  db.close();
});

test('where filter and params are ANDed into the batch query', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users WHERE id = 1').all();
  db.loadRelated(users, { as: 'big', type: 'hasMany', table: 'orders', foreignKey: 'user_id', where: 'total >= ?', params: [5] });
  assert.deepEqual(users[0].big.map((o) => o.id), [10]);
  db.close();
});

test('orderBy controls hasMany order', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users WHERE id = 1').all();
  db.loadRelated(users, { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id', orderBy: 'created_at ASC' });
  assert.deepEqual(users[0].orders.map((o) => o.id), [11, 10]);
  db.close();
});

test('nested relations load batched at each level', () => {
  const db = seeded();
  const orgs = db.prepare('SELECT * FROM orgs ORDER BY id').all();
  db.loadRelated(orgs, {
    as: 'users', type: 'hasMany', table: 'users', foreignKey: 'org_id',
    nested: { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id' }
  });
  assert.equal(orgs[0].users.length, 2);            // Platform: Ada, Grace
  const ada = orgs[0].users.find((u) => u.name === 'Ada');
  assert.deepEqual(ada.orders.map((o) => o.id).sort(), [10, 11]);
  assert.equal(orgs[1].users[0].orders.map((o) => o.id).join(), '13');
  db.close();
});

test('an array of relations loads several in one call; empty rows is a no-op; bad input throws', () => {
  const db = seeded();
  const users = db.prepare('SELECT * FROM users ORDER BY id').all();
  db.loadRelated(users, [
    { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id' },
    { as: 'org', type: 'belongsTo', table: 'orgs', localKey: 'org_id' }
  ]);
  assert.ok(Array.isArray(users[0].orders) && users[0].org.name === 'Platform');
  assert.deepEqual(db.loadRelated([], { as: 'x', type: 'hasMany', table: 'orders', foreignKey: 'user_id' }), []);
  assert.throws(() => db.loadRelated(users, { as: '', type: 'hasMany', table: 'orders', foreignKey: 'user_id' }), TypeError);
  assert.throws(() => db.loadRelated(users, { as: 'x', type: 'bogus', table: 'orders', foreignKey: 'user_id' }), TypeError);
  assert.throws(() => db.loadRelated(users, { as: 'x', type: 'hasMany', table: 'ghost', foreignKey: 'user_id' }), /No such table/);
  db.close();
});
