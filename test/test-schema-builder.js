'use strict';

// Tests for lib/schema-builder.js: the programmatic DDL builder. Verifies the
// generated schema through the introspection methods (db.columns, db.indexes,
// db.tableSchema, db.foreignKeys) rather than by string-matching the SQL.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
// Mix the builder onto the prototype for the test (lib/database.js wires it in
// production; this file is a standalone check of the module).
Object.assign(Database.prototype, require('../lib/schema-builder'));

test('createTable: pk/autoincrement, notNull, unique, defaults (literal and raw)', () => {
  const db = new Database(':memory:');
  const ret = db.createTable('users', (t) => {
    t.integer('id').primaryKey({ autoincrement: true });
    t.text('name').notNull().unique();
    t.integer('active').notNull().default(true);
    t.text('role').default('member');
    t.text('created_at').default(t.raw('CURRENT_TIMESTAMP'));
  });
  assert.equal(ret, db, 'chainable');
  const cols = db.columns('users');
  assert.deepEqual(cols.map((c) => c.name), ['id', 'name', 'active', 'role', 'created_at']);
  assert.equal(cols[0].primaryKey, 1);
  assert.equal(cols[1].notnull, true);
  assert.equal(cols[2].default, '1', 'boolean true renders as 1');
  assert.equal(cols[3].default, "'member'", 'string default is a quoted literal');
  assert.equal(cols[4].default, 'CURRENT_TIMESTAMP', 'raw default is left as an expression');
  // AUTOINCREMENT + UNIQUE both present in the recorded SQL.
  assert.match(db.tableSchema('users'), /AUTOINCREMENT/);
  assert.match(db.tableSchema('users'), /UNIQUE/);
  // The unique column actually enforces uniqueness.
  db.prepare('INSERT INTO users (name) VALUES (?)').run('a');
  assert.throws(() => db.prepare('INSERT INTO users (name) VALUES (?)').run('a'), /UNIQUE/);
  db.close();
});

test('createTable: foreign key with onDelete, check constraint, generated column', () => {
  const db = new Database(':memory:');
  db.createTable('orgs', (t) => t.integer('id').primaryKey());
  db.createTable('members', (t) => {
    t.integer('id').primaryKey();
    t.integer('org_id').references('orgs', 'id', { onDelete: 'cascade' });
    t.integer('age').check('age >= 0');
    t.integer('age_doubled').generatedAs('age * 2');
    t.integer('age_stored').generatedAs(t.raw('age + 1'), { stored: true });
  });
  const fks = db.foreignKeys('members');
  assert.equal(fks.length, 1);
  assert.equal(fks[0].table, 'orgs');
  assert.equal(fks[0].from, 'org_id');
  assert.equal(fks[0].onDelete, 'CASCADE');
  assert.match(db.tableSchema('members'), /CHECK \(age >= 0\)/);
  assert.match(db.tableSchema('members'), /GENERATED ALWAYS AS \(age \* 2\) VIRTUAL/);
  assert.match(db.tableSchema('members'), /GENERATED ALWAYS AS \(age \+ 1\) STORED/);
  // The generated column computes.
  db.exec('PRAGMA foreign_keys = ON');
  db.prepare('INSERT INTO orgs (id) VALUES (1)').run();
  db.prepare('INSERT INTO members (id, org_id, age) VALUES (1, 1, 20)').run();
  assert.equal(db.prepare('SELECT age_doubled FROM members WHERE id = 1').pluck().get(), 40);
  // The check constraint bites.
  assert.throws(() => db.prepare('INSERT INTO members (id, org_id, age) VALUES (2, 1, -1)').run(), /CHECK/);
  db.close();
});

test('createTable: composite primary key, table-level unique and foreignKey', () => {
  const db = new Database(':memory:');
  db.createTable('parent', (t) => t.integer('id').primaryKey());
  db.createTable('link', (t) => {
    t.integer('a');
    t.integer('b');
    t.text('code');
    t.primaryKey('a', 'b');
    t.unique('code');
    t.foreignKey('a').references('parent', 'id', { onDelete: 'restrict' });
  });
  const cols = db.columns('link');
  assert.equal(cols.find((c) => c.name === 'a').primaryKey, 1);
  assert.equal(cols.find((c) => c.name === 'b').primaryKey, 2);
  assert.equal(db.foreignKeys('link')[0].onDelete, 'RESTRICT');
  assert.match(db.tableSchema('link'), /UNIQUE \("code"\)/);
  db.close();
});

test('createTable: STRICT and WITHOUT ROWID options', () => {
  const db = new Database(':memory:');
  db.createTable('strict_t', (t) => { t.integer('id').primaryKey(); t.text('v'); }, { strict: true });
  assert.match(db.tableSchema('strict_t'), /STRICT/);
  // STRICT rejects a value of the wrong type.
  assert.throws(() => db.prepare('INSERT INTO strict_t (id, v) VALUES (?, ?)').run('not-an-int', 'x'), /datatype|STRICT|constraint/i);

  db.createTable('wr', (t) => { t.text('k').primaryKey(); t.text('v'); }, { withoutRowid: true });
  assert.match(db.tableSchema('wr'), /WITHOUT ROWID/);

  db.createTable('both', (t) => { t.text('k').primaryKey(); }, { strict: true, withoutRowid: true });
  assert.match(db.tableSchema('both'), /WITHOUT ROWID, STRICT/);
  db.close();
});

test('createIndex: plain, unique and partial; dropIndex', () => {
  const db = new Database(':memory:');
  db.createTable('t', (t) => { t.integer('id').primaryKey(); t.text('email'); t.integer('deleted'); });
  db.createIndex('ix_email', 't', 'email');
  db.createIndex('ux_email_live', 't', ['email'], { unique: true, where: 'deleted = 0' });
  const idx = db.indexes('t');
  const live = idx.find((i) => i.name === 'ux_email_live');
  assert.ok(live);
  assert.equal(live.unique, true);
  assert.equal(live.partial, true);
  assert.deepEqual(live.columns, ['email']);
  assert.ok(idx.some((i) => i.name === 'ix_email' && !i.unique));
  db.dropIndex('ix_email');
  assert.ok(!db.indexes('t').some((i) => i.name === 'ix_email'));
  db.dropIndex('nope', { ifExists: true });   // no throw
  db.close();
});

test('createTableIfNotExists, renameTable, addColumn, dropTable', () => {
  const db = new Database(':memory:');
  db.createTableIfNotExists('t', (t) => t.integer('id').primaryKey());
  db.createTableIfNotExists('t', (t) => t.integer('id').primaryKey());   // no throw second time
  db.renameTable('t', 't2');
  assert.ok(db.tableExists('t2') && !db.tableExists('t'));
  db.addColumn('t2', (t) => t.text('label').notNull().default('none'));
  assert.ok(db.columns('t2').some((c) => c.name === 'label' && c.default === "'none'"));
  db.dropTable('t2');
  assert.ok(!db.tableExists('t2'));
  db.dropTable('t2', { ifExists: true });   // no throw
  db.close();
});

test('validation: bad arguments throw TypeError', () => {
  const db = new Database(':memory:');
  assert.throws(() => db.createTable('', () => {}), TypeError);
  assert.throws(() => db.createTable('t', 'not a function'), TypeError);
  assert.throws(() => db.createTable('t', () => {}), /at least one column/);
  assert.throws(() => db.createTable('t', (t) => t.integer('a').default({})), /DEFAULT must be/);
  assert.throws(() => db.createIndex('i', 't', []), /one or more column/);
  assert.throws(() => db.addColumn('t', (t) => { t.integer('a'); t.integer('b'); }), /exactly one column/);
  db.close();
});
