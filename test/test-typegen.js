'use strict';

// Tests for the schema -> TypeScript type generator (lib/typegen.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/typegen'));

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE user_accounts (
      id       INTEGER PRIMARY KEY,
      name     TEXT NOT NULL,
      score    REAL,
      avatar   BLOB,
      note                       -- no declared type
    );
    CREATE TABLE orgs (id INTEGER PRIMARY KEY, title VARCHAR(40) NOT NULL, ratio DECIMAL);
    CREATE VIEW active AS SELECT id, name FROM user_accounts;
  `);
  return db;
}

test('emits an exported interface per table with affinity-mapped, nullable fields', () => {
  const db = seeded();
  const ts = db.generateTypes();
  assert.match(ts, /export interface UserAccounts \{/);
  assert.match(ts, /\bid: number;/);
  assert.match(ts, /\bname: string;/);
  assert.match(ts, /\bscore: number \| null;/);        // nullable, not-null absent
  assert.match(ts, /\bavatar: Uint8Array \| null;/);   // BLOB
  assert.match(ts, /\bnote: unknown \| null;/);         // no declared type
  assert.match(ts, /export interface Orgs \{/);
  assert.match(ts, /\btitle: string;/);                 // VARCHAR -> string, NOT NULL
  assert.match(ts, /\bratio: number \| null;/);         // DECIMAL -> number
  db.close();
});

test('primary keys are non-null even without NOT NULL', () => {
  const db = seeded();
  // Scope to the base table: a PK column is non-null. (A view has no PK info,
  // so its columns are correctly nullable — checked elsewhere.)
  const ts = db.generateTypes({ tables: ['user_accounts'] });
  assert.match(ts, /\bid: number;/);
  assert.doesNotMatch(ts, /id: number \| null/);
  db.close();
});

test('prefix and suffix affect the interface name; PascalCase from snake_case', () => {
  const db = seeded();
  const ts = db.generateTypes({ prefix: 'DB', suffix: 'Row', tables: ['user_accounts'] });
  assert.match(ts, /export interface DBUserAccountsRow \{/);
  db.close();
});

test('bigint option flips INTEGER columns', () => {
  const db = seeded();
  const ts = db.generateTypes({ bigint: true, tables: ['orgs'] });
  assert.match(ts, /\bid: number \| bigint;/);
  db.close();
});

test('views are included by default and excluded with views:false', () => {
  const db = seeded();
  assert.match(db.generateTypes(), /export interface Active \{/);
  assert.doesNotMatch(db.generateTypes({ views: false }), /interface Active/);
  db.close();
});

test('table whitelist restricts output', () => {
  const db = seeded();
  const ts = db.generateTypes({ tables: ['orgs'] });
  assert.match(ts, /interface Orgs/);
  assert.doesNotMatch(ts, /interface UserAccounts/);
  assert.doesNotMatch(ts, /interface Active/);
  db.close();
});

test('export:false emits plain interfaces', () => {
  const db = seeded();
  const ts = db.generateTypes({ export: false, tables: ['orgs'] });
  assert.match(ts, /\binterface Orgs \{/);
  assert.doesNotMatch(ts, /export interface Orgs/);
  db.close();
});

test('databaseInterface emits a schema map keyed by table name', () => {
  const db = seeded();
  const ts = db.generateTypes({ databaseInterface: true, views: false });
  assert.match(ts, /export interface DatabaseSchema \{/);
  assert.match(ts, /\buser_accounts: UserAccounts;/);
  assert.match(ts, /\borgs: Orgs;/);
  const named = db.generateTypes({ databaseInterface: 'Schema', tables: ['orgs'] });
  assert.match(named, /export interface Schema \{/);
  db.close();
});

test('header banner is prepended; output is deterministic and sorted', () => {
  const db = seeded();
  const ts = db.generateTypes({ header: '// generated — do not edit' });
  assert.ok(ts.startsWith('// generated — do not edit\n'));
  // orgs sorts before user_accounts
  assert.ok(ts.indexOf('interface Active') < ts.indexOf('interface Orgs'));
  assert.ok(ts.indexOf('interface Orgs') < ts.indexOf('interface UserAccounts'));
  assert.equal(db.generateTypes(), db.generateTypes());   // stable
  db.close();
});

test('weird column names are quoted', () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t ("weird name" TEXT, "has-dash" INTEGER)');
  const ts = db.generateTypes();
  assert.match(ts, /"weird name": string \| null;/);
  assert.match(ts, /"has-dash": number \| null;/);
  db.close();
});

test('the static forDatabase form works too', () => {
  const db = seeded();
  const gen = require('../lib/typegen').forDatabase;
  assert.equal(gen(db, { tables: ['orgs'] }), db.generateTypes({ tables: ['orgs'] }));
  db.close();
});
