'use strict';

// Tests for the database-infrastructure extensions added on top of the
// better-sqlite3 surface: schema introspection (tables/views/columns/indexes/
// foreignKeys/tableExists), operational maintenance (checkpoint/vacuum/analyze/
// optimize/integrityCheck/quickCheck/foreignKeyCheck/userVersion), the
// migration runner (migrate), the prepared-statement cache (prepareCached/
// clearPreparedCache), and the boolean-toggle validation restored on the row
// modes. Everything runs against a real database on the installed node:sqlite.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const Database = require('../index.js');
const dictionary = require('../lib/dictionary');

const tmp = () => path.join(os.tmpdir(), `better-sqlite-infra-${process.pid}-${Math.random().toString(16).slice(2)}.db`);

function seeded() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE orgs (id INTEGER PRIMARY KEY, title TEXT);
    CREATE TABLE users (
      id     INTEGER PRIMARY KEY,
      name   TEXT NOT NULL UNIQUE COLLATE NOCASE,
      email  TEXT,
      org_id INTEGER REFERENCES orgs(id) ON DELETE CASCADE
    );
    CREATE INDEX idx_users_email ON users(email);
    CREATE VIEW named AS SELECT id, name FROM users;
  `);
  return db;
}

// ---- introspection --------------------------------------------------------

test('tables() and views() list user objects and skip sqlite_ and package tables', () => {
  const db = seeded();
  // A package-internal table must be filtered out, just as the sync's are.
  db.exec(`CREATE TABLE ${dictionary.tables.migrations} (name TEXT PRIMARY KEY, applied_at INTEGER)`);
  assert.deepEqual(db.tables(), ['orgs', 'users']);
  assert.deepEqual(db.views(), ['named']);
  db.close();
});

test('tableExists() covers tables and views but not missing names', () => {
  const db = seeded();
  assert.equal(db.tableExists('users'), true);
  assert.equal(db.tableExists('named'), true);   // a view counts
  assert.equal(db.tableExists('ghost'), false);
  assert.throws(() => db.tableExists(1), TypeError);
  db.close();
});

test('columns() reports name, type, notnull, default and primary-key position', () => {
  const db = seeded();
  db.exec('CREATE TABLE t (a INTEGER, b TEXT NOT NULL DEFAULT \'x\', c, PRIMARY KEY (a, c))');
  const cols = db.columns('t');
  assert.deepEqual(cols.map((c) => c.name), ['a', 'b', 'c']);
  assert.equal(cols[1].notnull, true);
  assert.equal(cols[1].default, "'x'");
  assert.equal(cols[0].primaryKey, 1);
  assert.equal(cols[2].primaryKey, 2);   // composite key, second position
  assert.equal(cols[1].primaryKey, 0);
  assert.throws(() => db.columns('ghost'), /No such table or view: ghost/);
  db.close();
});

test('indexes() lists explicit and implicit indexes, per table and across all', () => {
  const db = seeded();
  const onUsers = db.indexes('users');
  const explicit = onUsers.find((i) => i.name === 'idx_users_email');
  assert.ok(explicit);
  assert.deepEqual(explicit.columns, ['email']);
  assert.equal(explicit.unique, false);
  assert.equal(explicit.origin, 'c');
  // The UNIQUE constraint on name creates an implicit unique index.
  assert.ok(onUsers.some((i) => i.unique && i.origin === 'u' && i.columns.includes('name')));
  // No argument: indexes across every user table.
  assert.ok(db.indexes().some((i) => i.table === 'users' && i.name === 'idx_users_email'));
  assert.throws(() => db.indexes('ghost'), /No such table or view: ghost/);
  db.close();
});

test('foreignKeys() reports declared references', () => {
  const db = seeded();
  const fks = db.foreignKeys('users');
  assert.equal(fks.length, 1);
  assert.equal(fks[0].table, 'orgs');
  assert.equal(fks[0].from, 'org_id');
  assert.equal(fks[0].to, 'id');
  assert.equal(fks[0].onDelete, 'CASCADE');
  assert.deepEqual(db.foreignKeys('orgs'), []);
  assert.throws(() => db.foreignKeys('ghost'), /No such table or view: ghost/);
  db.close();
});

// ---- maintenance ----------------------------------------------------------

test('checkpoint() validates the mode and returns a shaped result on a WAL file', () => {
  const file = tmp();
  const db = new Database(file);
  db.pragma('journal_mode = WAL');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)');
  const r = db.checkpoint('truncate');
  assert.deepEqual(Object.keys(r).sort(), ['busy', 'checkpointed', 'log']);
  assert.equal(r.busy, 0);
  assert.throws(() => db.checkpoint('bogus'), /passive, full, restart, truncate/);
  assert.throws(() => db.checkpoint(5), TypeError);
  db.close();
  fs.rmSync(file, { force: true });
  fs.rmSync(`${file}-wal`, { force: true });
  fs.rmSync(`${file}-shm`, { force: true });
});

test('vacuum(), analyze() and optimize() run and are chainable', () => {
  const db = seeded();
  db.exec("INSERT INTO orgs (title) VALUES ('a'), ('b')");
  assert.equal(db.vacuum(), db);
  assert.equal(db.analyze(), db);
  assert.equal(db.analyze('users'), db);
  assert.equal(db.optimize(), db);
  assert.throws(() => db.vacuum(''), TypeError);
  assert.throws(() => db.analyze(42), TypeError);
  db.close();
});

test('vacuum(into) writes a compacted copy and leaves the source open', () => {
  const db = seeded();
  db.exec("INSERT INTO orgs (title) VALUES ('a')");
  const out = tmp();
  db.vacuum(out);
  assert.ok(fs.existsSync(out));
  const copy = new Database(out, { readonly: true });
  assert.equal(copy.prepare('SELECT count(*) AS n FROM orgs').pluck().get(), 1);
  copy.close();
  assert.equal(db.open, true);
  db.close();
  fs.rmSync(out, { force: true });
});

test('integrityCheck() and quickCheck() report the health sentinel on a good database', () => {
  const db = seeded();
  assert.deepEqual(db.integrityCheck(), ['ok']);
  assert.deepEqual(db.quickCheck(), ['ok']);
  assert.throws(() => db.integrityCheck(0), TypeError);
  assert.throws(() => db.quickCheck(-1), TypeError);
  db.close();
});

test('foreignKeyCheck() finds violations created while enforcement was off', () => {
  const db = seeded();
  db.pragma('foreign_keys = OFF');
  db.exec('INSERT INTO users (id, name, org_id) VALUES (1, \'a\', 999)');   // 999 does not exist
  const bad = db.foreignKeyCheck('users');
  assert.equal(bad.length, 1);
  assert.equal(bad[0].table, 'users');
  assert.equal(bad[0].parent, 'orgs');
  assert.equal(typeof bad[0].foreignKey, 'number');
  // Whole-database form finds it too.
  assert.ok(db.foreignKeyCheck().some((v) => v.table === 'users'));
  db.close();
});

test('userVersion() reads and sets the schema-version slot', () => {
  const db = seeded();
  assert.equal(db.userVersion(), 0);
  assert.equal(db.userVersion(42), db);
  assert.equal(db.userVersion(), 42);
  assert.throws(() => db.userVersion(-1), TypeError);
  assert.throws(() => db.userVersion(1.5), TypeError);
  db.close();
});

// ---- migrations -----------------------------------------------------------

test('migrate() applies pending migrations once, in order, transactionally', () => {
  const db = new Database(':memory:');
  const r1 = db.migrate([
    { name: '001-orgs', up: (d) => d.exec('CREATE TABLE orgs (id INTEGER PRIMARY KEY)') },
    { name: '002-users', up: (d) => d.exec('CREATE TABLE users (id INTEGER PRIMARY KEY)') }
  ]);
  assert.deepEqual(r1, { applied: ['001-orgs', '002-users'], pending: 0, current: 2 });
  assert.deepEqual(db.tables(), ['orgs', 'users']);

  // Re-running is a no-op for already-applied names; a new one is applied.
  const r2 = db.migrate([
    { name: '001-orgs', up: () => { throw new Error('must not run'); } },
    { name: '002-users', up: () => { throw new Error('must not run'); } },
    { name: '003-index', up: (d) => d.exec('CREATE INDEX i ON users(id)') }
  ]);
  assert.deepEqual(r2, { applied: ['003-index'], pending: 0, current: 3 });
  db.close();
});

test('migrate() rolls back a failing migration and aborts, keeping earlier ones', () => {
  const db = new Database(':memory:');
  assert.throws(() => db.migrate([
    { name: 'a', up: (d) => d.exec('CREATE TABLE a (id INTEGER PRIMARY KEY)') },
    { name: 'b', up: (d) => { d.exec('CREATE TABLE b (id INTEGER PRIMARY KEY)'); throw new Error('boom'); } },
    { name: 'c', up: (d) => d.exec('CREATE TABLE c (id INTEGER PRIMARY KEY)') }
  ]), /boom/);
  // 'a' committed; 'b' rolled back (no table b); 'c' never ran.
  assert.deepEqual(db.tables(), ['a']);
  // A retry resumes at 'b'.
  const r = db.migrate([
    { name: 'a', up: () => { throw new Error('must not run'); } },
    { name: 'b', up: (d) => d.exec('CREATE TABLE b (id INTEGER PRIMARY KEY)') },
    { name: 'c', up: (d) => d.exec('CREATE TABLE c (id INTEGER PRIMARY KEY)') }
  ]);
  assert.deepEqual(r.applied, ['b', 'c']);
  assert.deepEqual(db.tables(), ['a', 'b', 'c']);
  db.close();
});

test('migrate() validates its input', () => {
  const db = new Database(':memory:');
  assert.throws(() => db.migrate('nope'), /array of migrations/);
  assert.throws(() => db.migrate([{ up() {} }]), /non-empty "name"/);
  assert.throws(() => db.migrate([{ name: 'x' }]), /must have an "up" function/);
  assert.throws(() => db.migrate([{ name: 'x', up() {} }, { name: 'x', up() {} }]), /Duplicate migration name: x/);
  assert.throws(() => db.migrate([{ name: 'p', up: () => Promise.resolve() }]), /cannot return a promise/);
  db.close();
});

test('migrate() records under the dictionary table name, hidden from tables()', () => {
  const db = new Database(':memory:');
  db.migrate([{ name: 'init', up: (d) => d.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)') }]);
  const recorded = db.prepare(`SELECT name FROM ${dictionary.tables.migrations}`).pluck().all();
  assert.deepEqual(recorded, ['init']);
  assert.ok(!db.tables().includes(dictionary.tables.migrations));
  db.close();
});

test('rollback() undoes migrations newest-first, by steps and by name', () => {
  const db = new Database(':memory:');
  const migs = [
    { name: '001-a', up: (d) => d.exec('CREATE TABLE a (id INTEGER PRIMARY KEY)'), down: (d) => d.exec('DROP TABLE a') },
    { name: '002-b', up: (d) => d.exec('CREATE TABLE b (id INTEGER PRIMARY KEY)'), down: (d) => d.exec('DROP TABLE b') },
    { name: '003-c', up: (d) => d.exec('CREATE TABLE c (id INTEGER PRIMARY KEY)'), down: (d) => d.exec('DROP TABLE c') }
  ];
  db.migrate(migs);
  assert.deepEqual(db.tables(), ['a', 'b', 'c']);
  assert.deepEqual(db.rollback(migs), { rolledBack: ['003-c'] });
  assert.deepEqual(db.tables(), ['a', 'b']);
  assert.deepEqual(db.rollback(migs, { to: '001-a' }), { rolledBack: ['002-b'] });
  assert.deepEqual(db.tables(), ['a']);
  // migrate() then re-applies the rolled-back ones.
  assert.deepEqual(db.migrate(migs).applied, ['002-b', '003-c']);
  // a migration selected for rollback must have a down()
  assert.throws(() => db.rollback([{ name: '003-c', up() {} }]), /no "down" function/);
  db.close();
});

test('Database.loadMigrations() reads a directory in filename order', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'better-sqlite-migs-'));
  fs.writeFileSync(path.join(dir, '002-next.js'), "module.exports={up:d=>d.exec('CREATE TABLE t2(id INTEGER PRIMARY KEY)')};");
  fs.writeFileSync(path.join(dir, '001-init.js'), "module.exports={up:d=>d.exec('CREATE TABLE t1(id INTEGER PRIMARY KEY)'),down:d=>d.exec('DROP TABLE t1')};");
  const loaded = Database.loadMigrations(dir);
  assert.deepEqual(loaded.map((m) => m.name), ['001-init', '002-next']);
  assert.equal(typeof loaded[0].down, 'function');
  assert.equal(loaded[1].down, undefined);
  const db = new Database(':memory:');
  assert.deepEqual(db.migrate(loaded).applied, ['001-init', '002-next']);
  assert.deepEqual(db.tables(), ['t1', 't2']);
  db.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---- prepared-statement cache ---------------------------------------------

test('prepareCached() returns the same statement for the same SQL and clears', () => {
  const db = seeded();
  const a = db.prepareCached('SELECT 1 AS x');
  const b = db.prepareCached('SELECT 1 AS x');
  assert.equal(a, b);
  assert.equal(a.get().x, 1);
  const c = db.prepareCached('SELECT 2 AS x');
  assert.notEqual(a, c);
  assert.equal(db.clearPreparedCache(), db);
  assert.notEqual(db.prepareCached('SELECT 1 AS x'), a);
  assert.throws(() => db.prepareCached(1), TypeError);
  db.close();
});

test('prepareCached() evicts least-recently-used past the cap and clears on close', () => {
  const db = new Database(':memory:');
  const first = db.prepareCached('SELECT 0 AS x');
  for (let i = 1; i <= 120; i++) db.prepareCached(`SELECT ${i} AS x`);
  // The first statement was evicted (cap is 100), so a new object comes back.
  assert.notEqual(db.prepareCached('SELECT 0 AS x'), first);
  db.close();
  assert.throws(() => db.prepareCached('SELECT 1 AS x'), /not open/);
});

// ---- restored boolean validation on row-mode toggles ----------------------

test('row-mode toggles and safeIntegers reject a non-boolean argument', () => {
  const db = seeded();
  const stmt = db.prepare('SELECT id FROM users');
  for (const method of ['pluck', 'raw', 'expand', 'safeIntegers']) {
    assert.throws(() => stmt[method]('yes'), TypeError, `${method}("yes")`);
    assert.equal(stmt[method](), stmt);       // no-arg form still works (defaults true)
    assert.equal(stmt[method](false), stmt);
  }
  assert.throws(() => db.defaultSafeIntegers('yes'), TypeError);
  assert.equal(db.defaultSafeIntegers(false), db);
  db.close();
});

test('Statement#readonly throws (no node:sqlite counterpart), as documented', () => {
  const db = seeded();
  const stmt = db.prepare('SELECT id FROM users');
  assert.throws(() => stmt.readonly, /not supported by node:sqlite/);
  db.close();
});

// ---- introspection, wave 2 ------------------------------------------------

test('databases(), tableSchema() and dumpSchema()', () => {
  const db = seeded();
  const dbs = db.databases();
  assert.equal(dbs[0].name, 'main');
  assert.equal(dbs[0].file, '');
  assert.match(db.tableSchema('users'), /^CREATE TABLE users/);
  assert.equal(db.tableSchema('idx_users_email').startsWith('CREATE INDEX'), true);
  assert.throws(() => db.tableSchema('ghost'), /No such schema object: ghost/);
  const dump = db.dumpSchema();
  assert.match(dump, /CREATE TABLE orgs/);
  assert.match(dump, /CREATE TABLE users/);
  assert.match(dump, /CREATE INDEX idx_users_email/);
  assert.match(dump, /CREATE VIEW named/);
  assert.ok(dump.trim().endsWith(';'));
  // Internal tables are excluded from the dump.
  db.exec(`CREATE TABLE ${dictionary.tables.migrations} (name TEXT PRIMARY KEY, applied_at INTEGER)`);
  assert.ok(!db.dumpSchema().includes(dictionary.tables.migrations));
  db.close();
});

test('count() and exists() with and without a filter', () => {
  const db = seeded();
  db.exec("INSERT INTO orgs (id, title) VALUES (1, 'x'), (2, 'y')");
  db.exec("INSERT INTO users (id, name, org_id) VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2)");
  assert.equal(db.count('users'), 3);
  assert.equal(db.count('users', { where: 'org_id = ?', params: [1] }), 2);
  assert.equal(db.exists('users', { where: 'name = ?', params: ['a'] }), true);
  assert.equal(db.exists('users', { where: 'name = ?', params: ['zzz'] }), false);
  assert.throws(() => db.count('ghost'), /No such table or view/);
  assert.throws(() => db.count('users', { where: 5 }), /"where"/);
  assert.throws(() => db.count('users', { params: 'no' }), /"params"/);
  db.close();
});

// ---- attach / pragma helpers ----------------------------------------------

test('attach() and detach() add and remove a schema', () => {
  const db = seeded();
  const file = tmp();
  const other = new Database(file);
  other.exec('CREATE TABLE ext (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO ext VALUES (1, \'hi\')');
  other.close();

  assert.equal(db.attach(file, 'aux'), db);
  assert.ok(db.databases().some((d) => d.name === 'aux'));
  assert.equal(db.prepare('SELECT v FROM aux.ext WHERE id = 1').pluck().get(), 'hi');
  assert.equal(db.detach('aux'), db);
  assert.ok(!db.databases().some((d) => d.name === 'aux'));
  assert.throws(() => db.attach('', 'aux'), TypeError);
  assert.throws(() => db.attach(file, ''), TypeError);
  db.close();
  fs.rmSync(file, { force: true });
});

test('journalMode(), foreignKeysEnabled() and walAutocheckpoint() read and set', () => {
  const file = tmp();
  const db = new Database(file);
  assert.equal(db.journalMode('wal'), db);
  assert.equal(db.journalMode(), 'wal');
  assert.throws(() => db.journalMode('bogus'), /journal mode/);

  assert.equal(typeof db.foreignKeysEnabled(), 'boolean');
  assert.equal(db.foreignKeysEnabled(false), db);
  assert.equal(db.foreignKeysEnabled(), false);
  assert.equal(db.foreignKeysEnabled(true), db);
  assert.equal(db.foreignKeysEnabled(), true);
  assert.throws(() => db.foreignKeysEnabled('yes'), TypeError);

  assert.equal(typeof db.walAutocheckpoint(), 'number');
  assert.equal(db.walAutocheckpoint(0), db);
  assert.equal(db.walAutocheckpoint(), 0);
  assert.throws(() => db.walAutocheckpoint(-1), TypeError);
  db.close();
  for (const suffix of ['', '-wal', '-shm']) fs.rmSync(`${file}${suffix}`, { force: true });
});

test('foreignKeys(table) still lists keys after the pragma rename (no collision)', () => {
  const db = seeded();
  // introspection form, not the pragma toggle
  assert.equal(db.foreignKeys('users')[0].table, 'orgs');
  db.close();
});

// ---- bulk insert ----------------------------------------------------------

test('insertMany() writes rows in one transaction, honouring mode', () => {
  const db = seeded();
  db.exec("INSERT INTO orgs (id, title) VALUES (1, 'x')");
  const r = db.insertMany('users', [
    { id: 1, name: 'a', email: 'a@x', org_id: 1 },
    { id: 2, name: 'b', email: null, org_id: 1 },
    { id: 3, name: 'c', email: 'c@x', org_id: 1 }
  ]);
  assert.deepEqual(r, { inserted: 3 });
  assert.equal(db.count('users'), 3);

  // ignore skips the conflicting id 1; replace overwrites it.
  assert.deepEqual(db.insertMany('users', [{ id: 1, name: 'dup', org_id: 1 }, { id: 4, name: 'd', org_id: 1 }], { mode: 'ignore' }), { inserted: 1 });
  assert.equal(db.count('users'), 4);
  db.insertMany('users', [{ id: 1, name: 'A', org_id: 1 }], { mode: 'replace' });
  assert.equal(db.prepare('SELECT name FROM users WHERE id = 1').pluck().get(), 'A');

  // empty input is a no-op; explicit columns are honoured.
  assert.deepEqual(db.insertMany('users', []), { inserted: 0 });
  db.insertMany('orgs', [{ id: 9, title: 't', extra: 'ignored-not-a-column' }], { columns: ['id', 'title'] });
  assert.equal(db.count('orgs', { where: 'id = 9' }), 1);
  db.close();
});

test('insertMany() validates its arguments and rolls back on error', () => {
  const db = seeded();
  db.exec("INSERT INTO orgs (id, title) VALUES (1, 'x')");
  assert.throws(() => db.insertMany('', []), TypeError);
  assert.throws(() => db.insertMany('users', {}), /array of row objects/);
  assert.throws(() => db.insertMany('ghost', [{ id: 1 }]), /No such table: ghost/);
  assert.throws(() => db.insertMany('users', [{ id: 1, name: 'a', org_id: 1 }], { mode: 'bogus' }), /"mode"/);
  assert.throws(() => db.insertMany('users', [{ nope: 1 }]), /No such column in users: nope/);
  // A NOT NULL violation partway through rolls the whole batch back.
  assert.throws(() => db.insertMany('users', [{ id: 1, name: 'a', org_id: 1 }, { id: 2, name: null, org_id: 1 }]));
  assert.equal(db.count('users'), 0, 'the successful first row was rolled back with the failing second');
  db.close();
});
