'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('../');

const { SqliteError } = Database;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'better-sqlite-'));
const tmpFile = (name) => path.join(tmp, name);

function fresh() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE t (
      id    INTEGER PRIMARY KEY AUTOINCREMENT,
      name  TEXT NOT NULL UNIQUE COLLATE NOCASE,
      n     INTEGER CHECK (n IS NULL OR n >= 0),
      data  BLOB
    );
    INSERT INTO t (name, n) VALUES ('alpha', 1), ('beta', 2), ('gamma', 3);
  `);
  return db;
}

// ---------------------------------------------------------------- opening

test('opens in-memory, temporary, and file databases', () => {
  const mem = new Database(':memory:');
  assert.equal(mem.name, ':memory:');
  assert.equal(mem.memory, true);
  assert.equal(mem.open, true);
  assert.equal(mem.readonly, false);

  const anon = new Database();
  assert.equal(anon.name, '');
  assert.equal(anon.memory, true);

  const file = tmpFile('open.db');
  const disk = new Database(file);
  assert.equal(disk.name, file);
  assert.equal(disk.memory, false);
  disk.exec('CREATE TABLE x (a)');
  disk.close();
  assert.ok(fs.existsSync(file));
});

test('works without new and exposes SqliteError', () => {
  const db = Database(':memory:'); // eslint-disable-line new-cap
  assert.ok(db instanceof Database);
  assert.equal(typeof SqliteError, 'function');
  assert.throws(() => new SqliteError('x', 5), TypeError);
  const err = new SqliteError('boom', 'SQLITE_BUSY');
  assert.equal(err.name, 'SqliteError');
  assert.equal(err.code, 'SQLITE_BUSY');
  assert.ok(err instanceof Error);
});

test('validates constructor arguments like better-sqlite3', () => {
  assert.throws(() => new Database(42), { name: 'TypeError', message: 'Expected first argument to be a string' });
  assert.throws(() => new Database(':memory:', 'x'), { message: 'Expected second argument to be an options object' });
  assert.throws(() => new Database(':memory:', { readOnly: true }), { message: 'Misspelled option "readOnly" should be "readonly"' });
  assert.throws(() => new Database(':memory:', { readonly: 'yes' }), { message: 'Expected the "readonly" option to be a boolean' });
  assert.throws(() => new Database(':memory:', { readonly: true }), { message: 'In-memory/temporary databases cannot be readonly' });
  assert.throws(() => new Database(':memory:', { timeout: -1 }), { message: 'Expected the "timeout" option to be a positive integer' });
  assert.throws(() => new Database(':memory:', { timeout: 0x80000000 }), { name: 'RangeError' });
  assert.throws(() => new Database(':memory:', { verbose: 1 }), { message: 'Expected the "verbose" option to be a function' });
  assert.throws(() => new Database(path.join(tmp, 'missing-dir', 'x.db')), { message: 'Cannot open database because the directory does not exist' });
  assert.throws(() => new Database(':memory:', { memory: true }), { message: /Option "memory" was removed/ });
});

test('fileMustExist and readonly', () => {
  const file = tmpFile('ro.db');
  assert.throws(() => new Database(file, { fileMustExist: true }), { name: 'SqliteError', code: 'SQLITE_CANTOPEN' });
  const writer = new Database(file);
  writer.exec('CREATE TABLE x (a); INSERT INTO x VALUES (1)');
  writer.close();

  const reader = new Database(file, { readonly: true, fileMustExist: true });
  assert.equal(reader.readonly, true);
  assert.equal(reader.prepare('SELECT a FROM x').pluck().get(), 1);
  assert.throws(() => reader.exec('INSERT INTO x VALUES (2)'), { name: 'SqliteError', code: 'SQLITE_READONLY' });
  reader.close();
});

test('timeout sets busy_timeout; foreign keys are on by default', () => {
  const db = new Database(':memory:', { timeout: 1234 });
  assert.equal(db.pragma('busy_timeout', { simple: true }), 1234);
  assert.equal(new Database(':memory:').pragma('busy_timeout', { simple: true }), 5000);
  assert.equal(db.pragma('foreign_keys', { simple: true }), 1);
});

test('verbose receives every SQL string executed, with bound values expanded', () => {
  const seen = [];
  const db = new Database(':memory:', { verbose: (sql) => seen.push(sql) });
  db.exec('CREATE TABLE v (a)');
  db.prepare('INSERT INTO v VALUES (?)').run(1);
  db.prepare('SELECT a FROM v WHERE a = @a').all({ a: 'x' });
  db.pragma('user_version');
  assert.deepEqual(seen, ['CREATE TABLE v (a)', 'INSERT INTO v VALUES (1.0)', "SELECT a FROM v WHERE a = 'x'", 'PRAGMA user_version']);
});

// ---------------------------------------------------------------- exec / pragma

test('exec runs multiple statements and returns the database', () => {
  const db = new Database(':memory:');
  assert.equal(db.exec('CREATE TABLE a (x); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2)'), db);
  assert.equal(db.prepare('SELECT count(*) FROM a').pluck().get(), 2);
  assert.throws(() => db.exec(1), { name: 'TypeError' });
  assert.throws(() => db.exec('NOT SQL'), { name: 'SqliteError', code: 'SQLITE_ERROR', message: /syntax error/ });
});

test('pragma returns rows, simple values, and handles pragmas without output', () => {
  const file = tmpFile('wal.db');
  const db = new Database(file);
  assert.deepEqual(db.pragma('journal_mode = WAL'), [{ journal_mode: 'wal' }]);
  assert.equal(db.pragma('journal_mode', { simple: true }), 'wal');
  assert.deepEqual(db.pragma('user_version = 7'), []);
  assert.equal(db.pragma('user_version', { simple: true }), 7);
  assert.ok(db.pragma('table_info(sqlite_master)').length > 0);
  assert.throws(() => db.pragma(1), { name: 'TypeError' });
  assert.throws(() => db.pragma('user_version', { simple: 'y' }), { message: 'Expected the "simple" option to be a boolean' });
  db.close();
});

// ---------------------------------------------------------------- prepare

test('prepare rejects empty, multi-statement, numbered-parameter, and invalid SQL', () => {
  const db = fresh();
  assert.throws(() => db.prepare(''), { message: 'The supplied SQL string contains no statements' });
  assert.throws(() => db.prepare('  -- only a comment\n /* and another */ '), { message: 'The supplied SQL string contains no statements' });
  assert.throws(() => db.prepare('SELECT 1; SELECT 2'), { message: 'The supplied SQL string contains more than one statement' });
  assert.doesNotThrow(() => db.prepare('SELECT 1;   '));
  assert.doesNotThrow(() => db.prepare("SELECT ';' AS semi; -- trailing comment"));
  assert.throws(() => db.prepare('SELECT ?1'), { message: /Numbered parameters/ });
  assert.throws(() => db.prepare('SELECT * FROM nope'), { name: 'SqliteError', code: 'SQLITE_ERROR', message: 'no such table: nope' });
  assert.throws(() => db.prepare(null), { name: 'TypeError' });
});

test('statement metadata: source, reader, database, columns', () => {
  const db = fresh();
  const select = db.prepare('SELECT t.id, name, n + 1 AS next FROM t');
  assert.equal(select.source, 'SELECT t.id, name, n + 1 AS next FROM t');
  assert.equal(select.reader, true);
  assert.equal(select.database, db);
  assert.equal(select.busy, false);
  assert.deepEqual(select.columns(), [
    { name: 'id', column: 'id', table: 't', database: 'main', type: 'INTEGER' },
    { name: 'name', column: 'name', table: 't', database: 'main', type: 'TEXT' },
    { name: 'next', column: null, table: null, database: null, type: null }
  ]);

  const insert = db.prepare('INSERT INTO t (name) VALUES (?)');
  assert.equal(insert.reader, false);
  assert.throws(() => insert.get('x'), { message: 'This statement does not return data. Use run() instead' });
  assert.throws(() => insert.all('x'), { message: 'This statement does not return data. Use run() instead' });
  assert.throws(() => insert.iterate('x'), { message: 'This statement does not return data. Use run() instead' });
  assert.throws(() => insert.columns(), { message: 'The columns() method is only for statements that return data' });
  assert.throws(() => insert.pluck(), { message: 'The pluck() method is only for statements that return data' });
  assert.throws(() => insert.raw(), { message: 'The raw() method is only for statements that return data' });
  assert.throws(() => insert.expand(), { message: 'The expand() method is only for statements that return data' });
  assert.equal(db.prepare('INSERT INTO t (name) VALUES (?) RETURNING id').reader, true);
});

// ---------------------------------------------------------------- run / get / all / iterate

test('run reports changes and lastInsertRowid; get/all return plain objects', () => {
  const db = fresh();
  const info = db.prepare("INSERT INTO t (name, n) VALUES ('delta', 4)").run();
  assert.deepEqual(info, { changes: 1, lastInsertRowid: 4 });
  assert.deepEqual(db.prepare('UPDATE t SET n = n + 10 WHERE n >= 3').run(), { changes: 2, lastInsertRowid: 4 });

  const row = db.prepare('SELECT id, name, n FROM t WHERE id = ?').get(1);
  assert.deepEqual(row, { id: 1, name: 'alpha', n: 1 });
  assert.equal(Object.getPrototypeOf(row), Object.prototype);
  assert.equal(db.prepare('SELECT * FROM t WHERE id = ?').get(999), undefined);

  const rows = db.prepare('SELECT name FROM t ORDER BY id').all();
  assert.deepEqual(rows, [{ name: 'alpha' }, { name: 'beta' }, { name: 'gamma' }, { name: 'delta' }]);
  assert.deepEqual(db.prepare('SELECT * FROM t WHERE id > 100').all(), []);
  assert.deepEqual(db.prepare('SELECT * FROM t').run(), { changes: 0, lastInsertRowid: 4 }, 'run() on a reader executes and changes nothing');
});

test('iterate yields rows lazily, marks the statement busy, and releases on early exit', () => {
  const db = fresh();
  const stmt = db.prepare('SELECT name FROM t ORDER BY id');
  const it = stmt.iterate();
  assert.equal(stmt.busy, true, 'busy from iterate() on');
  const first = it.next();
  assert.deepEqual(first, { value: { name: 'alpha' }, done: false });
  assert.equal(stmt.busy, true);
  assert.throws(() => stmt.all(), { message: 'This statement is busy executing a query' });
  it.return();
  assert.equal(stmt.busy, false);
  assert.deepEqual([...stmt.iterate()].map((r) => r.name), ['alpha', 'beta', 'gamma']);
  assert.equal(stmt.busy, false);

  let count = 0;
  for (const row of stmt.iterate()) {
    count++;
    if (row.name === 'beta') break;
  }
  assert.equal(count, 2);
  assert.equal(stmt.busy, false, 'break releases the iterator');
});

// ---------------------------------------------------------------- parameters

test('binds positional, array, named, and mixed parameters', () => {
  const db = fresh();
  assert.deepEqual(db.prepare('SELECT ? AS a, ? AS b').get(1, 'two'), { a: 1, b: 'two' });
  assert.deepEqual(db.prepare('SELECT ? AS a, ? AS b').get([1, 'two']), { a: 1, b: 'two' });
  assert.deepEqual(db.prepare('SELECT ? AS a, ? AS b, ? AS c').get([1], 2, [3]), { a: 1, b: 2, c: 3 });
  assert.deepEqual(db.prepare('SELECT @a AS a, $b AS b, :c AS c').get({ a: 1, b: 2, c: 3 }), { a: 1, b: 2, c: 3 });
  assert.deepEqual(db.prepare('SELECT @a AS a, @a AS again, ? AS p').get({ a: 'x' }, 'y'), { a: 'x', again: 'x', p: 'y' });
  assert.deepEqual(db.prepare('SELECT ? AS p, @a AS a').get('y', { a: 'x' }), { p: 'y', a: 'x' });
  assert.deepEqual(db.prepare('SELECT @a AS a').get({ a: 1, unused: 2 }), { a: 1 }, 'extra keys are ignored');
  assert.deepEqual(db.prepare('SELECT @a AS a').get(Object.create(null, { a: { value: 5, enumerable: true } })), { a: 5 }, 'null-prototype objects are plain');
});

test('rejects missing, surplus, and unbindable parameters with better-sqlite3 messages', () => {
  const db = fresh();
  assert.throws(() => db.prepare('SELECT @a AS a, @b AS b').get({ a: 1 }), { name: 'RangeError', message: 'Missing named parameter "b"' });
  assert.throws(() => db.prepare('SELECT @a AS a').get(), { name: 'TypeError', message: 'Missing named parameters' });
  assert.throws(() => db.prepare('SELECT @a AS a').get(1), { name: 'RangeError', message: 'Too many parameter values were provided' });
  assert.throws(() => db.prepare('SELECT ? AS a, ? AS b').get(1), { name: 'RangeError', message: 'Too few parameter values were provided' });
  assert.throws(() => db.prepare('SELECT ? AS a').get(1, 2), { name: 'RangeError', message: 'Too many parameter values were provided' });
  assert.throws(() => db.prepare('SELECT @a AS a').get({ a: 1 }, { a: 2 }), { name: 'TypeError', message: 'You cannot specify named parameters in two different objects' });
  assert.throws(() => db.prepare('SELECT @a AS a').get(new Date()), { name: 'TypeError', message: 'Named parameters can only be passed within plain objects' });
  assert.throws(() => db.prepare('SELECT ? AS a').get(true), { name: 'TypeError', message: 'SQLite3 can only bind numbers, strings, bigints, buffers, and null' });
  assert.throws(() => db.prepare('SELECT ? AS a').get(() => 1), { name: 'TypeError', message: 'SQLite3 can only bind numbers, strings, bigints, buffers, and null' });
  assert.throws(() => db.prepare('SELECT ? AS a').get(2n ** 64n), { name: 'RangeError', message: 'The bound string, buffer, or bigint is too big' });
  assert.throws(() => db.prepare('SELECT @a AS a').get({ a: Symbol('s') }), { name: 'TypeError' });
});

test('binds null, undefined, bigint, Buffer, and Uint8Array; BLOBs read back as Buffer', () => {
  const db = fresh();
  assert.deepEqual(db.prepare('SELECT ? AS a, @b AS b').get(undefined, { b: null }), { a: null, b: null });
  assert.deepEqual(db.prepare('SELECT ? AS a').get(42n), { a: 42 });
  assert.deepEqual(db.prepare('SELECT typeof(?) AS t').get(1.5), { t: 'real' });

  db.prepare('UPDATE t SET data = ? WHERE id = 1').run(Buffer.from('hello'));
  db.prepare('UPDATE t SET data = ? WHERE id = 2').run(new Uint8Array([1, 2, 3]));
  const blob = db.prepare('SELECT data FROM t WHERE id = 1').pluck().get();
  assert.ok(Buffer.isBuffer(blob));
  assert.equal(blob.toString(), 'hello');
  assert.deepEqual(db.prepare('SELECT data FROM t WHERE id = 2').pluck().get(), Buffer.from([1, 2, 3]));
  assert.deepEqual(db.prepare('SELECT id, data FROM t WHERE id = 1').raw().get(), [1, Buffer.from('hello')]);
  assert.deepEqual(db.prepare('SELECT data FROM t WHERE id = 1').expand().get(), { t: { data: Buffer.from('hello') } });
});

test('parameter scanner ignores placeholders inside strings, identifiers, and comments', () => {
  const db = fresh();
  const stmt = db.prepare(`
    SELECT '?' AS q, 1 AS "@a", 2 AS [$b], 3 AS \`:c\`, x'ff' AS blob, -- ? @nope
      /* ? @nope2 */ 'it''s' AS escaped, @a AS a, ? AS p
    FROM t WHERE id = 1
  `);
  const row = stmt.get({ a: 1 }, 2);
  assert.equal(row.q, '?');
  assert.equal(row['@a'], 1);
  assert.equal(row.$b, 2);
  assert.equal(row[':c'], 3);
  assert.equal(row.a, 1);
  assert.equal(row.p, 2);
  assert.equal(row.escaped, "it's");
  assert.deepEqual(row.blob, Buffer.from([0xff]));
  assert.throws(() => stmt.get({ a: 1, nope: 0, nope2: 0 }), { message: 'Too few parameter values were provided' }, 'commented placeholders are not parameters');
});

test('bind() fixes parameters permanently and only once', () => {
  const db = fresh();
  const stmt = db.prepare('SELECT name FROM t WHERE id = @id').bind({ id: 2 });
  assert.deepEqual(stmt.get(), { name: 'beta' });
  assert.deepEqual(stmt.all(), [{ name: 'beta' }]);
  assert.throws(() => stmt.get({ id: 1 }), { message: 'This statement already has bound parameters' });
  assert.throws(() => stmt.bind({ id: 1 }), { message: 'The bind() method can only be invoked once per statement object' });
  assert.throws(() => db.prepare('SELECT ?').bind(), { name: 'RangeError', message: 'Too few parameter values were provided' });
});

// ---------------------------------------------------------------- row modes

test('pluck, raw, expand, and switching back', () => {
  const db = fresh();
  const stmt = db.prepare('SELECT t.id, name, n * 2 AS twice FROM t WHERE id = 1');
  assert.equal(stmt.pluck().get(), 1);
  assert.deepEqual(stmt.raw().get(), [1, 'alpha', 2]);
  assert.deepEqual(stmt.expand().get(), { t: { id: 1, name: 'alpha' }, $: { twice: 2 } });
  assert.deepEqual(stmt.expand(false).get(), { id: 1, name: 'alpha', twice: 2 });
  assert.deepEqual(stmt.raw().pluck(false).get(), [1, 'alpha', 2], 'turning off a mode that is not active keeps the active one');
  assert.deepEqual(stmt.raw(false).get(), { id: 1, name: 'alpha', twice: 2 });
  assert.deepEqual(db.prepare('SELECT name FROM t ORDER BY id').pluck().all(), ['alpha', 'beta', 'gamma']);
  assert.deepEqual(db.prepare('SELECT id, name FROM t ORDER BY id LIMIT 2').raw().all(), [[1, 'alpha'], [2, 'beta']]);
  assert.deepEqual([...db.prepare('SELECT id FROM t ORDER BY id').pluck().iterate()], [1, 2, 3]);
});

test('safeIntegers per statement and by default', () => {
  const db = fresh();
  db.exec('INSERT INTO t (name, n) VALUES (\'big\', 9007199254740993)');
  const stmt = db.prepare("SELECT n FROM t WHERE name = 'big'").pluck();
  assert.throws(() => stmt.get(), { name: 'RangeError', message: /safeIntegers/ });
  assert.equal(stmt.safeIntegers().get(), 9007199254740993n);
  assert.equal(stmt.safeIntegers(false), stmt);
  assert.throws(() => stmt.get(), { name: 'RangeError' });

  assert.equal(db.prepare('SELECT 1 AS one').pluck().get(), 1);
  db.defaultSafeIntegers();
  assert.equal(db.prepare('SELECT 1 AS one').pluck().get(), 1n);
  assert.equal(db.prepare("SELECT n FROM t WHERE name = 'big'").pluck().get(), 9007199254740993n);
  assert.equal(db.prepare('SELECT 1 AS one').safeIntegers(false).pluck().get(), 1);
  db.defaultSafeIntegers(false);
  assert.equal(db.prepare('SELECT 1 AS one').pluck().get(), 1);
});

// ---------------------------------------------------------------- errors

test('SQLite errors carry named result codes', () => {
  const db = fresh();
  const insert = db.prepare('INSERT INTO t (id, name, n) VALUES (@id, @name, @n)');
  const expectCode = (fn, code) => assert.throws(fn, (err) => err instanceof SqliteError && err.name === 'SqliteError' && err.code === code);
  expectCode(() => insert.run({ id: null, name: 'ALPHA', n: 0 }), 'SQLITE_CONSTRAINT_UNIQUE');
  expectCode(() => insert.run({ id: 1, name: 'new', n: 0 }), 'SQLITE_CONSTRAINT_PRIMARYKEY');
  expectCode(() => insert.run({ id: null, name: null, n: 0 }), 'SQLITE_CONSTRAINT_NOTNULL');
  expectCode(() => insert.run({ id: null, name: 'neg', n: -1 }), 'SQLITE_CONSTRAINT_CHECK');
  expectCode(() => db.exec('SELECT * FROM missing'), 'SQLITE_ERROR');
  expectCode(() => db.prepare('SELECT json(?)').get('{'), 'SQLITE_ERROR');

  db.exec('CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (pid REFERENCES parent(id))');
  expectCode(() => db.exec('INSERT INTO child VALUES (99)'), 'SQLITE_CONSTRAINT_FOREIGNKEY');

  try {
    insert.run({ id: null, name: 'ALPHA', n: 0 });
  } catch (err) {
    assert.equal(err.message, 'UNIQUE constraint failed: t.name');
    assert.ok(err.stack.includes('test.js'), 'stack points at the caller');
  }
});

// ---------------------------------------------------------------- transactions

test('transaction commits on return and rolls back on throw', () => {
  const db = fresh();
  const insert = db.prepare('INSERT INTO t (name) VALUES (?)');
  const count = () => db.prepare('SELECT count(*) FROM t').pluck().get();

  const insertMany = db.transaction(function (names, extra) {
    for (const name of names) insert.run(name);
    return `${this.tag}:${names.length + extra}`;
  });
  assert.equal(insertMany.call({ tag: 'ok' }, ['d', 'e'], 1), 'ok:3', 'this and arguments pass through, result is returned');
  assert.equal(count(), 5);
  assert.equal(db.inTransaction, false);

  assert.throws(() => insertMany(['f', 'ALPHA']), { code: 'SQLITE_CONSTRAINT_UNIQUE' });
  assert.equal(count(), 5, 'f was rolled back with the failure');
  assert.equal(db.inTransaction, false);

  assert.throws(() => db.transaction(async () => {})(), { message: 'Transaction function cannot return a promise' });
  assert.throws(() => db.transaction('nope'), { message: 'Expected first argument to be a function' });
});

test('transaction variants and nesting via savepoints', () => {
  const db = fresh();
  const insert = db.prepare('INSERT INTO t (name) VALUES (?)');
  const names = () => db.prepare('SELECT name FROM t WHERE id > 3 ORDER BY id').pluck().all();

  const inner = db.transaction((name, fail) => {
    insert.run(name);
    assert.equal(db.inTransaction, true);
    if (fail) throw new Error('inner failed');
  });
  const outer = db.transaction(() => {
    insert.run('outer-1');
    inner('inner-ok');
    assert.throws(() => inner('inner-bad', true), { message: 'inner failed' });
    assert.equal(db.inTransaction, true, 'outer transaction survives the inner rollback');
    insert.run('outer-2');
  });
  outer();
  assert.deepEqual(names(), ['outer-1', 'inner-ok', 'outer-2']);

  for (const variant of ['default', 'deferred', 'immediate', 'exclusive']) {
    assert.equal(typeof outer[variant], 'function');
    assert.equal(outer[variant].database, db);
    assert.equal(outer[variant].deferred, outer.deferred, 'variants are shared across each other');
  }
  outer.immediate.database.exec("DELETE FROM t WHERE name LIKE 'outer-%' OR name LIKE 'inner-%'");
  outer.exclusive();
  assert.deepEqual(names(), ['outer-1', 'inner-ok', 'outer-2']);
});

test('inTransaction reflects transactions started with raw SQL too', () => {
  const db = fresh();
  assert.equal(db.inTransaction, false);
  db.exec('BEGIN');
  assert.equal(db.inTransaction, true);
  db.prepare("INSERT INTO t (name) VALUES ('tx')").run();
  db.exec('ROLLBACK');
  assert.equal(db.inTransaction, false);
  assert.equal(db.prepare("SELECT count(*) FROM t WHERE name = 'tx'").pluck().get(), 0);
  db.exec('SAVEPOINT sp');
  assert.equal(db.inTransaction, true);
  db.exec('RELEASE sp');
  assert.equal(db.inTransaction, false);
});

// ---------------------------------------------------------------- user functions

test('function() registers scalar functions with options', () => {
  const db = fresh();
  assert.equal(db.function('twice', (x) => x * 2), db);
  assert.equal(db.prepare('SELECT twice(n) FROM t WHERE id = 3').pluck().get(), 6);

  db.function('join_all', { varargs: true }, (...args) => args.join('|'));
  assert.equal(db.prepare("SELECT join_all('a', 1, NULL)").pluck().get(), 'a|1|');

  db.function('det', { deterministic: true }, () => 1);
  db.exec('CREATE INDEX t_det ON t (det())');

  db.function('kind', { safeIntegers: true }, (x) => typeof x);
  assert.equal(db.prepare('SELECT kind(1)').pluck().get(), 'bigint');

  db.function('explode', () => { throw new Error('kaboom'); });
  assert.throws(() => db.prepare('SELECT explode()').get(), { message: /kaboom/ });

  assert.throws(() => db.function(1, () => {}), { name: 'TypeError' });
  assert.throws(() => db.function('x', 'nope'), { name: 'TypeError' });
  assert.throws(() => db.function('', () => {}), { message: 'User-defined function name cannot be an empty string' });
  assert.throws(() => db.function('x', { varargs: 'y' }, () => {}), { message: 'Expected the "varargs" option to be a boolean' });
});

const { engine } = Database;

test('Database.engine reports what the running node:sqlite supports', () => {
  assert.deepEqual(Object.keys(engine).sort(), ['aggregate', 'arrays', 'backup', 'defensive', 'deserialize', 'isTransaction', 'serialize', 'sessions']);
  assert.ok(Object.values(engine).every((v) => typeof v === 'boolean'));
  assert.ok(Object.isFrozen(engine));
});

test('aggregate() follows better-sqlite3 semantics', { skip: !engine.aggregate && 'engine lacks DatabaseSync#aggregate' }, () => {
  const db = fresh();
  db.aggregate('collect', { start: () => [], step: (acc, v) => { acc.push(v); }, result: (acc) => JSON.stringify(acc) });
  assert.equal(db.prepare('SELECT collect(n) FROM t').pluck().get(), '[1,2,3]', 'undefined from step keeps the accumulator');
  db.aggregate('total', { start: 100, step: (acc, v) => acc + v });
  assert.equal(db.prepare('SELECT total(n) FROM t').pluck().get(), 106, 'a plain start value');
  db.aggregate('wsum', { start: 0, step: (acc, v) => acc + v, inverse: (acc, v) => acc - v });
  assert.deepEqual(db.prepare('SELECT wsum(n) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t').pluck().all(), [1, 3, 5]);
  assert.throws(() => db.prepare('SELECT total(n, n) FROM t').get(), { name: 'SqliteError' }, 'argument count comes from step.length');
  db.aggregate('any', { varargs: true, start: 0, step: (acc, ...args) => acc + args.length });
  assert.equal(db.prepare('SELECT any(n, n, n) FROM t').pluck().get(), 9);
  db.aggregate('big', { safeIntegers: true, start: 0n, step: (acc, v) => acc + v });
  assert.equal(db.prepare('SELECT big(n) FROM t').pluck().safeIntegers().get(), 6n);
  assert.throws(() => db.aggregate('bad', { step: 1 }), { message: 'Expected the "step" option to be a function' });
  assert.throws(() => db.aggregate('bad', {}), { message: 'Missing required option "step"' });
  assert.throws(() => db.aggregate('', { step: () => {} }), { message: 'User-defined function name cannot be an empty string' });
});

test('aggregate() reports clearly when the engine lacks it', { skip: engine.aggregate && 'engine has aggregate()' }, () => {
  const db = fresh();
  assert.throws(() => db.aggregate('x', { start: 0, step: (a) => a }), { name: 'TypeError', message: /aggregate\(\) is not supported by node:sqlite on v\d+/ });
});

test('table() is unavailable; serialize() and Buffer databases follow the engine', () => {
  const db = fresh();
  assert.throws(() => db.table('vt'), { name: 'TypeError', message: /virtual tables.*not supported by node:sqlite/ });
  assert.throws(() => db.table(''), { message: 'Virtual table module name cannot be an empty string' });
  if (engine.serialize) {
    const bytes = db.serialize();
    assert.ok(Buffer.isBuffer(bytes));
    assert.equal(bytes.subarray(0, 15).toString(), 'SQLite format 3');
    assert.throws(() => db.serialize({ attached: 1 }), { message: 'Expected the "attached" option to be a string' });
    assert.throws(() => db.serialize({ attached: '' }), { message: 'The "attached" option cannot be an empty string' });
    if (engine.deserialize) {
      const copy = new Database(bytes);
      assert.equal(copy.memory, true);
      assert.equal(copy.name, ':memory:');
      assert.deepEqual(copy.prepare('SELECT name FROM t ORDER BY id').pluck().all(), ['alpha', 'beta', 'gamma']);
      copy.exec("INSERT INTO t (name) VALUES ('delta')");
      copy.close();
      const frozen = new Database(bytes, { readonly: true });
      assert.equal(frozen.readonly, true);
      assert.throws(() => frozen.exec("INSERT INTO t (name) VALUES ('x')"), { name: 'SqliteError', code: 'SQLITE_READONLY' });
      frozen.close();
    }
  } else {
    assert.throws(() => db.serialize(), { message: /serialize\(\) is not supported by node:sqlite on v\d+/ });
  }
  if (!engine.deserialize) {
    assert.throws(() => new Database(Buffer.alloc(0)), { name: 'TypeError', message: /not supported/ });
  }
});

test('loadExtension validates input and surfaces SQLite errors', () => {
  const db = fresh();
  assert.throws(() => db.loadExtension(1), { name: 'TypeError' });
  assert.throws(() => db.loadExtension(tmpFile('no-such-extension')), { name: 'SqliteError', code: 'SQLITE_ERROR' });
  assert.equal(db.prepare('SELECT 1').pluck().get(), 1, 'connection still usable');
});

// ---------------------------------------------------------------- backup

test('backup() copies the database to a file and reports progress', async () => {
  const db = fresh();
  const dest = tmpFile('backup.db');
  const result = await db.backup(dest);
  assert.equal(result.remainingPages, 0);
  assert.ok(result.totalPages > 0);

  const copy = new Database(dest, { fileMustExist: true });
  assert.deepEqual(copy.prepare('SELECT name FROM t ORDER BY id').pluck().all(), ['alpha', 'beta', 'gamma']);
  copy.close();

  // Progress fires after each 100-page step that leaves pages behind, so it
  // needs a database larger than one step.
  const insert = db.prepare('INSERT INTO t (name, data) VALUES (?, ?)');
  const fill = db.transaction(() => { for (let i = 0; i < 300; i++) insert.run(`row-${i}`, Buffer.alloc(4000, 1)); });
  fill();
  const steps = [];
  const big = await db.backup(tmpFile('backup-big.db'), { progress: (p) => steps.push(p) });
  assert.ok(big.totalPages > 200, `totalPages=${big.totalPages}`);
  assert.ok(steps.length >= 3, `progress calls=${steps.length}`);
  assert.deepEqual(steps[0], { totalPages: big.totalPages, remainingPages: big.totalPages }, 'first call reports the whole database');
  assert.ok(steps.slice(1).every((p) => p.totalPages === big.totalPages && p.remainingPages > 0 && p.remainingPages < p.totalPages));

  // A throwing progress callback rejects and removes a destination it created.
  const aborted = tmpFile('backup-aborted.db');
  await assert.rejects(db.backup(aborted, { progress: () => { throw new Error('stop'); } }), { message: 'stop' });
  assert.equal(fs.existsSync(aborted), false);
  await assert.rejects(db.backup(aborted, { progress: () => 'x' }), { name: 'TypeError', message: 'Expected progress callback to return a number or undefined' });
  // A number returned from the first call sets the pages per step.
  const fine = [];
  await db.backup(tmpFile('backup-fine.db'), { progress: (p) => { fine.push(p.remainingPages); return 1000; } });
  assert.equal(fine.length, 1, 'one step of 1000 pages copies everything');

  await assert.rejects(db.backup(1), { name: 'TypeError', message: 'Expected first argument to be a string' });
  await assert.rejects(db.backup('  '), { message: 'Backup filename cannot be an empty string' });
  await assert.rejects(db.backup(':memory:'), { message: 'Invalid backup filename ":memory:"' });
  await assert.rejects(db.backup(dest, { attached: '' }), { message: 'The "attached" option cannot be an empty string' });
  await assert.rejects(db.backup(dest, { attached: 1 }), { message: 'Expected the "attached" option to be a string' });
  await assert.rejects(db.backup(dest, { progress: 1 }), { message: 'Expected the "progress" option to be a function' });
  await assert.rejects(db.backup(path.join(tmp, 'missing', 'x.db')), { message: 'Cannot save backup because the directory does not exist' });
  await assert.rejects(db.backup(dest, { attached: 'nope' }), { name: 'SqliteError' });
});

// ---------------------------------------------------------------- close

test('close() is idempotent and later use throws a TypeError', () => {
  const db = fresh();
  const stmt = db.prepare('SELECT 1');
  assert.equal(db.close(), db);
  assert.equal(db.open, false);
  assert.equal(db.inTransaction, false);
  assert.equal(db.close(), db);
  assert.throws(() => db.prepare('SELECT 1'), { name: 'TypeError', message: 'The database connection is not open' });
  assert.throws(() => db.exec('SELECT 1'), { name: 'TypeError', message: 'The database connection is not open' });
  assert.throws(() => stmt.get(), { name: 'TypeError' });
  assert.throws(() => db.loadExtension('x'), { message: 'The database connection is not open' });
  assert.throws(() => db.transaction(() => {})(), { name: 'TypeError' });
});

test('unsafeMode and defaultSafeIntegers are chainable', () => {
  const db = fresh();
  assert.equal(db.unsafeMode(), db);
  assert.equal(db.unsafeMode(false), db);
  assert.equal(db.defaultSafeIntegers(false), db);
});

test('does not print the node:sqlite ExperimentalWarning', () => {
  const { spawnSync } = require('node:child_process');
  const script = 'require(process.argv[1]); process.emitWarning("still works", "DeprecationWarning")';
  const child = spawnSync(process.execPath, ['-e', script, path.join(__dirname, '..', 'index.js')], { encoding: 'utf8' });
  assert.equal(child.status, 0, child.stderr);
  assert.doesNotMatch(child.stderr, /ExperimentalWarning/);
  assert.match(child.stderr, /DeprecationWarning: still works/, 'other warnings still print');
});

// ---------------------------------------------------------------- integrity guards

test('close(), exec(), pragma() and writes are refused while a statement is iterating', () => {
  const db = fresh();
  const reader = db.prepare('SELECT name FROM t ORDER BY id');
  const writer = db.prepare('UPDATE t SET n = n + 1');
  let rows = 0;
  for (const row of reader.iterate()) {
    rows++;
    assert.ok(row.name);
    assert.throws(() => db.close(), { name: 'TypeError', message: 'This database connection is busy executing a query' });
    assert.throws(() => db.exec('SELECT 1'), { name: 'TypeError' });
    assert.throws(() => db.pragma('user_version'), { name: 'TypeError' });
    assert.throws(() => writer.run(), { name: 'TypeError' });
    assert.throws(() => db.transaction(() => {})(), { name: 'TypeError' });
    assert.throws(() => db.function(`f${rows}`, () => 1), { name: 'TypeError' });
    assert.throws(() => reader.all(), { message: 'This statement is busy executing a query' });
    assert.throws(() => reader.pluck(), { message: 'This statement is busy executing a query' });
    // Reads on other statements, prepare() and metadata stay allowed.
    assert.equal(db.prepare('SELECT count(*) AS c FROM t').get().c, 3);
    assert.equal(reader.columns().length, 1);
    assert.equal(db.open, true);
  }
  assert.equal(rows, 3);
  assert.equal(writer.run().changes, 3);
  db.close();
});

test('unsafeMode() is accepted but never lifts the iterator guard or the defensive flag', () => {
  const db = fresh();
  assert.equal(db.unsafeMode(), db);
  assert.equal(db.unsafeMode(true), db);
  assert.throws(() => db.unsafeMode('yes'), { name: 'TypeError' });

  const writer = db.prepare('UPDATE t SET n = n + 1');
  let rows = 0;
  for (const row of db.prepare('SELECT id FROM t').iterate()) {
    assert.ok(row.id);
    assert.throws(() => writer.run(), { name: 'TypeError', message: 'This database connection is busy executing a query' });
    assert.throws(() => db.exec('SELECT 1'), { name: 'TypeError' });
    assert.throws(() => db.pragma('user_version'), { name: 'TypeError' });
    assert.throws(() => db.close(), { name: 'TypeError' });
    rows++;
  }
  assert.equal(rows, 3);
  assert.equal(writer.run().changes, 3, 'the connection is usable again once the iterator finishes');

  if (engine.defensive) {
    // With the defensive flag pinned on, journal_mode = OFF and
    // writable_schema = ON are ignored whatever unsafeMode() was told.
    db.unsafeMode(true);
    db.pragma('journal_mode = OFF');
    db.pragma('writable_schema = ON');
    assert.notEqual(db.pragma('journal_mode', { simple: true }), 'off');
    assert.throws(() => db.exec("UPDATE sqlite_master SET name = 'x' WHERE name = 't'"), { name: 'SqliteError' });
  }
  db.close();
});

test('the connection counts as busy inside user-defined functions', () => {
  const db = fresh();
  let checked = false;
  db.function('probe', () => {
    assert.throws(() => db.close(), { message: 'This database connection is busy executing a query' });
    assert.throws(() => db.prepare('SELECT 1'), { name: 'TypeError' });
    assert.throws(() => db.exec('SELECT 1'), { name: 'TypeError' });
    assert.throws(() => db.prepare('SELECT 1'), { name: 'TypeError' });
    checked = true;
    return 1;
  });
  assert.equal(db.prepare('SELECT probe() AS v').get().v, 1);
  assert.equal(checked, true);
  assert.equal(db.open, true);
  db.close();
});

test('iterators are plain objects without throw(); errors mid-iteration release the statement', () => {
  const db = fresh();
  db.function('boom', (v) => { if (v === 2) throw new Error('boom'); return v; });
  const stmt = db.prepare('SELECT boom(n) AS v FROM t ORDER BY id');
  const it = stmt.iterate();
  assert.equal(typeof it.throw, 'undefined');
  assert.equal(it[Symbol.iterator](), it);
  assert.throws(() => { for (const row of it) assert.equal(row.v, 1); }, { message: 'boom' });
  assert.equal(stmt.busy, false);
  assert.deepEqual(it.next(), { value: undefined, done: true });
  db.close();
});

// ---------------------------------------------------------------- reference-behaviour details

test('run() reports 0 changes for statements that do not modify rows', () => {
  const db = fresh();
  assert.equal(db.prepare('UPDATE t SET n = n + 1').run().changes, 3);
  assert.equal(db.prepare('BEGIN').run().changes, 0);
  assert.equal(db.prepare('COMMIT').run().changes, 0);
  assert.equal(db.prepare('CREATE TABLE x (a)').run().changes, 0);
  assert.equal(db.prepare('SELECT * FROM t').run().changes, 0);
  assert.equal(db.prepare('DROP TABLE x').run().changes, 0);
  const info = db.defaultSafeIntegers().prepare("INSERT INTO t (name) VALUES ('z')").run();
  assert.equal(info.changes, 1, 'changes stays a number under safeIntegers');
  assert.equal(info.lastInsertRowid, 4n);
});

test('toString() returns the source, or the bound parameters expanded', () => {
  const db = fresh();
  const plain = db.prepare('SELECT * FROM t WHERE name = ? AND n = @n');
  assert.equal(plain.toString(), 'SELECT * FROM t WHERE name = ? AND n = @n');
  assert.equal(String(plain), plain.source);
  plain.get('alpha', { n: 1 });
  assert.equal(plain.toString(), plain.source, 'temporary parameters are not kept');
  const bound = db.prepare('INSERT INTO t (name, n, data) VALUES (?, ?, ?)').bind("it's", 25, Buffer.from([0xde, 0xad]));
  assert.equal(bound.toString(), "INSERT INTO t (name, n, data) VALUES ('it''s', 25.0, x'dead')");
  const long = db.prepare('SELECT ?').bind('x'.repeat(40));
  assert.equal(long.toString(), `SELECT '${'x'.repeat(32)}'/*+8 bytes*/`);
  const named = db.prepare('SELECT @a AS a, $b AS b, :c AS c').bind({ a: null, b: 1.5, c: 10n });
  assert.equal(named.toString(), 'SELECT NULL AS a, 1.5 AS b, 10 AS c');
  db.close();
  assert.equal(bound.toString(), bound.source, 'the source again once closed');
});

test('verbose fires before execution and a throwing logger aborts the statement', () => {
  const calls = [];
  let fail = false;
  const db = new Database(':memory:', { verbose(...args) { calls.push([this, ...args]); if (fail) throw new Error('logger'); } });
  db.exec('CREATE TABLE v (a)');
  const insert = db.prepare('INSERT INTO v VALUES (?)');
  insert.run('x');
  db.prepare('SELECT a FROM v').iterate().return();  // never stepped: not logged
  for (const row of db.prepare('SELECT a FROM v').iterate()) assert.equal(row.a, 'x');
  assert.deepEqual(calls, [
    [undefined, 'CREATE TABLE v (a)'],
    [undefined, "INSERT INTO v VALUES ('x')"],
    [undefined, 'SELECT a FROM v']
  ]);
  fail = true;
  assert.throws(() => insert.run('y'), { message: 'logger' });
  assert.throws(() => db.exec('DELETE FROM v'), { message: 'logger' });
  fail = false;
  assert.equal(db.prepare('SELECT count(*) AS c FROM v').get().c, 1, 'aborted statements did not run');
});

test('explain() returns the bytecode program or the query plan, with parameters unbound', () => {
  const db = fresh();
  const program = db.explain('SELECT * FROM t WHERE name = ? AND n = @n');
  assert.ok(program.length > 0);
  assert.deepEqual(Object.keys(program[0]), ['addr', 'opcode', 'p1', 'p2', 'p3', 'p4', 'p5', 'comment']);
  assert.ok(program.some((row) => row.opcode === 'Halt'));
  const plan = db.explain('QUERY PLAN SELECT * FROM t WHERE id = 1');
  assert.deepEqual(Object.keys(plan[0]), ['id', 'parent', 'notused', 'detail']);
  assert.throws(() => db.explain('SELECT * FROM nope'), { name: 'SqliteError', code: 'SQLITE_ERROR' });
  assert.throws(() => db.explain(1), { name: 'TypeError' });
});

test('prepare() and pragma() reject empty and multi-statement SQL with a RangeError', () => {
  const db = fresh();
  assert.throws(() => db.prepare(''), { name: 'RangeError', message: 'The supplied SQL string contains no statements' });
  assert.throws(() => db.prepare('SELECT 1; SELECT 2'), { name: 'RangeError', message: 'The supplied SQL string contains more than one statement' });
  assert.throws(() => db.pragma('user_version; PRAGMA user_version'), { name: 'RangeError' });
  assert.throws(() => db.pragma('PRAGMA user_version'), { name: 'SqliteError' });
});

test('bind() copies buffers so later mutation does not leak into the statement', () => {
  const db = fresh();
  const buffer = Buffer.alloc(4, 0xdd);
  db.prepare('INSERT INTO t (name, data) VALUES (?, ?)').bind('buf', buffer).run();
  buffer.fill(0xaa);
  assert.deepEqual(db.prepare("SELECT data FROM t WHERE name = 'buf'").pluck().get(), Buffer.alloc(4, 0xdd));
});

test('function() validates function.length and reports unconvertible return values as TypeError', () => {
  const db = fresh();
  const withLength = (length) => Object.defineProperty(() => 1, 'length', { value: length });
  assert.throws(() => db.function('a', withLength(-1)), { name: 'TypeError', message: 'Expected function.length to be a positive integer' });
  assert.throws(() => db.function('b', withLength(1.5)), { name: 'TypeError' });
  assert.throws(() => db.function('c', withLength(101)), { name: 'RangeError' });
  db.function('obj', () => ({}));
  assert.throws(() => db.prepare('SELECT obj()').get(), { name: 'TypeError' });
  db.function('two', (a, b) => a + b);
  assert.throws(() => db.prepare('SELECT two(1)').get(), { name: 'SqliteError' });
  assert.equal(db.prepare('SELECT two(1, 2) AS v').get().v, 3);
});

test('duplicate column names in object, raw() and expand() rows', () => {
  const db = fresh();
  db.exec('CREATE TABLE u (id INTEGER PRIMARY KEY, v); INSERT INTO u VALUES (2, 20), (3, 30), (4, 40)');
  const stmt = db.prepare('SELECT t.id, u.id, t.n + 1 AS n FROM t JOIN u ON u.id = t.id + 1 ORDER BY t.id');
  assert.deepEqual(stmt.get(), { id: 2, n: 2 }, 'the last duplicate wins, as in better-sqlite3');
  if (engine.arrays) {
    assert.deepEqual(stmt.raw().get(), [1, 2, 2]);
    assert.deepEqual(stmt.raw(false).expand().get(), { t: { id: 1 }, u: { id: 2 }, $: { n: 2 } });
  } else {
    assert.deepEqual(stmt.raw().get(), [2, 2, 2], 'rows arrive keyed by name on this engine');
    assert.deepEqual(stmt.raw(false).expand().get(), { t: { id: 2 }, u: { id: 2 }, $: { n: 2 } });
  }
});

test('inTransaction while iterating and after close', () => {
  const db = fresh();
  db.exec('BEGIN');
  for (const row of db.prepare('SELECT id FROM t').iterate()) {
    assert.ok(row.id);
    assert.equal(db.inTransaction, true);
  }
  db.exec('COMMIT');
  db.close();
  assert.equal(db.inTransaction, false);
});
