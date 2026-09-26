# better-sqlite3

The [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) API, implemented
on Node's built-in `node:sqlite` module. Same classes, same method names, same
parameter binding rules, same error codes and messages. No native addon, no
`node-gyp`, no C++ toolchain.

This is an internal package for our own services and for our partners;
Everything better-sqlite3 offers that the engine underneath can deliver is
here, not only the calls the dashboard makes today. A JavaScript
implementation, not a fork. The `1.x` line is this package's own; the API it
mirrors is better-sqlite3 v13.0.3.

## Requirements

Node 22.13 or later (the releases where `node:sqlite` loads without a flag).
Developed on Node 23.11; the unit and conformance suites run on Node 22, 23 and
24 (`npm run test:matrix`, `npm run test:conformance`).

## Install

The package is not on a registry. Link it from a sibling checkout:

```json
"dependencies": {
  "better-sqlite3": "file:../better-sqlite3"
}
```

### Collision check

On first load, this package warns on stderr (once per process) if another
SQLite driver is present in the host project. There is no install hook — the
check runs at runtime and is advisory only: it prints a notice and never fails
your install or crashes your process. It reads the host project's
`package.json`, lockfile (`package-lock.json`, `npm-shrinkwrap.json`,
`yarn.lock`, `pnpm-lock.yaml`) and `node_modules`, and reports `better-sqlite3`,
`sqlite3`, `sqlite`, `sql.js`, `@libsql/client`, `libsql`,
`@sqlite.org/sqlite-wasm`, `node-sqlite3-wasm`, `@vscode/sqlite3`,
`@journeyapps/sqlcipher` or `better-sqlite3-multiple-ciphers`. Two engines in
one process means two SQLite builds, two error hierarchies and two
near-identical module names. The message names each package and where it was
found. Silence it with `BETTER_SQLITE_COLLISION=off`, or in the host
`package.json` with `"better-sqlite3": { "collisions": "off" }` (or an `"allow"`
list of driver names to permit).

The check is local-only: no network, no child processes, and any internal
error is swallowed so it can never affect your process.

To keep a second driver on purpose, set the mode via the env var or the
consuming project's `package.json` (the env var wins if both are set):

```json
"better-sqlite3": {
  "collisions": "off",
  "allow": ["sqlite3"]
}
```

The `allow` list drops named packages from the report; `collisions` defaults
to `error`. At runtime `error` and `warn` both simply warn on stderr — a
loaded library cannot fail anything — and `off` is silent.

## Usage

```js
const Database = require('better-sqlite3');

const db = new Database('data/app.db');
db.pragma('journal_mode = WAL');
db.exec(`CREATE TABLE IF NOT EXISTS services (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  name   TEXT NOT NULL UNIQUE COLLATE NOCASE,
  owner  TEXT NOT NULL
)`);

const insert = db.prepare('INSERT INTO services (name, owner) VALUES (@name, @owner)');
const { lastInsertRowid } = insert.run({ name: 'Search API', owner: 'Platform' });

db.prepare('SELECT * FROM services WHERE id = ?').get(lastInsertRowid);
// { id: 1, name: 'Search API', owner: 'Platform' }

const insertMany = db.transaction((rows) => { for (const r of rows) insert.run(r); });
insertMany([{ name: 'Billing', owner: 'Finance' }, { name: 'Auth', owner: 'Platform' }]);

try {
  insert.run({ name: 'search api', owner: 'X' });
} catch (err) {
  err instanceof Database.SqliteError;   // true
  err.code;                              // 'SQLITE_CONSTRAINT_UNIQUE'
}
```

## API

### `new Database(filename, [options])`

`filename` is a path, `':memory:'`, or `''` (temporary on-disk database). The
constructor also works without `new`.

| Option          | Default | Meaning                                                  |
|-----------------|---------|----------------------------------------------------------|
| `readonly`      | `false` | Open read-only                                           |
| `fileMustExist` | `false` | Throw `SQLITE_CANTOPEN` instead of creating the file     |
| `timeout`       | `5000`  | Milliseconds to wait on a locked database (busy_timeout) |
| `verbose`       | `null`  | Function called with every SQL string executed           |

Properties: `name`, `open`, `inTransaction`, `readonly`, `memory`.

Methods, all returning the database unless noted:

| Method                               | Notes                                                                    |
|--------------------------------------|--------------------------------------------------------------------------|
| `prepare(sql)`                       | Returns a `Statement`. Exactly one statement per string                  |
| `exec(sql)`                          | Runs any number of statements, no parameters                             |
| `pragma(string, { simple })`         | `db.pragma('journal_mode')` → `[{ journal_mode: 'wal' }]`; `simple` unwraps the first value |
| `transaction(fn)`                    | Returns a function that runs `fn` in a transaction; see below            |
| `function(name, [options], fn)`      | Scalar SQL function. Options: `varargs`, `deterministic`, `directOnly`, `safeIntegers` |
| `aggregate(name, options)`           | Passed to the engine when it has `DatabaseSync#aggregate`; throws on Node 23.11 |
| `loadExtension(path)`                | Loads a SQLite extension; loading is enabled only for the duration of the call |
| `backup(destination, { attached, progress })` | Promise of `{ totalPages, remainingPages: 0 }`. `progress` gets `{ totalPages, remainingPages }` after each step that leaves pages |
| `serialize()`                        | Passed to the engine when it has `DatabaseSync#serialize`; throws on Node 23.11 |
| `table(name, definition)`            | Virtual tables have no `node:sqlite` counterpart; throws                 |
| `defaultSafeIntegers([toggle])`      | New statements return INTEGERs as `BigInt`                               |
| `unsafeMode([toggle])`               | Accepted for API parity and returns the database; never lifts any guard (see below) |
| `close()`                            | Idempotent. Later use throws `TypeError: The database connection is not open` |

### `Statement`

Properties: `database`, `source`, `reader` (whether it returns rows), `busy`.

| Method                | Returns                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| `run(...params)`      | `{ changes, lastInsertRowid }`                                          |
| `get(...params)`      | First row, or `undefined`                                               |
| `all(...params)`      | Array of rows                                                           |
| `iterate(...params)`  | Iterator of rows; the statement is `busy` until it finishes or `return()`s |
| `pluck([toggle])`     | Rows become the first column's value                                    |
| `raw([toggle])`       | Rows become arrays                                                      |
| `expand([toggle])`    | Rows become `{ table: { column: value }, $: { expression: value } }`     |
| `safeIntegers([toggle])` | INTEGERs as `BigInt`                                                 |
| `columns()`           | `[{ name, column, table, database, type }]`                             |
| `bind(...params)`     | Binds parameters permanently; allowed once                              |

`get`, `all`, `iterate` and the row-mode toggles throw a `TypeError` on
statements that return no data (`INSERT` without `RETURNING`, `CREATE`, ...).

### Parameters

Identical to better-sqlite3:

```js
stmt.run(1, 'two');                 // positional, for ?
stmt.run([1, 'two']);               // arrays are spread
stmt.run({ name: 'x', owner: 'y' }); // one plain object for @name / $name / :name
stmt.run({ name: 'x' }, 7);          // both, in any order
```

Values may be numbers, strings, `BigInt` (64-bit), `Buffer`/`Uint8Array`,
`null`, or `undefined` (bound as NULL). Anything else throws
`TypeError: SQLite3 can only bind numbers, strings, bigints, buffers, and null`.
A missing named parameter throws `RangeError: Missing named parameter "x"`;
surplus keys in the object are ignored; wrong positional counts throw
`Too few` / `Too many parameter values were provided`. BLOB columns are read
back as `Buffer`.

### Transactions

```js
const move = db.transaction((from, to, amount) => {
  debit.run(amount, from);
  credit.run(amount, to);
  return amount;
});

move(1, 2, 50);              // BEGIN ... COMMIT; throws roll back
move.immediate(1, 2, 50);    // BEGIN IMMEDIATE
move.deferred(...); move.exclusive(...);
move.database === db;
```

Calling a transaction function from inside another becomes a `SAVEPOINT`, so an
inner failure rolls back only the inner work. Functions that return a promise
throw, because the transaction would end before the work does.

### Errors

Everything SQLite rejects throws `Database.SqliteError` with `code` set to the
result code name: `SQLITE_CONSTRAINT_UNIQUE`, `SQLITE_CONSTRAINT_NOTNULL`,
`SQLITE_CONSTRAINT_FOREIGNKEY`, `SQLITE_READONLY`, `SQLITE_BUSY`, `SQLITE_ERROR`,
and so on (`lib/codes.js` carries the full table from `sqlite3.h`). Misuse of
the API throws `TypeError` or `RangeError` with better-sqlite3's messages.

## Database infrastructure

Beyond the better-sqlite3 surface, this package adds operational methods our
services need directly on the connection. They are built on `prepare`, `pragma`
and `exec`, so the same iterator/busy guards and `verbose` hook apply, and they
add no engine dependency. Strings they create (the migrations table) live in
`lib/dictionary.js` like every other.

### Schema introspection (read-only)

| Method                 | Returns                                                                          |
|------------------------|---------------------------------------------------------------------------------|
| `tables()`             | User table names, sorted (excludes `sqlite_*` and this package's own tables)     |
| `views()`              | User view names, sorted                                                          |
| `tableExists(name)`    | Whether a table or view exists                                                   |
| `columns(name)`        | `[{ name, type, notnull, default, primaryKey }]`; `primaryKey` is the 1-based key position or 0 |
| `indexes(name?)`       | `[{ name, table, unique, origin, partial, columns }]` for one table or all       |
| `foreignKeys(name)`    | `[{ id, table, from, to, onUpdate, onDelete, match }]`                            |
| `databases()`          | Attached databases (`PRAGMA database_list`): `[{ seq, name, file }]`             |
| `tableSchema(name)`    | The `CREATE` statement for a table/view/index/trigger, or `null`                 |
| `dumpSchema()`         | Every user `CREATE` statement as one SQL string (DDL only)                       |
| `count(table, opts?)`  | Row count, with an optional `{ where, params }` filter                           |
| `exists(table, opts?)` | Whether any row matches (`{ where, params }`)                                    |

### Maintenance and operations

| Method                    | Notes                                                                         |
|---------------------------|-------------------------------------------------------------------------------|
| `checkpoint(mode?)`       | WAL checkpoint (`passive` default, `full`, `restart`, `truncate`). Returns `{ busy, log, checkpointed }` |
| `vacuum(into?)`           | `VACUUM`, or `VACUUM INTO <file>` for a compacted copy                          |
| `analyze(target?)`        | `ANALYZE` the database or one table/index                                       |
| `optimize()`              | `PRAGMA optimize`                                                               |
| `integrityCheck(max?)`    | `PRAGMA integrity_check`; array of problems, `['ok']` when healthy              |
| `quickCheck(max?)`        | `PRAGMA quick_check`; same shape                                                |
| `foreignKeyCheck(table?)` | `[{ table, rowid, parent, foreignKey }]`, one per violation                     |
| `userVersion([value])`    | Read `PRAGMA user_version`, or set it (a non-negative 32-bit integer)           |
| `attach(file, schema)`    | `ATTACH` a database file under a schema name                                    |
| `detach(schema)`          | `DETACH` a database attached with `attach()`                                    |
| `journalMode([mode])`     | Read or set the journal mode (`delete`/`truncate`/`persist`/`memory`/`wal`/`off`) |
| `foreignKeysEnabled([on])`| Read or set foreign-key enforcement (distinct from `foreignKeys(table)` above)  |
| `walAutocheckpoint([n])`  | Read or set the WAL auto-checkpoint threshold in pages                          |

### Bulk insert

`insertMany(table, rows, options?)` inserts an array of row objects in one
transaction. Columns default to the keys of the first row (or `options.columns`);
`options.mode` is `insert` (default), `replace` (`INSERT OR REPLACE`) or `ignore`
(`INSERT OR IGNORE`). Returns `{ inserted }`, the number of rows actually written.

```js
db.insertMany('services', [
  { name: 'Search', owner: 'Platform' },
  { name: 'Billing', owner: 'Finance' },
], { mode: 'ignore' });
```

### Prepared-statement cache

`prepareCached(sql)` compiles a statement once per connection and returns the
same `Statement` for the same SQL (an LRU capped at 100 per connection); the
cache is emptied by `clearPreparedCache()` and by `close()`. Because the same
object is returned, its row-mode toggles and any `bind()` persist across calls.

### Migrations

`migrate(migrations)` applies an array of `{ name, up }`, each in its own
transaction, skipping names already recorded in `_better_sqlite_migrations`. It
returns `{ applied, pending, current }`. Idempotent, so it is safe to call on
every boot; a migration that throws rolls back and aborts the run, leaving the
ones before it committed.

```js
db.migrate([
  { name: '001-services', up: (db) => db.exec('CREATE TABLE services (id INTEGER PRIMARY KEY, name TEXT)') },
  { name: '002-owner',    up: (db) => db.exec('ALTER TABLE services ADD COLUMN owner TEXT') },
]);
```

Migrations may also carry a `down` for `rollback(migrations, { steps?, to? })`,
which undoes the most recently applied ones newest-first (each in a
transaction) and removes their record. `Database.loadMigrations(dir)` reads a
directory of `.js` migration modules (`module.exports = { up, down? }`), named by
filename and sorted, ready for `migrate()`/`rollback()`.

## Data-processing framework

On top of the infrastructure above, the package provides a small framework for
building and processing data in SQLite. Every layer is built on the same
connection primitives — parameters bound, identifiers quoted, execution through
the same guards — and each is independent, so use only what you need.

### Query builder

Fluent, parameterized SELECT/INSERT/UPDATE/DELETE. Values are always bound;
identifiers are always quoted; raw fragments (a `where` string, a join `ON`, a
`set` expression) are trusted caller SQL with `?` placeholders.

```js
db.from('users')
  .select('id', 'name', { expr: 'count(*)', as: 'n' })
  .join('orders', 'orders.user_id = users.id')
  .where({ active: true, region: ['us', 'eu'] })   // = and IN
  .andWhere('created_at > ?', since)
  .groupBy('users.id').having('n > ?', 1)
  .orderBy('n', 'desc').limit(10)
  .all();

db.insertInto('users').values([{ name: 'a' }, { name: 'b' }])
  .onConflict('name').doUpdate({ name: 'a' }).returning('id').run();
db.updateTable('users').set({ active: false }).where({ id: 7 }).run();
db.deleteFrom('users').where('id = ?', 7).run();
```

`from()` also has `.get()`, `.iterate()`, `.pluck()`, `.count()` and `.toSQL()`.

### Schema builder

Programmatic DDL, verified by the introspection API above.

```js
db.createTable('users', (t) => {
  t.integer('id').primaryKey({ autoincrement: true });
  t.text('name').notNull().unique();
  t.json('profile');
  t.boolean('active').notNull().default(true);
  t.integer('org_id').references('orgs', 'id', { onDelete: 'CASCADE' });
  t.text('created_at').default(t.raw('CURRENT_TIMESTAMP'));
}, { strict: true });
db.createIndex('idx_users_org', 'users', 'org_id');
```

Also `createTableIfNotExists`, `dropTable`, `renameTable`, `addColumn`,
`dropIndex`, composite `t.primaryKey(...)`/`t.unique(...)`/`t.foreignKey(...)`,
`t.check(...)` and generated columns (`t.integer('x2').generatedAs('x * 2')`).

### Models

A typed repository per table: transparent JSON/boolean/date columns, optional
timestamps, validation and hooks.

```js
const Users = db.model('users', {
  columns: { profile: 'json', active: 'boolean', joined: 'date' },
  timestamps: true,
  validate: (row) => { if (!row.name) throw new Error('name required'); },
});
const u = Users.insert({ name: 'Ada', profile: { role: 'admin' }, active: true, joined: new Date() });
Users.find(u.id); Users.where({ active: true }); Users.update(u.id, { active: false });
Users.upsert({ id: u.id, name: 'Ada L.' });
Users.paginate({ page: 1, perPage: 20, orderBy: { column: 'joined', direction: 'desc' } });
```

### Connection pool

A WAL pool over one file: one writer plus read-only reader connections, so a
long read never holds the writer busy.

```js
const pool = Database.pool('data/app.db', { readers: 4 });
pool.write((db) => db.prepare('INSERT INTO t (v) VALUES (?)').run(1));
const n = pool.read((db) => db.prepare('SELECT count(*) FROM t').pluck().get());
pool.transaction((db) => { /* ... */ });
pool.close();
```

### Streaming pipeline (ETL)

Lazy, bounded-memory transforms from a source (array, iterable, query, or a
CSV/NDJSON file) to a sink (a table, a file, an array, a reducer).

```js
await db.pipeline({ file: 'events.csv' })
  .map((r) => ({ ...r, ts: Date.parse(r.ts) }))
  .filter((r) => r.ts > cutoff)
  .batch(1000)               // group for efficient commits
  .flatMap((rows) => rows)
  .into('events', { mode: 'ignore', batchSize: 1000 });

await db.pipeline({ query: 'SELECT * FROM events WHERE ts > ?', params: [cutoff] })
  .map(project)
  .toFile('out.ndjson');
```

Also `.tap`, `.take`, `.drop`, `.toArray()`, `.forEach()`, `.reduce()`,
`.count()`. Transform functions may be sync or async.

### Analytics

`db.analytics(table)` builds grouped aggregations, time buckets, pivots and
window columns.

```js
db.analytics('events')
  .where({ type: 'purchase' })
  .timeBucket('ts', 'day')                 // ts is epoch-ms
  .agg({ orders: 'count', revenue: { fn: 'sum', column: 'amount' } })
  .orderBy('bucket').all();

db.analytics('sales').pivot('region', 'quarter', { fn: 'sum', column: 'amount' }).all();
```

### Eager loading (no N+1)

`db.loadRelated(rows, spec)` attaches related rows in one batched query per
relation (with nesting), avoiding N+1.

```js
const users = db.from('users').all();
db.loadRelated(users, [
  { as: 'orders', type: 'hasMany', table: 'orders', foreignKey: 'user_id',
    nested: { as: 'items', type: 'hasMany', table: 'items', foreignKey: 'order_id' } },
  { as: 'org', type: 'belongsTo', table: 'orgs', localKey: 'org_id' },
]);
```

### Bulk mutations

`db.updateMany(table, rows)`, `db.upsertMany(table, rows)` and
`db.deleteMany(table, keys)` apply many changes in one transaction (keyed by the
primary key or explicit key columns; `deleteMany` chunks large `IN` lists).

### Schema diff

`db.schemaDiff(target)` computes the **additive** DDL to bring this database up
to a target (another `Database`, an array of `CREATE` statements, or a SQL
string): missing tables, columns and indexes. It never drops or rewrites;
incompatible column changes are reported in `summary.incompatible`. Pass
`{ apply: true }` to run the statements in one transaction.

### Job scheduler

`new Database.Scheduler(db, { queue })` enqueues due jobs onto a queue on a
tick. Schedules persist in `_better_sqlite_schedules`, so a restart resumes.

```js
const sched = new Database.Scheduler(db, { queue: jobs });   // jobs is a Database.Queue
sched.every(60_000, 'heartbeat', {});
sched.cron('0 3 * * *', 'nightly-report', { kind: 'full' });  // 03:00 UTC daily
sched.at(new Date('2026-01-01T00:00:00Z'), 'new-year', {});
```

### SQL composition DSL

`Database.sql` (a tagged template) and `Database.expr` compose parameterized SQL
safely — values become bound `?` params, identifiers go through `expr.id`.

```js
const { sql, expr } = Database;
const q = sql`SELECT * FROM ${expr.id('users')} WHERE ${expr.and(
  expr.eq('active', true),
  expr.in('region', ['us', 'eu']),
)} ORDER BY id`;
db.prepare(q.sql).all(...q.params);
```

### Common table expressions (WITH)

`db.with(name, source)` / `db.withRecursive(...)` compose CTEs (source is raw SQL,
`{sql,params}`, or a builder), then `.from(cte)` reuses the query builder or
`.query(...)` runs a main query. Params stay ordered (CTE bodies first).

```js
db.withRecursive('cnt', 'SELECT 1 AS x UNION ALL SELECT x+1 FROM cnt WHERE x < 100', ['x'])
  .from('cnt').where('x % 2 = 0').all();
```

### Views and materialized views

`db.createView(name, select)` / `db.dropView(name)`. SQLite has no materialized
views, so `db.materialize(name, select, { indexes, unique })` builds a real table
from a SELECT and `db.refreshView(name)` recomputes it atomically;
`db.dropMaterialized(name)` removes it.

### Change tracking (audit / CDC)

`db.trackChanges(table)` installs AFTER INSERT/UPDATE/DELETE triggers that append
JSON change rows (op, old/new, timestamp) to an audit table; read them with
`db.auditLog({ table, op, since, limit })`, trim with `db.clearAudit({ before })`,
and remove the triggers with `db.stopTracking(table)`.

### Query cache

`db.cached(key, ttlMs, () => query())` memoizes a result per connection (LRU,
optional `tags`); `db.invalidate(key | { tag } | predicate)`, `db.cacheClear()`
and `db.cacheStats()` manage it. `ttlMs <= 0` caches until invalidated.

### Validation

`Database.validator(spec)` builds a reusable row validator (types, `required`,
`min`/`max`, `pattern`, `enum`, `default`, `coerce`, custom `validate`), with
`.validate(row)`, `.assert(row)` (throws `Database.ValidationError`),
`.isValid(row)` and `.coerce(row)` — handy for validating ingest in a pipeline.

### Full-text search

`node:sqlite` ships without FTS5, so this is an inverted-index shim:
`db.createSearchIndex(table, columns)` builds a shadow index kept current by
triggers (tokenized lazily on the next read), and `db.search(table, query,
{ match, prefix, limit })` returns ranked rows with a `_score`. `db.reindex()`
rebuilds; `db.dropSearchIndex()` removes it.

### Set operations

`db.union(...sources)`, `db.unionAll`, `db.intersect`, `db.except` combine query
builders, `{sql,params}` or raw SQL into a compound query, chainable with
`.orderBy/.limit/.offset` and further set ops.

### Time series

`db.timeseries(table, { time, value, unit })` buckets by `minute…year`, with
`.agg()`, `.fillGaps({from,to,fill})` (gap-filled via a recursive CTE),
`.movingAverage(n)` and `.rate()` (window functions).

```js
db.timeseries('metrics', { time: 'ts', value: 'cpu' })
  .bucket('hour').agg('avg').movingAverage(3).all();
```

### Scopes and soft deletes

`db.scoped(table, { softDelete: 'deleted_at', scopes })` returns a reusable query
that hides soft-deleted rows, with `.softDelete()/.restore()/.hardDelete()`,
named `.scope(name, ...)`, and `.withDeleted()/.onlyDeleted()`.

### Query observability

`db.observe({ slowMs, onSlow })` times `run/get/all` per statement;
`db.queryStats()` returns per-SQL `{ count, totalMs, avgMs, maxMs, rows }` (slowest
first); `db.resetStats()` / `db.unobserve()` manage it.

### Cloud backups

`new Database.BackupManager(db, { store, everyMs, snapshotEvery, retain })` drives
periodic snapshots/changesets to an ObjectStore and prunes old generations.
`backupNow()`, `prune()`, `start/stop/drain/close`, and
`'backup'`/`'snapshot'`/`'pruned'`/`'error'` events.

### JSON columns

`db.json(table, column)` reads and edits a JSON column with SQLite's JSON1
functions: `.get(pk, path)`, `.set(pk, path, value)`, `.merge(pk, obj)`,
`.remove(pk, path)`, `.where(path, value)`, `.each(pk)`, `.valid(pk)`.

### Advisory locks

`db.lock(name, { ttlMs })` / `db.unlock(name)` / `db.withLock(name, fn, { wait })`
coordinate workers across processes on one file (writers serialize in SQLite);
`db.locks()` lists what's held. Used to guard the scheduler/queue in multi-process
deployments.

### Key-value store

`db.kv(namespace)` is a typed KV over SQLite with TTL: `.get`/`.set`/`.has`/
`.delete`/`.keys`/`.entries`/`.incr`/`.getSet`/`.clear`/`.size`. Values keep their
JS type.

### Busy-retry transactions

`db.retryTransaction(fn, { retries, mode })` runs `fn` in a transaction and retries
the whole thing on `SQLITE_BUSY`/`SQLITE_LOCKED` with exponential backoff — for
concurrent writers on a WAL file.

### TypeScript types from the schema

`db.generateTypes()` (or `Database.generateTypes(db, opts)`) introspects the schema
and returns a TypeScript source string — one `interface` per table/view, SQLite
affinity mapped to TS, nullable columns as `| null`.

### Table partitioning

`db.partitioned(table, { column, by, interval })` splits a large logical table
into child tables by date or integer range, routes inserts to the right child
(created on demand), and builds a `<table>_all` UNION view for reads:
`.insert`/`.insertMany`, `.partitions()`, `.drop(key)`, `.view()`, `.count()`.

### Message brokers

Beyond the three clouds, `better-sqlite-adapters` also integrates non-cloud
brokers on the same `MessageSink`/`MessageSource` shapes: `adapters.kafka.Sink`
(Apache Kafka, sink-only), and `adapters.redis.StreamSink` / `StreamSource`
(Redis Streams with a consumer group). SDKs are optional, injectable, and the
tests run against fakes.

## Engine defaults

Matches better-sqlite3's build: foreign keys enforced, double-quoted strings
are identifiers only (no `"string"` literals), busy timeout 5 s.

## Differences from better-sqlite3

These follow from what `node:sqlite` exposes; each is deliberate and tested.

- **Numbered parameters** (`?1`, `?2`) are rejected at `prepare()`. Use `?` or names.
- **Large integers.** An INTEGER outside `Number`'s safe range throws a
  `RangeError` that points at `safeIntegers()`, instead of returning a lossy
  double.
- **Duplicate column names** (`SELECT a.id, b.id`) behave exactly as in
  better-sqlite3 for object, `raw()` and `expand()` rows on engines with
  `StatementSync#setReturnArrays` (Node 22.16+, 24+). Older engines deliver
  rows keyed by name, so a duplicate keeps its last value.
- **`inTransaction`** reads the engine's `isTransaction` where it exists (Node
  22.16+, 24+). Older engines are asked directly: a `BEGIN` that is refused
  means a transaction is open, one that succeeds is committed at once. Either
  way it also reflects transactions started with raw `exec('BEGIN')`.
- **`backup()` progress** can set the pages-per-step rate only from its first
  call (the one reporting the whole database, before copying starts). Later
  return values do not change the rate, which `node:sqlite` fixes when the copy
  begins.
- **Engine-dependent features** follow `Database.engine`: `aggregate()`,
  `serialize()` and opening a database from a `Buffer` work where `node:sqlite`
  has `aggregate`, `serialize` and `deserialize` (Node 24 does; Node 23.11 does
  not) and otherwise throw a `TypeError` naming the Node version. The upstream
  aggregate and serialize suites pass on Node 24 (`npm run test:conformance`).
  `table()` (virtual tables) and `Statement#readonly` have no counterpart on
  any Node and always throw.
- **`unsafeMode()` never lifts anything.** In better-sqlite3 it allows
  `exec()`, `pragma()` and `run()` while an iterator is open and clears
  SQLite's defensive flag. Here the guarded sequences are the ones that crash
  `node:sqlite`, and the defensive flag is what stops `journal_mode = OFF` or
  `writable_schema = ON` from corrupting a file, so the method is a documented
  no-op: it validates its argument and returns the database. Code that depends
  on unsafe mode fails at the guarded call with the usual
  `TypeError: This database connection is busy executing a query` instead of
  running.
- Node's `ExperimentalWarning` for `node:sqlite` is muted while this library
  loads the module. Every other warning still prints.

## Event queues

The queue above can deliver to a queue instead of a URL: any route may be a
`MessageSink`. Jobs claimed in one tick are grouped by route and published in
one call per sink, which is what a message queue, topic, event bus or stream
batches on. Envelopes carry the same JSON body as the HTTP path plus string
attributes (`idempotency-key`, `x-job-kind`, `x-job-attempt`) and an ordering
key (the job kind), which a queue or topic carries as its message group or
ordering key. A sink reports one outcome per envelope: `ok` deletes the job,
`retry` backs off, `dead` dead-letters at once, exactly as HTTP 2xx / 5xx /
4xx do.

`Database.Consumer` is the consumer side: exactly once *in effect* on top of an
at-least-once queue. Each message's id is inserted into
`_better_sqlite_inbox` in the same transaction that runs your synchronous
handler, so the handler's writes and the "seen" marker commit together or
not at all, and a redelivery finds the marker and is acked without running
anything. Two processes on one file serialize on that marker. A handler that
throws rolls back and the message is nacked with backoff; after `attempts`
(8) it is stored with its body as dead-lettered and acked so the queue stops
redelivering it. A body that is not JSON, or a handler that returns a
promise, is dead-lettered at once.

```js
const consumer = new Database.Consumer(db, {
  source: new QueueSource({ queueUrl }),
  handler(envelope, message) {              // runs inside a transaction; must be synchronous
    apply.run(envelope.payload.orderId);
    jobs.enqueue('order.confirmed', { id: envelope.payload.orderId });   // async follow-ups go through the queue
  }
});
consumer.on('dead', (message, err) => alert(err));
consumer.dead(); consumer.purge(30 * 86_400_000);
await consumer.drain();
```

Unlike the queue, a running consumer keeps the process alive: a consumer is a
server loop. `stop()` ends it after the receive in progress; `drain()`
resolves when the loop has exited. `purge()` shrinks the deduplication
window, so keep it longer than the queue's retention.
`lib/contract.js` is the suite every adapter
must pass; run it against an emulator with a client pointed at it.

## Cloud adapters

`adapters/` (the `better-sqlite-adapters` sub-package) implements the interfaces
above on real cloud services. The SDKs are optional peer dependencies, loaded
lazily; pass `{ client }` or `{ sdk }` to inject one (the tests do, so the suite
runs offline). Each provider passes `lib/contract.js`.

| Provider    | ObjectStore            | MessageSink                              | MessageSource        |
|-------------|------------------------|------------------------------------------|----------------------|
| AWS (flat)  | S3                     | SQS `QueueSink`, SNS `TopicSink`, EventBridge `EventBusSink`, Kinesis `StreamSink` | SQS `QueueSource` |
| `gcp.*`     | Cloud Storage          | Pub/Sub `QueueSink`                       | Pub/Sub `QueueSource` |
| `azure.*`   | Blob Storage           | Service Bus `QueueSink`, Event Hubs `EventHubSink`, Storage Queue `StorageQueueSink` | Service Bus `QueueSource`, Storage Queue `StorageQueueSource` |
| `kafka.*`   | —                      | Kafka `Sink`                             | — (Kafka consumer model is push-based) |
| `redis.*`   | —                      | Redis Streams `StreamSink`                | Redis Streams `StreamSource` |

```js
const adapters = require('better-sqlite-adapters');
const store  = new adapters.ObjectStore({ bucket: 'b' });                 // AWS S3
const gcs    = new adapters.gcp.ObjectStore({ bucket: 'b' });             // Google Cloud Storage
const blob   = new adapters.azure.ObjectStore({ container: 'c', connectionString });
const sink   = new adapters.gcp.QueueSink({ topic: 'projects/p/topics/t' });
const bus    = new adapters.azure.QueueSink({ queueName: 'q', connectionString });
```

Outcome mapping is uniform: a per-message failure the service marks as the
sender's fault (bad request, too large, not found) is `dead`; throttling,
timeouts and internal/transport errors are `retry`, and a whole-batch client
error is retried one message at a time so one bad message cannot dead-letter its
neighbours.

## Test

```bash
npm test
```

`test.js` covers opening options, parameters, row modes, transactions and
savepoints, error codes, user functions, backup, extension loading, closing, and
the warning filter. `test-queue.js` covers the queue against a
scriptable HTTP server: transactional enqueue, retry and dead-letter paths,
leases, concurrency, drain, and a child process that delivers then exits.
`test-consumer.js` covers exactly-once handling, retry, dead-letter and the queue → queue → consumer round
trip; `test-importer.js` covers NDJSON and CSV loads, decoding,
validation, modes and resume. `test-collision.js` covers the runtime
driver-collision check against throwaway consumer projects with each lockfile format. 

```bash
npm run test:conformance
```

Runs better-sqlite3 v13.0.3's own test suite against this library. The suite is
vendored unmodified under `conformance/test/` (MIT, `conformance/LICENSE`), and
mocha, chai and fs-extra are pinned devDependencies, so the run is offline and
reproducible from `npm ci` alone. `scripts/conformance-baseline.json` records
the expected pass count per file for each Node major; a lower count fails CI,
a higher one asks to be recorded with `--update`.

## Layout

| File                   | Role                                                            |
|------------------------|-----------------------------------------------------------------|
| `index.js`             | Entry point; `Database` with `.SqliteError`, `.Queue`, `.Consumer`, `.Importer` |
| `lib/database.js`      | `Database`: open, exec, pragma, functions, backup, close        |
| `lib/statement.js`     | `Statement`: binding, execution, row modes                      |
| `lib/introspect.js`    | Read-only schema introspection mixed onto `Database` (`tables`, `columns`, `indexes`, ...) |
| `lib/maintenance.js`   | Maintenance/operational methods (`checkpoint`, `vacuum`, `integrityCheck`, ...)  |
| `lib/migrate.js`       | `db.migrate()` / `db.rollback()` migration runner               |
| `lib/bulk.js`          | `db.insertMany()` transactional bulk insert                     |
| `lib/query.js`         | Fluent SELECT/INSERT/UPDATE/DELETE query builder                |
| `lib/schema-builder.js`| Programmatic DDL (`db.createTable()`, indexes, columns)         |
| `lib/model.js`         | `db.model()` typed repository (data-mapper)                     |
| `lib/pool.js`          | WAL connection pool (`Database.pool()`)                         |
| `lib/pipeline.js`      | Streaming ETL pipeline (`db.pipeline()`)                        |
| `lib/analytics.js`     | Grouped aggregation, time buckets, pivots (`db.analytics()`)    |
| `lib/relations.js`     | Batched eager loading (`db.loadRelated()`)                      |
| `lib/batch.js`         | Bulk `updateMany`/`upsertMany`/`deleteMany`                     |
| `lib/diff.js`          | Additive schema diff (`db.schemaDiff()`)                        |
| `lib/scheduler.js`     | Job scheduler over a queue (`Database.Scheduler`)              |
| `lib/expr.js`          | Safe SQL composition (`Database.sql` / `Database.expr`)         |
| `lib/cte.js`           | Common table expressions (`db.with()` / `withRecursive()`)     |
| `lib/views.js`         | Views and materialized views (`db.createView()`, `materialize()`) |
| `lib/audit.js`         | Trigger-based change tracking (`db.trackChanges()`)            |
| `lib/cache.js`         | Per-connection query-result cache (`db.cached()`)             |
| `lib/validate.js`      | Reusable row/schema validator (`Database.validator`)           |
| `lib/search.js`        | Full-text search shim (`db.createSearchIndex()`, `search()`)   |
| `lib/setops.js`        | UNION / INTERSECT / EXCEPT composition                         |
| `lib/timeseries.js`    | Time bucketing, gap fill, moving average (`db.timeseries()`)   |
| `lib/scope.js`         | Scopes and soft deletes (`db.scoped()`)                        |
| `lib/observe.js`       | Per-connection query timing (`db.observe()`)                   |
| `lib/backup-manager.js`| Scheduled cloud backups (`Database.BackupManager`)            |
| `lib/json.js`          | JSON1 column helpers (`db.json()`)                             |
| `lib/lock.js`          | Cross-process advisory locks (`db.lock()`, `withLock()`)      |
| `lib/kv.js`            | Typed key-value store with TTL (`db.kv()`)                    |
| `lib/retry.js`         | Busy-retry transactions (`db.retryTransaction()`)             |
| `lib/typegen.js`       | TypeScript types from the schema (`db.generateTypes()`)       |
| `lib/partition.js`     | Date/range table partitioning (`db.partitioned()`)           |
| `lib/transaction.js`   | `db.transaction()` with savepoint nesting                       |
| `lib/params.js`        | SQL scanner that finds parameters and counts statements         |
| `lib/errors.js`        | Maps `node:sqlite` errors to `SqliteError` / `TypeError`         |
| `lib/codes.js`         | SQLite result code table                                        |
| `lib/backend.js`       | Loads `node:sqlite`, muting its experimental warning            |
| `lib/queue.js`         | Transactional queue: durable fire-and-forget jobs to HTTP or queues |
| `lib/consumer.js`      | Queue consumer with exactly-once-in-effect handling             |
| `lib/export.js`        | NDJSON row export of one table with its `schema.json` sidecar   |
| `lib/schema.js`        | `sqlite_master` lookups: user tables, columns, schema fingerprint |
| `lib/staging.js`       | Temp files hashed and sized before upload                       |
| `lib/importer.js`      | Verified restore and resumable bulk load from an ObjectStore    |
| `lib/changeset.js`     | Framing for multi-table changeset objects                       |
| `lib/contract.js`      | Contract suite adapters must pass                               |
| `lib/dictionary.js`    | Every string the library spells, under plain-English names; modules import from it |
| `lib/collision.js`     | Runtime check for a second SQLite driver in the host project (advisory, warns once) |
| `scripts/conformance.js` | Runs the vendored better-sqlite3 suite against a per-Node baseline |
| `conformance/`         | Unmodified copy of better-sqlite3 v13.0.3's `test/`, plus the shim it requires |
