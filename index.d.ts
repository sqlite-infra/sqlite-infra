// Type declarations for better-sqlite3.
//
// The runtime implements the better-sqlite3 API, so these declarations follow
// @types/better-sqlite3 (MIT, DefinitelyTyped contributors) and are maintained
// here by hand rather than pulled in as a dependency.

/// <reference types="node" />

import QueueClass = require('./lib/queue');
import ConsumerClass = require('./lib/consumer');
import SyncClass = require('./lib/sync');
import Interfaces = require('./lib/interfaces');
import ImporterModule = require('./lib/importer');

type VariableArgFunction = (...params: any[]) => unknown;
type ArgumentTypes<F extends VariableArgFunction> = F extends (...args: infer A) => unknown ? A : never;
type ElementOf<T> = T extends Array<infer E> ? E : T;

declare namespace Database {
  interface RunResult {
    changes: number;
    lastInsertRowid: number | bigint;
  }

  interface Options {
    readonly?: boolean | undefined;
    fileMustExist?: boolean | undefined;
    timeout?: number | undefined;
    verbose?: ((message?: unknown, ...additionalArgs: unknown[]) => void) | undefined;
  }

  interface SerializeOptions {
    attached?: string | undefined;
  }

  interface PragmaOptions {
    simple?: boolean | undefined;
  }

  interface RegistrationOptions {
    varargs?: boolean | undefined;
    deterministic?: boolean | undefined;
    safeIntegers?: boolean | undefined;
    directOnly?: boolean | undefined;
  }

  interface AggregateOptions<T = unknown> extends RegistrationOptions {
    start?: T | (() => T);
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
    step: (total: T, next: ElementOf<T>) => T | void;
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
    inverse?: ((total: T, dropped: ElementOf<T>) => T | void) | undefined;
    result?: ((total: T) => unknown) | undefined;
  }

  interface BackupMetadata {
    totalPages: number;
    remainingPages: number;
  }

  interface BackupOptions {
    /** Schema to back up. Default 'main'. */
    attached?: string | undefined;
    /**
     * Called with the whole database before copying starts and after each
     * step that leaves pages. A number returned from the first call sets the
     * pages copied per step (default 100); later return values are ignored.
     */
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
    progress?: ((info: BackupMetadata) => number | void) | undefined;
  }

  interface VirtualTableOptions {
    rows: (...params: unknown[]) => Generator;
    columns: string[];
    parameters?: string[] | undefined;
    safeIntegers?: boolean | undefined;
    directOnly?: boolean | undefined;
  }

  interface ColumnDefinition {
    name: string;
    column: string | null;
    table: string | null;
    database: string | null;
    type: string | null;
  }

  /** One row of `db.explain(sql)`: SQLite's bytecode listing. */
  interface ExplainRow {
    addr: number;
    opcode: string;
    p1: number;
    p2: number;
    p3: number;
    p4: string | null;
    p5: number;
    comment: string | null;
  }

  interface Statement<BindParameters extends unknown[] = unknown[], Result = unknown> {
    database: Database;
    source: string;
    reader: boolean;
    /** Whether the statement only reads. Throws on node:sqlite (no counterpart). */
    readonly: boolean;
    busy: boolean;

    run(...params: BindParameters): RunResult;
    get(...params: BindParameters): Result | undefined;
    all(...params: BindParameters): Result[];
    iterate(...params: BindParameters): IterableIterator<Result>;
    pluck(toggleState?: boolean): this;
    expand(toggleState?: boolean): this;
    raw(toggleState?: boolean): this;
    bind(...params: BindParameters): this;
    columns(): ColumnDefinition[];
    safeIntegers(toggleState?: boolean): this;
    /** The source SQL, or once bind() was called the SQL with the bound values expanded. */
    toString(): string;
  }

  /** A column of a table or view, from `db.columns(name)`. */
  interface ColumnInfo {
    name: string;
    type: string | null;
    notnull: boolean;
    default: unknown;
    /** 1-based position in the primary key, or 0 for a non-key column. */
    primaryKey: number;
  }

  /** An index, from `db.indexes(name?)`. */
  interface IndexInfo {
    name: string;
    table: string;
    unique: boolean;
    /** 'c' (CREATE INDEX), 'u' (UNIQUE constraint) or 'pk' (PRIMARY KEY). */
    origin: string;
    partial: boolean;
    /** Indexed column names in order; an expression column is null. */
    columns: Array<string | null>;
  }

  /** A foreign key, from `db.foreignKeys(name)`. */
  interface ForeignKeyInfo {
    id: number;
    table: string;
    from: string;
    to: string | null;
    onUpdate: string;
    onDelete: string;
    match: string;
  }

  /** A foreign-key violation, from `db.foreignKeyCheck(table?)`. */
  interface ForeignKeyViolation {
    table: string;
    rowid: number | null;
    parent: string;
    foreignKey: number;
  }

  /** The result of `db.checkpoint(mode?)`. */
  interface CheckpointResult {
    busy: number;
    log: number;
    checkpointed: number;
  }

  type CheckpointMode = 'passive' | 'full' | 'restart' | 'truncate';

  /** One schema migration for `db.migrate()` / `db.rollback()`. */
  interface Migration {
    /** Unique, stable name; a migration already recorded under this name is skipped. */
    name: string;
    /** Synchronous schema change; receives the database. Throwing aborts the run. */
    up: (db: Database) => void;
    /** Undo, for rollback(). Required only for migrations that will be rolled back. */
    down?: (db: Database) => void;
  }

  interface RollbackOptions {
    /** How many applied migrations to undo (newest first). Default 1. */
    steps?: number;
    /** Roll back until (not including) this migration name. */
    to?: string;
  }

  /** The result of `db.rollback()`. */
  interface RollbackResult { rolledBack: string[] }

  /** The result of `db.migrate()`. */
  interface MigrateResult {
    /** Names applied by this call, in order. */
    applied: string[];
    /** Migrations still pending after this call (0 on success). */
    pending: number;
    /** Total migrations recorded after this call. */
    current: number;
  }

  /** An attached database, from `db.databases()`. */
  interface DatabaseInfo {
    seq: number;
    name: string;
    /** '' for in-memory and temporary databases. */
    file: string;
  }

  /** Options for `db.count()` and `db.exists()`. */
  interface CountOptions {
    /** SQL after WHERE, written by the caller (never from untrusted input). */
    where?: string;
    params?: unknown[];
  }

  type JournalMode = 'delete' | 'truncate' | 'persist' | 'memory' | 'wal' | 'off';

  /** Options for `db.insertMany()`. */
  interface InsertManyOptions {
    /** Conflict behaviour. Default 'insert'. */
    mode?: 'insert' | 'replace' | 'ignore';
    /** Columns to insert; default the keys of the first row. */
    columns?: string[];
  }

  /** The result of `db.insertMany()`. */
  interface InsertManyResult {
    /** Rows the database actually wrote (below the input length when 'ignore' skips conflicts). */
    inserted: number;
  }

  interface Transaction<F extends VariableArgFunction = VariableArgFunction> {
    (...params: ArgumentTypes<F>): ReturnType<F>;
    default(...params: ArgumentTypes<F>): ReturnType<F>;
    deferred(...params: ArgumentTypes<F>): ReturnType<F>;
    immediate(...params: ArgumentTypes<F>): ReturnType<F>;
    exclusive(...params: ArgumentTypes<F>): ReturnType<F>;
    database: Database;
  }

  // ---- framework layer (extensions beyond better-sqlite3) -------------------

  /** A WHERE: a SQL fragment with `?` placeholders, or `{ col: value }` (array → IN, null → IS NULL). */
  type WhereCondition = string | Record<string, unknown>;
  type SelectColumn = string | { expr: string; as?: string };

  interface SelectBuilder<Result = unknown> {
    select(...columns: SelectColumn[]): this;
    distinct(): this;
    where(condition: WhereCondition, ...params: unknown[]): this;
    andWhere(condition: WhereCondition, ...params: unknown[]): this;
    orWhere(condition: WhereCondition, ...params: unknown[]): this;
    join(table: string, on: string, ...params: unknown[]): this;
    leftJoin(table: string, on: string, ...params: unknown[]): this;
    groupBy(...columns: string[]): this;
    having(condition: string, ...params: unknown[]): this;
    orderBy(column: string, direction?: 'asc' | 'desc'): this;
    limit(n: number): this;
    offset(n: number): this;
    pluck(toggle?: boolean): this;
    all(): Result[];
    get(): Result | undefined;
    iterate(): IterableIterator<Result>;
    count(): number;
    toSQL(): { sql: string; params: unknown[] };
  }

  interface InsertBuilder {
    values(rows: Record<string, unknown> | Array<Record<string, unknown>>): this;
    onConflict(...columns: string[]): this;
    doNothing(): this;
    doUpdate(set: Record<string, unknown>): this;
    returning(...columns: string[]): this;
    run(): RunResult | Array<Record<string, unknown>>;
  }

  interface UpdateBuilder {
    set(values: Record<string, unknown> | string, ...params: unknown[]): this;
    where(condition: WhereCondition, ...params: unknown[]): this;
    andWhere(condition: WhereCondition, ...params: unknown[]): this;
    orWhere(condition: WhereCondition, ...params: unknown[]): this;
    returning(...columns: string[]): this;
    run(): RunResult | Array<Record<string, unknown>>;
  }

  interface DeleteBuilder {
    where(condition: WhereCondition, ...params: unknown[]): this;
    andWhere(condition: WhereCondition, ...params: unknown[]): this;
    orWhere(condition: WhereCondition, ...params: unknown[]): this;
    returning(...columns: string[]): this;
    run(): RunResult | Array<Record<string, unknown>>;
  }

  interface ColumnReferenceOptions { onDelete?: string; onUpdate?: string }
  /** A column definition inside a `createTable` callback. */
  interface ColumnDef {
    primaryKey(options?: { autoincrement?: boolean }): this;
    notNull(): this;
    unique(): this;
    default(value: unknown): this;
    references(table: string, column?: string, options?: ColumnReferenceOptions): this;
    check(expr: string): this;
    collate(name: string): this;
    generatedAs(expr: string, options?: { stored?: boolean }): this;
  }
  /** The table builder passed to `createTable`/`addColumn`. */
  interface TableBuilder {
    integer(name: string): ColumnDef;
    text(name: string): ColumnDef;
    real(name: string): ColumnDef;
    blob(name: string): ColumnDef;
    numeric(name: string): ColumnDef;
    boolean(name: string): ColumnDef;
    json(name: string): ColumnDef;
    primaryKey(...columns: string[]): void;
    unique(...columns: string[]): void;
    check(expr: string): void;
    foreignKey(...columns: string[]): { references(table: string, columns: string | string[], options?: ColumnReferenceOptions): void };
    raw(sql: string): unknown;
  }
  interface CreateTableOptions { strict?: boolean; withoutRowid?: boolean; ifNotExists?: boolean }
  interface CreateIndexOptions { unique?: boolean; where?: string }

  type ColumnType = 'text' | 'integer' | 'real' | 'blob' | 'json' | 'boolean' | 'date';
  interface ModelHooks {
    beforeInsert?: (row: Record<string, unknown>) => void;
    afterInsert?: (row: Record<string, unknown>) => void;
    beforeUpdate?: (patch: Record<string, unknown>, pk: unknown) => void;
    afterUpdate?: (row: Record<string, unknown>) => void;
    beforeDelete?: (pk: unknown) => void;
    afterDelete?: (pk: unknown) => void;
  }
  interface ModelOptions {
    primaryKey?: string;
    columns?: Record<string, ColumnType>;
    timestamps?: boolean;
    validate?: (row: Record<string, unknown>) => void | string | string[];
    hooks?: ModelHooks;
  }
  interface Paginated<Row> { rows: Row[]; page: number; perPage: number; total: number; pages: number }
  /** A repository for one table, from `db.model()`. */
  interface Repository<Row = Record<string, unknown>> {
    readonly table: string;
    readonly primaryKey: string;
    find(pk: unknown): Row | undefined;
    findBy(criteria: Record<string, unknown>): Row | undefined;
    first(criteria?: Record<string, unknown>): Row | undefined;
    all(criteria?: Record<string, unknown>): Row[];
    where(criteria: Record<string, unknown>): Row[];
    insert(row: Partial<Row>): Row;
    insertMany(rows: Array<Partial<Row>>): { inserted: number };
    update(pk: unknown, patch: Partial<Row>): Row | undefined;
    updateWhere(criteria: Record<string, unknown>, patch: Partial<Row>): number;
    upsert(row: Partial<Row>, options?: { onConflict?: string | string[] }): Row;
    delete(pk: unknown): boolean;
    deleteWhere(criteria: Record<string, unknown>): number;
    count(criteria?: Record<string, unknown>): number;
    exists(criteria?: Record<string, unknown>): boolean;
    paginate(options?: { page?: number; perPage?: number; where?: Record<string, unknown>; orderBy?: string | { column: string; direction?: 'asc' | 'desc' } }): Paginated<Row>;
  }

  interface PoolOptions {
    /** Read-only reader connections to open. Default 4. */
    readers?: number;
    /** Options passed to every connection. */
    dbOptions?: Options;
    /** Journal mode set on the writer. Default 'WAL'. */
    journalMode?: string;
  }
  /** A WAL connection pool: one writer plus read-only readers over one file. */
  interface Pool {
    readonly size: number;
    readonly open: boolean;
    writer(): Database;
    reader(): Database;
    read<T>(fn: (reader: Database) => T): T;
    write<T>(fn: (writer: Database) => T): T;
    transaction<T>(fn: (writer: Database) => T): T;
    close(): this;
  }

  type PipelineSource =
    | Iterable<unknown>
    | AsyncIterable<unknown>
    | { query: string; params?: unknown[] }
    | { file: string; format?: 'csv' | 'ndjson' };
  interface PipelineIntoOptions { mode?: 'insert' | 'replace' | 'ignore'; batchSize?: number; columns?: string[] }
  /** A lazy streaming transform chain, from `db.pipeline()`. */
  interface Pipeline {
    map(fn: (item: any) => any): Pipeline;
    filter(fn: (item: any) => unknown): Pipeline;
    flatMap(fn: (item: any) => any): Pipeline;
    tap(fn: (item: any) => void): Pipeline;
    batch(n: number): Pipeline;
    take(n: number): Pipeline;
    drop(n: number): Pipeline;
    into(table: string, options?: PipelineIntoOptions): Promise<{ inserted: number }>;
    toArray(): Promise<any[]>;
    forEach(fn: (item: any) => void | Promise<void>): Promise<void>;
    reduce<T>(fn: (acc: T, item: any) => T, initial: T): Promise<T>;
    count(): Promise<number>;
    toFile(path: string, options?: { format?: 'csv' | 'ndjson' }): Promise<{ file: string; rows: number }>;
  }

  // ---- analytics ----
  type AggFn = 'count' | 'sum' | 'avg' | 'min' | 'max' | 'total';
  type AggSpec = Record<string, AggFn | { fn: AggFn; column?: string; distinct?: boolean }>;
  interface AnalyticsBuilder {
    where(condition: WhereCondition, ...params: unknown[]): this;
    andWhere(condition: WhereCondition, ...params: unknown[]): this;
    orWhere(condition: WhereCondition, ...params: unknown[]): this;
    groupBy(...columns: string[]): this;
    agg(spec: AggSpec): this;
    timeBucket(column: string, interval: 'hour' | 'day' | 'week' | 'month' | 'year', options?: { as?: string; unit?: 'ms' | 's' | 'iso' }): this;
    window(expr: string, options?: { as?: string }): this;
    pivot(rowColumn: string, columnColumn: string, valueAgg: { fn: AggFn; column?: string }): this;
    having(condition: string, ...params: unknown[]): this;
    orderBy(column: string, direction?: 'asc' | 'desc'): this;
    limit(n: number): this;
    all(): Array<Record<string, unknown>>;
    get(): Record<string, unknown> | undefined;
    toSQL(): { sql: string; params: unknown[] };
  }

  // ---- relations (eager loading) ----
  interface Relation {
    as: string;
    type: 'hasMany' | 'hasOne' | 'belongsTo';
    table: string;
    localKey?: string;
    foreignKey?: string;
    columns?: string[];
    where?: string;
    params?: unknown[];
    orderBy?: string;
    nested?: Relation | Relation[];
  }

  // ---- schema diff ----
  interface SchemaDiffResult {
    statements: string[];
    summary: {
      tablesToAdd: string[];
      columnsToAdd: Array<{ table: string; column: string }>;
      indexesToAdd: string[];
      extra: { tables: string[]; columns: Array<{ table: string; column: string }>; indexes: string[] };
      incompatible: Array<{ table: string; column: string; reason: string }>;
    };
    additive: boolean;
  }

  // ---- bulk mutations ----
  interface UpdateManyOptions { keyColumns?: string | string[]; columns?: string[] }
  interface UpsertManyOptions { conflictColumns?: string | string[]; update?: string[]; columns?: string[]; mode?: 'ignore' }
  interface DeleteManyOptions { keyColumn?: string }

  // ---- scheduler ----
  interface SchedulerOptions { queue: { enqueue(kind: string, payload: unknown, options?: unknown): number }; tickMs?: number; autoStart?: boolean }
  interface ScheduleRow { name: string; kind: string; payload: unknown; type: string; next_run: number; last_run: number | null; created_at: number }
  interface Scheduler {
    every(intervalMs: number, kind: string, payload: unknown, options?: { name?: string; startAt?: Date | number }): string;
    cron(expr: string, kind: string, payload: unknown, options?: { name?: string }): string;
    at(date: Date | number, kind: string, payload: unknown, options?: { name?: string }): string;
    remove(name: string): boolean;
    get(name: string): ScheduleRow | undefined;
    list(): ScheduleRow[];
    tick(now?: number): number;
    start(): this;
    stop(): this;
    drain(): Promise<void>;
    on(event: 'fired' | 'error', listener: (...args: any[]) => void): this;
  }

  // ---- SQL expression DSL ----
  interface Fragment {
    readonly sql: string;
    readonly params: unknown[];
    append(part: Fragment | string, ...params: unknown[]): this;
    toString(): string;
  }
  interface Expr {
    id(name: string): Fragment;
    raw(text: string): Fragment;
    list(values: unknown[]): Fragment;
    and(...fragments: Array<Fragment | null | undefined>): Fragment;
    or(...fragments: Array<Fragment | null | undefined>): Fragment;
    join(fragments: Fragment[], separator: string): Fragment;
    eq(column: string, value: unknown): Fragment;
    ne(column: string, value: unknown): Fragment;
    gt(column: string, value: unknown): Fragment;
    gte(column: string, value: unknown): Fragment;
    lt(column: string, value: unknown): Fragment;
    lte(column: string, value: unknown): Fragment;
    like(column: string, pattern: unknown): Fragment;
    in(column: string, values: unknown[]): Fragment;
    between(column: string, a: unknown, b: unknown): Fragment;
    isNull(column: string): Fragment;
    notNull(column: string): Fragment;
  }

  // ---- CTE (WITH) builder ----
  type CteSource = string | { sql: string; params?: unknown[] } | { toSQL(): { sql: string; params: unknown[] } };
  interface CteResult {
    all(): Array<Record<string, unknown>>;
    get(): Record<string, unknown> | undefined;
    iterate(): IterableIterator<Record<string, unknown>>;
    pluck(toggle?: boolean): this;
    toSQL(): { sql: string; params: unknown[] };
  }
  interface CteSelect extends SelectBuilder { count(): number }
  interface CteBuilder {
    with(name: string, source: CteSource, columns?: string[]): CteBuilder;
    withRecursive(name: string, source: CteSource, columns?: string[]): CteBuilder;
    query(source: CteSource): CteResult;
    from(table: string): CteSelect;
  }

  // ---- audit / change tracking ----
  interface TrackChangesOptions { into?: string; events?: Array<'insert' | 'update' | 'delete'>; columns?: string[] }
  interface AuditLogOptions { table?: string; op?: 'insert' | 'update' | 'delete'; since?: number; limit?: number; into?: string }
  interface AuditRow {
    id: number;
    table: string;
    op: 'insert' | 'update' | 'delete';
    rowId: number;
    old: Record<string, unknown> | null;
    new: Record<string, unknown> | null;
    at: number;
  }

  // ---- query cache ----
  interface CacheOptions { ttlMs?: number; tags?: string[]; max?: number }
  interface CacheStats { size: number; hits: number; misses: number; evictions: number }
  type InvalidateArg = string | { tag: string } | { tags: string[] } | ((key: string) => boolean);

  // ---- validator ----
  type ValidatorType = 'string' | 'number' | 'integer' | 'boolean' | 'date' | 'json' | 'any';
  interface ValidatorRule {
    type: ValidatorType;
    required?: boolean;
    default?: unknown | (() => unknown);
    min?: number;
    max?: number;
    pattern?: RegExp | string;
    enum?: unknown[];
    coerce?: boolean;
    validate?: (value: unknown, row: Record<string, unknown>) => boolean | string;
  }
  interface ValidatorResult { valid: boolean; errors: Array<{ field: string; message: string }>; value: Record<string, unknown> }
  interface Validator {
    validate(row: Record<string, unknown>): ValidatorResult;
    assert(row: Record<string, unknown>): Record<string, unknown>;
    isValid(row: Record<string, unknown>): boolean;
    coerce(row: Record<string, unknown>): Record<string, unknown>;
  }

  // ---- full-text search shim ----
  interface SearchIndexOptions { into?: string; minToken?: number; stopWords?: string[] }
  interface SearchOptions { into?: string; limit?: number; prefix?: boolean; match?: 'all' | 'any'; columns?: string[] }

  // ---- query observability ----
  interface ObserveOptions { slowMs?: number; onSlow?: (sql: string, ms: number) => void; onQuery?: (sql: string, ms: number) => void }
  interface QueryStat { sql: string; count: number; totalMs: number; avgMs: number; maxMs: number; rows: number }

  // ---- set operations ----
  interface SetQuery {
    union(...sources: Array<CteSource>): SetQuery;
    unionAll(...sources: Array<CteSource>): SetQuery;
    intersect(...sources: Array<CteSource>): SetQuery;
    except(...sources: Array<CteSource>): SetQuery;
    orderBy(column: string, direction?: 'asc' | 'desc'): this;
    limit(n: number): this;
    offset(n: number): this;
    all(): Array<Record<string, unknown>>;
    get(): Record<string, unknown> | undefined;
    iterate(): IterableIterator<Record<string, unknown>>;
    toSQL(): { sql: string; params: unknown[] };
  }

  // ---- time series ----
  interface TimeseriesOptions { time: string; value?: string; where?: string; params?: unknown[]; unit?: 'ms' | 's' | 'iso' }
  interface TimeseriesBuilder {
    bucket(interval: 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year'): this;
    agg(fn: 'sum' | 'avg' | 'count' | 'min' | 'max'): this;
    fillGaps(options: { from: number | Date; to: number | Date; fill?: number }): this;
    movingAverage(n: number, options?: { as?: string }): this;
    rate(options?: { as?: string }): this;
    orderBy(direction?: 'asc' | 'desc'): this;
    limit(n: number): this;
    all(): Array<Record<string, unknown>>;
    toSQL(): { sql: string; params: unknown[] };
  }

  // ---- scopes / soft delete ----
  interface ScopeOptions {
    softDelete?: string;
    scopes?: Record<string, Record<string, unknown> | { where: string; params?: unknown[] } | ((query: ScopedQuery, ...args: any[]) => ScopedQuery)>;
    primaryKey?: string;
  }
  interface ScopedQuery {
    where(criteria: Record<string, unknown>): ScopedQuery;
    scope(name: string, ...args: unknown[]): ScopedQuery;
    withDeleted(): ScopedQuery;
    onlyDeleted(): ScopedQuery;
    orderBy(column: string, direction?: 'asc' | 'desc'): ScopedQuery;
    limit(n: number): ScopedQuery;
    offset(n: number): ScopedQuery;
    all(): Array<Record<string, unknown>>;
    first(): Record<string, unknown> | undefined;
    get(pk: unknown): Record<string, unknown> | undefined;
    count(): number;
    pluck(column: string): { all(): unknown[]; get(): unknown };
    softDelete(pkOrCriteria: unknown): number;
    restore(pkOrCriteria: unknown): number;
    hardDelete(pkOrCriteria: unknown): number;
  }

  // ---- cloud backup manager ----
  interface BackupManagerOptions {
    store: ObjectStore;
    prefix?: string;
    gzip?: boolean;
    changesets?: boolean;
    everyMs?: number;
    snapshotEvery?: number;
    retain?: number;
    autoStart?: boolean;
  }
  interface BackupManager {
    readonly generation: number;
    readonly seq: number;
    readonly running: boolean;
    backupNow(options?: { snapshot?: boolean }): Promise<SnapshotInfo | ChangesetInfo | null>;
    prune(): Promise<number>;
    manifest(): Promise<Manifest | null>;
    start(): this;
    stop(): this;
    drain(): Promise<void>;
    close(): void;
    on(event: 'backup' | 'snapshot' | 'pruned' | 'error', listener: (...args: any[]) => void): this;
  }

  // ---- JSON column helpers ----
  interface JsonColumn {
    get(pk: unknown, path?: string): unknown;
    set(pk: unknown, path: string, value: unknown): RunResult;
    merge(pk: unknown, obj: Record<string, unknown>): RunResult;
    remove(pk: unknown, path: string): RunResult;
    where(path: string, value: unknown): Array<Record<string, unknown>>;
    each(pk: unknown, path?: string): Array<{ key: string | number; value: unknown; type: string; atom: unknown; path: string; fullkey: string }>;
    valid(pk: unknown): boolean;
  }

  // ---- advisory locks ----
  interface LockOptions { ttlMs?: number; owner?: string }
  interface WithLockOptions extends LockOptions { wait?: number }
  interface LockInfo { name: string; owner: string; acquiredAt: number; expiresAt: number | null }

  // ---- key-value store ----
  interface KvStore {
    readonly namespace: string;
    get<T = unknown>(key: string): T | undefined;
    set(key: string, value: unknown, options?: { ttlMs?: number }): this;
    has(key: string): boolean;
    delete(key: string): boolean;
    keys(): string[];
    values(): unknown[];
    entries(): Array<[string, unknown]>;
    incr(key: string, by?: number): number;
    getSet<T>(key: string, producer: () => T, options?: { ttlMs?: number }): T;
    clear(): number;
    size(): number;
  }

  // ---- busy-retry transactions ----
  interface RetryOptions { retries?: number; baseMs?: number; maxMs?: number; mode?: 'deferred' | 'immediate' | 'exclusive' }

  // ---- table partitioning ----
  interface PartitionOptions {
    column: string;
    by?: 'date' | 'range';
    interval?: 'day' | 'week' | 'month' | 'year' | number;
    unit?: 'ms' | 'iso';
    like?: string;
    createSql?: string;
  }
  interface PartitionInfo { key: string; table: string; rows: number }
  interface PartitionManager {
    keyFor(value: unknown): string;
    tableFor(value: unknown): string;
    ensure(value: unknown): string;
    insert(row: Record<string, unknown>): RunResult;
    insertMany(rows: Array<Record<string, unknown>>): { inserted: number };
    partitions(): PartitionInfo[];
    drop(key: string, options?: { ifExists?: boolean }): this;
    view(): string;
    count(): number;
  }

  // ---- TypeScript type generation ----
  interface GenerateTypesOptions {
    tables?: string[];
    views?: boolean;
    bigint?: boolean;
    export?: boolean;
    header?: string;
    prefix?: string;
    suffix?: string;
    databaseInterface?: boolean | string;
  }

  interface Database {
    memory: boolean;
    readonly: boolean;
    name: string;
    open: boolean;
    inTransaction: boolean;

    prepare<BindParameters extends unknown[] | {} = unknown[], Result = unknown>(
      source: string
    ): BindParameters extends unknown[] ? Statement<BindParameters, Result> : Statement<[BindParameters], Result>;
    /** Like prepare(), but the compiled Statement is cached per connection (LRU) and returned again for the same SQL. Extension beyond better-sqlite3. */
    prepareCached<BindParameters extends unknown[] | {} = unknown[], Result = unknown>(
      source: string
    ): BindParameters extends unknown[] ? Statement<BindParameters, Result> : Statement<[BindParameters], Result>;
    /** Drops every statement held by prepareCached(). */
    clearPreparedCache(): this;
    transaction<F extends VariableArgFunction>(fn: F): Transaction<F>;
    exec(source: string): this;
    pragma(source: string, options?: PragmaOptions): unknown;
    /** Bytecode program for `source`, or the query plan for 'QUERY PLAN ...'. Parameters stay unbound. */
    explain(source: string): ExplainRow[] | unknown[];
    function(name: string, cb: (...params: any[]) => any): this;
    function(name: string, options: RegistrationOptions, cb: (...params: any[]) => any): this;
    aggregate<T>(name: string, options: AggregateOptions<T>): this;
    loadExtension(path: string): this;
    close(): this;
    defaultSafeIntegers(toggleState?: boolean): this;
    backup(destinationFile: string, options?: BackupOptions): Promise<BackupMetadata>;
    /** Always throws: virtual tables have no node:sqlite counterpart. */
    table(name: string, options: VirtualTableOptions): this;
    /** Accepted for API parity; never lifts the iterator guard or the defensive flag. */
    unsafeMode(unsafe?: boolean): this;
    /** Throws a TypeError on engines without DatabaseSync#serialize (see `Database.engine`). */
    serialize(options?: SerializeOptions): Buffer;

    // ---- extensions beyond better-sqlite3 (see README "Database infrastructure") ----

    /** User table names (excludes sqlite_* and this package's own tables), sorted. */
    tables(): string[];
    /** User view names, sorted. */
    views(): string[];
    /** Whether a table or view with this name exists. */
    tableExists(name: string): boolean;
    /** Columns of a table or view. Throws for an unknown name. */
    columns(name: string): ColumnInfo[];
    /** Indexes on one table, or every user index when no name is given. */
    indexes(name?: string): IndexInfo[];
    /** Foreign keys declared on a table. Throws for an unknown table. */
    foreignKeys(name: string): ForeignKeyInfo[];
    /** Attached databases (PRAGMA database_list). */
    databases(): DatabaseInfo[];
    /** The CREATE statement recorded for a schema object, or null; throws for an unknown name. */
    tableSchema(name: string): string | null;
    /** Every user CREATE statement as one SQL string (DDL only). */
    dumpSchema(): string;
    /** Row count of a table or view, optionally filtered. */
    count(table: string, options?: CountOptions): number;
    /** Whether any row matches. */
    exists(table: string, options?: CountOptions): boolean;

    /** Run a WAL checkpoint. Default mode 'passive'. */
    checkpoint(mode?: CheckpointMode): CheckpointResult;
    /** VACUUM the database, or VACUUM INTO a file when `into` is given. */
    vacuum(into?: string): this;
    /** ANALYZE the database, or one table/index when `target` is given. */
    analyze(target?: string): this;
    /** PRAGMA optimize. */
    optimize(): this;
    /** PRAGMA integrity_check. Returns problem descriptions; `['ok']` means healthy. */
    integrityCheck(maxErrors?: number): string[];
    /** PRAGMA quick_check (skips index checks). Returns problems; `['ok']` means healthy. */
    quickCheck(maxErrors?: number): string[];
    /** PRAGMA foreign_key_check for the whole database, or one table. */
    foreignKeyCheck(table?: string): ForeignKeyViolation[];
    /** Read PRAGMA user_version, or set it when `value` is given. */
    userVersion(): number;
    userVersion(value: number): this;
    /** ATTACH a database file under a schema name. */
    attach(file: string, schema: string): this;
    /** DETACH a database attached with attach(). */
    detach(schema: string): this;
    /** Read the journal mode, or set it. */
    journalMode(): string;
    journalMode(mode: JournalMode): this;
    /** Read whether foreign-key enforcement is on, or set it. */
    foreignKeysEnabled(): boolean;
    foreignKeysEnabled(enabled: boolean): this;
    /** Read the WAL auto-checkpoint threshold in pages, or set it. */
    walAutocheckpoint(): number;
    walAutocheckpoint(pages: number): this;

    /** Insert an array of row objects in one transaction. */
    insertMany(table: string, rows: Array<Record<string, unknown>>, options?: InsertManyOptions): InsertManyResult;

    /** Apply pending migrations (keyed by name, each in a transaction). Idempotent. */
    migrate(migrations: Migration[]): MigrateResult;
    /** Undo the most recently applied migrations via their `down` functions. */
    rollback(migrations: Migration[], options?: RollbackOptions): RollbackResult;

    // ---- query builder ----
    from<Result = unknown>(table: string): SelectBuilder<Result>;
    insertInto(table: string): InsertBuilder;
    updateTable(table: string): UpdateBuilder;
    deleteFrom(table: string): DeleteBuilder;

    // ---- schema builder ----
    createTable(name: string, build: (t: TableBuilder) => void, options?: CreateTableOptions): this;
    createTableIfNotExists(name: string, build: (t: TableBuilder) => void, options?: CreateTableOptions): this;
    dropTable(name: string, options?: { ifExists?: boolean }): this;
    renameTable(from: string, to: string): this;
    addColumn(table: string, build: (t: TableBuilder) => ColumnDef): this;
    createIndex(name: string, table: string, columns: string | string[], options?: CreateIndexOptions): this;
    dropIndex(name: string, options?: { ifExists?: boolean }): this;

    // ---- model / repository ----
    model<Row = Record<string, unknown>>(name: string, options?: ModelOptions): Repository<Row>;

    // ---- streaming ETL pipeline ----
    pipeline(source: PipelineSource): Pipeline;

    // ---- analytics ----
    analytics(table: string): AnalyticsBuilder;

    // ---- eager loading ----
    loadRelated<Row = Record<string, unknown>>(rows: Row[], spec: Relation | Relation[]): Row[];

    // ---- schema diff ----
    schemaDiff(target: Database | string | string[], options?: { apply?: boolean }): SchemaDiffResult;

    // ---- bulk mutations ----
    updateMany(table: string, rows: Array<Record<string, unknown>>, options?: UpdateManyOptions): { updated: number };
    upsertMany(table: string, rows: Array<Record<string, unknown>>, options?: UpsertManyOptions): { inserted: number };
    deleteMany(table: string, keys: unknown[], options?: DeleteManyOptions): { deleted: number };

    // ---- CTE builder ----
    with(name: string, source: CteSource, columns?: string[]): CteBuilder;
    withRecursive(name: string, source: CteSource, columns?: string[]): CteBuilder;

    // ---- views / materialized views ----
    createView(name: string, source: CteSource, options?: { ifNotExists?: boolean; temporary?: boolean }): this;
    dropView(name: string, options?: { ifExists?: boolean }): this;
    materialize(name: string, source: CteSource, options?: { indexes?: Array<string | string[]>; unique?: string | string[] }): { name: string; rows: number };
    refreshView(name: string): { name: string; rows: number };
    dropMaterialized(name: string, options?: { ifExists?: boolean }): this;

    // ---- audit / change tracking ----
    trackChanges(table: string, options?: TrackChangesOptions): this;
    stopTracking(table: string, options?: { into?: string }): this;
    auditLog(options?: AuditLogOptions): AuditRow[];
    clearAudit(options?: { before?: number; table?: string; into?: string }): number;

    // ---- query cache ----
    cached<T>(key: string, ttlMsOrOptions: number | CacheOptions, producer: () => T): T;
    invalidate(arg: InvalidateArg): number;
    cacheClear(): this;
    cacheStats(): CacheStats;

    // ---- full-text search shim ----
    createSearchIndex(table: string, columns: string | string[], options?: SearchIndexOptions): this;
    dropSearchIndex(table: string, options?: { into?: string }): this;
    search(table: string, query: string, options?: SearchOptions): Array<Record<string, unknown> & { _score: number }>;
    reindex(table: string, options?: { into?: string }): this;

    // ---- query observability ----
    observe(options?: ObserveOptions): this;
    unobserve(): this;
    queryStats(): QueryStat[];
    resetStats(): this;

    // ---- set operations ----
    union(...sources: Array<CteSource>): SetQuery;
    unionAll(...sources: Array<CteSource>): SetQuery;
    intersect(...sources: Array<CteSource>): SetQuery;
    except(...sources: Array<CteSource>): SetQuery;

    // ---- time series ----
    timeseries(table: string, options: TimeseriesOptions): TimeseriesBuilder;

    // ---- scopes / soft delete ----
    scoped(table: string, options?: ScopeOptions): ScopedQuery;

    // ---- JSON column helpers ----
    json(table: string, column: string, options?: { primaryKey?: string }): JsonColumn;

    // ---- advisory locks ----
    lock(name: string, options?: LockOptions): boolean;
    unlock(name: string, options?: { owner?: string }): boolean;
    withLock<T>(name: string, fn: () => T, options?: WithLockOptions): T;
    locks(): LockInfo[];

    // ---- key-value store ----
    kv(namespace?: string): KvStore;

    // ---- busy-retry transactions ----
    retryTransaction<T>(fn: () => T, options?: RetryOptions): T;

    // ---- TypeScript type generation ----
    generateTypes(options?: GenerateTypesOptions): string;

    // ---- table partitioning ----
    partitioned(name: string, options: PartitionOptions): PartitionManager;
  }

  /** Which optional node:sqlite features the running Node provides. */
  interface Engine {
    readonly isTransaction: boolean;
    readonly aggregate: boolean;
    readonly serialize: boolean;
    readonly deserialize: boolean;
    readonly defensive: boolean;
    readonly arrays: boolean;
    readonly backup: boolean;
    readonly sessions: boolean;
  }

  type SqliteError = SqliteErrorType;

  type Queue = QueueClass;
  type QueueOptions = QueueClass.Options;
  type QueueRoutes = QueueClass.Routes;
  type QueueJob = QueueClass.Job;
  type QueueDeadJob = QueueClass.DeadJob;
  type QueueEnvelope = QueueClass.Envelope;

  type Consumer = ConsumerClass;
  type ConsumerOptions = ConsumerClass.Options;
  type ConsumerDeadMessage = ConsumerClass.DeadMessage;

  type Sync = SyncClass;
  type SyncOptions = SyncClass.Options;
  type SnapshotInfo = SyncClass.SnapshotInfo;
  type ChangesetInfo = SyncClass.ChangesetInfo;
  type RowExportOptions = SyncClass.RowExportOptions;
  type RowExportInfo = SyncClass.RowExportInfo;
  type Manifest = SyncClass.Manifest;

  type ObjectStore = Interfaces.ObjectStore;
  type MessageSink = Interfaces.MessageSink;
  type MessageSource = Interfaces.MessageSource;
  type Envelope = Interfaces.Envelope;
  type Outcome = Interfaces.Outcome;
  type Message = Interfaces.Message;
  type PutMeta = Interfaces.PutMeta;

  type RestoreOptions = ImporterModule.RestoreOptions;
  type RestoreResult = ImporterModule.RestoreResult;
  type LoadOptions = ImporterModule.LoadOptions;
  type LoadResult = ImporterModule.LoadResult;

  interface Importer {
    /** Newest generation of `dbId` from the store, verified, written to `to`. */
    restore(options: ImporterModule.RestoreOptions): Promise<ImporterModule.RestoreResult>;
    /** NDJSON or CSV rows from one object into an existing table, resumably. */
    load(db: Database, options: ImporterModule.LoadOptions): Promise<ImporterModule.LoadResult>;
    readonly TABLE: string;
  }
}

declare class SqliteErrorClass extends Error {
  name: string;
  message: string;
  /** SQLite result code name, e.g. 'SQLITE_CONSTRAINT_UNIQUE'. */
  code: string;
  constructor(message: string, code: string);
}

type SqliteErrorType = typeof SqliteErrorClass;

interface DatabaseConstructor {
  new (filename?: string | Buffer, options?: Database.Options): Database.Database;
  (filename?: string | Buffer, options?: Database.Options): Database.Database;
  prototype: Database.Database;
  SqliteError: SqliteErrorType;
  engine: Database.Engine;
  Queue: typeof QueueClass;
  Consumer: typeof ConsumerClass;
  Sync: typeof SyncClass;
  Importer: Database.Importer;
  interfaces: typeof Interfaces;
  default: DatabaseConstructor;
  /** A WAL connection pool over one file (one writer + read-only readers). */
  pool(filename: string, options?: Database.PoolOptions): Database.Pool;
  Pool: { new (filename: string, options?: Database.PoolOptions): Database.Pool };
  /** The streaming ETL pipeline class. */
  Pipeline: { new (db: Database.Database, source: Database.PipelineSource): Database.Pipeline };
  /** Load a directory of migration modules for migrate()/rollback(). */
  loadMigrations(dir: string): Database.Migration[];
  /** Job scheduler over a Queue-like sink. */
  Scheduler: { new (db: Database.Database, options: Database.SchedulerOptions): Database.Scheduler; readonly TABLE: string };
  /** Tagged template for safe SQL composition → a Fragment. */
  sql(strings: TemplateStringsArray, ...values: unknown[]): Database.Fragment;
  /** SQL expression helpers. */
  expr: Database.Expr;
  /** The SQL Fragment class. */
  Fragment: { new (sql?: string, params?: unknown[]): Database.Fragment };
  /** Build a reusable row/schema validator. */
  validator(spec: Record<string, Database.ValidatorRule>, options?: { stripUnknown?: boolean; strict?: boolean }): Database.Validator;
  /** Error thrown by Validator#assert, carrying `.errors`. */
  ValidationError: { new (message: string, errors?: Array<{ field: string; message: string }>): Error & { errors: Array<{ field: string; message: string }> } };
  /** Scheduled snapshot/changeset backups to an ObjectStore, with pruning. */
  BackupManager: { new (db: Database.Database, options: Database.BackupManagerOptions): Database.BackupManager };
  /** Generate TypeScript row interfaces from a database's schema. */
  generateTypes(db: Database.Database, options?: Database.GenerateTypesOptions): string;
}

declare const Database: DatabaseConstructor;

export = Database;
