// Compile-time check of the public types: better-sqlite3's surface plus this package's additions.
import Database = require('./');

const db: Database.Database = new Database(':memory:', { readonly: false, timeout: 1000 });
db.pragma('journal_mode = WAL');
const stmt: Database.Statement<[number], { id: number; name: string }> = db.prepare('SELECT id, name FROM t WHERE id = ?');
const row: { id: number; name: string } | undefined = stmt.get(1);
const rows: { id: number; name: string }[] = stmt.all(1);
const info: Database.RunResult = db.prepare('INSERT INTO t (name) VALUES (@name)').run({ name: 'x' });
const tx: Database.Transaction<(n: number) => number> = db.transaction((n: number) => n + 1);
const result: number = tx.immediate(1);
const err = new Database.SqliteError('boom', 'SQLITE_BUSY');
const isSqliteError: boolean = err instanceof Database.SqliteError && err.code === 'SQLITE_BUSY';
const backup: Promise<Database.BackupMetadata> = db.backup('/tmp/x.db', { progress: () => 100 });
db.function('f', { deterministic: true }, (a: number) => a);
db.aggregate<number>('sum2', { start: 0, step: (total, next) => total + next });
db.serialize({ attached: 'main' });
db.close();
void [row, rows, info, result, err, isSqliteError, backup];

// Additions of this package over better-sqlite3.
const engine: Database.Engine = Database.engine;
const hasAggregate: boolean = engine.aggregate;
const plan: Database.ExplainRow[] | unknown[] = db.explain('SELECT 1');
const sql: string = stmt.toString();
const viaCall: Database.Database = Database(':memory:');
const withAttached: Promise<Database.BackupMetadata> = db.backup('/tmp/y.db', { attached: 'main', progress: () => {} });
void [hasAggregate, plan, sql, viaCall, withAttached];

// Transactional queue.
const jobs: Database.Queue = new Database.Queue(db, {
  routes: { enrich: 'http://enrich.internal/jobs', report: { url: 'http://c/r', timeoutMs: 30000 }, '*': 'http://events/n' },
  concurrency: 4,
  autoStart: false
});
const jobId: number = jobs.enqueue('enrich', { orderId: 1 }, { delayMs: 1000 });
jobs.enqueue('report', null, { runAt: new Date() });
jobs.on('delivered', (job: Database.QueueJob) => void job.id);
jobs.on('retry', (job, err: Error, delayMs: number) => void [job.kind, err.message, delayMs]);
jobs.on('dead', (job, err) => void [job.attempts, err]);
const deadJobs: Database.QueueDeadJob[] = jobs.dead();
const requeued: boolean = jobs.retry(jobId);
const purged: number = jobs.purge(86_400_000);
const drained: Promise<void> = jobs.drain();
const table: string = Database.Queue.TABLE;
void [deadJobs, requeued, purged, drained, table, jobs.pending(), jobs.stop().start()];

// Cloud interfaces, sync, importer, consumer.
const store: Database.ObjectStore = new Database.interfaces.MemoryObjectStore();
const queue: Database.MessageSink & Database.MessageSource = new Database.interfaces.MemoryQueue({ maxBatch: 5 });
const httpSink: Database.MessageSink = new Database.interfaces.HttpSink('http://svc/hook', { timeoutMs: 5000 });
const mixed: Database.Queue = new Database.Queue(db, { routes: { audit: queue, http: httpSink, '*': { sink: queue, attempts: 3 } }, autoStart: false });
const custom: Database.MessageSink = {
  async publish(envelopes: Database.Envelope[]): Promise<Database.Outcome[]> {
    return envelopes.map((e) => ({ id: e.id, status: 'ok' as const }));
  }
};
void [mixed, custom, Database.interfaces.outcomeForStatus('x', 503).status];

const sync: Database.Sync = new Database.Sync(db, { store, prefix: 'sqlite', everyMs: 60_000, snapshotEvery: 100 });
const snapInfo: Promise<Database.SnapshotInfo> = sync.snapshot();
const cs: Promise<Database.ChangesetInfo | null> = sync.changeset();
const rows2: Promise<Database.RowExportInfo> = sync.exportRows('t', { where: 'id > ?', params: [0], transform: (r) => r, partition: false });
const manifest: Promise<Database.Manifest | null> = sync.manifest();
sync.on('snapshot', (info) => void info.generation);
sync.on('error', (err: Error) => void err);
const gen: number = sync.generation;
const unsupported: string[] = sync.unsupportedTables();
void [snapInfo, cs, rows2, manifest, gen, unsupported, sync.keys().manifest, sync.start().stop(), sync.drain(), Database.engine.sessions];

const restored: Promise<Database.RestoreResult> = Database.Importer.restore({ store, prefix: 'sqlite', dbId: sync.dbId, to: '/tmp/r.db', onConflict: 'replace', upToSeq: 5 });
const loaded: Promise<Database.LoadResult> = Database.Importer.load(db, { store, key: 'x.ndjson.gz', table: 't', mode: 'upsert', batch: 100, transform: (row, line) => (line > 0 ? row : null) });
void [restored, loaded, Database.Importer.TABLE];

const consumer: Database.Consumer = new Database.Consumer(db, {
  source: queue,
  handler(payload: unknown, message: Database.Message) { void [payload, message.attributes['x-job-kind']]; },
  maxBatch: 10,
  autoStart: false
});
consumer.on('dead', (message, err) => void [message.id, err.message]);
consumer.on('retry', (message, err, delayMs: number) => void [message, err, delayMs]);
const deadMessages: Database.ConsumerDeadMessage[] = consumer.dead();
void [deadMessages, consumer.purge(1000), consumer.start().stop(), consumer.drain(), Database.Consumer.TABLE];
