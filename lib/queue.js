'use strict';

// Queue: fire-and-forget offload of work to HTTP services or message queues,
// made reliable by storing each job in the database it belongs to.
//
//   const jobs = new Database.Queue(db, {
//     routes: {
//       enrich: 'http://enrich.internal/jobs',            // HTTP POST per job
//       audit:  { sink: new QueueSink({ queueUrl }) },      // any MessageSink (adapters/util.js)
//       '*':    'http://events.internal/notify'
//     }
//   });
//   db.transaction((order) => {
//     insert.run(order);
//     jobs.enqueue('enrich', { orderId: order.id });   // same transaction
//   })(order);
//
// Why a table and not an in-memory queue: the caller's data change and the
// job that follows from it commit together or not at all, a crash between
// enqueue and delivery loses nothing, and a second process attached to the
// same file can share the work. This is the transactional outbox pattern;
// the price is one INSERT per job on the caller's connection.
//
// Delivery is at-least-once. A job is claimed with a lease (an atomic
// UPDATE ... RETURNING), handed to its route's sink as a JSON envelope, and
// deleted when the sink reports 'ok'. 'retry' (a 5xx, a timeout, a refused
// connection, a throttled queue) reschedules it with exponential backoff and
// jitter; 'dead' (a 4xx other than 408/425/429, a message the queue rejects
// as malformed) is a bad request that no retry will fix, so the job goes
// straight to the dead-letter state (failed_at set) for a human to inspect
// via dead() and retry(). Every envelope carries an idempotency key the
// receiving service should honour, because "at least once" means a slow
// success can be retried after its lease expires.
//
// A route is a URL (delivered by HttpSink, one POST per job) or any object
// with publish(envelopes) (a MessageSink, adapters/util.js), so the same job table feeds an
// HTTP service, a queue, a topic, an event bus or a stream. Jobs claimed in
// one tick are grouped by route and published in one call per sink, which
// is what the queue APIs batch on.
//
// This is the opposite discipline from lib/keepalive.js on purpose:
// keepalive is lossy and must never hold the process open; the queue
// is durable and keeps the process alive while work is in flight, then lets
// it exit when idle (anything still queued is dispatched next start).

const os = require('os');
const { EventEmitter } = require('events');
const { databaseId } = require('./identity');
const { kDb, isPlainObject, positiveInt, backoff, hasMethods, assertMessageSink } = require('../adapters/util');
const { HttpSink } = require('./http-sink');
const { tables, defaults, events, queue: names, outcomes } = require('./dictionary');

const TABLE = tables.queue;

const DEFAULTS = defaults.queue;

// Resolves a delivery target to a MessageSink. `spec` is a URL string or
// { url, timeoutMs? } (becomes an HttpSink), or a sink or { sink } (validated
// and returned as is). `what` names the option in error messages;
// `options.timeoutMs` is the HttpSink default when the spec gives none.
function toMessageSink(spec, what = 'sink', { timeoutMs } = {}) {
  const shape = `Expected "${what}" to be a URL string, a sink, or an object with a "url" or "sink"`;
  const route = typeof spec === 'string' ? { url: spec } : hasMethods(spec, ['publish']) ? { sink: spec } : spec;
  if (!isPlainObject(route)) throw new TypeError(shape);
  if (route.sink !== undefined) {
    if (route.url !== undefined) throw new TypeError(`"${what}" cannot have both "url" and "sink"`);
    return assertMessageSink(route.sink, `${what}.sink`);
  }
  if (typeof route.url !== 'string') throw new TypeError(shape);
  try {
    return new HttpSink(route.url, { timeoutMs: positiveInt(route, 'timeoutMs', timeoutMs) });
  } catch (err) {
    throw new TypeError(`"${what}": ${err.message} (${route.url})`);
  }
}

// routes: one spec for every kind, or { kind: spec, '*': spec }, where a spec
// is anything toMessageSink() accepts plus an optional per-route
// `attempts`. Each entry becomes { sink, attempts }.
function normalizeRoutes(routes, timeoutMs) {
  if (typeof routes === 'string' || (routes && typeof routes.publish === 'function')) {
    routes = { [names.anyRoute]: routes };
  }
  if (!isPlainObject(routes) || Object.keys(routes).length === 0) {
    throw new TypeError('Expected the "routes" option to be a URL string, a sink, or an object mapping job kinds to URLs or sinks');
  }
  const out = new Map();
  for (const [kind, spec] of Object.entries(routes)) {
    out.set(kind, {
      sink: toMessageSink(spec, `routes.${kind}`, { timeoutMs }),
      attempts: isPlainObject(spec) ? positiveInt(spec, 'attempts', undefined) : undefined
    });
  }
  return out;
}

class Queue extends EventEmitter {
  constructor(db, options) {
    super();
    if (!db || typeof db !== 'object' || !db[kDb]) throw new TypeError('Expected first argument to be a Database');
    if (!isPlainObject(options)) throw new TypeError('Expected second argument to be an options object');
    if (db.readonly) throw new TypeError('Cannot attach a Queue to a readonly database');

    this.db = db;
    this.timeoutMs = positiveInt(options, 'timeoutMs', DEFAULTS.timeoutMs);
    this.routes = normalizeRoutes(options.routes, this.timeoutMs);
    this.concurrency = positiveInt(options, 'concurrency', DEFAULTS.concurrency);
    this.pollMs = positiveInt(options, 'pollMs', DEFAULTS.pollMs);
    this.batch = positiveInt(options, 'batch', DEFAULTS.batch);
    this.attempts = positiveInt(options, 'attempts', DEFAULTS.attempts);
    this.backoffMs = positiveInt(options, 'backoffMs', DEFAULTS.backoffMs);
    this.maxBackoffMs = positiveInt(options, 'maxBackoffMs', DEFAULTS.maxBackoffMs);
    // A lease outlives the longest request by a margin so a process that
    // dies mid-POST releases its jobs, but a slow success is not redelivered
    // while still in flight.
    this.leaseMs = positiveInt(options, 'leaseMs', this.timeoutMs + DEFAULTS.leaseExtraMs);

    this.inflight = 0;
    this.running = false;
    this.timer = null;
    this.draining = null;
    this.dbId = databaseId(db.name, db.memory);

    db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLE} (
        id           INTEGER PRIMARY KEY,
        kind         TEXT    NOT NULL,
        payload      TEXT    NOT NULL,
        run_at       INTEGER NOT NULL,
        attempts     INTEGER NOT NULL DEFAULT 0,
        locked_until INTEGER,
        last_error   TEXT,
        failed_at    INTEGER,
        created_at   INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ${TABLE}_due ON ${TABLE} (failed_at, run_at, id);
    `);

    this.stmts = {
      insert: db.prepare(`INSERT INTO ${TABLE} (kind, payload, run_at, created_at) VALUES (?, ?, ?, ?)`),
      claim: db.prepare(`
        UPDATE ${TABLE}
           SET locked_until = ?, attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM ${TABLE}
            WHERE failed_at IS NULL AND run_at <= ? AND (locked_until IS NULL OR locked_until <= ?)
            ORDER BY run_at, id
            LIMIT ?)
        RETURNING id, kind, payload, attempts, created_at`),
      done: db.prepare(`DELETE FROM ${TABLE} WHERE id = ?`),
      retry: db.prepare(`UPDATE ${TABLE} SET run_at = ?, locked_until = NULL, last_error = ? WHERE id = ?`),
      dead: db.prepare(`UPDATE ${TABLE} SET failed_at = ?, locked_until = NULL, last_error = ? WHERE id = ?`),
      pending: db.prepare(`SELECT count(*) AS n FROM ${TABLE} WHERE failed_at IS NULL`),
      due: db.prepare(`SELECT count(*) AS n FROM ${TABLE} WHERE failed_at IS NULL AND run_at <= ? AND (locked_until IS NULL OR locked_until <= ?)`),
      listDead: db.prepare(`SELECT id, kind, payload, attempts, last_error, failed_at, created_at FROM ${TABLE} WHERE failed_at IS NOT NULL ORDER BY failed_at, id`),
      revive: db.prepare(`UPDATE ${TABLE} SET failed_at = NULL, attempts = 0, run_at = ?, last_error = NULL WHERE id = ? AND failed_at IS NOT NULL`),
      purge: db.prepare(`DELETE FROM ${TABLE} WHERE failed_at IS NOT NULL AND failed_at < ?`)
    };

    if (options.autoStart !== false) this.start();
  }

  // ---- producer side (synchronous; runs on the caller's connection) -------

  // Stores a job. Inside db.transaction() this commits with the caller's
  // writes or not at all. Throws synchronously for anything that no retry
  // could fix (unknown kind, unserializable payload) so the bug surfaces in
  // the caller's stack, not in a dead-letter table an hour later.
  enqueue(kind, payload, options = {}) {
    if (typeof kind !== 'string' || kind === '' || kind === names.anyRoute) throw new TypeError('Expected job kind to be a non-empty string');
    if (!this.routes.has(kind) && !this.routes.has(names.anyRoute)) throw new TypeError(`No route for job kind "${kind}" and no "${names.anyRoute}" fallback`);
    if (!isPlainObject(options)) throw new TypeError('Expected third argument to be an options object');
    if (payload === undefined) throw new TypeError('Expected a payload (use null for none)');

    let json;
    try {
      json = JSON.stringify(payload);
    } catch (err) {
      throw new TypeError(`Payload for "${kind}" is not JSON-serializable: ${err.message}`);
    }
    if (json === undefined) throw new TypeError(`Payload for "${kind}" is not JSON-serializable`);

    const now = Date.now();
    let runAt = now;
    if ('runAt' in options) {
      runAt = options.runAt instanceof Date ? options.runAt.getTime() : options.runAt;
      if (!Number.isFinite(runAt)) throw new TypeError('Expected the "runAt" option to be a Date or epoch milliseconds');
    } else if ('delayMs' in options) {
      if (!Number.isInteger(options.delayMs) || options.delayMs < 0) throw new TypeError('Expected the "delayMs" option to be a non-negative integer');
      runAt = now + options.delayMs;
    }

    const { lastInsertRowid } = this.stmts.insert.run(kind, json, runAt, now);
    // The row is only visible to the dispatcher once the caller's transaction
    // commits, so wake on the next turn of the event loop rather than now.
    this._schedule(0);
    return Number(lastInsertRowid);
  }

  // ---- consumer side --------------------------------------------------------

  start() {
    if (this.running) return this;
    this.running = true;
    this._schedule(0);
    return this;
  }

  // Stops claiming new jobs. In-flight requests finish on their own; their
  // rows are settled when they do (or their leases expire if the process
  // dies first).
  stop() {
    this.running = false;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    return this;
  }

  // Dispatches everything that is due, waits for in-flight requests, then
  // stops. For graceful shutdown: `await jobs.drain(); db.close();`
  drain() {
    if (this.draining) return this.draining;
    this.draining = new Promise((resolve) => {
      const finish = () => { this.stop(); this.draining = null; resolve(); };
      const step = () => {
        if (!this.db.open) return finish();
        this._tick(true);
        if (this.inflight === 0 && this._dueCount() === 0) return finish();
        setTimeout(step, 25);
      };
      step();
    });
    return this.draining;
  }

  pending() { return this.stmts.pending.get().n; }

  dead() {
    return this.stmts.listDead.all().map((row) => Object.assign(row, { payload: JSON.parse(row.payload) }));
  }

  // Puts a dead-lettered job back in the queue with a fresh attempt budget.
  retry(id) {
    const { changes } = this.stmts.revive.run(Date.now(), id);
    if (changes) this._schedule(0);
    return changes === 1;
  }

  // Deletes dead-lettered jobs that failed before `olderThanMs` ago.
  purge(olderThanMs = 0) {
    return this.stmts.purge.run(Date.now() - olderThanMs).changes;
  }

  // ---- internals ------------------------------------------------------------

  // Arms the next tick. No-op while stopped, so callers can wake the
  // dispatcher unconditionally.
  _schedule(ms) {
    if (!this.running) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => { this.timer = null; this._tick(false); }, ms);
    // Hold the process only while there is (or will imminently be) work.
    // Idle polling must not keep a finished script alive: queued jobs are
    // on disk and will be dispatched by the next process that attaches.
    if (ms === 0 || this.inflight > 0) this.timer.ref(); else this.timer.unref();
  }

  _dueCount() {
    try { const now = Date.now(); return this.stmts.due.get(now, now).n; } catch { return 0; }
  }

  _tick(fromDrain) {
    if (!this.running && !fromDrain) return;
    if (!this.db.open) { this.stop(); return; }
    const room = this.concurrency - this.inflight;
    if (room <= 0) { if (!fromDrain) this._schedule(this.pollMs); return; }

    let claimed = [];
    try {
      const now = Date.now();
      claimed = this.stmts.claim.all(now + this.leaseMs, now, now, Math.min(room, this.batch));
    } catch (err) {
      // The connection is busy (an iterator is open, a user function is
      // running) or was closed under us. Neither is ours to fix; try later.
      this.emit(events.queue.stalled, err);
      if (!fromDrain) this._schedule(this.pollMs);
      return;
    }

    // One publish() per route: queue APIs batch, and HttpSink fans out itself.
    const groups = new Map();
    for (const job of claimed) {
      const route = this.routes.get(job.kind) || this.routes.get(names.anyRoute);
      if (!groups.has(route)) groups.set(route, []);
      groups.get(route).push(job);
    }
    for (const [route, jobs] of groups) this._dispatch(route, jobs);
    if (!fromDrain) this._schedule(claimed.length === this.batch ? 0 : this.pollMs);
  }

  _envelope(job, identity) {
    // Stable across processes and retries: the same job row always presents
    // the same key, so a receiver can dedupe.
    const key = `${this.dbId}:${job.id}`;
    const body = {
      id: job.id,
      kind: job.kind,
      payload: JSON.parse(job.payload),
      attempt: job.attempts,
      enqueuedAt: job.created_at,
      source: Object.assign({ db: this.dbId }, identity)
    };
    return {
      id: key,
      kind: job.kind,
      attempt: job.attempts,
      body: JSON.stringify(body),
      attributes: {
        [names.attributes.idempotencyKey]: key,
        [names.attributes.kind]: job.kind,
        [names.attributes.attempt]: String(job.attempts)
      },
      orderingKey: job.kind
    };
  }

  _dispatch(route, jobs) {
    const maxAttempts = route.attempts || this.attempts;
    const identity = { host: os.hostname(), pid: process.pid };
    const envelopes = jobs.map((job) => this._envelope(job, identity));
    const retry = (error) => ({ status: outcomes.retry, error });

    this.inflight += jobs.length;
    Promise.resolve()
      .then(() => route.sink.publish(envelopes))
      .then((results) => {
        // Every job gets exactly one outcome. A sink that returns fewer, or
        // one for an unknown id, leaves the rest as 'retry': safe, since a
        // redelivery is what the idempotency key is for.
        const byId = new Map();
        for (const outcome of Array.isArray(results) ? results : []) {
          if (outcome && !byId.has(outcome.id)) byId.set(outcome.id, outcome);
        }
        jobs.forEach((job, i) => this._settle(job, byId.get(envelopes[i].id) || retry('sink returned no outcome'), maxAttempts));
      }, (err) => {
        for (const job of jobs) this._settle(job, retry(err), maxAttempts);
      })
      .catch((err) => this.emit(events.queue.stalled, err))   // the database went away
      .finally(() => {
        this.inflight -= jobs.length;
        this._schedule(this.inflight === 0 ? 0 : this.pollMs);
      });
  }

  // Applies one sink Outcome ({ status, error? }) to a claimed job: delete
  // on 'ok', dead-letter on 'dead' or an exhausted budget, else reschedule.
  _settle(job, outcome, maxAttempts) {
    if (!this.db.open) return;   // lease will expire; the next process retries
    if (outcome.status === outcomes.ok) {
      this.stmts.done.run(job.id);
      this.emit(events.queue.delivered, job);
      return;
    }
    const err = outcome.error instanceof Error ? outcome.error
      : new Error(outcome.error ? String(outcome.error) : `sink reported ${outcome.status}`);
    const message = err.message || String(err);
    const now = Date.now();
    if (outcome.status === outcomes.dead || job.attempts >= maxAttempts) {
      this.stmts.dead.run(now, message, job.id);
      this.emit(events.queue.dead, job, err);
      return;
    }
    const delay = backoff(job.attempts, this.backoffMs, this.maxBackoffMs);
    this.stmts.retry.run(now + delay, message, job.id);
    this.emit(events.queue.retry, job, err, delay);
  }
}

Queue.TABLE = TABLE;
module.exports = Queue;
