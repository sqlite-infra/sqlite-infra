'use strict';

// Consumer: consume a message queue into the database, exactly once in effect.
//
//   const consumer = new Database.Consumer(db, {
//     source: new QueueSource({ queueUrl }),          // any MessageSource (lib/interfaces.js)
//     handler(payload, message) {                   // synchronous, inside a transaction
//       apply.run(payload.orderId, payload.status);
//     }
//   });
//
// The mirror image of lib/queue.js. A cloud queue delivers at least once,
// so the same message can arrive twice (a slow ack, a crashed consumer, a
// redrive). Here the message id is inserted into a table with a primary key
// in the same transaction that runs the handler: the handler's writes and
// the "seen" marker commit together or not at all, and a second delivery
// finds the marker and is acked without running anything. That is what
// "exactly once" can honestly mean: at-least-once delivery, idempotent
// processing, no double effects.
//
// The handler is synchronous on purpose. Transactions on this driver are
// synchronous, and the guarantee above only holds while the handler's work
// and the marker share one. Anything asynchronous (a follow-up HTTP call, a
// publish) belongs in the queue, enqueued from inside the handler so it
// commits with the rest.
//
// A handler that throws rolls the transaction back; the attempt is recorded
// outside it and the message is nacked with exponential backoff. A locked
// database (SQLITE_BUSY) is not the message's fault: it is nacked for one
// poll period and no attempt is spent. When the budget is spent the message
// is stored with its body as dead-lettered (failed_at set), acked so the
// queue stops redelivering it, and exposed via dead(). A body that is not
// JSON, or a handler that returns a promise, is dead-lettered at once: no
// retry fixes either.
//
// Unlike the queue, a running consumer keeps the process alive: a consumer is a
// server loop. stop() ends it after the current receive; drain() resolves
// when the loop has exited.

const { EventEmitter } = require('events');
const { kDb, isPlainObject, positiveInt, backoff } = require('./util');
const { assertMessageSource } = require('./interfaces');
const { tables, defaults, events, queue: queueNames } = require('./dictionary');

const TABLE = tables.consumer;

const DEFAULTS = defaults.consumer;

// An error that no retry can fix. Same discipline as a 4xx in the queue.
class PermanentError extends Error {
  constructor(message, cause) {
    super(message, cause ? { cause } : undefined);
    this.name = 'PermanentError';
  }
}

class Consumer extends EventEmitter {
  constructor(db, options) {
    super();
    if (!db || typeof db !== 'object' || !db[kDb]) throw new TypeError('Expected first argument to be a Database');
    if (!isPlainObject(options)) throw new TypeError('Expected second argument to be an options object');
    if (db.readonly) throw new TypeError('Cannot attach a Consumer to a readonly database');
    if (typeof options.handler !== 'function') throw new TypeError('Expected the "handler" option to be a function');

    this.db = db;
    this.source = assertMessageSource(options.source, 'source');
    this.handler = options.handler;
    // maxBatch, pollMs, attempts, backoffMs, maxBackoffMs: see the dictionary.
    for (const key of Object.keys(DEFAULTS)) this[key] = positiveInt(options, key, DEFAULTS[key]);

    this.running = false;
    this.loop = null;        // promise of the running loop
    this.wakeup = null;      // resolves the idle sleep early
    this.handled = 0;

    db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLE} (
        message_id  TEXT    PRIMARY KEY,
        kind        TEXT,
        body        TEXT,
        attributes  TEXT,
        attempts    INTEGER NOT NULL DEFAULT 0,
        last_error  TEXT,
        handled_at  INTEGER,
        failed_at   INTEGER,
        received_at INTEGER NOT NULL
      ) WITHOUT ROWID;
      CREATE INDEX IF NOT EXISTS ${TABLE}_handled ON ${TABLE} (handled_at) WHERE handled_at IS NOT NULL;
    `);

    this.stmts = {
      get: db.prepare(`SELECT message_id, attempts, handled_at, failed_at FROM ${TABLE} WHERE message_id = ?`),
      // The marker. `changes` is 0 when another delivery got here first.
      mark: db.prepare(`
        INSERT INTO ${TABLE} (message_id, kind, attempts, handled_at, received_at)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT (message_id) DO UPDATE
           SET handled_at = excluded.handled_at, attempts = ${TABLE}.attempts + 1, kind = excluded.kind
         WHERE handled_at IS NULL AND failed_at IS NULL`),
      // A failed attempt, recorded outside the rolled-back transaction.
      fail: db.prepare(`
        INSERT INTO ${TABLE} (message_id, kind, attempts, last_error, received_at)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT (message_id) DO UPDATE
           SET attempts = ${TABLE}.attempts + 1, last_error = excluded.last_error
        RETURNING attempts`),
      dead: db.prepare(`UPDATE ${TABLE} SET failed_at = ?, body = ?, attributes = ?, last_error = ? WHERE message_id = ?`),
      listDead: db.prepare(`SELECT message_id, kind, body, attributes, attempts, last_error, failed_at, received_at FROM ${TABLE} WHERE failed_at IS NOT NULL ORDER BY failed_at, message_id`),
      purge: db.prepare(`DELETE FROM ${TABLE} WHERE handled_at IS NOT NULL AND handled_at <= ?`)
    };

    // One transaction per message: marker + handler, BEGIN IMMEDIATE so two
    // processes on one file serialize on the marker rather than both running.
    this.apply = db.transaction((message, payload) => {
      const now = Date.now();
      if (this.stmts.mark.run(message.id, kindOf(message, payload), now, now).changes === 0) return false;
      this._invoke(payload, message);
      return true;
    });

    if (options.autoStart !== false) this.start();
  }

  // ---- lifecycle ------------------------------------------------------------

  start() {
    if (this.running) return this;
    this.running = true;
    this.loop = this._run().finally(() => { this.running = false; this.loop = null; });
    return this;
  }

  // Ends the loop after the receive in progress. Messages already received
  // are still processed and acked or nacked before the loop exits.
  stop() {
    this.running = false;
    if (this.wakeup) { this.wakeup(); this.wakeup = null; }
    return this;
  }

  // stop(), then wait for the loop to exit. For graceful shutdown:
  // `await consumer.drain(); db.close();`
  drain() {
    const loop = this.loop;
    this.stop();
    return loop ? loop.then(() => undefined) : Promise.resolve();
  }

  // ---- introspection ----------------------------------------------------------

  dead() {
    return this.stmts.listDead.all().map((row) => ({
      id: row.message_id,
      kind: row.kind,
      payload: parseOrNull(row.body),
      body: row.body,
      attributes: parseOrNull(row.attributes) || {},
      attempts: row.attempts,
      last_error: row.last_error,
      failed_at: row.failed_at,
      received_at: row.received_at
    }));
  }

  // Deletes handled markers older than `olderThanMs`. This shrinks the
  // deduplication window: a redelivery older than that would run again.
  // Keep it longer than the queue's maximum retention.
  purge(olderThanMs) {
    if (!Number.isInteger(olderThanMs) || olderThanMs < 0) throw new TypeError('Expected olderThanMs to be a non-negative integer');
    return this.stmts.purge.run(Date.now() - olderThanMs).changes;
  }

  // ---- internals ------------------------------------------------------------

  async _run() {
    while (this.running && this.db.open) {
      try {
        const messages = await this.source.receive(this.maxBatch);
        if (Array.isArray(messages) && messages.length > 0) { await this._batch(messages); continue; }
      } catch (err) {
        // receive() failed, or the bookkeeping did (the database is locked or
        // gone). The queue redelivers what was not acked; report and back off.
        this.emit(events.consumer.stalled, err);
      }
      if (this.running) await this._sleep(this.pollMs);
    }
  }

  async _batch(messages) {
    const acks = [];
    const nacks = new Map();   // delayMs -> tokens
    for (const message of messages) {
      if (!this.db.open) return;
      const verdict = this._process(message);
      if (verdict.ack) acks.push(message.ackToken);
      else {
        if (!nacks.has(verdict.delayMs)) nacks.set(verdict.delayMs, []);
        nacks.get(verdict.delayMs).push(message.ackToken);
      }
    }
    try {
      if (acks.length) await this.source.ack(acks);
      for (const [delayMs, tokens] of nacks) await this.source.nack(tokens, delayMs);
    } catch (err) {
      // The queue will redeliver whatever we failed to ack; the marker makes
      // that harmless. Report it and move on.
      this.emit(events.consumer.stalled, err);
    }
  }

  // Synchronous: decides ack or nack for one message and records the outcome.
  _process(message) {
    if (!message || typeof message.id !== 'string' || typeof message.body !== 'string') {
      this.emit(events.consumer.stalled, new TypeError('MessageSource returned a message without string id and body'));
      return { ack: false, delayMs: this.pollMs };
    }
    const existing = this.stmts.get.get(message.id);
    if (existing && (existing.handled_at !== null || existing.failed_at !== null)) {
      this.emit(events.consumer.duplicate, message);
      return { ack: true };
    }

    let payload;
    try {
      payload = JSON.parse(message.body);
    } catch (err) {
      return this._failed(message, new PermanentError(`Message body is not JSON: ${err.message}`, err), true);
    }

    let ran;
    try {
      ran = this.apply.immediate(message, payload);
    } catch (err) {
      // Another connection holds the write lock: not the message's fault, so
      // no attempt is spent. Recording the attempt would need the lock too.
      if (err && (err.code === 'SQLITE_BUSY' || err.code === 'SQLITE_LOCKED')) {
        this.emit(events.consumer.stalled, err);
        return { ack: false, delayMs: this.pollMs };
      }
      return this._failed(message, err, err instanceof PermanentError);
    }
    if (!ran) {
      this.emit(events.consumer.duplicate, message);
      return { ack: true };
    }
    this.handled += 1;
    this.emit(events.consumer.handled, message, payload);
    return { ack: true };
  }

  _failed(message, err, permanent) {
    const now = Date.now();
    const text = String(err && err.message || err);
    const { attempts } = this.stmts.fail.get(message.id, kindOf(message, null), text, now);
    if (permanent || attempts >= this.attempts) {
      this.stmts.dead.run(now, message.body, JSON.stringify(message.attributes || {}), text, message.id);
      this.emit(events.consumer.dead, message, err);
      return { ack: true };   // the queue must stop redelivering it
    }
    const delayMs = backoff(attempts, this.backoffMs, this.maxBackoffMs);
    this.emit(events.consumer.retry, message, err, delayMs);
    return { ack: false, delayMs };
  }

  // Calls the handler inside the caller's transaction. A returned promise is
  // rejected: the transaction commits before it settles, so nothing it did
  // would be covered by the marker.
  _invoke(payload, message) {
    const result = this.handler.call(undefined, payload, message);
    if (result && typeof result.then === 'function') {
      throw new PermanentError('Consumer handler must be synchronous; it returned a promise');
    }
  }

  _sleep(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this.wakeup = null; resolve(); }, ms);
      this.wakeup = () => { clearTimeout(timer); resolve(); };
    });
  }
}

function kindOf(message, payload) {
  const attrs = message.attributes || {};
  if (typeof attrs[queueNames.attributes.kind] === 'string') return attrs[queueNames.attributes.kind];
  if (typeof attrs.kind === 'string') return attrs.kind;
  if (payload && typeof payload.kind === 'string') return payload.kind;
  return null;
}

function parseOrNull(text) {
  if (text == null) return null;
  try { return JSON.parse(text); } catch { return null; }
}

Consumer.TABLE = TABLE;
Consumer.PermanentError = PermanentError;
module.exports = Consumer;
