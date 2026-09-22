'use strict';

// Scheduler: recurring and delayed jobs on top of a Queue.
//
//   const jobs = new Database.Queue(db, { routes });
//   const cron = new Database.Scheduler(db, { queue: jobs });
//   cron.every(60_000, 'heartbeat', {});                 // every minute
//   cron.cron('0 3 * * *', 'nightly.rollup', { day: 1 }); // 03:00 UTC daily
//   cron.at(Date.parse('2026-01-01T00:00:00Z'), 'welcome', {});
//
// A schedule is a persisted rule (in `_better_sqlite_schedules`) with a
// next_run time. On each tick the scheduler finds the rules that are due,
// enqueues one job per rule into the Queue, and rolls next_run forward
// (recurring) or removes the rule (one-shot). Because the rules and their
// next_run live in the database, a restart resumes exactly where it left off,
// and two processes on one file both see the same schedule.
//
// The scheduler only decides WHEN; the Queue owns delivery, retries and
// dead-lettering. `queue` is anything with enqueue(kind, payload, options?),
// so a Database.Queue in production and a plain object in tests both work.
//
// Like the Queue, the tick timer is unref'd, so the scheduler never keeps a
// process alive on its own, and a failure inside a tick is emitted as 'error'
// rather than thrown into the timer.

const { EventEmitter } = require('events');
const crypto = require('crypto');
const { kDb, isPlainObject, positiveInt } = require('../adapters/util');
const { quote } = require('./schema');
const { tables } = require('./dictionary');

const TABLE = tables.schedules;

const TYPE = { every: 'every', cron: 'cron', at: 'at' };

class Scheduler extends EventEmitter {
  constructor(db, options = {}) {
    super();
    if (!db || typeof db !== 'object' || !db[kDb]) throw new TypeError('Expected first argument to be a Database');
    if (!isPlainObject(options)) throw new TypeError('Expected second argument to be an options object');
    if (db.readonly) throw new TypeError('Cannot attach a Scheduler to a readonly database');
    if (!options.queue || typeof options.queue.enqueue !== 'function') {
      throw new TypeError('Expected the "queue" option to be a Queue (an object with enqueue())');
    }

    this.db = db;
    this.queue = options.queue;
    this.tickMs = positiveInt(options, 'tickMs', 1000);
    this.running = false;
    this.timer = null;

    db.exec(`
      CREATE TABLE IF NOT EXISTS ${quote(TABLE)} (
        name       TEXT PRIMARY KEY,
        kind       TEXT    NOT NULL,
        payload    TEXT    NOT NULL,
        spec       TEXT    NOT NULL,
        type       TEXT    NOT NULL,
        next_run   INTEGER NOT NULL,
        last_run   INTEGER,
        created_at INTEGER NOT NULL
      )`);

    this._upsert = db.prepare(`
      INSERT INTO ${quote(TABLE)} (name, kind, payload, spec, type, next_run, last_run, created_at)
      VALUES (@name, @kind, @payload, @spec, @type, @next_run, NULL, @created_at)
      ON CONFLICT(name) DO UPDATE SET
        kind = @kind, payload = @payload, spec = @spec, type = @type, next_run = @next_run`);
    this._due = db.prepare(`SELECT * FROM ${quote(TABLE)} WHERE next_run <= ? ORDER BY next_run, name`);
    this._reschedule = db.prepare(`UPDATE ${quote(TABLE)} SET next_run = ?, last_run = ? WHERE name = ?`);
    this._markRun = db.prepare(`UPDATE ${quote(TABLE)} SET last_run = ? WHERE name = ?`);
    this._delete = db.prepare(`DELETE FROM ${quote(TABLE)} WHERE name = ?`);
    this._selectOne = db.prepare(`SELECT * FROM ${quote(TABLE)} WHERE name = ?`);
    this._selectAll = db.prepare(`SELECT * FROM ${quote(TABLE)} ORDER BY name`);

    if (options.autoStart !== false) this.start();
  }

  // ---- registration ------------------------------------------------------

  every(intervalMs, kind, payload, options = {}) {
    if (!Number.isInteger(intervalMs) || intervalMs <= 0) throw new TypeError('Expected the interval to be a positive integer of milliseconds');
    const now = Date.now();
    const startAt = options.startAt === undefined ? now + intervalMs : toMs(options.startAt, 'startAt');
    return this._register(TYPE.every, String(intervalMs), kind, payload, startAt, options.name);
  }

  cron(expr, kind, payload, options = {}) {
    const parsed = parseCron(expr);               // throws on a bad expression
    const next = cronNext(parsed, Date.now());
    return this._register(TYPE.cron, expr, kind, payload, next, options.name);
  }

  at(date, kind, payload, options = {}) {
    return this._register(TYPE.at, '', kind, payload, toMs(date, 'date'), options.name);
  }

  _register(type, spec, kind, payload, nextRun, name) {
    if (typeof kind !== 'string' || !kind) throw new TypeError('Expected the kind to be a non-empty string');
    let body;
    try {
      body = JSON.stringify(payload === undefined ? null : payload);
    } catch (err) {
      throw new TypeError(`Schedule payload is not JSON-serializable: ${err.message}`);
    }
    const finalName = name || `${type}:${kind}:${crypto.randomBytes(6).toString('hex')}`;
    this._upsert.run({ name: finalName, kind, payload: body, spec, type, next_run: nextRun, created_at: Date.now() });
    return finalName;
  }

  remove(name) {
    return this._delete.run(name).changes > 0;
  }

  get(name) {
    return decode(this._selectOne.get(name));
  }

  list() {
    return this._selectAll.all().map(decode);
  }

  // ---- ticking -----------------------------------------------------------

  // Synchronous: enqueue every rule that is due at `now`, then advance it.
  // Returns the number of rules fired. Never throws for a single rule; an
  // enqueue failure is emitted as 'error' and the rest still run.
  tick(now = Date.now()) {
    let fired = 0;
    for (const row of this._due.all(now)) {
      let ok = true;
      try {
        this.queue.enqueue(row.kind, JSON.parse(row.payload), {});
      } catch (err) {
        ok = false;
        this.emit('error', err);
      }
      if (row.type === TYPE.at) {
        // One-shot: remove only once it has been handed off, so a failed
        // enqueue is retried on the next tick (at-least-once).
        if (ok) { this._delete.run(row.name); fired += 1; this.emit('fired', row.name, row.kind); }
      } else {
        const next = row.type === TYPE.cron
          ? cronNext(parseCron(row.spec), now)
          : now + Number(row.spec);        // 'every': from now, so a backlog never bursts
        this._reschedule.run(next, now, row.name);
        if (ok) { fired += 1; this.emit('fired', row.name, row.kind); }
      }
    }
    return fired;
  }

  // ---- lifecycle ---------------------------------------------------------

  start() {
    if (this.running) return this;
    this.running = true;
    this.timer = setInterval(() => {
      try { this.tick(); } catch (err) { this.emit('error', err); }
    }, this.tickMs);
    this.timer.unref();
    return this;
  }

  stop() {
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
    this.running = false;
    return this;
  }

  drain() {
    this.stop();
    return Promise.resolve();
  }
}

Scheduler.TABLE = TABLE;

function toMs(value, what) {
  const ms = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(ms)) throw new TypeError(`Expected "${what}" to be a Date or epoch milliseconds`);
  return Math.round(ms);
}

function decode(row) {
  if (!row) return undefined;
  return Object.assign({}, row, { payload: JSON.parse(row.payload) });
}

// ---- cron ----------------------------------------------------------------
//
// Standard 5-field cron in UTC: minute hour day-of-month month day-of-week.
// Each field is '*', a value, a list (a,b), a range (a-b) or a step (*/n,
// a-b/n). Day-of-week is 0-6 with 0 = Sunday (7 is accepted as Sunday too).
// When both day-of-month and day-of-week are restricted, a match on EITHER
// fires (the traditional Vixie-cron rule).

const FIELDS = [
  { min: 0, max: 59 },   // minute
  { min: 0, max: 23 },   // hour
  { min: 1, max: 31 },   // day of month
  { min: 1, max: 12 },   // month
  { min: 0, max: 6 }     // day of week
];

function parseCron(expr) {
  if (typeof expr !== 'string') throw new TypeError('Expected a cron expression string');
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) throw new TypeError(`Expected a 5-field cron expression, got "${expr}"`);
  const fields = parts.map((part, i) => parseField(part, FIELDS[i].min, FIELDS[i].max, expr));
  return {
    minute: fields[0], hour: fields[1], dom: fields[2], month: fields[3], dow: fields[4],
    domStar: parts[2] === '*', dowStar: parts[4] === '*'
  };
}

function parseField(field, min, max, expr) {
  const set = new Set();
  for (const piece of field.split(',')) {
    const [rangePart, stepPart] = piece.split('/');
    const step = stepPart === undefined ? 1 : Number(stepPart);
    if (!Number.isInteger(step) || step <= 0) throw new TypeError(`Invalid step in cron field "${piece}" of "${expr}"`);
    let lo;
    let hi;
    if (rangePart === '*') { lo = min; hi = max; }
    else if (rangePart.includes('-')) { const [a, b] = rangePart.split('-').map(Number); lo = a; hi = b; }
    else { lo = hi = Number(rangePart); }
    if (![lo, hi].every(Number.isInteger)) throw new TypeError(`Invalid cron field "${piece}" of "${expr}"`);
    // Day-of-week 7 means Sunday.
    if (max === 6) { if (lo === 7) lo = 0; if (hi === 7) hi = 0; }
    if (lo > hi || lo < min || hi > max) throw new TypeError(`Cron field "${piece}" out of range in "${expr}"`);
    for (let v = lo; v <= hi; v += step) set.add(v);
  }
  return set;
}

function dayMatches(cron, date) {
  const dom = date.getUTCDate();
  const dow = date.getUTCDay();
  if (cron.domStar && cron.dowStar) return true;
  if (cron.domStar) return cron.dow.has(dow);
  if (cron.dowStar) return cron.dom.has(dom);
  return cron.dom.has(dom) || cron.dow.has(dow);   // Vixie "either" rule
}

// The first minute strictly after `fromMs` that matches `cron` (UTC epoch ms).
function cronNext(cron, fromMs) {
  const d = new Date(fromMs);
  d.setUTCSeconds(0, 0);
  d.setUTCMinutes(d.getUTCMinutes() + 1);
  // Bound the search well past a 4-year leap cycle so an impossible spec
  // (e.g. Feb 30) fails loudly instead of looping forever.
  const limit = 5 * 366 * 24 * 60;
  for (let i = 0; i < limit; i += 1) {
    if (!cron.month.has(d.getUTCMonth() + 1)) {
      // Jump to the first day of the next month at 00:00.
      d.setUTCMonth(d.getUTCMonth() + 1, 1);
      d.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!dayMatches(cron, d)) { d.setUTCDate(d.getUTCDate() + 1); d.setUTCHours(0, 0, 0, 0); continue; }
    if (!cron.hour.has(d.getUTCHours())) { d.setUTCHours(d.getUTCHours() + 1, 0, 0, 0); continue; }
    if (!cron.minute.has(d.getUTCMinutes())) { d.setUTCMinutes(d.getUTCMinutes() + 1, 0, 0); continue; }
    return d.getTime();
  }
  throw new Error('cron expression has no next occurrence within 5 years');
}

module.exports = { Scheduler, internals: { parseCron, cronNext } };
