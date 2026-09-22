'use strict';

// db.timeseries(table, options): time-series aggregation over a timestamp
// column, mixed onto Database.prototype by lib/database.js. Buckets rows by a
// calendar interval, aggregates a measure per bucket, and (optionally) fills
// empty buckets, computes a moving average and a per-bucket rate (delta).
//
// Timestamps may be stored as epoch milliseconds ('ms', default), epoch seconds
// ('s') or ISO-8601 text ('iso'). All bucketing is done in UTC (SQLite's
// default), so a bucket label like '2026-01-02' is the UTC day.
//
// An extension beyond the better-sqlite3 surface. Values are always bound as
// parameters; identifiers are always quoted. `fillGaps` aligns its window in
// JavaScript (UTC) and generates the bucket series with a recursive CTE, so no
// timestamp arithmetic depends on untrusted input.

const { quote, tableExists } = require('./schema');

const INTERVALS = new Set(['minute', 'hour', 'day', 'week', 'month', 'year']);
const AGGS = new Set(['sum', 'avg', 'count', 'min', 'max']);
const UNITS = new Set(['ms', 's', 'iso']);

// strftime label format per interval (the bucket key).
const LABEL = {
  minute: '%Y-%m-%d %H:%M',
  hour: '%Y-%m-%d %H',
  day: '%Y-%m-%d',
  week: '%Y-%m-%d',     // the week's Monday date
  month: '%Y-%m',
  year: '%Y'
};

// datetime() step per interval, for the gap-filling series.
const STEP = {
  minute: '+1 minute', hour: '+1 hour', day: '+1 day',
  week: '+7 days', month: '+1 month', year: '+1 year'
};

// A normalized "epoch seconds as REAL" SQL expression for the time column.
function normSeconds(timeExpr, unit) {
  switch (unit) {
    case 's': return `CAST(${timeExpr} AS REAL)`;
    case 'iso': return `CAST(strftime('%s', ${timeExpr}) AS REAL)`;
    case 'ms':
    default: return `CAST(${timeExpr} AS REAL) / 1000.0`;
  }
}

// The bucket-label SQL expression for a row's normalized-seconds expression N.
function bucketExpr(N, interval) {
  if (interval === 'week') {
    // Monday of the row's week: subtract (dow-Monday) days. %w is 0=Sun..6=Sat.
    return `strftime('%Y-%m-%d', ${N}, 'unixepoch', '-' || ((strftime('%w', ${N}, 'unixepoch') + 6) % 7) || ' days')`;
  }
  return `strftime('${LABEL[interval]}', ${N}, 'unixepoch')`;
}

function toEpochMs(value, what) {
  const ms = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(ms)) throw new TypeError(`Expected "${what}" to be a Date or epoch-ms number`);
  return ms;
}

// Aligns an epoch-ms instant to the start of its UTC bucket and formats it as
// 'YYYY-MM-DD HH:MM:SS' (UTC), matching SQLite's datetime() text.
function alignStart(ms, interval) {
  const d = new Date(ms);
  d.setUTCMilliseconds(0);
  d.setUTCSeconds(0);
  if (interval === 'minute') { /* seconds already zeroed */ }
  else if (interval === 'hour') { d.setUTCMinutes(0); }
  else {
    d.setUTCMinutes(0); d.setUTCHours(0);
    if (interval === 'day') { /* done */ }
    else if (interval === 'week') { const dow = (d.getUTCDay() + 6) % 7; d.setUTCDate(d.getUTCDate() - dow); }
    else if (interval === 'month') { d.setUTCDate(1); }
    else if (interval === 'year') { d.setUTCMonth(0, 1); }
  }
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}

class TimeseriesBuilder {
  constructor(db, table, options) {
    if (!tableExists(db, table)) throw new TypeError(`No such table or view: ${table}`);
    if (options === null || typeof options !== 'object') throw new TypeError('Expected options to be an object');
    if (typeof options.time !== 'string' || !options.time) throw new TypeError('Expected the "time" option to be a column name');
    const unit = options.unit === undefined ? 'ms' : options.unit;
    if (!UNITS.has(unit)) throw new TypeError(`Expected "unit" to be one of: ${[...UNITS].join(', ')}`);
    const where = options.where === undefined ? '' : options.where;
    if (typeof where !== 'string') throw new TypeError('Expected the "where" option to be a string');
    const params = options.params === undefined ? [] : options.params;
    if (!Array.isArray(params)) throw new TypeError('Expected the "params" option to be an array');

    this._db = db;
    this._table = table;
    this._time = options.time;
    this._value = options.value;
    this._unit = unit;
    this._where = where;
    this._whereParams = params;
    this._interval = null;
    this._agg = 'sum';
    this._as = 'value';
    this._fill = null;         // { from, to, fill }
    this._windows = [];        // extra { expr, as }
    this._dir = 'ASC';
    this._limit = null;
  }

  bucket(interval) {
    if (!INTERVALS.has(interval)) throw new TypeError(`Expected the interval to be one of: ${[...INTERVALS].join(', ')}`);
    this._interval = interval;
    return this;
  }

  agg(fn = 'sum') {
    if (!AGGS.has(fn)) throw new TypeError(`Expected the aggregate to be one of: ${[...AGGS].join(', ')}`);
    this._agg = fn;
    return this;
  }

  fillGaps(options = {}) {
    if (options === null || typeof options !== 'object') throw new TypeError('Expected fillGaps options to be an object');
    if (options.from === undefined || options.to === undefined) throw new TypeError('fillGaps needs "from" and "to"');
    const fill = options.fill === undefined ? 0 : options.fill;
    this._fill = { from: toEpochMs(options.from, 'from'), to: toEpochMs(options.to, 'to'), fill };
    return this;
  }

  movingAverage(n, options = {}) {
    if (!Number.isInteger(n) || n < 1) throw new TypeError('Expected the window size to be a positive integer');
    const as = options.as || 'moving_avg';
    this._windows.push({ expr: `avg(value) OVER (ORDER BY bucket ROWS BETWEEN ${n - 1} PRECEDING AND CURRENT ROW)`, as });
    return this;
  }

  rate(options = {}) {
    const as = options.as || 'rate';
    this._windows.push({ expr: `value - lag(value) OVER (ORDER BY bucket)`, as });
    return this;
  }

  orderBy(dir = 'asc') {
    const d = String(dir).toLowerCase();
    if (d !== 'asc' && d !== 'desc') throw new TypeError('Expected the direction to be "asc" or "desc"');
    this._dir = d.toUpperCase();
    return this;
  }

  limit(n) {
    if (!Number.isInteger(n) || n < 0) throw new TypeError('Expected the limit to be a non-negative integer');
    this._limit = n;
    return this;
  }

  // ---- build ----

  _aggExpr() {
    if (this._agg === 'count') return 'count(*)';
    if (typeof this._value !== 'string' || !this._value) {
      throw new TypeError(`The "${this._agg}" aggregate needs a "value" column`);
    }
    return `${this._agg}(${quote(this._value)})`;
  }

  toSQL() {
    if (!this._interval) throw new TypeError('Call bucket(interval) before running a time-series query');
    const N = normSeconds(quote(this._time), this._unit);
    const label = bucketExpr(N, this._interval);
    const inner = `SELECT ${label} AS bucket, ${this._aggExpr()} AS value FROM ${quote(this._table)}` +
      `${this._where ? ` WHERE ${this._where}` : ''} GROUP BY bucket`;

    let prefix = '';
    let core;
    const params = [];
    if (this._fill) {
      const from = alignStart(this._fill.from, this._interval);
      const to = alignStart(this._fill.to, this._interval);
      const step = STEP[this._interval];
      prefix = `WITH RECURSIVE _series(_t) AS (` +
        `SELECT ? UNION ALL SELECT datetime(_t, '${step}') FROM _series WHERE datetime(_t, '${step}') <= ?` +
        `), _labels(bucket) AS (SELECT strftime('${LABEL[this._interval]}', _t) FROM _series) `;
      core = `SELECT _labels.bucket AS bucket, COALESCE(a.value, ?) AS value ` +
        `FROM _labels LEFT JOIN (${inner}) a ON a.bucket = _labels.bucket`;
      params.push(from, to, this._fill.fill, ...this._whereParams);
    } else {
      core = inner;
      params.push(...this._whereParams);
    }

    let sql;
    if (this._windows.length) {
      const cols = this._windows.map((w) => `${w.expr} AS ${quote(w.as)}`).join(', ');
      sql = `${prefix}SELECT c.bucket AS bucket, c.value AS value, ${cols} FROM (${core}) c ORDER BY c.bucket ${this._dir}`;
    } else {
      sql = `${prefix}${core} ORDER BY bucket ${this._dir}`;
    }
    if (this._limit !== null) sql += ` LIMIT ${this._limit}`;
    return { sql, params };
  }

  all() {
    const { sql, params } = this.toSQL();
    return this._db.prepare(sql).all(...params);
  }
}

function timeseries(table, options = {}) {
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a table name');
  return new TimeseriesBuilder(this, table, options);
}

module.exports = { timeseries, TimeseriesBuilder };
