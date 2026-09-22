'use strict';

// db.partitioned(name, options): split a large logical table into child tables
// keyed by a date period or an integer range, with a UNION ALL view for unified
// reads. Mixed onto Database.prototype by lib/database.js.
//
// A child table for partition key `k` is `<name>__p_<k>` and shares the schema
// of a template table (an existing table `<name>`, or `options.like`, or a
// `(col defs)` body in `options.createSql`). Reads across all partitions go
// through the view `<name>_all`.
//
// An extension beyond the better-sqlite3 surface, useful for big time-series or
// event data where one table would grow unbounded. All values are bound; all
// identifiers are quoted.

const { quote, tableInfo, tableExists } = require('./schema');

const DATE_INTERVALS = new Set(['day', 'week', 'month', 'year']);
const UNITS = new Set(['ms', 'iso']);
const DAY_MS = 86400000;

function pad(n) { return String(n).padStart(2, '0'); }

// Replaces the `CREATE TABLE [IF NOT EXISTS] <name>` prefix of a recorded table
// definition with a new (IF NOT EXISTS) name, keeping the whole column/constraint
// body intact.
function rewriteCreate(sql, childName) {
  return sql.replace(
    /^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|[A-Za-z0-9_$]+)/i,
    `CREATE TABLE IF NOT EXISTS ${quote(childName)}`
  );
}

function partitioned(name, options = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected first argument to be a non-empty string');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  const db = this;

  const column = options.column;
  if (typeof column !== 'string' || !column) throw new TypeError('partitioned() needs a "column"');
  const by = options.by === undefined ? 'date' : options.by;
  if (by !== 'date' && by !== 'range') throw new TypeError('Expected "by" to be "date" or "range"');

  let interval = options.interval;
  let unit = 'ms';
  if (by === 'date') {
    if (interval === undefined) interval = 'month';
    if (!DATE_INTERVALS.has(interval)) throw new TypeError(`Expected "interval" to be one of: ${[...DATE_INTERVALS].join(', ')}`);
    unit = options.unit === undefined ? 'ms' : options.unit;
    if (!UNITS.has(unit)) throw new TypeError(`Expected "unit" to be one of: ${[...UNITS].join(', ')}`);
  } else {
    if (!Number.isInteger(interval) || interval <= 0) throw new TypeError('range partitioning needs a positive integer "interval"');
  }

  // Resolve the template schema (for the child CREATE and column list).
  const like = options.like === undefined ? name : options.like;
  let templateSql = null;      // a full CREATE TABLE to rewrite per child
  if (typeof options.createSql === 'string' && options.createSql.trim()) {
    templateSql = `CREATE TABLE __x ${options.createSql}`;   // body; name gets rewritten
  } else {
    if (!tableExists(db, like)) {
      throw new TypeError(`partitioned() needs a template table "${like}" or a "createSql" body`);
    }
    templateSql = db.prepare('SELECT sql FROM sqlite_master WHERE type = ? AND name = ?').pluck().get('table', like);
    if (!templateSql) throw new TypeError(`Template "${like}" has no recorded CREATE TABLE`);
    if (tableExists(db, like)) {
      const cols = tableInfo(db, like).map((c) => c.name);
      if (!cols.includes(column)) throw new TypeError(`No such column in ${like}: ${column}`);
    }
  }

  const viewName = `${name}_all`;
  const childPrefix = `${name}__p_`;
  let columns = null;   // resolved lazily from the first child (always accurate)

  const toMs = (value) => {
    if (value instanceof Date) return value.getTime();
    if (unit === 'iso') {
      const ms = typeof value === 'number' ? value : Date.parse(value);
      if (!Number.isFinite(ms)) throw new TypeError(`Cannot parse timestamp: ${value}`);
      return ms;
    }
    const ms = Number(value);
    if (!Number.isFinite(ms)) throw new TypeError(`Expected an epoch-ms timestamp, got: ${value}`);
    return ms;
  };

  const keyFor = (value) => {
    if (by === 'range') {
      const n = Number(value);
      if (!Number.isFinite(n)) throw new TypeError(`Expected a numeric value, got: ${value}`);
      return String(Math.floor(n / interval) * interval);
    }
    const d = new Date(toMs(value));
    const y = d.getUTCFullYear();
    if (interval === 'year') return `${y}`;
    if (interval === 'month') return `${y}_${pad(d.getUTCMonth() + 1)}`;
    if (interval === 'week') {
      const dow = (d.getUTCDay() + 6) % 7;   // 0 = Monday
      const m = new Date(d.getTime() - dow * DAY_MS);
      return `${m.getUTCFullYear()}_${pad(m.getUTCMonth() + 1)}_${pad(m.getUTCDate())}`;
    }
    return `${y}_${pad(d.getUTCMonth() + 1)}_${pad(d.getUTCDate())}`;   // day
  };

  const tableFor = (value) => `${childPrefix}${keyFor(value)}`;
  const childName = (key) => `${childPrefix}${key}`;

  const ensureTable = (key) => {
    const child = childName(key);
    if (!tableExists(db, child)) db.exec(rewriteCreate(templateSql, child));
    if (!columns) columns = tableInfo(db, child).map((c) => c.name);
    return child;
  };

  const existingChildren = () => db.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ? ESCAPE '\\' ORDER BY name"
  ).pluck().all(`${childPrefix.replace(/[\\%_]/g, '\\$&')}%`);

  const keyOf = (child) => child.slice(childPrefix.length);

  const rebuildView = () => {
    const children = existingChildren();
    db.exec(`DROP VIEW IF EXISTS ${quote(viewName)}`);
    if (children.length === 0) return viewName;
    const union = children.map((c) => `SELECT * FROM ${quote(c)}`).join(' UNION ALL ');
    db.exec(`CREATE VIEW ${quote(viewName)} AS ${union}`);
    return viewName;
  };

  const insertInto = (child, row) => {
    if (!columns) columns = tableInfo(db, child).map((c) => c.name);
    const keys = Object.keys(row);
    for (const k of keys) if (!columns.includes(k)) throw new TypeError(`No such column in ${name}: ${k}`);
    if (keys.length === 0) throw new TypeError('Cannot insert an empty row');
    const sql = `INSERT INTO ${quote(child)} (${keys.map(quote).join(', ')}) VALUES (${keys.map((k) => `@${k}`).join(', ')})`;
    return db.prepare(sql).run(row);
  };

  const valueOf = (row) => {
    if (row === null || typeof row !== 'object') throw new TypeError('Each row must be an object');
    if (!(column in row)) throw new TypeError(`Row is missing the partition column "${column}"`);
    return row[column];
  };

  return {
    keyFor,
    tableFor,

    ensure(value) {
      const child = ensureTable(keyFor(value));
      rebuildView();
      return child;
    },

    insert(row) {
      const child = ensureTable(keyFor(valueOf(row)));
      const info = insertInto(child, row);
      rebuildView();
      return info;
    },

    insertMany(rows) {
      if (!Array.isArray(rows)) throw new TypeError('Expected an array of rows');
      if (rows.length === 0) return { inserted: 0 };
      const groups = new Map();
      for (const row of rows) {
        const key = keyFor(valueOf(row));
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(row);
      }
      let inserted = 0;
      const run = db.transaction(() => {
        for (const [key, group] of groups) {
          const child = ensureTable(key);
          for (const row of group) inserted += insertInto(child, row).changes;
        }
      });
      run();
      rebuildView();
      return { inserted };
    },

    partitions() {
      return existingChildren().map((child) => ({
        key: keyOf(child),
        table: child,
        rows: db.prepare(`SELECT count(*) FROM ${quote(child)}`).pluck().get()
      }));
    },

    drop(key, opts = {}) {
      const child = childName(String(key));
      if (!opts.ifExists && !tableExists(db, child)) throw new TypeError(`No such partition: ${key}`);
      db.exec(`DROP TABLE IF EXISTS ${quote(child)}`);
      rebuildView();
      return this;
    },

    view() { return rebuildView(); },

    count() {
      return existingChildren().reduce((total, child) =>
        total + db.prepare(`SELECT count(*) FROM ${quote(child)}`).pluck().get(), 0);
    }
  };
}

module.exports = { partitioned };
