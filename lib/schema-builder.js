'use strict';

// Programmatic schema/DDL builder, mixed onto Database.prototype by
// lib/database.js. Lets a service declare tables and indexes in JavaScript
// instead of hand-writing CREATE statements, which pairs with the migration
// runner (db.migrate) for versioned schema changes.
//
// Extensions beyond the better-sqlite3 surface. Every identifier is quoted
// through lib/schema.js's quote(); DEFAULT/CHECK values are rendered as SQL
// literals (or left raw when wrapped in t.raw()). Each method issues an exec(),
// whose guard throws the usual TypeError on a closed connection, and returns
// the database so calls chain.

const { quote } = require('./schema');

// A raw SQL fragment, used where an expression (not a literal) is wanted, e.g.
// a DEFAULT of CURRENT_TIMESTAMP or a generated-column expression.
class Raw {
  constructor(sql) { this.sql = String(sql); }
}

// Renders a JS value as a SQL literal for DEFAULT.
function literal(value) {
  if (value instanceof Raw) return value.sql;
  if (value === null || value === undefined) return 'NULL';
  switch (typeof value) {
    case 'string': return `'${value.replace(/'/g, "''")}'`;
    case 'number': if (!Number.isFinite(value)) throw new TypeError('DEFAULT cannot be a non-finite number'); return String(value);
    case 'bigint': return value.toString();
    case 'boolean': return value ? '1' : '0';
    default: throw new TypeError('DEFAULT must be a string, number, bigint, boolean, null, or t.raw(expr)');
  }
}

function fkActions(opts) {
  let s = '';
  if (opts && opts.onDelete) s += ` ON DELETE ${String(opts.onDelete).toUpperCase()}`;
  if (opts && opts.onUpdate) s += ` ON UPDATE ${String(opts.onUpdate).toUpperCase()}`;
  return s;
}

// One column definition; every method mutates and returns `this` so
// constraints chain in source order.
class ColumnDef {
  constructor(name, type) {
    if (typeof name !== 'string' || !name) throw new TypeError('A column needs a non-empty name');
    this.name = name;
    this.type = type;
    this.parts = [];
  }
  primaryKey(options = {}) {
    let clause = 'PRIMARY KEY';
    if (options && options.autoincrement) clause += ' AUTOINCREMENT';
    this.parts.push(clause);
    return this;
  }
  notNull() { this.parts.push('NOT NULL'); return this; }
  unique() { this.parts.push('UNIQUE'); return this; }
  default(value) { this.parts.push(`DEFAULT ${literal(value)}`); return this; }
  references(table, column, options = {}) {
    if (typeof table !== 'string' || !table) throw new TypeError('references() needs a table name');
    if (column && typeof column === 'object' && !Array.isArray(column)) { options = column; column = undefined; }
    let clause = `REFERENCES ${quote(table)}`;
    if (column) clause += ` (${quote(column)})`;
    this.parts.push(clause + fkActions(options));
    return this;
  }
  check(expr) { this.parts.push(`CHECK (${assertExpr(expr)})`); return this; }
  collate(name) { if (typeof name !== 'string' || !name) throw new TypeError('collate() needs a name'); this.parts.push(`COLLATE ${name}`); return this; }
  generatedAs(expr, options = {}) {
    this.parts.push(`GENERATED ALWAYS AS (${assertExpr(expr)}) ${options && options.stored ? 'STORED' : 'VIRTUAL'}`);
    return this;
  }
  toSQL() {
    return `${quote(this.name)} ${this.type}${this.parts.length ? ` ${this.parts.join(' ')}` : ''}`;
  }
}

function assertExpr(expr) {
  if (expr instanceof Raw) return expr.sql;
  if (typeof expr !== 'string' || !expr) throw new TypeError('Expected a non-empty SQL expression string');
  return expr;
}

// The `t` handed to a builder function.
class TableBuilder {
  constructor() { this.columns = []; this.constraints = []; }
  _col(name, type) { const c = new ColumnDef(name, type); this.columns.push(c); return c; }
  integer(name) { return this._col(name, 'INTEGER'); }
  text(name) { return this._col(name, 'TEXT'); }
  real(name) { return this._col(name, 'REAL'); }
  blob(name) { return this._col(name, 'BLOB'); }
  numeric(name) { return this._col(name, 'NUMERIC'); }
  boolean(name) { return this._col(name, 'INTEGER'); }   // stored 0/1
  json(name) { return this._col(name, 'TEXT'); }         // stored as JSON text
  raw(sql) { return new Raw(sql); }
  primaryKey(...cols) { this.constraints.push(`PRIMARY KEY (${cols.map(quote).join(', ')})`); return this; }
  unique(...cols) { this.constraints.push(`UNIQUE (${cols.map(quote).join(', ')})`); return this; }
  check(expr) { this.constraints.push(`CHECK (${assertExpr(expr)})`); return this; }
  foreignKey(...cols) {
    const self = this;
    return {
      references(table, refCols, options = {}) {
        if (typeof table !== 'string' || !table) throw new TypeError('references() needs a table name');
        const rc = Array.isArray(refCols) ? refCols : [refCols];
        self.constraints.push(`FOREIGN KEY (${cols.map(quote).join(', ')}) REFERENCES ${quote(table)} (${rc.map(quote).join(', ')})${fkActions(options)}`);
        return self;
      }
    };
  }
}

function buildColumns(builderFn) {
  if (typeof builderFn !== 'function') throw new TypeError('Expected a builder function');
  const t = new TableBuilder();
  builderFn(t);
  return t;
}

// db.createTable(name, builderFn, options?): options { strict, withoutRowid,
// ifNotExists }.
function createTable(name, builderFn, options = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected first argument to be a non-empty string');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected third argument to be an options object');
  const t = buildColumns(builderFn);
  if (t.columns.length === 0) throw new TypeError('A table needs at least one column');
  const defs = [...t.columns.map((c) => c.toSQL()), ...t.constraints];
  const ifNot = options.ifNotExists ? 'IF NOT EXISTS ' : '';
  let sql = `CREATE TABLE ${ifNot}${quote(name)} (\n  ${defs.join(',\n  ')}\n)`;
  const tail = [];
  if (options.withoutRowid) tail.push('WITHOUT ROWID');
  if (options.strict) tail.push('STRICT');
  if (tail.length) sql += ` ${tail.join(', ')}`;
  this.exec(sql);
  return this;
}

function createTableIfNotExists(name, builderFn, options = {}) {
  return createTable.call(this, name, builderFn, Object.assign({}, options, { ifNotExists: true }));
}

function dropTable(name, options = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected first argument to be a non-empty string');
  this.exec(`DROP TABLE ${options && options.ifExists ? 'IF EXISTS ' : ''}${quote(name)}`);
  return this;
}

function renameTable(from, to) {
  if (typeof from !== 'string' || !from || typeof to !== 'string' || !to) throw new TypeError('renameTable(from, to) needs two non-empty strings');
  this.exec(`ALTER TABLE ${quote(from)} RENAME TO ${quote(to)}`);
  return this;
}

// db.addColumn(table, builderFn): builderFn defines exactly one column.
function addColumn(table, builderFn) {
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a non-empty string');
  const t = buildColumns(builderFn);
  if (t.columns.length !== 1) throw new TypeError('addColumn() must define exactly one column');
  this.exec(`ALTER TABLE ${quote(table)} ADD COLUMN ${t.columns[0].toSQL()}`);
  return this;
}

// db.createIndex(name, table, columns, options?): columns is a string or array;
// options { unique, where }.
function createIndex(name, table, columns, options = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('An index needs a non-empty name');
  if (typeof table !== 'string' || !table) throw new TypeError('createIndex needs a table name');
  const cols = Array.isArray(columns) ? columns : [columns];
  if (cols.length === 0 || cols.some((c) => typeof c !== 'string' || !c)) throw new TypeError('createIndex needs one or more column names');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected the options to be an object');
  let sql = `CREATE ${options.unique ? 'UNIQUE ' : ''}INDEX ${quote(name)} ON ${quote(table)} (${cols.map(quote).join(', ')})`;
  if (options.where !== undefined) {
    if (typeof options.where !== 'string' || !options.where) throw new TypeError('Expected the "where" option to be a non-empty string');
    sql += ` WHERE ${options.where}`;
  }
  this.exec(sql);
  return this;
}

function dropIndex(name, options = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected first argument to be a non-empty string');
  this.exec(`DROP INDEX ${options && options.ifExists ? 'IF EXISTS ' : ''}${quote(name)}`);
  return this;
}

module.exports = {
  createTable, createTableIfNotExists, dropTable, renameTable, addColumn, createIndex, dropIndex
};
