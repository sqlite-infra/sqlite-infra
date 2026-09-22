'use strict';

const { translate } = require('./util');
const { expand } = require('./params');
const {
  requireOpen, requireNotBusy, requireNoIterators, unsupported, messages
} = require('./util');

// The mode toggles and safeIntegers() take an optional boolean; a non-boolean
// is a mistake, and better-sqlite3 throws for it (as db.unsafeMode() does here).
function requireBoolean(toggle) {
  if (typeof toggle !== 'boolean') throw new TypeError('Expected first argument to be a boolean');
  return toggle;
}

// Statement: a prepared SQL statement, wrapping a node:sqlite StatementSync.
//
// Mirrors the better-sqlite3 Statement API:
//   run(...params)      -> { changes, lastInsertRowid }
//   get(...params)      -> first row or undefined
//   all(...params)      -> array of rows
//   iterate(...params)  -> iterator of rows
//   pluck() / expand() / raw() / safeIntegers()  -> this (mode toggles)
//   columns()           -> column descriptors
//   bind(...params)     -> this (binds once, permanently)
//   toString()          -> source SQL, with bound parameters expanded
//   .source .reader .database .busy
//
// Parameters work the way better-sqlite3 binds them: positional values for
// `?`, arrays are spread into positional values, and one plain object supplies
// `@name` / `$name` / `:name` parameters by bare key. Missing or surplus values
// are reported with better-sqlite3's messages instead of silently binding NULL.
//
// Rows: when the engine has StatementSync#setReturnArrays (Node 22.16+, 24+)
// rows are read as arrays and shaped here from columns(), so duplicate column
// names (SELECT a.id, b.id) behave exactly as in better-sqlite3 for raw() and
// expand(). Older engines deliver rows keyed by name, where a duplicate name
// keeps its last value.

const kPrivate = Symbol('better-sqlite.statement');

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

// A "plain" object is one whose prototype is null or a root prototype
// (Object.prototype of some realm); anything else is a value to bind.
function isPlainObject(value) {
  const proto = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

// Validates one value the way better-sqlite3 does and returns what node:sqlite
// should receive for it. `copy` duplicates buffers, for bind(): a permanently
// bound buffer must not change when the caller mutates the original.
function toBindable(value, copy) {
  switch (typeof value) {
    case 'number':
    case 'string':
      return value;
    case 'bigint':
      if (value < INT64_MIN || value > INT64_MAX) {
        throw new RangeError('The bound string, buffer, or bigint is too big');
      }
      return value;
    case 'undefined':
      return null;
    case 'object':
      if (value === null) return value;
      if (value instanceof Uint8Array) return copy ? Buffer.from(value) : value;
      break;
    default:
      break;
  }
  throw new TypeError('SQLite3 can only bind numbers, strings, bigints, buffers, and null');
}

// BLOBs come back from node:sqlite as Uint8Array; better-sqlite3 returns Buffer.
function fromColumn(value) {
  if (value instanceof Uint8Array && !Buffer.isBuffer(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  return value;
}

class Statement {
  // `state` is the owning Database's private state (open/busy/iterators/...).
  constructor(token, database, state, backend, source, params, options = {}) {
    if (token !== kPrivate) {
      throw new TypeError('Statements can only be constructed by the db.prepare() method');
    }
    const arrays = typeof backend.setReturnArrays === 'function';
    if (arrays) backend.setReturnArrays(true);
    const reader = backend.columns().length > 0;
    this[kPrivate] = {
      state,
      backend,
      params,
      arrays,
      bound: null,        // { list, positional, named } once bind() was called
      locked: false,      // an iterator is open on this statement
      mode: 'object',     // 'object' | 'pluck' | 'expand' | 'raw'
      pragma: Boolean(options.pragma),
      explain: Boolean(options.explain)
    };
    Object.defineProperties(this, {
      database: { value: database, enumerable: true },
      source: { value: source, enumerable: true },
      reader: { value: reader, enumerable: true }
    });
  }

  get busy() {
    return this[kPrivate].locked;
  }

  // better-sqlite3 exposes Statement#readonly (whether the statement only
  // reads). node:sqlite has no equivalent (no sqlite3_stmt_readonly), so, like
  // the other engine-gated surfaces, it throws a TypeError naming the reason
  // rather than returning a value that could be wrong. See README.
  get readonly() {
    throw unsupported('Statement#readonly');
  }

  // ---- execution ----

  run(...args) {
    const p = this[kPrivate];
    requireOpen(p.state);
    requireNotBusy(p.state);
    requireNoIterators(p.state);
    this._requireNotLocked();
    const bound = this._bind(args);
    const { state } = p;
    // node:sqlite reports sqlite3_changes(), which still holds the count of
    // the last DML statement after BEGIN, CREATE, DROP, ... better-sqlite3
    // reports 0 for those; total_changes() tells the two cases apart.
    this._trace(bound);
    const before = state.totalChanges();
    const info = translate(() => {
      const raw = p.backend.run(...bound.list);
      const changes = raw.changes !== 0 && state.totalChanges() === before ? 0 : raw.changes;
      return { changes: Number(changes), lastInsertRowid: raw.lastInsertRowid };
    });
    return info;
  }

  get(...args) {
    const p = this[kPrivate];
    this._requireReader();
    requireOpen(p.state);
    requireNotBusy(p.state);
    this._requireNotLocked();
    const bound = this._bind(args);
    this._trace(bound);
    const row = translate(() => p.backend.get(...bound.list));
    return row === undefined ? undefined : this._row(row, this._columns());
  }

  all(...args) {
    const p = this[kPrivate];
    this._requireReader();
    requireOpen(p.state);
    requireNotBusy(p.state);
    this._requireNotLocked();
    const bound = this._bind(args);
    this._trace(bound);
    const rows = translate(() => p.backend.all(...bound.list));
    if (rows.length === 0) return rows;
    const columns = this._columns();
    return rows.map((row) => this._row(row, columns));
  }

  iterate(...args) {
    const p = this[kPrivate];
    this._requireReader();
    requireOpen(p.state);
    requireNotBusy(p.state);
    this._requireNotLocked();
    const bound = this._bind(args);
    const inner = translate(() => p.backend.iterate(...bound.list));
    return this._iterator(inner, bound);
  }

  // A plain iterator object (no throw(), like better-sqlite3's). The statement
  // is locked and the database counts one more open iterator until the rows
  // run out, return() is called, or a step throws. verbose fires on the first
  // step, so an iterator returned before stepping is not logged.
  _iterator(inner, bound) {
    const p = this[kPrivate];
    const { state } = p;
    const columns = this._columns();
    let finished = false;
    let started = false;

    p.locked = true;
    state.iterators += 1;

    const finish = () => {
      if (finished) return;
      finished = true;
      p.locked = false;
      state.iterators -= 1;
      if (typeof inner.return === 'function') {
        try { inner.return(); } catch { /* already finished */ }
      }
    };
    const iterator = {
      next: () => {
        if (finished) return { value: undefined, done: true };
        requireNotBusy(state);
        let step;
        try {
          if (!started) {
            started = true;
            this._trace(bound);
          }
          step = translate(() => inner.next());
        } catch (err) {
          finish();
          throw err;
        }
        if (step.done) {
          finish();
          return { value: undefined, done: true };
        }
        return { value: this._row(step.value, columns), done: false };
      },
      return: (value) => {
        if (!finished) requireNotBusy(state);
        finish();
        return { value, done: true };
      },
      [Symbol.iterator]() {
        return iterator;
      }
    };
    return iterator;
  }

  // ---- modes ----

  pluck(toggle = true) {
    return this._mode('pluck', toggle);
  }

  expand(toggle = true) {
    return this._mode('expand', toggle);
  }

  raw(toggle = true) {
    return this._mode('raw', toggle);
  }

  safeIntegers(toggle = true) {
    requireBoolean(toggle);
    const p = this[kPrivate];
    requireNotBusy(p.state);
    this._requireNotLocked();
    // Allowed on a closed database, like the other mode toggles; the
    // finalized statement simply has nothing left to configure.
    if (p.state.open) p.backend.setReadBigInts(Boolean(toggle));
    return this;
  }

  columns() {
    const p = this[kPrivate];
    if (!this.reader) throw new TypeError('The columns() method is only for statements that return data');
    requireOpen(p.state);
    requireNotBusy(p.state);
    return this._columns().map((c) => ({
      name: c.name,
      column: c.column,
      table: c.table,
      database: c.database,
      type: c.type
    }));
  }

  bind(...args) {
    const p = this[kPrivate];
    requireOpen(p.state);
    requireNotBusy(p.state);
    this._requireNotLocked();
    if (p.bound) throw new TypeError('The bind() method can only be invoked once per statement object');
    p.bound = this._arguments(args, true);
    return this;
  }

  // The source SQL; once parameters are bound with bind(), the SQL with those
  // values expanded in place, like sqlite3_expanded_sql() (strings and blobs
  // longer than 32 bytes are cut short, as in better-sqlite3's build). After
  // close() the source SQL again.
  toString() {
    const p = this[kPrivate];
    if (!p.bound || !p.state.open) return this.source;
    return expand(this.source, p.params.tokens, p.bound.positional, p.bound.named);
  }

  // ---- internals ----

  _mode(name, toggle) {
    requireBoolean(toggle);
    if (!this.reader) throw new TypeError(`The ${name}() method is only for statements that return data`);
    const p = this[kPrivate];
    requireNotBusy(p.state);
    this._requireNotLocked();
    p.mode = toggle ? name : (p.mode === name ? 'object' : p.mode);
    return this;
  }

  _requireReader() {
    if (!this.reader && !this[kPrivate].pragma) {
      throw new TypeError('This statement does not return data. Use run() instead');
    }
  }

  _requireNotLocked() {
    if (this[kPrivate].locked) throw new TypeError(messages.STMT_BUSY);
  }

  // verbose: called before execution with the SQL and the values about to be
  // bound expanded in place, the way better-sqlite3's trace hook reports it.
  // A logger that throws aborts the execution.
  _trace(bound) {
    const { state, params } = this[kPrivate];
    if (!state.verbose) return;
    state.verbose.call(undefined, expand(this.source, params.tokens, bound.positional, bound.named));
  }

  _columns() {
    return this[kPrivate].backend.columns();
  }

  // Resolves the arguments for one execution, honouring a permanent bind().
  // Returns { list, positional, named }: `list` is what node:sqlite receives.
  _bind(args) {
    const p = this[kPrivate];
    if (p.bound) {
      if (args.length) throw new TypeError('This statement already has bound parameters');
      return p.bound;
    }
    return this._arguments(args);
  }

  // Turns better-sqlite3-style arguments into the node:sqlite call list:
  // an optional named-parameter object first, then positional values.
  _arguments(args, copy = false) {
    const { params, explain } = this[kPrivate];
    const positional = [];
    let named = null;

    const push = (value) => positional.push(toBindable(value, copy));
    for (const arg of args) {
      if (Array.isArray(arg)) {
        for (const value of arg) push(value);
      } else if (arg !== null && typeof arg === 'object' && !(arg instanceof Uint8Array)) {
        if (!isPlainObject(arg)) throw new TypeError('Named parameters can only be passed within plain objects');
        if (named) throw new TypeError('You cannot specify named parameters in two different objects');
        named = arg;
      } else {
        push(arg);
      }
    }

    let bound = null;
    if (named) {
      bound = {};
      for (const name of params.names) {
        if (!Object.prototype.hasOwnProperty.call(named, name)) {
          throw new RangeError(`Missing named parameter "${name}"`);
        }
        bound[name] = toBindable(named[name], copy);
      }
    }

    if (positional.length > params.anonymous) {
      throw new RangeError('Too many parameter values were provided');
    }
    if (!explain) {
      // EXPLAIN runs with unbound parameters (NULL), like better-sqlite3's
      // db.explain(); every other statement must be fully bound.
      if (!named && params.names.length) throw new TypeError('Missing named parameters');
      if (positional.length < params.anonymous) throw new RangeError('Too few parameter values were provided');
    }

    return { list: bound ? [bound, ...positional] : positional, positional, named: bound };
  }

  // Shapes one node:sqlite row per the current mode. With setReturnArrays the
  // row is an array in column order; otherwise a null-prototype object.
  _row(row, columns) {
    const { mode, arrays } = this[kPrivate];
    if (arrays) {
      switch (mode) {
        case 'pluck':
          return fromColumn(row[0]);
        case 'raw':
          return row.map(fromColumn);
        case 'expand': {
          const out = {};
          for (let i = 0; i < columns.length; i++) {
            const group = columns[i].table || '$';
            (out[group] || (out[group] = {}))[columns[i].name] = fromColumn(row[i]);
          }
          return out;
        }
        default: {
          const out = {};
          for (let i = 0; i < columns.length; i++) out[columns[i].name] = fromColumn(row[i]);
          return out;
        }
      }
    }
    switch (mode) {
      case 'pluck':
        return fromColumn(row[columns[0].name]);
      case 'raw':
        return columns.map((c) => fromColumn(row[c.name]));
      case 'expand': {
        const out = {};
        for (const c of columns) {
          const group = c.table || '$';
          (out[group] || (out[group] = {}))[c.name] = fromColumn(row[c.name]);
        }
        return out;
      }
      default: {
        const out = {};
        for (const key of Object.keys(row)) out[key] = fromColumn(row[key]);
        return out;
      }
    }
  }
}

module.exports = { Statement, kPrivate };
