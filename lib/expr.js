'use strict';

// Safe SQL composition: a tagged template and a set of expression helpers that
// build parameterized SQL as a Fragment ({ sql, params }) you can run with
// db.prepare(frag.sql).all(...frag.params).
//
//   const { sql, expr } = require('better-sqlite3').;  // exposed as Database.sql / Database.expr
//   const f = sql`SELECT * FROM users WHERE id = ${id} AND status IN ${expr.list(['a','b'])}`;
//   db.prepare(f.sql).all(...f.params);
//
// The safety contract: every interpolated VALUE becomes a bound `?` parameter.
// Raw SQL only ever enters through a Fragment — either one you built with the
// tag, or expr.id() (a quoted identifier) / expr.raw() (trusted, caller-owned
// text). A plain interpolated value can never become SQL. Fragments nest, and
// params keep positional order across the nesting.

const { quote } = require('./schema');

class Fragment {
  constructor(sql = '', params = []) {
    this.sql = sql;
    this.params = params;
  }

  // Append a Fragment (splices its sql + params) or a raw string with its own
  // bound params. Returns this, for incremental building.
  append(part, ...params) {
    if (part instanceof Fragment) {
      this.sql += part.sql;
      this.params.push(...part.params);
    } else {
      this.sql += String(part);
      this.params.push(...params);
    }
    return this;
  }

  toString() { return this.sql; }
}

// Tagged template. Interpolated Fragments are spliced in as raw SQL (with their
// params); every other interpolated value becomes a `?` placeholder + param.
function sql(strings, ...values) {
  let out = '';
  const params = [];
  for (let i = 0; i < strings.length; i += 1) {
    out += strings[i];
    if (i < values.length) {
      const v = values[i];
      if (v instanceof Fragment) {
        out += v.sql;
        params.push(...v.params);
      } else {
        out += '?';
        params.push(v);
      }
    }
  }
  return new Fragment(out, params);
}

// A dotted identifier becomes a qualified, quoted identifier: a.b -> "a"."b".
function identifier(name) {
  if (name instanceof Fragment) return name.sql;
  return String(name).split('.').map(quote).join('.');
}

function toFragment(x) {
  return x instanceof Fragment ? x : new Fragment(String(x));
}

// Combine fragments with a separator, parenthesized; null/undefined and empty
// fragments are skipped, so `expr.and(a, cond && b)` composes cleanly. Zero
// meaningful parts yields an empty (no-op) fragment.
function combine(fragments, separator) {
  const parts = fragments
    .filter((f) => f !== null && f !== undefined)
    .map(toFragment)
    .filter((f) => f.sql.trim() !== '');
  if (parts.length === 0) return new Fragment('');
  const params = [];
  for (const p of parts) params.push(...p.params);
  return new Fragment(`(${parts.map((p) => p.sql).join(separator)})`, params);
}

function comparison(column, operator, value) {
  return new Fragment(`${identifier(column)} ${operator} ?`, [value]);
}

const expr = {
  /** A quoted (optionally dotted/qualified) identifier; adds no parameter. */
  id(name) { return new Fragment(identifier(name)); },
  /** Trusted, caller-owned raw SQL; adds no parameter. */
  raw(text) { return new Fragment(String(text)); },
  /** `(?, ?, ...)` with the values bound; throws on an empty array. */
  list(values) {
    if (!Array.isArray(values) || values.length === 0) throw new TypeError('expr.list() requires a non-empty array');
    return new Fragment(`(${values.map(() => '?').join(', ')})`, values.slice());
  },
  and(...fragments) { return combine(fragments, ' AND '); },
  or(...fragments) { return combine(fragments, ' OR '); },
  /** Join arbitrary fragments/strings with a separator (default ', '). */
  join(fragments, separator = ', ') { return combine(fragments, separator); },

  eq(column, value) { return comparison(column, '=', value); },
  ne(column, value) { return comparison(column, '<>', value); },
  gt(column, value) { return comparison(column, '>', value); },
  gte(column, value) { return comparison(column, '>=', value); },
  lt(column, value) { return comparison(column, '<', value); },
  lte(column, value) { return comparison(column, '<=', value); },
  like(column, pattern) { return comparison(column, 'LIKE', pattern); },

  in(column, values) {
    const list = expr.list(values);
    return new Fragment(`${identifier(column)} IN ${list.sql}`, list.params);
  },
  between(column, low, high) {
    return new Fragment(`${identifier(column)} BETWEEN ? AND ?`, [low, high]);
  },
  isNull(column) { return new Fragment(`${identifier(column)} IS NULL`); },
  notNull(column) { return new Fragment(`${identifier(column)} IS NOT NULL`); }
};

module.exports = { sql, expr, Fragment };
