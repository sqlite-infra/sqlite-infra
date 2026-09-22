'use strict';

// Scans a SQL string the way SQLite's tokenizer would, just far enough to know
// its bind parameters and whether it holds exactly one statement. It includes
// the filesystem path builders the enable it.
//
// node:sqlite does not expose sqlite3_bind_parameter_count/_name, so this is
// how a Statement knows which names a parameter object must contain and how
// many anonymous `?` slots there are. String literals ('...'), quoted
// identifiers ("...", `...`, [...]) and comments (-- and /* */) are skipped so
// a `?` or `@name` inside them is not mistaken for a parameter.
//

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const isIdChar = (c) => /[A-Za-z0-9_$\u0080-\uFFFF]/.test(c);
const isDigit = (c) => c >= '0' && c <= '9';
const isSpace = (c) => /\s/.test(c);

// Returns the index just past the closing quote. A doubled quote inside the
// literal ('it''s') is an escape, not a terminator.
function skipQuoted(sql, start, quote) {
  let i = start + 1;
  while (i < sql.length) {
    if (sql[i] === quote) {
      if (sql[i + 1] === quote) { i += 2; continue; }
      return i + 1;
    }
    i++;
  }
  return sql.length;
}

function scan(sql) {
  const seen = new Set();      // full parameter tokens, e.g. '@name', for the slot count
  const names = new Set();     // bare names, e.g. 'name', which a parameter object must provide
  const tokens = [];           // every parameter occurrence, in source order, for toString()
  let count = 0;               // what sqlite3_bind_parameter_count would report
  let anonymous = 0;           // number of plain `?` slots
  let significant = false;     // any token seen in the current statement
  let terminated = false;      // a `;` ended a statement
  let multiple = false;        // tokens after that `;`

  const token = () => {
    if (terminated) multiple = true;
    significant = true;
  };

  const n = sql.length;
  let i = 0;
  while (i < n) {
    const c = sql[i];
    if (c === "'" || c === '"' || c === '`') { i = skipQuoted(sql, i, c); token(); continue; }
    if (c === '[') { const j = sql.indexOf(']', i + 1); i = j < 0 ? n : j + 1; token(); continue; }
    if (c === '-' && sql[i + 1] === '-') { const j = sql.indexOf('\n', i); i = j < 0 ? n : j + 1; continue; }
    if (c === '/' && sql[i + 1] === '*') { const j = sql.indexOf('*/', i + 2); i = j < 0 ? n : j + 2; continue; }
    if (isSpace(c)) { i++; continue; }
    if (c === ';') {
      if (significant) { terminated = true; significant = false; }
      i++;
      continue;
    }
    if (c === '?') {
      let j = i + 1;
      while (j < n && isDigit(sql[j])) j++;
      if (j > i + 1) {
        throw new TypeError('Numbered parameters (?NNN) are not supported; use ? or @name');
      }
      tokens.push({ start: i, end: j, position: anonymous });
      count++;
      anonymous++;
      i = j;
      token();
      continue;
    }
    if ((c === '@' || c === '$' || c === ':') && (i === 0 || !isIdChar(sql[i - 1]))) {
      let j = i + 1;
      while (j < n && isIdChar(sql[j])) j++;
      if (j > i + 1) {
        const full = sql.slice(i, j);
        if (!seen.has(full)) { seen.add(full); count++; }
        names.add(sql.slice(i + 1, j));
        tokens.push({ start: i, end: j, name: sql.slice(i + 1, j) });
        i = j;
        token();
        continue;
      }
    }
    i++;
    token();
  }

  return {
    count,
    anonymous,
    names: [...names],
    tokens,
    empty: !significant && !terminated,
    multiple
  };
}

// Renders one bound value as SQLite's sqlite3_expanded_sql() would in
// better-sqlite3's build (SQLITE_TRACE_SIZE_LIMIT=32: strings and blobs are
// cut at 32 bytes, followed by a comment with the omitted length). JS numbers
// are bound as doubles (node:sqlite and better-sqlite3 agree on that), so 25
// prints as 25.0. Doubles that need exponent notation are rendered with
// JavaScript's formatting, which can differ from SQLite's %!.15g in the last
// digits; the result is informational, not SQL to execute.
const SIZE_LIMIT = 32;

function literal(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return value > 0 ? '1e999' : value < 0 ? '-1e999' : 'NULL';
    return Number.isInteger(value) && Math.abs(value) < 1e15 ? `${value}.0` : String(value);
  }
  if (typeof value === 'string') {
    const buf = Buffer.from(value);
    if (buf.length <= SIZE_LIMIT) return `'${value.replace(/'/g, "''")}'`;
    // Match sqlite3_expanded_sql: cut at SIZE_LIMIT bytes, then extend past any
    // UTF-8 continuation bytes (0x80-0xBF) so a multi-byte character is never
    // split into a replacement char and the omitted-byte count stays exact.
    let nOut = SIZE_LIMIT;
    while (nOut < buf.length && (buf[nOut] & 0xc0) === 0x80) nOut++;
    const head = buf.subarray(0, nOut).toString().replace(/'/g, "''");
    return `'${head}'/*+${buf.length - nOut} bytes*/`;
  }
  if (value instanceof Uint8Array) {
    const buf = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    if (buf.length <= SIZE_LIMIT) return `x'${buf.toString('hex')}'`;
    return `x'${buf.subarray(0, SIZE_LIMIT).toString('hex')}'/*+${buf.length - SIZE_LIMIT} bytes*/`;
  }
  return String(value);
}

// Substitutes bound values into `sql` at the parameter tokens `scan` found.
function expand(sql, tokens, positional, named) {
  let out = '';
  let last = 0;
  for (const t of tokens) {
    out += sql.slice(last, t.start);
    out += literal(t.name !== undefined ? (named ? named[t.name] : null) : positional[t.position]);
    last = t.end;
  }
  return out + sql.slice(last);
}

// A dotfile-prefixed, collision-resistant staging path: next to the database file,
// or in os.tmpdir() for in-memory and URI-named databases.
function tempPath(db, tag) {
  const uri = db.name.startsWith('file:');
  const dir = db.memory || uri ? os.tmpdir() : path.dirname(path.resolve(db.name));
  const base = db.memory ? 'memory' : path.basename(db.name);
  return path.join(dir, `.${base}.${tag}-${crypto.randomBytes(6).toString('hex')}`);
}

// The host project root: INIT_CWD when it holds a package.json (a runtime
// invoked under an npm script), otherwise walk up from cwd until we leave
// node_modules and find a package.json. Returns null if none is found.
function findRoot(env = process.env, cwd = process.cwd()) {
  let dir = path.resolve(cwd);
  for (;;) {
    const parent = path.dirname(dir);
    const inNodeModules = dir.split(path.sep).includes('node_modules');
    if (!inNodeModules && fs.existsSync(path.join(dir, 'package.json'))) return dir;
    if (parent === dir) return null;
    dir = parent;
  }
}

module.exports = { scan, expand, literal, tempPath, findRoot };
