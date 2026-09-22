'use strict';

// db.generateTypes(options?): emit TypeScript interfaces for the database's
// user tables and views, from schema introspection. A row of each table becomes
// one `interface`, so application code can be typed against the live schema
// instead of a hand-kept copy. Read-only; returns a string (the caller writes
// it wherever it wants). Mixed onto Database.prototype by lib/database.js, and
// also exposed as the static Database.generateTypes(db, options) via forDatabase.
//
// An extension beyond the better-sqlite3 surface. Column types follow SQLite's
// type affinity, so declared types like VARCHAR(30) or BIGINT map correctly.

const schema = require('./schema');

// SQLite declared type -> TypeScript type, by affinity (the same rules SQLite
// uses to pick a column's storage class).
function tsType(sqlType, bigint) {
  const t = String(sqlType || '').toUpperCase();
  if (!t) return 'unknown';                                   // no declared type
  if (t.includes('INT')) return bigint ? 'number | bigint' : 'number';
  if (t.includes('CHAR') || t.includes('CLOB') || t.includes('TEXT')) return 'string';
  if (t.includes('BLOB')) return 'Uint8Array';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) return 'number';
  if (t.includes('NUMERIC') || t.includes('DECIMAL') || t.includes('NUMBER') || t.includes('BOOL')) return 'number';
  return 'unknown';
}

// table_name / weird name -> PascalCase identifier.
function pascalCase(name) {
  const parts = String(name).split(/[^A-Za-z0-9]+/).filter(Boolean);
  const pascal = parts.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('');
  return /^[0-9]/.test(pascal) || !pascal ? `T${pascal}` : pascal;
}

const IDENT = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
function propName(name) {
  return IDENT.test(name) ? name : JSON.stringify(name);
}

function interfaceName(table, options) {
  return `${options.prefix || ''}${pascalCase(table)}${options.suffix || ''}`;
}

function generateTypes(options = {}) {
  if (options === null || typeof options !== 'object') throw new TypeError('Expected first argument to be an options object');
  const bigint = Boolean(options.bigint);
  const exported = options.export !== false;
  const keyword = `${exported ? 'export ' : ''}interface`;

  let names = schema.userTables(this).slice();
  if (options.views !== false) names = names.concat(schema.userViews(this));
  if (options.tables !== undefined) {
    if (!Array.isArray(options.tables)) throw new TypeError('Expected the "tables" option to be an array');
    const wanted = new Set(options.tables);
    names = names.filter((n) => wanted.has(n));
  }
  names.sort();

  const out = [];
  if (options.header) out.push(`${options.header.replace(/\n?$/, '')}\n`);

  const mapping = [];
  for (const table of names) {
    const iface = interfaceName(table, options);
    mapping.push({ table, iface });
    const lines = [`${keyword} ${iface} {`];
    for (const col of schema.tableInfo(this, table)) {
      let ts = tsType(col.type, bigint);
      // Nullable unless declared NOT NULL or part of the primary key.
      if (!col.notnull && !col.pk) ts += ' | null';
      lines.push(`  ${propName(col.name)}: ${ts};`);
    }
    lines.push('}');
    out.push(lines.join('\n'));
  }

  if (options.databaseInterface) {
    const dbName = typeof options.databaseInterface === 'string' ? options.databaseInterface : 'DatabaseSchema';
    const lines = [`${keyword} ${dbName} {`];
    for (const { table, iface } of mapping) lines.push(`  ${propName(table)}: ${iface};`);
    lines.push('}');
    out.push(lines.join('\n'));
  }

  return `${out.join('\n\n')}\n`;
}

module.exports = { generateTypes };
// The static Database.generateTypes(db, options) form.
module.exports.forDatabase = (db, options) => generateTypes.call(db, options);
