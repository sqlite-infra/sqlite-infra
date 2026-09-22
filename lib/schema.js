'use strict';

// Catalog lookups against sqlite_master, shared by the sync and the row
// export. "User" tables are everything that is neither SQLite's own nor
// this package's (`_better_sqlite_*`: the queue, the consumer, the sync's
// bookkeeping).

const crypto = require('crypto');
const { tables } = require('./dictionary');

const INTERNAL = `${tables.prefix}%`;

// A SQL identifier, double-quoted.
function quote(ident) {
  return `"${String(ident).replace(/"/g, '""')}"`;
}

function userTables(db) {
  return db.prepare(`
    SELECT name FROM sqlite_master
     WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE ?
     ORDER BY name`).pluck().all(INTERNAL);
}

function userViews(db) {
  return db.prepare(`
    SELECT name FROM sqlite_master
     WHERE type = 'view' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE ?
     ORDER BY name`).pluck().all(INTERNAL);
}

function tableInfo(db, table) {
  return db.prepare(`PRAGMA table_info(${quote(table)})`).all();
}

function tableExists(db, table) {
  return db.prepare(`SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?`).pluck().get(table) === 1;
}

// The session extension records only tables with an explicit PRIMARY KEY.
function hasPrimaryKey(db, table) {
  return tableInfo(db, table).some((c) => c.pk > 0);
}

// Fingerprint of the user schema: changes when a table, index, view or
// trigger is created, dropped or altered.
function schemaHash(db) {
  const rows = db.prepare(`SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE ? ORDER BY type, name`).raw().all(INTERNAL);
  return crypto.createHash('sha256').update(JSON.stringify(rows)).digest('hex').slice(0, 16);
}

module.exports = { quote, userTables, userViews, tableInfo, tableExists, hasPrimaryKey, schemaHash };
