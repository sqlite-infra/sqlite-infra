'use strict';

// Loads node:sqlite. Node tags the module as experimental and prints an
// ExperimentalWarning the first time it is required. This library is the
// party that chose that dependency, so it mutes exactly that one warning
// while requiring the module and leaves process.emitWarning untouched
// afterwards. Every other warning still surfaces.

const original = process.emitWarning;

process.emitWarning = function emitWarning(warning, ...rest) {
  const type = typeof rest[0] === 'string' ? rest[0]
    : rest[0] && typeof rest[0] === 'object' ? rest[0].type
      : warning && typeof warning === 'object' ? warning.name : undefined;
  const text = typeof warning === 'string' ? warning : warning && warning.message;
  if (type === 'ExperimentalWarning' && /sqlite/i.test(text || '')) return undefined;
  return original.call(process, warning, ...rest);
};

let sqlite;
try {
  sqlite = require('node:sqlite');
} catch (err) {
  process.emitWarning = original;
  const wrapped = new Error(`better-sqlite3 needs Node's built-in node:sqlite module, which ${process.version} does not provide: ${err.message}`);
  wrapped.cause = err;
  throw wrapped;
}
process.emitWarning = original;

module.exports = sqlite;
