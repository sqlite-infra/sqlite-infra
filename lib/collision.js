'use strict';

// Collision check: warn when this package runs next to another SQLite driver.
//
// This package IS a SQLite driver (the better-sqlite3 API on node:sqlite).
// A project that also pulls in better-sqlite3, sqlite3, sql.js, libsql or a
// wasm build ends up with two engines, two sets of error classes and two
// nearly identical import names, and nothing in the toolchain says so. This
// module says so.
//
// It runs at RUNTIME, once, the first time this package is loaded in a process
// (index.js calls report() on a deferred tick) — not as an install lifecycle
// script. A library must never fail its host's install or crash its host's
// process, so the check is advisory only: it prints one line to stderr and
// returns. `report()` never throws.
//
// What it reads (all local, read-only, no network, no child processes):
//   <root>/package.json           dependency fields
//   <root>/package-lock.json      or npm-shrinkwrap.json, yarn.lock, pnpm-lock.yaml
//   <root>/node_modules/<name>    already-installed copies
// where <root> is the host project. At runtime that is found by walking up
// from process.cwd() out of node_modules (INIT_CWD is honoured when present,
// e.g. under an npm script).
//
// How to silence it, in order of precedence:
//   BETTER_SQLITE_COLLISION=off                    env var
//   "better-sqlite3": { "collisions": "off",       consumer package.json
//                       "allow": ["sqlite3"] }
//   default: report

const fs = require('fs');
const path = require('path');

const SELF = require('../package.json').name;
const { env: ENV, defaults, collision } = require('./dictionary');
const { findRoot } = require('./params');

// Packages that provide a SQLite engine or driver for Node. ORMs and query
// builders (knex, drizzle, typeorm, prisma) are deliberately absent: they
// sit on top of one of these and are not a collision by themselves.
const CATALOG = {
  'better-sqlite3-multiple-ciphers':
    'better-sqlite3 fork with SQLCipher; same API surface as this package',
  'sqlite3':
    'node-sqlite3, callback-based native driver',
  'sqlite':
    'promise wrapper around sqlite3; brings the native driver with it',
  '@vscode/sqlite3':
    'VS Code fork of node-sqlite3',
  '@journeyapps/sqlcipher':
    'node-sqlite3 fork with SQLCipher',
  'sql.js':
    'SQLite compiled to wasm/asm.js with its own in-memory engine',
  '@sqlite.org/sqlite-wasm':
    'official SQLite wasm build, its own engine',
  'node-sqlite3-wasm':
    'wasm build of SQLite with a better-sqlite3-like API',
  '@libsql/client':
    'libSQL client; embeds its own SQLite fork for local files',
  'libsql':
    'libSQL native bindings with a better-sqlite3-compatible API'
};

// This package's own name can never be a foreign driver: a host that depends on
// it (`"better-sqlite3": "file:.."`) would otherwise be warned about itself, and
// npm cannot resolve a second package under the same name. Drop the self entry.
delete CATALOG[SELF];

const MODES = new Set(Object.values(collision.modes));

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (err) {
    return null;
  }
}

function readText(file) {
  try {
    return fs.readFileSync(file, 'utf8');
  } catch (err) {
    return null;
  }
}

// The host project root is found by findRoot (lib/params.js): INIT_CWD when it
// holds a package.json, else the first package.json above cwd outside
// node_modules. Re-exported below for the test suite.

const DEP_FIELDS = ['dependencies', 'devDependencies', 'optionalDependencies', 'peerDependencies'];

function scanManifest(pkg, hits) {
  if (!pkg) return;
  for (const field of DEP_FIELDS) {
    const deps = pkg[field];
    if (!deps || typeof deps !== 'object') continue;
    for (const name of Object.keys(deps)) {
      if (name in CATALOG) hits(name, `package.json ${field}`);
    }
  }
}

function scanNpmLock(root, hits) {
  for (const file of ['npm-shrinkwrap.json', 'package-lock.json']) {
    const lock = readJson(path.join(root, file));
    if (!lock) continue;
    // lockfileVersion 2/3: "packages": { "node_modules/<name>": {...} }
    for (const key of Object.keys(lock.packages || {})) {
      const m = /(?:^|\/)node_modules\/((?:@[^/]+\/)?[^/]+)$/.exec(key);
      if (m && m[1] in CATALOG) hits(m[1], file);
    }
    // lockfileVersion 1: "dependencies": { "<name>": {...} } (top level only)
    for (const name of Object.keys(lock.dependencies || {})) {
      if (name in CATALOG) hits(name, file);
    }
    return; // shrinkwrap takes precedence over package-lock, as in npm
  }
}

function scanYarnLock(root, hits) {
  const text = readText(path.join(root, 'yarn.lock'));
  if (!text) return;
  // Entry headers look like:   "name@^1.0.0", name@^1.0.0:   (v1)
  //                            "name@npm:^1.0.0":             (berry)
  for (const line of text.split('\n')) {
    if (!line || /^\s/.test(line) || line.startsWith('#')) continue;
    for (const spec of line.split(',')) {
      const m = /^"?((?:@[^/@"]+\/)?[^@"/]+)@/.exec(spec.trim());
      if (m && m[1] in CATALOG) hits(m[1], 'yarn.lock');
    }
  }
}

function scanPnpmLock(root, hits) {
  const text = readText(path.join(root, 'pnpm-lock.yaml'));
  if (!text) return;
  // packages:            importers:
  //   /name@1.0.0:         dependencies:
  //   name@1.0.0:            name:
  //   '@scope/name@1.0.0':     specifier: ^1.0.0
  const seen = new Set();
  for (const line of text.split('\n')) {
    let m = /^\s+'?\/?((?:@[^/@']+\/)?[^@/'\s]+)@[^:]*:\s*$/.exec(line);
    if (!m) m = /^\s+'?((?:@[^/@']+\/)?[^@/'\s:]+)'?:\s*$/.exec(line);
    if (m && m[1] in CATALOG && !seen.has(m[1])) {
      seen.add(m[1]);
      hits(m[1], 'pnpm-lock.yaml');
    }
  }
}

function scanNodeModules(root, hits) {
  for (const name of Object.keys(CATALOG)) {
    if (fs.existsSync(path.join(root, 'node_modules', name, 'package.json'))) {
      hits(name, 'node_modules');
    }
  }
}

function resolveMode(env, pkg) {
  const config = (pkg && pkg[SELF] && typeof pkg[SELF] === 'object') ? pkg[SELF] : {};
  let mode = defaults.collision;
  if (MODES.has(config[collision.fields.mode])) mode = config[collision.fields.mode];
  if (MODES.has(env[ENV.collision])) mode = env[ENV.collision];
  const allow = new Set(Array.isArray(config[collision.fields.allow]) ? config[collision.fields.allow].filter((n) => typeof n === 'string') : []);
  return { mode, allow };
}

// Returns { skipped, reason } or { mode, findings: [{ name, why, sources }] }.
// `off` (env or package.json) short-circuits to skipped; at runtime every other
// mode reports, since there is no install to fail.
function check(root, env = process.env) {
  if (!root) return { skipped: true, reason: 'no project root found' };
  const pkg = readJson(path.join(root, 'package.json'));
  if (pkg && pkg.name === SELF) return { skipped: true, reason: 'loaded inside this package itself' };

  const { mode, allow } = resolveMode(env, pkg);
  if (mode === collision.modes.off) return { skipped: true, reason: 'disabled', mode };

  const found = new Map();
  const hits = (name, source) => {
    if (allow.has(name)) return;
    if (!found.has(name)) found.set(name, new Set());
    found.get(name).add(source);
  };

  scanManifest(pkg, hits);
  scanNpmLock(root, hits);
  scanYarnLock(root, hits);
  scanPnpmLock(root, hits);
  scanNodeModules(root, hits);

  const findings = [...found].map(([name, sources]) => ({
    name, why: CATALOG[name], sources: [...sources]
  }));
  return { mode, findings, root };
}

function format(result) {
  const lines = [];
  lines.push(`${SELF}: another SQLite driver is present in ${result.root}.`);
  lines.push('');
  for (const f of result.findings) {
    lines.push(`  ${f.name}  (${f.sources.join(', ')})`);
    lines.push(`      ${f.why}`);
  }
  lines.push('');
  lines.push(`${SELF} implements the better-sqlite3 API on node:sqlite. Keeping a second`);
  lines.push('engine alongside it means two SQLite builds, two error hierarchies and two');
  lines.push('near-identical module names in one process. Remove the other driver, or');
  lines.push('silence this notice:');
  lines.push('');
  lines.push(`  ${ENV.collision}=${collision.modes.off} <your command>       # environment`);
  lines.push(`  "${SELF}": { "${collision.fields.mode}": "${collision.modes.off}" }             # in package.json`);
  lines.push(`  "${SELF}": { "${collision.fields.allow}": ["<name>"] }          # allow a specific driver`);
  return lines.join('\n');
}

// Run the check at most once per process. Deferred and fully guarded by the
// caller; here we simply never throw and never emit twice.
let reported = false;

function report(options = {}) {
  if (reported && !options.force) return null;
  reported = true;
  const env = options.env || process.env;
  const emit = options.emit || ((text) => process.stderr.write(text + '\n'));
  try {
    const result = check(findRoot(env, options.cwd || process.cwd()), env);
    if (result.skipped || !result.findings || result.findings.length === 0) return null;
    emit(format(result));
    return result;
  } catch (err) {
    return null; // advisory only: a runtime library must not crash its host
  }
}

// For the test suite: let a test start from a clean once-guard.
function _resetForTests() {
  reported = false;
}

module.exports = { CATALOG, check, findRoot, format, report, _resetForTests };
