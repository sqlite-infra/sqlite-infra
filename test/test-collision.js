'use strict';

// Tests for lib/collision.js: the runtime check that this package is not
// loaded next to another SQLite driver. The check is advisory — it prints to
// stderr once per process and never throws — so these tests exercise the pure
// scan (check/findRoot) and the emit behaviour of report(), not exit codes.

const test = require('node:test');
const assert = require('node:assert');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');

const collision = require('../lib/collision');
const { check, findRoot, CATALOG } = collision;

const dirs = [];
test.after(() => { for (const d of dirs) fs.rmSync(d, { recursive: true, force: true }); });

// Builds a throwaway consumer project. `files` maps relative path -> content
// (objects are JSON-encoded). Returns its absolute path.
function project(files) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'better-sqlite-collision-'));
  dirs.push(dir);
  const all = Object.assign({ 'package.json': { name: 'consumer', version: '0.0.0' } }, files);
  for (const [rel, content] of Object.entries(all)) {
    const file = path.join(dir, rel);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, typeof content === 'string' ? content : JSON.stringify(content, null, 2));
  }
  return fs.realpathSync(dir);
}

// Runs report() the way index.js does, but against a chosen root and env, and
// captures what it would write to stderr. Resets the once-guard first so each
// test starts clean.
function capture(root, env = {}) {
  collision._resetForTests();
  let out = '';
  const result = collision.report({ cwd: root, env, emit: (text) => { out += text + '\n'; } });
  return { out, result };
}

const names = (result) => result.findings.map((f) => f.name).sort();

// ---- the check itself -----------------------------------------------------

test('clean project: no findings, report is silent', () => {
  const root = project({});
  assert.deepStrictEqual(names(check(root, {})), []);
  const { out, result } = capture(root);
  assert.strictEqual(out, '');
  assert.strictEqual(result, null);
});

test('better-sqlite3-multiple-ciphers in dependencies is reported with an explanation', () => {
  const root = project({
    'package.json': { name: 'consumer', dependencies: { 'better-sqlite3-multiple-ciphers': '^11.0.0' } }
  });
  const result = check(root, {});
  assert.deepStrictEqual(names(result), ['better-sqlite3-multiple-ciphers']);
  assert.deepStrictEqual(result.findings[0].sources, ['package.json dependencies']);

  const { out } = capture(root);
  assert.match(out, /another SQLite driver is present/);
  assert.match(out, /better-sqlite3-multiple-ciphers  \(package\.json dependencies\)/);
  assert.match(out, /BETTER_SQLITE_COLLISION=off/);
});

test('every dependency field is scanned', () => {
  const root = project({
    'package.json': {
      name: 'consumer',
      devDependencies: { sqlite3: '5' },
      optionalDependencies: { 'sql.js': '1' },
      peerDependencies: { '@libsql/client': '0.5' }
    }
  });
  const result = check(root, {});
  assert.deepStrictEqual(names(result), ['@libsql/client', 'sql.js', 'sqlite3']);
  const bySource = Object.fromEntries(result.findings.map((f) => [f.name, f.sources[0]]));
  assert.strictEqual(bySource.sqlite3, 'package.json devDependencies');
  assert.strictEqual(bySource['sql.js'], 'package.json optionalDependencies');
  assert.strictEqual(bySource['@libsql/client'], 'package.json peerDependencies');
});

test('unrelated packages, ORMs, our own @types dependency and this package itself are not collisions', () => {
  const root = project({
    'package.json': {
      name: 'consumer',
      dependencies: {
        knex: '3', 'drizzle-orm': '0.30', prisma: '5', express: '4',
        '@types/better-sqlite3': '9', 'better-sqlite3': 'file:../better-sqlite3'
      }
    }
  });
  assert.deepStrictEqual(names(check(root, {})), []);
});

// ---- lockfiles and node_modules -------------------------------------------

test('package-lock.json v3: transitive driver is found even when not a direct dependency', () => {
  const root = project({
    'package-lock.json': {
      lockfileVersion: 3,
      packages: {
        '': { name: 'consumer' },
        'node_modules/some-orm': { version: '1.0.0' },
        'node_modules/some-orm/node_modules/better-sqlite3-multiple-ciphers': { version: '9.0.0' },
        'node_modules/@sqlite.org/sqlite-wasm': { version: '3.45.0' },
        'node_modules/@types/better-sqlite3': { version: '9.6.0' }
      }
    }
  });
  const result = check(root, {});
  assert.deepStrictEqual(names(result), ['@sqlite.org/sqlite-wasm', 'better-sqlite3-multiple-ciphers']);
  assert.deepStrictEqual(result.findings[0].sources, ['package-lock.json']);
});

test('package-lock.json v1 top-level dependencies are read', () => {
  const root = project({
    'package-lock.json': { lockfileVersion: 1, dependencies: { sqlite3: { version: '5.1.6' } } }
  });
  assert.deepStrictEqual(names(check(root, {})), ['sqlite3']);
});

test('npm-shrinkwrap.json wins over package-lock.json', () => {
  const root = project({
    'npm-shrinkwrap.json': { lockfileVersion: 3, packages: { '': {} } },
    'package-lock.json': { lockfileVersion: 3, packages: { 'node_modules/sqlite3': {} } }
  });
  assert.deepStrictEqual(names(check(root, {})), []);
});

test('yarn.lock v1 and berry entries are found', () => {
  const root = project({
    'yarn.lock': [
      '# yarn lockfile v1',
      '',
      '"@libsql/client@^0.5.0":',
      '  version "0.5.6"',
      '',
      'better-sqlite3-multiple-ciphers@^11.0.0, better-sqlite3-multiple-ciphers@^11.1.0:',
      '  version "11.1.2"',
      '',
      '"sql.js@npm:^1.10.0":',
      '  version: 1.10.3',
      '',
      'express@^4.0.0:',
      '  version "4.19.2"',
      ''
    ].join('\n')
  });
  assert.deepStrictEqual(names(check(root, {})), ['@libsql/client', 'better-sqlite3-multiple-ciphers', 'sql.js']);
});

test('pnpm-lock.yaml importers and packages are found', () => {
  const root = project({
    'pnpm-lock.yaml': [
      "lockfileVersion: '9.0'",
      'importers:',
      '  .:',
      '    dependencies:',
      '      express:',
      '        specifier: ^4.0.0',
      '        version: 4.19.2',
      '      sqlite3:',
      '        specifier: ^5.0.0',
      '        version: 5.1.7',
      'packages:',
      '  /better-sqlite3-multiple-ciphers@11.1.2:',
      '    resolution: {integrity: sha512-x}',
      "  '@sqlite.org/sqlite-wasm@3.45.0':",
      '    resolution: {integrity: sha512-y}',
      '  express@4.19.2:',
      '    resolution: {integrity: sha512-z}',
      ''
    ].join('\n')
  });
  assert.deepStrictEqual(names(check(root, {})), ['@sqlite.org/sqlite-wasm', 'better-sqlite3-multiple-ciphers', 'sqlite3']);
});

test('an installed copy in node_modules is found without any manifest entry', () => {
  const root = project({
    'node_modules/libsql/package.json': { name: 'libsql', version: '0.4.0' },
    'node_modules/@vscode/sqlite3/package.json': { name: '@vscode/sqlite3', version: '5.1.6' },
    'node_modules/lodash/package.json': { name: 'lodash', version: '4.17.21' }
  });
  const result = check(root, {});
  assert.deepStrictEqual(names(result), ['@vscode/sqlite3', 'libsql']);
  assert.deepStrictEqual(result.findings[0].sources, ['node_modules']);
});

test('one package seen in several places is reported once with every source', () => {
  const root = project({
    'package.json': { name: 'consumer', dependencies: { sqlite3: '5' } },
    'package-lock.json': { lockfileVersion: 3, packages: { 'node_modules/sqlite3': {} } },
    'node_modules/sqlite3/package.json': { name: 'sqlite3' }
  });
  const result = check(root, {});
  assert.strictEqual(result.findings.length, 1);
  assert.deepStrictEqual(result.findings[0].sources,
    ['package.json dependencies', 'package-lock.json', 'node_modules']);
});

// ---- modes and overrides --------------------------------------------------

test('BETTER_SQLITE_COLLISION=warn still reports (runtime cannot fail, only warn)', () => {
  const root = project({ 'package.json': { name: 'consumer', dependencies: { sqlite3: '5' } } });
  const { out } = capture(root, { BETTER_SQLITE_COLLISION: 'warn' });
  assert.match(out, /another SQLite driver is present/);
  assert.match(out, /sqlite3/);
});

test('BETTER_SQLITE_COLLISION=off silences the notice', () => {
  const root = project({ 'package.json': { name: 'consumer', dependencies: { sqlite3: '5' } } });
  const { out, result } = capture(root, { BETTER_SQLITE_COLLISION: 'off' });
  assert.strictEqual(out, '');
  assert.strictEqual(result, null);
  assert.strictEqual(check(root, { BETTER_SQLITE_COLLISION: 'off' }).skipped, true);
});

test('an unknown env value falls back to the default mode', () => {
  const root = project({ 'package.json': { name: 'consumer', dependencies: { sqlite3: '5' } } });
  assert.strictEqual(check(root, { BETTER_SQLITE_COLLISION: 'maybe' }).mode, 'error');
});

// ---- report() guards ------------------------------------------------------

test('report fires at most once per process unless forced', () => {
  const root = project({ 'package.json': { name: 'consumer', dependencies: { sqlite3: '5' } } });
  collision._resetForTests();
  let calls = 0;
  const emit = () => { calls += 1; };
  collision.report({ cwd: root, env: {}, emit });
  collision.report({ cwd: root, env: {}, emit });
  assert.strictEqual(calls, 1);
  collision.report({ cwd: root, env: {}, emit, force: true });
  assert.strictEqual(calls, 2);
});

test('report never throws, even on a broken root', () => {
  collision._resetForTests();
  assert.strictEqual(collision.report({ cwd: null, env: {}, emit: () => {} }), null);
  const nowhere = path.join(os.tmpdir(), 'better-sqlite-does-not-exist-' + process.pid);
  collision._resetForTests();
  assert.strictEqual(collision.report({ cwd: nowhere, env: {}, emit: () => {} }), null);
});

// ---- root detection and self-load -----------------------------------------

test('loaded inside this package itself is skipped', () => {
  const result = check(path.join(__dirname, '..'), {});
  assert.strictEqual(result.skipped, true);
  assert.match(result.reason, /itself/);
  const { out } = capture(path.join(__dirname, '..'));
  assert.strictEqual(out, '');
});

test('findRoot walks up out of node_modules when INIT_CWD is absent', () => {
  const root = project({
    'package.json': { name: 'consumer', dependencies: { sqlite3: '5' } },
    'node_modules/better-sqlite/package.json': { name: 'better-sqlite' }
  });
  const cwd = path.join(root, 'node_modules', 'better-sqlite');
  assert.strictEqual(findRoot({}, cwd), root);
  // and the report path: cwd inside node_modules, no INIT_CWD, a driver at the root
  assert.match(capture(cwd).out, /sqlite3/);
});

test('a missing project root never emits and never throws', () => {
  assert.strictEqual(check(null, {}).skipped, true);
});

test('malformed package.json and lockfile are tolerated', () => {
  const root = project({
    'package.json': '{ not json',
    'package-lock.json': 'also not json',
    'node_modules/sqlite3/package.json': { name: 'sqlite3' }
  });
  const result = check(root, {});
  assert.deepStrictEqual(names(result), ['sqlite3']);
  assert.match(capture(root).out, /sqlite3/);
});

// ---- the catalog ----------------------------------------------------------

test('catalog entries all carry a reason and none is this package', () => {
  for (const [name, why] of Object.entries(CATALOG)) {
    assert.ok(typeof why === 'string' && why.length > 20, name);
    assert.notStrictEqual(name, 'better-sqlite3');
  }
  // This package's own name is excluded (see lib/collision.js): it cannot be a
  // foreign driver, and a host depending on it must not be warned about itself.
  assert.ok(!('better-sqlite3' in CATALOG));
  assert.ok('sqlite3' in CATALOG);
});

test('no install lifecycle hook, and the check ships inside lib/, not as a script', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  assert.strictEqual(pkg.scripts.preinstall, undefined);
  assert.strictEqual(pkg.scripts.install, undefined);
  assert.strictEqual(pkg.scripts.postinstall, undefined);
  assert.ok(!pkg.files.includes('scripts/preinstall.js'));
  // the runtime check ships as part of lib/ and nothing else does the scan
  const out = execFileSync('npm', ['pack', '--dry-run', '--json'], { cwd: path.join(__dirname, '..'), encoding: 'utf8' });
  const filesInTarball = JSON.parse(out)[0].files.map((f) => f.path);
  assert.ok(filesInTarball.includes('lib/collision.js'), filesInTarball.join(', '));
  assert.ok(!filesInTarball.some((f) => f.startsWith('scripts/')), filesInTarball.join(', '));
});
