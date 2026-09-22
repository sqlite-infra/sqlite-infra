'use strict';

// Runs better-sqlite3's own test suite against this library.
//
//   node scripts/conformance.js            # summary per file, exit 1 on regression
//   node scripts/conformance.js --verbose  # also list every failing test
//   node scripts/conformance.js --update   # rewrite scripts/conformance-baseline.json
//
// The upstream tests are the reference for "same API". They live in
// conformance/test/ as an unmodified copy of better-sqlite3 v13.0.3's test/
// directory (MIT, see conformance/LICENSE); conformance/index.js makes their
// `require('../.')` resolve to this package. mocha, chai and fs-extra are
// pinned devDependencies, so `npm ci` is all the setup there is and the run
// needs no network.
//
// Each file runs in its own mocha process so a crash in one file (the upstream
// integrity suite used to SIGSEGV node:sqlite) is reported instead of taking
// the rest of the run down. Files that need the compiled addon itself
// (entrypoints, the test extension) are skipped. Every other failure is either
// a documented difference (README, "Differences from better-sqlite3") or a bug;
// the baseline records the expected pass count per file and per Node major so
// a change that loses conformance fails CI.

const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawnSync } = require('child_process');

const SKIP = new Set(['02.entrypoints.js', '35.database.load-extension.js']);
const root = path.join(__dirname, '..');
const suite = path.join(root, 'conformance');
const mocha = require.resolve('mocha/bin/mocha.js', { paths: [root] });
const baselinePath = path.join(__dirname, 'conformance-baseline.json');
const verbose = process.argv.includes('--verbose');
const update = process.argv.includes('--update');

function runFile(file) {
  const args = [mocha, '--exit', '--timeout=5000', '--reporter=json', 'test/00.setup.js', `test/${file}`];
  const result = spawnSync(process.execPath, args, { cwd: suite, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  let report;
  try {
    report = JSON.parse(result.stdout.slice(result.stdout.indexOf('{')));
  } catch {
    return { file, pass: 0, fail: 0, crash: `exit ${result.status}${result.signal ? ` (${result.signal})` : ''}`, failures: [] };
  }
  return {
    file,
    pass: report.stats.passes,
    fail: report.stats.failures,
    crash: null,
    failures: report.failures.map((t) => `${t.fullTitle}: ${((t.err && t.err.message) || '').split('\n')[0]}`)
  };
}

const files = fs.readdirSync(path.join(suite, 'test'))
  .filter((f) => /^[1-9]\d\..*\.js$/.test(f) && !SKIP.has(f))
  .sort();
const results = files.map(runFile);

let pass = 0;
let fail = 0;
for (const r of results) {
  pass += r.pass;
  fail += r.fail;
  console.log(`${r.file.padEnd(32)} pass ${String(r.pass).padStart(3)}  fail ${String(r.fail).padStart(3)}${r.crash ? `  CRASH ${r.crash}` : ''}`);
  if (verbose) for (const f of r.failures) console.log(`    - ${f}`);
}
console.log(`\n${process.version} (${os.platform()}): ${pass} passing, ${fail} failing, ${results.filter((r) => r.crash).length} crashed`);

const key = `node${process.versions.node.split('.')[0]}`;
const baseline = fs.existsSync(baselinePath) ? JSON.parse(fs.readFileSync(baselinePath, 'utf8')) : {};
const current = Object.fromEntries(results.map((r) => [r.file, r.pass]));
if (update || !baseline[key]) {
  baseline[key] = current;
  fs.writeFileSync(baselinePath, `${JSON.stringify(baseline, null, 2)}\n`);
  console.log(`baseline for ${key} written to ${path.relative(root, baselinePath)}`);
  process.exit(0);
}
const regressions = results.filter((r) => r.crash || r.pass < (baseline[key][r.file] || 0));
if (regressions.length) {
  console.error(`\nREGRESSION against ${key} baseline:`);
  for (const r of regressions) console.error(`  ${r.file}: ${r.pass} passing, baseline ${baseline[key][r.file]}${r.crash ? `, ${r.crash}` : ''}`);
  process.exit(1);
}
const gains = results.filter((r) => r.pass > (baseline[key][r.file] || 0));
if (gains.length) console.log(`\n${gains.length} file(s) pass more than the baseline; run with --update to record.`);
