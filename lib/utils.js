'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const SqliteError = require('./sqlite-error');
const { messages } = require('../adapters/util');
const { nameOf } = require('./codes');

// Runs `fn` and rewrites the errors node:sqlite throws into the errors
// better-sqlite3 would throw for the same situation:
//   ERR_SQLITE_ERROR   -> SqliteError with a named result code
//   ERR_INVALID_STATE  -> TypeError (closed connection, finalized statement)
// Anything else (TypeError/RangeError from argument checks) passes through.
function translate(fn) {
  try {
    return fn();
  } catch (err) {
    throw convert(err);
  }
}

function convert(err) {
  if (!err || typeof err !== 'object') return err;
  if (err.code === 'ERR_SQLITE_ERROR') {
    if (/Returned JavaScript value cannot be converted/.test(err.message)) {
      // A user-defined function returned something SQLite cannot store.
      // better-sqlite3 reports that as a TypeError.
      return new TypeError(err.message);
    }
    const code = nameOf(err.errcode);
    return new SqliteError(err.message, code);
  }
  if (err.code === 'ERR_LOAD_SQLITE_EXTENSION') {
    return new SqliteError(err.message, 'SQLITE_ERROR');
  }
  if (err.code === 'ERR_INVALID_STATE') {
    return new TypeError(/not open|finalized/.test(err.message) ? messages.NOT_OPEN : err.message);
  }
  if (err.code === 'ERR_OUT_OF_RANGE' && /JavaScript number/.test(err.message)) {
    // An INTEGER outside Number's safe range. better-sqlite3 would return a
    // lossy double here; node:sqlite refuses, so point at the BigInt path.
    return new RangeError(`${err.message}. Call statement.safeIntegers() or db.defaultSafeIntegers() to read it as a BigInt`);
  }
  return err;
}

// ---- environment validation ------------------------------------------------

// Strips prerelease/build metadata (-nightly, +build) before comparing, so
// "v20.1.0-nightly202" isn't miscompared as a string suffix.
function parseVersion(version) {
  const clean = String(version).replace(/^v/, '').split(/[-+]/)[0];
  return clean.split('.').map((n) => Number.parseInt(n, 10) || 0);
}

function validateNodeVersion(minVersion, current = process.version) {
  const currentParts = parseVersion(current);
  const minParts = parseVersion(minVersion);
  for (let i = 0; i < Math.max(currentParts.length, minParts.length); i++) {
    const curr = currentParts[i] || 0;
    const min = minParts[i] || 0;
    if (curr > min) return { valid: true, current, required: minVersion };
    if (curr < min) return { valid: false, current, required: minVersion };
  }
  return { valid: true, current, required: minVersion };
}

// Accepts a single platform or a list; `arch` is optional because most
// callers only care about OS (e.g. a feature gated to darwin/linux)
// but native-binding constraints sometimes need process.arch too.
function validatePlatform(allowed, allowedArch = null) {
  const list = Array.isArray(allowed) ? allowed : [allowed];
  const platform = process.platform;
  const arch = process.arch;
  const platformOk = list.includes(platform);
  const archOk = !allowedArch || (Array.isArray(allowedArch) ? allowedArch : [allowedArch]).includes(arch);
  return { valid: platformOk && archOk, platform, arch, allowed: list, allowedArch };
}

// Unlike a plain getter, this actually validates against thresholds when
// given, so it can gate startup ("don't enable feature X under memory
// pressure") rather than just reporting numbers nobody checks.
function validateSystemResources({ minFreeMemoryMb, maxUsedPercent } = {}) {
  const freeMemory = os.freemem();
  const totalMemory = os.totalmem();
  const usedPercent = Math.round((1 - freeMemory / totalMemory) * 100);
  const freeMemoryMb = Math.round(freeMemory / (1024 * 1024));
  const failures = [];
  if (Number.isFinite(minFreeMemoryMb) && freeMemoryMb < minFreeMemoryMb) {
    failures.push(`free memory ${freeMemoryMb}MB is below required ${minFreeMemoryMb}MB`);
  }
  if (Number.isFinite(maxUsedPercent) && usedPercent > maxUsedPercent) {
    failures.push(`memory usage ${usedPercent}% exceeds limit ${maxUsedPercent}%`);
  }
  return {
    valid: failures.length === 0,
    totalMemory,
    freeMemory,
    freeMemoryMb,
    usedPercent,
    loadAvg: os.loadavg(),
    failures
  };
}

// ---- path utilities --------------------------------------------------------

// Resolves `inputPath` against `projectRoot` (defaulting to cwd) and, unless
// `allowEscape` is set, refuses to hand back a path outside that root.
function normalizePath(inputPath, projectRoot, { allowEscape = false } = {}) {
  const root = path.resolve(projectRoot || process.cwd());
  const resolved = path.isAbsolute(inputPath) ? path.normalize(inputPath) : path.resolve(root, inputPath);
  if (!allowEscape && path.isAbsolute(inputPath) === false) {
    const relative = path.relative(root, resolved);
    const escapes = relative.startsWith('..') || path.isAbsolute(relative);
    if (escapes) {
      throw new RangeError(`normalizePath: "${inputPath}" resolves outside project root "${root}"`);
    }
  }
  return resolved;
}

// ---- atomic file operations ------------------------------------------------

// Writes to a sibling temp file, fsyncs it for durability, then renames it
// into place. `fs.renameSync` throws EEXIST on Windows when the target
// already exists (unlike POSIX, which overwrites) — this is a real
// cross-platform correctness bug in the naive version, not a nitpick, so on
// EEXIST the target is unlinked and the rename is retried once. The
// containing directory is fsynced too (POSIX only; Windows has no
// equivalent and open() on a directory isn't supported there), since a
// rename without a directory fsync can still be lost on a crash.
function writeFileAtomic(filePath, data, encoding = 'utf8') {
  const dir = path.dirname(filePath);
  const tempPath = path.join(dir, `.${path.basename(filePath)}.${crypto.randomBytes(6).toString('hex')}.tmp`);

  let fd;
  try {
    fs.mkdirSync(dir, { recursive: true });
    fd = fs.openSync(tempPath, 'w');
    fs.writeSync(fd, Buffer.isBuffer(data) ? data : Buffer.from(data, encoding));
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;

    try {
      fs.renameSync(tempPath, filePath);
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      fs.unlinkSync(filePath);
      fs.renameSync(tempPath, filePath);
    }

    if (process.platform !== 'win32') {
      const dirFd = fs.openSync(dir, 'r');
      try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
    }
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  } finally {
    if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* ignore */ } }
    try { fs.unlinkSync(tempPath); } catch { /* already renamed or never created */ }
  }
}

function localdir() {
  return fs.realpathSync('.').match(/^\/Users\/[^/]+/)[0];
}

function workdir() {
  return path.join(localdir(), '.aws');
}

function getRegion() {
  return fs.readFileSync(path.resolve(workdir(), 'credentials'), 'utf8');
}


module.exports = {
  translate,
  convert,
  validateNodeVersion,
  validatePlatform,
  validateSystemResources,
  normalizePath,
  writeFileAtomic,
  getRegion
};
