'use strict';

// Files staged on disk before an upload. Staging makes an object's size and
// SHA-256 known before the PUT, which lets an adapter pick a single PUT or a
// multipart upload and lets lib/importer.js verify what it downloads.
//
// Temp staging paths are built by lib/params.js (tempPath): next to the
// database file, or in os.tmpdir() for in-memory and URI-named databases.

const fs = require('fs');
const zlib = require('zlib');
const crypto = require('crypto');
const { Transform } = require('stream');
const { pipeline } = require('stream/promises');

// Hashes and counts what passes through it; `digest()` after the stream ends.
class Hasher extends Transform {
  constructor() {
    super();
    this.hash = crypto.createHash('sha256');
    this.size = 0;
  }
  _transform(chunk, _enc, cb) {
    this.hash.update(chunk);
    this.size += chunk.length;
    cb(null, chunk);
  }
  digest() { return this.hash.digest('hex'); }
}

// Compresses (or copies) `src` into `dst`; resolves { size, sha256 } of dst.
async function stage(src, dst, gzip) {
  const hasher = new Hasher();
  const steps = [fs.createReadStream(src)];
  if (gzip) steps.push(zlib.createGzip());
  steps.push(hasher, fs.createWriteStream(dst));
  await pipeline(steps);
  return { size: hasher.size, sha256: hasher.digest() };
}

async function unlink(file) {
  await fs.promises.rm(file, { force: true }).catch(() => {});
}

// Filesystem- and key-safe ISO 8601: 2026-09-14T05-13-12.345Z
function timestamp(date = new Date()) {
  return date.toISOString().replace(/:/g, '-');
}

module.exports = { Hasher, stage, unlink, timestamp };
