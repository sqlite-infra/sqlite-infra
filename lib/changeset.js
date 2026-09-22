'use strict';

// Container format for one exported changeset object.
//
// The sync keeps one session per user table (see lib/sync.js for
// why), so a single "changeset" as shipped to the store is a list of SQLite
// changesets, one per table that changed. Rather than rely on byte-level
// concatenation of SQLite's own format, they are framed:
//
//   "BSQC"  4 bytes  magic
//   u32     count    number of parts, big-endian
//   repeat count times:
//     u32   length   big-endian
//     bytes          a SQLite changeset as returned by Session#changeset()
//
// Applying the parts in order is equivalent to applying one combined
// changeset, since parts touch disjoint tables.

const { changeset: names } = require('./dictionary');

const MAGIC = Buffer.from(names.magic);

function pack(parts) {
  const bufs = [MAGIC, Buffer.alloc(4)];
  bufs[1].writeUInt32BE(parts.length, 0);
  for (const part of parts) {
    const bytes = Buffer.isBuffer(part) ? part : Buffer.from(part.buffer, part.byteOffset, part.byteLength);
    const len = Buffer.alloc(4);
    len.writeUInt32BE(bytes.length, 0);
    bufs.push(len, bytes);
  }
  return Buffer.concat(bufs);
}

function unpack(buffer) {
  if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  if (buffer.length < 8 || !buffer.subarray(0, 4).equals(MAGIC)) {
    throw new TypeError('Not a better-sqlite3 changeset container (bad magic)');
  }
  const count = buffer.readUInt32BE(4);
  const parts = [];
  let offset = 8;
  for (let i = 0; i < count; i += 1) {
    if (offset + 4 > buffer.length) throw new TypeError('Truncated changeset container');
    const len = buffer.readUInt32BE(offset);
    offset += 4;
    if (offset + len > buffer.length) throw new TypeError('Truncated changeset container');
    parts.push(buffer.subarray(offset, offset + len));
    offset += len;
  }
  if (offset !== buffer.length) throw new TypeError('Trailing bytes after changeset container');
  return parts;
}

module.exports = { pack, unpack, MAGIC };
