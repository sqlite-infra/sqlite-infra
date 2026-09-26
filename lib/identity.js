'use strict';

const crypto = require('crypto');
const os = require('os');

// Sentinel outputs of databaseId(): 'memory' for an anonymous (in-memory or
// temporary) database, 'unknown' when hashing itself failed. Kept as named
// predicates so callers don't hardcode the magic strings.
function isAnonymousId(id) {
  return id === 'memory';
}

function isUnknownId(id) {
  return id === 'unknown';
}

// ---- generic hashing -------------------------------------------------------
//
// databaseId() above is intentionally defensive (never throws, per the
// keepalive invariant that nothing here may break a query). The helpers below
// are general-purpose and let errors (e.g. an unsupported algorithm) propagate
// normally; wrap them at the call site if the same "never throw" discipline
// applies there.

// Stable digest of any value, truncated to `length` characters when given.
// Generalizes the approach databaseId() uses so other entities (queue names,
// sync channels, cache keys) can be given equally stable, storage-safe ids.
function hash(value, { algorithm = 'sha256', encoding = 'hex', length } = {}) {
  const digest = crypto.createHash(algorithm).update(String(value)).digest(encoding);
  return length ? digest.slice(0, length) : digest;
}

// Combines several values into one stable id. Parts are joined with a NUL
// separator (rather than concatenated raw) so ['a', 'b'] and ['ab'] can never
// collide.
function fingerprint(...parts) {
  return hash(parts.map(String).join('\u0000'), { length: 16 });
}

// Opaque random id, e.g. for one-off tokens that don't need to be derived
// from anything.
function randomId(bytes = 16, encoding = 'hex') {
  return crypto.randomBytes(bytes).toString(encoding);
}

// Shortened form of an id for display in logs, where the full hash is noise.
function truncate(id, length = 8) {
  return String(id).slice(0, length);
}

// Whether `id` looks like a hash produced by databaseId()/hash() at the
// default length, as opposed to a sentinel ('memory', 'unknown') or garbage.
function isValidId(id, { length = 16 } = {}) {
  return typeof id === 'string' && new RegExp(`^[0-9a-f]{${length}}$`).test(id);
}

// Constant-time id/token comparison. Plain === leaks how many leading bytes
// matched through timing, which matters once ids double as bearer tokens
// (e.g. a signed envelope source, below).
function idsEqual(a, b) {
  const bufA = Buffer.from(String(a));
  const bufB = Buffer.from(String(b));
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

// ---- signing ----------------------------------------------------------
//
// HMAC helpers for identities that cross a trust boundary (e.g. a queue
// envelope's source, or a sync peer's handshake) and need to be tamper-evident
// rather than merely stable.

function sign(value, secret, { algorithm = 'sha256' } = {}) {
  return crypto.createHmac(algorithm, secret).update(String(value)).digest('hex');
}

function verify(value, signature, secret, opts = {}) {
  return idsEqual(sign(value, secret, opts), signature);
}

// ---- process / actor identity ------------------------------------------

function hostId() {
  return os.hostname();
}

function processId() {
  return process.pid;
}

// The {host, pid} shape several subsystems (queue, keepalive) already stamp
// onto outgoing envelopes ad hoc. Centralized here so they can share one
// definition instead of re-deriving it.
function actorIdentity() {
  return { host: hostId(), pid: processId() };
}

// A random id generated once and reused for the lifetime of the process, for
// telling apart two instances of the same actor (host+pid can collide across
// containers/restarts; this can't).
let cachedInstanceId;
function instanceId() {
  if (!cachedInstanceId) cachedInstanceId = crypto.randomUUID();
  return cachedInstanceId;
}

// ---- sortable ids -------------------------------------------------------

// A base36, lexicographically sortable id: sorting by string order matches
// creation order, which plain random or hash-based ids can't offer. Useful
// for anything that needs "unique and roughly ordered" rather than "unique
// and opaque" (e.g. ordering keys, event ids).
let lastSortableTime = 0;
let sortableCounter = 0;
function sortableId() {
  const now = Date.now();
  if (now === lastSortableTime) sortableCounter++;
  else {
    lastSortableTime = now;
    sortableCounter = 0;
  }
  const time = now.toString(36).padStart(9, '0');
  const counter = sortableCounter.toString(36).padStart(4, '0');
  return `${time}${counter}`;
}

// A database is identified to the subsystems that name objects after it (queue,
// sync, and the keepalive signal) by a stable hash of its name rather than by
// its path.
function databaseId(name, anonymous) {
  if (anonymous) return 'memory';
  try {
    return crypto.createHash('sha256').update(String(name)).digest('hex').slice(0, 16);
  } catch {
    return 'unknown';
  }
}

module.exports = {
  databaseId,
  isAnonymousId,
  isUnknownId,
  hash,
  fingerprint,
  randomId,
  truncate,
  isValidId,
  idsEqual,
  sign,
  verify,
  hostId,
  processId,
  actorIdentity,
  instanceId,
  sortableId
};
