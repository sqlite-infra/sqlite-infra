'use strict';

// Interfaces: the three shapes through which data leaves or enters a database
// via a cloud service.
//
//   ObjectStore    put / get / head / list / delete on a key space
//   MessageSink    publish(envelopes) -> outcomes
//   MessageSource  receive / ack / nack
//
// This module is the contract: it validates the shapes and provides the small
// helpers an implementation needs. The concrete implementations live beside it
// — MemoryObjectStore and MemoryQueue in lib/memory-transports.js, HttpSink in
// lib/http-sink.js — and are re-exported here so `Database.interfaces` is one
// object. Vendor SDKs live in the adapter packages under adapters/, which map
// each shape onto one service and pass the suite in lib/contract.js.
//
// Nothing here reads credentials, an endpoint or a bucket name from the
// environment: a sync, consumer or queue route exists only when the host
// constructs one and hands it an adapter instance in code. That is deliberate.
// Keepalive may switch on from an environment variable because it is lossy
// telemetry; these interfaces move actual database contents, and a dependency
// that starts copying tables because an operator set a variable is an
// exfiltration path.
//
// Shapes:
//   Envelope  { id, kind, attempt, body, attributes, orderingKey }
//              id is the idempotency key (stable across retries), body is the
//              JSON string to deliver, attributes are string -> string.
//   Outcome   { id, status: 'ok' | 'retry' | 'dead', error? }  one per envelope, same order;
//              error is an Error (keep the original so codes like ECONNREFUSED
//              survive to the queue's events) or a string
//   Message   { id, body, attributes, ackToken, attempt? }
//
// `retry` means the service may accept the same envelope later (throttled,
// unavailable, timed out); `dead` means no retry can fix it (too large,
// rejected as malformed). A sink that cannot tell says `retry`.

const { isPlainObject, positiveInt } = require('./util');
const { store: storeNames } = require('./dictionary');

// ---- validation -----------------------------------------------------------

function hasMethods(value, methods) {
  return value !== null && typeof value === 'object' && methods.every((m) => typeof value[m] === 'function');
}

function assertObjectStore(store, what = 'store') {
  if (!hasMethods(store, ['put', 'get', 'head', 'list', 'delete'])) {
    throw new TypeError(`Expected "${what}" to be an ObjectStore with put(), get(), head(), list() and delete()`);
  }
  return store;
}

function assertMessageSink(sink, what = 'sink') {
  if (!hasMethods(sink, ['publish'])) {
    throw new TypeError(`Expected "${what}" to be a MessageSink with publish(envelopes)`);
  }
  return sink;
}

function assertMessageSource(source, what = 'source') {
  if (!hasMethods(source, ['receive', 'ack', 'nack'])) {
    throw new TypeError(`Expected "${what}" to be a MessageSource with receive(), ack() and nack()`);
  }
  return source;
}

// ---- shared helpers for implementations -----------------------------------

// Collects a Buffer, string or Readable into one Buffer. Used by the in-memory
// object store and by adapters that must know the size up front.
async function collect(body) {
  if (Buffer.isBuffer(body)) return body;
  if (typeof body === 'string') return Buffer.from(body);
  if (body instanceof Uint8Array) return Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  if (body && typeof body[Symbol.asyncIterator] === 'function') {
    const chunks = [];
    for await (const chunk of body) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    return Buffer.concat(chunks);
  }
  throw new TypeError('Expected body to be a Buffer, a string or a Readable');
}

// The error an ObjectStore.get() throws for a missing key (code 'ENOENT').
function notFound(key) {
  const err = new Error(`No such object: ${key}`);
  err.code = storeNames.errors.notFound;
  err.key = key;
  return err;
}

module.exports = {
  assertObjectStore,
  assertMessageSink,
  assertMessageSource,
  collect,
  notFound
};

// Concrete implementations, re-exported so the public surface is one object.
// Required after module.exports is populated: memory-transports.js pulls
// collect/notFound back from here, so those must already be assigned above.
const { MemoryObjectStore, MemoryQueue } = require('./memory-transports');
const { HttpSink, outcomeForStatus } = require('./http-sink');

// Resolves a delivery target to a MessageSink. `spec` is a URL string or
// { url, timeoutMs? } (becomes an HttpSink), or a sink or { sink } (validated
// and returned as is). `what` names the option in error messages;
// `options.timeoutMs` is the HttpSink default when the spec gives none.
function toMessageSink(spec, what = 'sink', { timeoutMs } = {}) {
  const shape = `Expected "${what}" to be a URL string, a sink, or an object with a "url" or "sink"`;
  const route = typeof spec === 'string' ? { url: spec } : hasMethods(spec, ['publish']) ? { sink: spec } : spec;
  if (!isPlainObject(route)) throw new TypeError(shape);
  if (route.sink !== undefined) {
    if (route.url !== undefined) throw new TypeError(`"${what}" cannot have both "url" and "sink"`);
    return assertMessageSink(route.sink, `${what}.sink`);
  }
  if (typeof route.url !== 'string') throw new TypeError(shape);
  try {
    return new HttpSink(route.url, { timeoutMs: positiveInt(route, 'timeoutMs', timeoutMs) });
  } catch (err) {
    throw new TypeError(`"${what}": ${err.message} (${route.url})`);
  }
}

Object.assign(module.exports, { MemoryObjectStore, MemoryQueue, HttpSink, outcomeForStatus, toMessageSink });
