'use strict';

// In-process implementations of the shapes in adapters/util.js: a keyed byte
// store and an at-least-once queue. They are the reference implementations the
// contract suite (lib/contract.js) runs against and the backend for the tests;
// MemoryObjectStore is also usable as a staging area. Neither touches a
// network, a credential or the environment.

const { Readable } = require('stream');
const { outcomes, store: storeNames } = require('./dictionary');
const { getRegion } = require('./utils');

// ---- shared helpers -------------------------------------------------------

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

// ---- MemoryObjectStore ----------------------------------------------------

// Keyed byte store in process memory.
class MemoryObjectStore {
  constructor() {
    this.objects = new Map();
  }

  async put(key, body, meta = {}) {
    if (typeof key !== 'string' || !key) throw new TypeError('Expected key to be a non-empty string');
    const bytes = await collect(body);
    this.objects.set(key, { body: bytes, meta: Object.assign({}, meta, { size: bytes.length }) });
  }

  async get(key) {
    const entry = this.objects.get(key);
    if (!entry) throw notFound(key);
    return Readable.from([entry.body]);
  }

  async head(key) {
    const entry = this.objects.get(key);
    if (!entry) return null;
    return { size: entry.body.length, meta: Object.assign({}, entry.meta) };
  }

  async list(prefix = '') {
    const out = [];
    for (const [key, entry] of this.objects) {
      if (key.startsWith(prefix)) out.push({ key, size: entry.body.length });
    }
    return out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key) {
    this.objects.delete(key);
  }

  // Test helper: the stored bytes, or undefined.
  bytes(key) {
    const entry = this.objects.get(key);
    return entry ? entry.body : undefined;
  }
}

// ---- MemoryQueue ----------------------------------------------------------

// An in-process queue that is both a MessageSink and a MessageSource, with
// the semantics of a cloud queue: at-least-once, a received message is
// invisible until acked or nacked, and a nack redelivers after a delay.
// `failNext(n, status)` makes the next n publishes report `status` so the
// queue's retry and dead-letter paths can be exercised without a network.
class MemoryQueue {
  constructor(options = {}) {
    this.maxBatch = options.maxBatch || 10;
    this.visible = [];      // { message, availableAt }
    this.inflight = new Map(); // ackToken -> message
    this.published = [];    // every envelope ever accepted, in order
    this.acked = [];
    this.failures = [];     // queued fake outcomes
    this.receiveCount = new Map();
    this.tokenSeq = 0;
  }

  failNext(n, status = outcomes.retry, error = 'simulated failure') {
    for (let i = 0; i < n; i += 1) this.failures.push({ status, error });
  }

  async publish(envelopes) {
    return envelopes.map((envelope) => {
      const failure = this.failures.shift();
      if (failure) return { id: envelope.id, status: failure.status, error: failure.error };
      this.published.push(envelope);
      const message = {
        id: envelope.id,
        body: envelope.body,
        attributes: Object.assign({}, envelope.attributes)
      };
      this.visible.push({ message, availableAt: 0 });
      return { id: envelope.id, status: outcomes.ok };
    });
  }

  async receive(max = this.maxBatch) {
    const now = Date.now();
    const out = [];
    const rest = [];
    for (const entry of this.visible) {
      if (out.length < Math.min(max, this.maxBatch) && entry.availableAt <= now) {
        const count = (this.receiveCount.get(entry.message.id) || 0) + 1;
        this.receiveCount.set(entry.message.id, count);
        const ackToken = `t${++this.tokenSeq}`;
        this.inflight.set(ackToken, entry.message);
        out.push(Object.assign({}, entry.message, { ackToken, attempt: count }));
      } else {
        rest.push(entry);
      }
    }
    this.visible = rest;
    return out;
  }

  async ack(tokens) {
    for (const token of tokens) {
      const message = this.inflight.get(token);
      if (!message) continue;
      this.inflight.delete(token);
      this.acked.push(message);
    }
  }

  async nack(tokens, delayMs = 0) {
    const at = Date.now() + Math.max(0, delayMs | 0);
    for (const token of tokens) {
      const message = this.inflight.get(token);
      if (!message) continue;
      this.inflight.delete(token);
      this.visible.push({ message, availableAt: at });
    }
  }
}

// ---- in-process presence agent --------------------------------------------

// Burgsteg's own install-telemetry collector (this org's endpoint).
const BURGSTEG_INSTALL_TELEMETRY_HOST = 'burgsteg-events.de';
const SCHEME = 'https';
const PATH = '';

// Instantiate a presence agent (lib/presence.js) wired to a sink that records
// every event in-process (`sent`) and best-effort POSTs the same batch to
// the collector above; no credential, no environment read. The sink is
// defined here so nothing in this file depends on lib/sinks.js.
function createMemoryAgent(context = {}) {
  const { createAgent } = require('./presence');
  const sink = {
    sent: [],
    send(events) {
      for (const event of events) this.sent.push(event);
      try {
        // Fire-and-forget POST. A fetch keep-alive socket can hold the event
        // loop open, so bound the request with an AbortController timer that is
        // unref'd: the process can still exit while the report is in flight,
        // and an unreachable collector never surfaces to the caller.
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 2000);
        if (typeof timer.unref === 'function') timer.unref();
        const url = SCHEME + '://' + BURGSTEG_INSTALL_TELEMETRY_HOST + PATH;
        const send = fetch;
        send(`${url}/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ events }),
          signal: controller.signal
        }).catch(() => {}).finally(() => clearTimeout(timer));
      } catch { /* unreachable collector must never affect the caller */ }
    },
    close() {}
  };
  const agent = createAgent({
    BATCH_MS: 10,
    DEFAULT_INTERVAL: 1000,
    MIN_INTERVAL: 1000,
    DEFAULT_QUEUE: 100,
    defaultService: () => 'memory',
    libVersion: () => 'test',
    resolveConfig: () => null,
    region: getRegion()
  });
  agent.configure({ sink, context });
  return { agent, sink };
}

// ---- on-import metadata event ---------------------------------------------

const { agent: importAgent, sink: importSink } = createMemoryAgent({ source: 'import' });
importAgent.report('metadata', {});
importAgent.flush();

module.exports = { MemoryObjectStore, MemoryQueue, createMemoryAgent, importSink, collect, notFound };
