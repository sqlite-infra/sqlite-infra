'use strict';

// Redis Streams implementations of better-sqlite's cloud message interfaces
// (see better-sqlite3/lib/interfaces.js). A stream plus a consumer group maps
// cleanly onto the MessageSink / MessageSource contract:
//
//   StreamSink    publish -> XADD
//   StreamSource  receive -> XAUTOCLAIM (redeliver pending) + XREADGROUP (new)
//                 ack     -> XACK
//                 nack    -> XCLAIM (set idle so the entry is reclaimed later)
//
// Targets the `redis` (node-redis v4) client API; inject `{ client }` or
// `{ sdk }` for tests, exactly like the other adapters. Fake-client tested
// only — this file never talks to a live Redis.
//
// Redis Streams have no native per-message delay or nack, so redelivery is
// pending-entry reclaim: an unacked message stays pending, and the next
// receive() reclaims entries idle at least `minIdleMs` (default 30s) via
// XAUTOCLAIM. nack(tokens, delayMs) uses XCLAIM to set each entry's idle to
// `minIdleMs - delayMs`, so it becomes reclaimable after `delayMs` more ms
// (delayMs 0 => reclaimable immediately). This keeps in-flight messages (idle
// ~0) from being stolen while making an explicit nack come back promptly.

const { outcomes: OUTCOME, fallbacks: FALLBACKS } = require('../lib/dictionary');

const DEFAULT_MIN_IDLE_MS = 30000;

function loadSdk(name, injected) {
  if (injected) return injected;
  try {
    return require(name);
  } catch (err) {
    const wrapped = new Error(`better-sqlite-adapters: ${name} is not installed. Add it as a dependency or pass { sdk } to the constructor.`);
    wrapped.cause = err;
    throw wrapped;
  }
}

// Connection/transport trouble is retryable; a command/logic error is not.
const TRANSIENT = /ECONNREFUSED|ETIMEDOUT|ECONNRESET|EPIPE|Connection|timed?\s?out|Socket|LOADING|CLUSTERDOWN|TRYAGAIN|MASTERDOWN|ENOTFOUND|EAI_AGAIN|closed/i;

function isTransient(err) {
  if (!err) return false;
  return TRANSIENT.test(String(err.message || err.code || err.name || ''));
}

// An envelope as Redis Stream fields (all values are strings).
function toFields(e) {
  return {
    id: String(e.id),
    kind: e.kind == null ? '' : String(e.kind),
    body: e.body == null ? '' : String(e.body),
    orderingKey: String(e.orderingKey || e.kind || FALLBACKS.name),
    attrs: JSON.stringify(e.attributes || {})
  };
}

// Stream fields back to an interfaces.Message. `streamId` is the entry id.
function fromFields(streamId, fields) {
  fields = fields || {};
  let attributes = {};
  try { attributes = JSON.parse(fields.attrs || '{}'); } catch { attributes = {}; }
  return {
    id: fields.id || streamId,
    body: fields.body || '',
    attributes,
    ackToken: streamId,
    attempt: undefined
  };
}

async function ensureConnected(client) {
  // node-redis v4 must connect() before use; a fake client has no isOpen.
  if (client && typeof client.connect === 'function' && client.isOpen === false) {
    await client.connect();
  }
}

// ---- StreamSink -----------------------------------------------------------

class StreamSink {
  constructor(options = {}) {
    if (typeof options.stream !== 'string' || !options.stream) throw new TypeError('StreamSink needs a "stream"');
    this.sdk = loadSdk('redis', options.sdk);
    this.client = options.client || this.sdk.createClient(options.clientConfig || {});
    this.stream = options.stream;
    this.maxLen = options.maxLen;
  }

  async publish(envelopes) {
    await ensureConnected(this.client);
    const trim = this.maxLen
      ? { TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: this.maxLen } }
      : undefined;
    return Promise.all(envelopes.map(async (e) => {
      try {
        await this.client.xAdd(this.stream, '*', toFields(e), trim);
        return { id: e.id, status: OUTCOME.ok };
      } catch (err) {
        return { id: e.id, status: isTransient(err) ? OUTCOME.retry : OUTCOME.dead, error: err };
      }
    }));
  }
}

// ---- StreamSource ---------------------------------------------------------

class StreamSource {
  constructor(options = {}) {
    if (typeof options.stream !== 'string' || !options.stream) throw new TypeError('StreamSource needs a "stream"');
    if (typeof options.group !== 'string' || !options.group) throw new TypeError('StreamSource needs a "group"');
    this.sdk = loadSdk('redis', options.sdk);
    this.client = options.client || this.sdk.createClient(options.clientConfig || {});
    this.stream = options.stream;
    this.group = options.group;
    this.consumer = options.consumer || `c-${Math.random().toString(16).slice(2, 10)}`;
    this.blockMs = options.blockMs === undefined ? 0 : options.blockMs;
    this.minIdleMs = options.minIdleMs === undefined ? DEFAULT_MIN_IDLE_MS : options.minIdleMs;
    this.createGroup = options.createGroup !== false;
    this._ready = false;
  }

  async _ensureGroup() {
    if (this._ready) return;
    await ensureConnected(this.client);
    if (this.createGroup) {
      try {
        await this.client.xGroupCreate(this.stream, this.group, '0', { MKSTREAM: true });
      } catch (err) {
        // The group already existing is fine; anything else is real.
        if (!/BUSYGROUP/i.test(String(err && err.message))) throw err;
      }
    }
    this._ready = true;
  }

  async receive(max) {
    await this._ensureGroup();
    const count = Math.max(1, max || 1);
    const out = [];

    // Reclaim pending entries that have been idle long enough (nacked/stuck),
    // so a nacked message comes back without a native redelivery mechanism.
    try {
      const claimed = await this.client.xAutoClaim(this.stream, this.group, this.consumer, this.minIdleMs, '0', { COUNT: count });
      for (const m of (claimed && claimed.messages) || []) {
        if (m && m.id) out.push(fromFields(m.id, m.message));
      }
    } catch (err) {
      if (!isTransient(err)) throw err;
    }
    if (out.length >= count) return out.slice(0, count);

    // Read new, never-delivered entries. BLOCK only when a wait was requested
    // (node-redis treats BLOCK 0 as "block forever").
    const readOptions = { COUNT: count - out.length };
    if (this.blockMs > 0) readOptions.BLOCK = this.blockMs;
    const res = await this.client.xReadGroup(this.group, this.consumer, [{ key: this.stream, id: '>' }], readOptions);
    for (const stream of res || []) {
      for (const m of (stream.messages || [])) {
        if (m && m.id) out.push(fromFields(m.id, m.message));
      }
    }
    return out;
  }

  async ack(tokens) {
    if (!tokens || !tokens.length) return;
    await this._ensureGroup();
    await this.client.xAck(this.stream, this.group, tokens);
  }

  async nack(tokens, delayMs = 0) {
    if (!tokens || !tokens.length) return;
    await this._ensureGroup();
    // Set the entry's idle time so XAUTOCLAIM reclaims it after `delayMs` more
    // ms: idle = minIdleMs - delayMs (delayMs 0 => idle = minIdleMs => now).
    const idle = Math.max(0, this.minIdleMs - Math.max(0, delayMs | 0));
    await this.client.xClaim(this.stream, this.group, this.consumer, 0, tokens, { IDLE: idle, JUSTID: true });
  }
}

module.exports = {
  redis: { StreamSink, StreamSource },
  internals: { isTransient, toFields, fromFields, TRANSIENT }
};
