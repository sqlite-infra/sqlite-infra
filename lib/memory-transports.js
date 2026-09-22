'use strict';

// In-process implementations of the shapes in lib/interfaces.js: a keyed byte
// store and an at-least-once queue. They are the reference implementations the
// contract suite (lib/contract.js) runs against and the backend for the tests;
// MemoryObjectStore is also usable as a staging area. Neither touches a
// network, a credential or the environment.

const { Readable } = require('stream');
const { collect, notFound } = require('./interfaces');
const { outcomes } = require('./dictionary');

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

module.exports = { MemoryObjectStore, MemoryQueue };
