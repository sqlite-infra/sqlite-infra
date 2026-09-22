'use strict';

// better-sqlite-adapters (Azure): Azure-backed implementations of better-sqlite's
// cloud interfaces (see better-sqlite3/lib/interfaces.js for the shapes and the
// contract). Mirrors the AWS adapter (adapters/index.js) in structure: lazy SDK
// loading, an injectable client for tests, and the same outcome mapping.
//
//   ObjectStore  an ObjectStore on Azure Blob Storage (@azure/storage-blob)
//   QueueSink    a MessageSink on an Azure Service Bus queue (@azure/service-bus)
//   QueueSource  a MessageSource on an Azure Service Bus queue (@azure/service-bus)
//
// Outcome mapping follows the interfaces contract: a per-call failure the
// service marks transient (throttling, server busy, timeout) is 'retry';
// anything else is a sender fault and 'dead'. When a whole batch call throws a
// sender fault, the batch is republished one message at a time so one bad
// message cannot dead-letter its neighbours.

const { Readable } = require('stream');
const {
  outcomes: OUTCOME,
  store: storeNames,
  queue: queueNames,
  contentTypes: CONTENT_TYPES
} = require('../lib/dictionary');

const SERVICE_BUS_BATCH = 100;

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

function notFound(key) {
  const err = new Error(`No such object: ${key}`);
  err.code = storeNames.errors.notFound;
  err.key = key;
  return err;
}

function isBlob404(err) {
  if (!err) return false;
  return err.statusCode === 404 || err.code === 'BlobNotFound' || (err.details && err.details.errorCode === 'BlobNotFound');
}

async function collect(body) {
  if (Buffer.isBuffer(body)) return body;
  if (typeof body === 'string') return Buffer.from(body);
  if (body instanceof Uint8Array) return Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  const chunks = [];
  for await (const c of body) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
  return Buffer.concat(chunks);
}

function toReadable(body) {
  if (!body) return Readable.from([]);
  if (typeof body.pipe === 'function') return body;
  if (typeof body[Symbol.asyncIterator] === 'function') return Readable.from(body);
  return Readable.from([Buffer.isBuffer(body) ? body : Buffer.from(body)]);
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

// Azure marks throttling and server-side conditions retryable; everything else
// (bad message, entity not found, quota exceeded) is a sender fault.
const TRANSIENT = /ServerBusy|ServiceBusy|Timeout|ServiceUnavailable|InternalError|Throttl|GeneralError|\b429\b|\b500\b|\b503\b/i;

function isTransient(err) {
  if (!err) return false;
  if (err.retryable === true) return true;
  const name = String(err.code || err.name || err.message || '');
  return TRANSIENT.test(name);
}

// Runs `sendBatch(batch)` for each chunk. A whole-call throw that is a sender
// fault isolates down to the single message that caused it; a transient throw
// retries the batch.
async function publishBatched(envelopes, size, sendBatch) {
  const outcomes = new Map();
  const run = async (batch) => {
    try {
      await sendBatch(batch);
      for (const e of batch) outcomes.set(e.id, { id: e.id, status: OUTCOME.ok });
    } catch (err) {
      if (!isTransient(err) && batch.length > 1) {
        for (const e of batch) await run([e]);
        return;
      }
      const status = isTransient(err) ? OUTCOME.retry : OUTCOME.dead;
      for (const e of batch) outcomes.set(e.id, { id: e.id, status, error: err });
    }
  };
  for (const batch of chunk(envelopes, size)) await run(batch);
  return envelopes.map((e) => outcomes.get(e.id) || { id: e.id, status: OUTCOME.retry, error: 'no result for entry' });
}

// ---- Azure Blob Storage ---------------------------------------------------

class ObjectStore {
  constructor(options = {}) {
    if (typeof options.container !== 'string' || !options.container) throw new TypeError('ObjectStore needs a "container"');
    this.sdk = loadSdk('@azure/storage-blob', options.sdk);
    let client = options.client;
    if (!client) {
      if (typeof options.connectionString !== 'string' || !options.connectionString) {
        throw new TypeError('ObjectStore needs a "client" or a "connectionString"');
      }
      client = this.sdk.BlobServiceClient.fromConnectionString(options.connectionString);
    }
    this.container = options.container;
    this.prefix = options.prefix ? String(options.prefix).replace(/^\/+|\/+$/g, '') : '';
    this.containerClient = client.getContainerClient(this.container);
  }

  _key(key) { return this.prefix ? `${this.prefix}/${key}` : key; }
  _unkey(full) { return this.prefix ? full.slice(this.prefix.length + 1) : full; }

  async put(key, body, meta = {}) {
    const buffer = await collect(body);
    await this.containerClient.getBlockBlobClient(this._key(key)).uploadData(buffer, {
      blobHTTPHeaders: { blobContentType: meta.contentType, blobContentEncoding: meta.contentEncoding },
      metadata: meta.sha256 ? { sha256: meta.sha256 } : undefined
    });
  }

  async get(key) {
    try {
      const res = await this.containerClient.getBlockBlobClient(this._key(key)).download();
      return toReadable(res.readableStreamBody);
    } catch (err) {
      if (isBlob404(err)) throw notFound(key);
      throw err;
    }
  }

  async head(key) {
    try {
      const p = await this.containerClient.getBlockBlobClient(this._key(key)).getProperties();
      const meta = Object.assign({}, p.metadata || {});
      if (p.contentType) meta.contentType = p.contentType;
      return { size: Number(p.contentLength), meta };
    } catch (err) {
      if (isBlob404(err)) return null;
      throw err;
    }
  }

  async list(prefix = '') {
    const out = [];
    for await (const b of this.containerClient.listBlobsFlat({ prefix: this._key(prefix) })) {
      out.push({ key: this._unkey(b.name), size: Number(b.properties.contentLength) });
    }
    return out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key) {
    await this.containerClient.getBlockBlobClient(this._key(key)).deleteIfExists();
  }
}

// ---- Azure Service Bus ----------------------------------------------------

function serviceBusClient(sdk, options, what) {
  if (options.client) return options.client;
  if (typeof options.connectionString !== 'string' || !options.connectionString) {
    throw new TypeError(`${what} needs a "client" or a "connectionString"`);
  }
  return new sdk.ServiceBusClient(options.connectionString);
}

class QueueSink {
  constructor(options = {}) {
    if (typeof options.queueName !== 'string' || !options.queueName) throw new TypeError('QueueSink needs a "queueName"');
    this.sdk = loadSdk('@azure/service-bus', options.sdk);
    this.client = serviceBusClient(this.sdk, options, 'QueueSink');
    this.queueName = options.queueName;
    this.sender = this.client.createSender(this.queueName);
  }

  publish(envelopes) {
    return publishBatched(envelopes, SERVICE_BUS_BATCH, async (batch) => {
      const messages = batch.map((e) => ({
        body: e.body,
        contentType: CONTENT_TYPES.json,
        messageId: e.id,
        // Delivered to the consumer as message.applicationProperties.
        applicationProperties: Object.assign({}, e.attributes),
        // A session id gives a Service Bus session queue its ordering guarantee,
        // the way orderingKey does for a FIFO queue elsewhere.
        sessionId: e.orderingKey || undefined
      }));
      await this.sender.sendMessages(messages);
    });
  }
}

class QueueSource {
  constructor(options = {}) {
    if (typeof options.queueName !== 'string' || !options.queueName) throw new TypeError('QueueSource needs a "queueName"');
    this.sdk = loadSdk('@azure/service-bus', options.sdk);
    this.client = serviceBusClient(this.sdk, options, 'QueueSource');
    this.queueName = options.queueName;
    this.maxWaitTimeMs = options.maxWaitTimeMs === undefined ? 20000 : options.maxWaitTimeMs;
    this.receiver = this.client.createReceiver(this.queueName, { receiveMode: 'peekLock' });
  }

  async receive(max) {
    const msgs = await this.receiver.receiveMessages(Math.max(1, max || 1), { maxWaitTimeInMs: this.maxWaitTimeMs });
    return (msgs || []).map((m) => {
      const attributes = m.applicationProperties || {};
      // deliveryCount is the number of prior deliveries; +1 is the current
      // attempt, matching the 1-based attempt the SQS adapter reports.
      return {
        id: attributes[queueNames.attributes.idempotencyKey] || m.messageId,
        body: typeof m.body === 'string' ? m.body : Buffer.from(m.body).toString(),
        attributes,
        ackToken: m,
        attempt: m.deliveryCount != null ? m.deliveryCount + 1 : undefined
      };
    });
  }

  async ack(tokens) {
    for (const token of tokens) await this.receiver.completeMessage(token);
  }

  async nack(tokens, delayMs = 0) {
    // Service Bus abandon() makes the message available again immediately and
    // has no per-message delay, so delayMs is best-effort and not honoured here
    // (the message's own backoff comes from its delivery count and lock).
    void delayMs;
    for (const token of tokens) await this.receiver.abandonMessage(token);
  }
}

module.exports = {
  ObjectStore,
  QueueSink,
  QueueSource,
  internals: { isTransient, publishBatched, collect, TRANSIENT }
};
