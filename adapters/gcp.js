'use strict';

// better-sqlite-adapters (Google Cloud): implementations of better-sqlite's
// cloud interfaces (see better-sqlite3/lib/interfaces.js for the shapes and the
// contract), backed by Google Cloud Storage and Pub/Sub.
//
//   ObjectStore   an ObjectStore on Google Cloud Storage
//   QueueSink     a MessageSink on Pub/Sub (publishMessage, ordering key = kind)
//   QueueSource   a MessageSource on Pub/Sub (pull / acknowledge / modifyAckDeadline)
//
// Same conventions as the AWS adapter in ./index.js: the SDK is loaded lazily
// (or injected for tests), a client may be passed in, and every wire string
// comes from better-sqlite3/lib/dictionary rather than being spelled here.
// Outcome mapping follows the interfaces contract: a fault the service says is
// the sender's (INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED) is 'dead';
// throttling, unavailability and transport errors are 'retry'.

const { Readable } = require('stream');
const { outcomes: OUTCOME, store: storeNames, queue: queueNames, fallbacks: FALLBACKS } = require('../lib/dictionary');

const IDEMPOTENCY_KEY = queueNames.attributes.idempotencyKey;

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

function is404(err) {
  return err && (err.code === 404 || err.code === '404');
}

async function collect(body) {
  if (Buffer.isBuffer(body)) return body;
  if (typeof body === 'string') return Buffer.from(body);
  if (body instanceof Uint8Array) return Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  const chunks = [];
  for await (const c of body) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
  return Buffer.concat(chunks);
}

// gRPC status codes that a retry may fix: CANCELLED(1), DEADLINE_EXCEEDED(4),
// RESOURCE_EXHAUSTED(8), ABORTED(10), INTERNAL(13), UNAVAILABLE(14).
const TRANSIENT_CODES = new Set([1, 4, 8, 10, 13, 14]);
const TRANSIENT = /UNAVAILABLE|DEADLINE_EXCEEDED|RESOURCE_EXHAUSTED|INTERNAL|ABORTED|CANCELLED|Throttl|TooManyRequests|\b(?:429|500|503|504)\b/i;

// True when an error looks like something a later attempt could succeed at.
// Anything else (INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, ...) is a
// permanent, sender-caused fault.
function isTransient(err) {
  if (!err) return false;
  if (typeof err.code === 'number' && TRANSIENT_CODES.has(err.code)) return true;
  const text = `${err.code || ''} ${err.name || ''} ${err.message || ''}`;
  return TRANSIENT.test(text);
}

// ---- Google Cloud Storage --------------------------------------------------

class ObjectStore {
  constructor(options = {}) {
    if (typeof options.bucket !== 'string' || !options.bucket) throw new TypeError('ObjectStore needs a "bucket"');
    this.sdk = loadSdk('@google-cloud/storage', options.sdk);
    this.client = options.client || new this.sdk.Storage(options.clientConfig || {});
    this.bucket = options.bucket;
    this.prefix = options.prefix ? String(options.prefix).replace(/^\/+|\/+$/g, '') : '';
  }

  _key(key) { return this.prefix ? `${this.prefix}/${key}` : key; }
  _unkey(full) { return this.prefix ? full.slice(this.prefix.length + 1) : full; }
  _file(key) { return this.client.bucket(this.bucket).file(this._key(key)); }

  async put(key, body, meta = {}) {
    const bytes = await collect(body);
    await this._file(key).save(bytes, {
      resumable: false,
      contentType: meta.contentType,
      metadata: {
        contentEncoding: meta.contentEncoding,
        metadata: meta.sha256 ? { sha256: meta.sha256 } : undefined
      }
    });
  }

  async get(key) {
    const file = this._file(key);
    const [exists] = await file.exists();
    if (!exists) throw notFound(key);
    return file.createReadStream();
  }

  async head(key) {
    try {
      const [md] = await this._file(key).getMetadata();
      const meta = Object.assign({}, md.metadata || {});
      if (md.contentType) meta.contentType = md.contentType;
      return { size: Number(md.size), meta };
    } catch (err) {
      if (is404(err)) return null;
      throw err;
    }
  }

  async list(prefix = '') {
    const [files] = await this.client.bucket(this.bucket).getFiles({ prefix: this._key(prefix) });
    return files
      .map((f) => ({ key: this._unkey(f.name), size: Number(f.metadata.size) }))
      .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key) {
    await this._file(key).delete({ ignoreNotFound: true });
  }
}

// ---- Pub/Sub (publish) -----------------------------------------------------

class QueueSink {
  constructor(options = {}) {
    if (typeof options.topic !== 'string' || !options.topic) throw new TypeError('QueueSink needs a "topic"');
    this.sdk = loadSdk('@google-cloud/pubsub', options.sdk);
    this.client = options.client || new this.sdk.PubSub(options.clientConfig || {});
    this.topic = options.topic;
  }

  publish(envelopes) {
    // Ordering is enabled so an ordering key is honoured; Pub/Sub batches on
    // the client side, so one publishMessage per envelope is one network batch.
    const topic = this.client.topic(this.topic, { messageOrdering: true });
    return Promise.all(envelopes.map((e) =>
      Promise.resolve()
        .then(() => topic.publishMessage({
          data: Buffer.from(e.body),
          attributes: e.attributes || {},
          orderingKey: e.orderingKey || e.kind || FALLBACKS.name
        }))
        .then(() => ({ id: e.id, status: OUTCOME.ok }),
          (err) => ({ id: e.id, status: isTransient(err) ? OUTCOME.retry : OUTCOME.dead, error: err }))));
  }
}

// ---- Pub/Sub (pull) --------------------------------------------------------

class QueueSource {
  constructor(options = {}) {
    if (typeof options.subscription !== 'string' || !options.subscription) throw new TypeError('QueueSource needs a "subscription"');
    this.sdk = loadSdk('@google-cloud/pubsub', options.sdk);
    this.client = options.client || new this.sdk.v1.SubscriberClient(options.clientConfig || {});
    this.subscription = options.subscription;
  }

  async receive(max) {
    const [res] = await this.client.pull({
      subscription: this.subscription,
      maxMessages: Math.max(1, max || 1)
    });
    return (res.receivedMessages || []).map((m) => {
      const attributes = (m.message && m.message.attributes) || {};
      return {
        id: attributes[IDEMPOTENCY_KEY] || (m.message && m.message.messageId),
        body: Buffer.from((m.message && m.message.data) || '').toString(),
        attributes,
        ackToken: m.ackId,
        attempt: m.deliveryAttempt || undefined
      };
    });
  }

  async ack(tokens) {
    if (!tokens || !tokens.length) return;
    await this.client.acknowledge({ subscription: this.subscription, ackIds: tokens });
  }

  async nack(tokens, delayMs = 0) {
    if (!tokens || !tokens.length) return;
    // ackDeadlineSeconds 0 makes the message immediately re-deliverable; a
    // positive value holds it that long, which is how a backoff is expressed.
    await this.client.modifyAckDeadline({
      subscription: this.subscription,
      ackIds: tokens,
      ackDeadlineSeconds: Math.max(0, Math.ceil((delayMs || 0) / 1000))
    });
  }
}

module.exports = {
  ObjectStore,
  QueueSink,
  QueueSource,
  internals: { isTransient, collect, notFound, TRANSIENT }
};
