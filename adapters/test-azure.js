'use strict';

// Tests for the Azure adapters against fake SDK modules and clients. They prove
// the inputs handed to each SDK call and the outcome mapping; they do not talk
// to Azure. The fakes model Blob Storage and Service Bus closely enough to run
// the contract suite from better-sqlite3/lib/contract.js.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Readable } = require('node:stream');

const contract = require('../lib/contract');
const azure = require('./azure');

// ---- fake @azure/storage-blob ------------------------------------------------

function blob404() {
  const err = new Error('BlobNotFound');
  err.statusCode = 404;
  err.code = 'BlobNotFound';
  return err;
}

class FakeBlockBlob {
  constructor(store, name) { this.store = store; this.name = name; }
  async uploadData(buffer, opts = {}) {
    const headers = opts.blobHTTPHeaders || {};
    this.store.blobs.set(this.name, {
      body: Buffer.from(buffer),
      contentType: headers.blobContentType,
      contentEncoding: headers.blobContentEncoding,
      metadata: opts.metadata || {}
    });
  }
  async download() {
    const b = this.store.blobs.get(this.name);
    if (!b) throw blob404();
    return { readableStreamBody: Readable.from([b.body]) };
  }
  async getProperties() {
    const b = this.store.blobs.get(this.name);
    if (!b) throw blob404();
    return { contentLength: b.body.length, contentType: b.contentType, metadata: b.metadata };
  }
  async deleteIfExists() {
    return { succeeded: this.store.blobs.delete(this.name) };
  }
}

class FakeContainerClient {
  constructor(store) { this.store = store; }
  getBlockBlobClient(name) { return new FakeBlockBlob(this.store, name); }
  // Yields in reverse order on purpose, so the adapter's own sort is exercised.
  async *listBlobsFlat({ prefix } = {}) {
    const names = [...this.store.blobs.keys()].filter((n) => n.startsWith(prefix || '')).sort().reverse();
    for (const name of names) yield { name, properties: { contentLength: this.store.blobs.get(name).body.length } };
  }
}

class FakeBlobService {
  constructor() { this.blobs = new Map(); }
  getContainerClient() { return new FakeContainerClient(this); }
}

const fakeBlob = { BlobServiceClient: { fromConnectionString: () => new FakeBlobService() } };

// ---- fake @azure/service-bus -------------------------------------------------

// One in-memory queue with a peek-lock model and a delivery count.
class FakeQueue {
  constructor() { this.messages = []; this.seq = 0; this.failIds = new Map(); }
}

class FakeSender {
  constructor(queue) { this.queue = queue; }
  async sendMessages(messages) {
    const list = Array.isArray(messages) ? messages : [messages];
    // A whole-call failure keyed on any message id, to exercise isolation.
    for (const m of list) {
      const failure = this.queue.failIds.get(m.messageId);
      if (failure) throw failure;
    }
    for (const m of list) {
      this.queue.seq += 1;
      this.queue.messages.push({
        _seq: this.queue.seq,
        messageId: m.messageId,
        body: m.body,
        contentType: m.contentType,
        applicationProperties: m.applicationProperties,
        sessionId: m.sessionId,
        deliveryCount: 0,     // prior deliveries; adapter reports deliveryCount + 1
        lockedUntil: 0
      });
    }
  }
  async close() {}
}

class FakeReceiver {
  constructor(queue) { this.queue = queue; }
  async receiveMessages(max, { maxWaitTimeInMs } = {}) {
    void maxWaitTimeInMs;
    const now = Date.now();
    const out = [];
    for (const m of this.queue.messages) {
      if (out.length >= max) break;
      if (m.lockedUntil > now) continue;
      m.lockedUntil = now + 30000;
      out.push(m);
    }
    return out;
  }
  async completeMessage(m) {
    this.queue.messages = this.queue.messages.filter((x) => x !== m);
  }
  async abandonMessage(m) {
    m.deliveryCount += 1;   // Service Bus counts the abandoned delivery
    m.lockedUntil = 0;      // available again immediately
  }
  async close() {}
}

class FakeServiceBus {
  constructor() { this.queue = new FakeQueue(); }
  createSender() { return new FakeSender(this.queue); }
  createReceiver() { return new FakeReceiver(this.queue); }
}

const fakeSb = { ServiceBusClient: class { createSender() {} createReceiver() {} } };

// ---- contract ----------------------------------------------------------------

contract.objectStore(test,
  () => new azure.ObjectStore({ container: 'c', prefix: 'pre', sdk: fakeBlob, client: new FakeBlobService() }),
  { name: 'Blob ObjectStore' });

contract.queue(test, () => {
  const bus = new FakeServiceBus();
  return {
    sink: new azure.QueueSink({ queueName: 'q', sdk: fakeSb, client: bus }),
    source: new azure.QueueSource({ queueName: 'q', sdk: fakeSb, client: bus, maxWaitTimeMs: 0 })
  };
}, { name: 'ServiceBus' });

const envelope = (id, kind, n, attempt) => contract.envelope(id, kind, { n }, attempt);

// ---- Blob specifics ----------------------------------------------------------

test('ObjectStore: 404 becomes ENOENT on get and null on head; connectionString builds a client', async () => {
  const store = new azure.ObjectStore({ container: 'c', sdk: fakeBlob, connectionString: 'X' });
  await assert.rejects(store.get('missing'), (err) => err.code === 'ENOENT');
  assert.equal(await store.head('missing'), null);
  await store.put('k', Buffer.from('hi'), { sha256: 'abc', contentType: 'application/json' });
  const head = await store.head('k');
  assert.equal(head.meta.sha256, 'abc');
  assert.equal(head.meta.contentType, 'application/json');
  assert.throws(() => new azure.ObjectStore({ sdk: fakeBlob, client: new FakeBlobService() }), /"container"/);
  assert.throws(() => new azure.ObjectStore({ container: 'c', sdk: fakeBlob }), /"client" or a "connectionString"/);
});

// ---- Service Bus specifics ---------------------------------------------------

test('QueueSink: messages carry messageId, applicationProperties and sessionId from the ordering key', async () => {
  const bus = new FakeServiceBus();
  const sink = new azure.QueueSink({ queueName: 'q', sdk: fakeSb, client: bus });
  const outcomes = await sink.publish([envelope('id-1', 'orders', 1)]);
  assert.deepEqual(outcomes, [{ id: 'id-1', status: 'ok' }]);
  const m = bus.queue.messages[0];
  assert.equal(m.messageId, 'id-1');
  assert.equal(m.contentType, 'application/json');
  assert.equal(m.sessionId, 'orders');                      // orderingKey === kind here
  assert.equal(m.applicationProperties['x-job-kind'], 'orders');
  assert.equal(m.applicationProperties['idempotency-key'], 'id-1');
});

test('QueueSink: a permanent send fault dead-letters just the bad message; transient retries', async () => {
  const bus = new FakeServiceBus();
  const sink = new azure.QueueSink({ queueName: 'q', sdk: fakeSb, client: bus });
  const envelopes = [envelope('a', 'k', 1), envelope('b', 'k', 2), envelope('c', 'k', 3)];
  bus.queue.failIds.set('b', Object.assign(new Error('MessageSizeExceeded'), { code: 'MessageSizeExceeded' }));
  const outcomes = await sink.publish(envelopes);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'dead', 'ok']);
  assert.equal(bus.queue.messages.length, 2, 'a and c were sent, b was not');

  const down = new FakeServiceBus();
  down.queue.failIds.set('a', Object.assign(new Error('ServerBusy'), { code: 'ServerBusy' }));
  const sink2 = new azure.QueueSink({ queueName: 'q', sdk: fakeSb, client: down });
  const out2 = await sink2.publish([envelope('a', 'k', 1)]);
  assert.equal(out2[0].status, 'retry');

  assert.equal(azure.internals.isTransient({ code: 'ServerBusy' }), true);
  assert.equal(azure.internals.isTransient({ retryable: true }), true);
  assert.equal(azure.internals.isTransient({ code: 'MessageSizeExceeded' }), false);
});

test('QueueSource: attempt reflects delivery count; ack completes; nack abandons and redelivers', async () => {
  const bus = new FakeServiceBus();
  const sink = new azure.QueueSink({ queueName: 'q', sdk: fakeSb, client: bus });
  const source = new azure.QueueSource({ queueName: 'q', sdk: fakeSb, client: bus, maxWaitTimeMs: 0 });
  await sink.publish([envelope('idem-1', 'k', 1)]);

  const [first] = await source.receive(5);
  assert.equal(first.id, 'idem-1', 'the idempotency key identifies the message');
  assert.equal(first.attributes['x-job-kind'], 'k');
  assert.equal(first.attempt, 1);
  assert.notEqual(first.ackToken, undefined);

  await source.nack([first.ackToken], 1000);
  const [again] = await source.receive(5);
  assert.equal(again.id, 'idem-1');
  assert.equal(again.attempt, 2, 'a redelivery counts as the second attempt');

  await source.ack([again.ackToken]);
  assert.deepEqual(await source.receive(5), []);
});

test('a missing SDK is reported with the package to install', () => {
  assert.throws(() => new azure.QueueSink({ queueName: 'q', sdk: undefined }), /@azure\/service-bus is not installed/);
  assert.throws(() => new azure.ObjectStore({ container: 'c', sdk: undefined }), /@azure\/storage-blob is not installed/);
});
