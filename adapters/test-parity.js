'use strict';

// Tests for the Azure parity adapters (Event Hubs + Queue Storage) against fake
// SDK modules and clients. They prove the inputs handed to each SDK call and the
// outcome mapping; they do not talk to Azure. The Storage Queue fake models a
// peek/visibility queue closely enough to run the contract suite from
// better-sqlite3/lib/contract.js.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const contract = require('../lib/contract');
const parity = require('./parity');

// ---- fake @azure/event-hubs --------------------------------------------------

class FakeEventHubProducer {
  constructor() { this.batches = []; this.failIds = new Map(); }
  async sendBatch(events, opts = {}) {
    for (const ev of events) {
      const idk = ev.properties && ev.properties['idempotency-key'];
      const failure = this.failIds.get(idk);
      if (failure) throw failure;
    }
    this.batches.push({ partitionKey: opts.partitionKey, events });
  }
  async close() {}
}

const fakeEh = { EventHubProducerClient: class { constructor() {} async sendBatch() {} async close() {} } };

// ---- fake @azure/storage-queue -----------------------------------------------

// One in-memory queue with a visibility model, pop receipts and dequeue counts.
class FakeQueueClient {
  constructor() { this.messages = []; this.seq = 0; this.failTexts = new Map(); }
  async sendMessage(text, opts = {}) {
    const failure = this.failTexts.get(text);
    if (failure) throw failure;
    this.seq += 1;
    const id = `m${this.seq}`;
    this.messages.push({
      messageId: id, messageText: text, popReceipt: `pr-${id}-0`,
      dequeueCount: 0, rc: 0,
      visibleAt: opts.visibilityTimeout ? Date.now() + opts.visibilityTimeout * 1000 : 0
    });
    return { messageId: id };
  }
  async receiveMessages({ numberOfMessages = 1, visibilityTimeout } = {}) {
    const now = Date.now();
    const out = [];
    for (const m of this.messages) {
      if (out.length >= numberOfMessages) break;
      if (m.visibleAt > now) continue;
      m.dequeueCount += 1;
      m.rc += 1;
      m.popReceipt = `pr-${m.messageId}-${m.rc}`;
      m.visibleAt = now + (visibilityTimeout !== undefined ? visibilityTimeout * 1000 : 30000);
      out.push({ messageId: m.messageId, messageText: m.messageText, popReceipt: m.popReceipt, dequeueCount: m.dequeueCount });
    }
    return { receivedMessageItems: out };
  }
  async deleteMessage(messageId, popReceipt) {
    const m = this.messages.find((x) => x.messageId === messageId);
    if (!m) throw Object.assign(new Error('MessageNotFound'), { statusCode: 404 });
    if (m.popReceipt !== popReceipt) throw Object.assign(new Error('PopReceiptMismatch'), { statusCode: 400 });
    this.messages = this.messages.filter((x) => x !== m);
  }
  async updateMessage(messageId, popReceipt, text, visibilityTimeout) {
    const m = this.messages.find((x) => x.messageId === messageId);
    if (!m) throw Object.assign(new Error('MessageNotFound'), { statusCode: 404 });
    m.rc += 1;
    m.popReceipt = `pr-${messageId}-${m.rc}`;
    if (text !== undefined) m.messageText = text;
    m.visibleAt = Date.now() + (visibilityTimeout ? visibilityTimeout * 1000 : 0);
    return { popReceipt: m.popReceipt };
  }
}

class FakeQueueService {
  constructor(queueClient) { this.queueClient = queueClient; }
  getQueueClient() { return this.queueClient; }
}

const fakeSq = { QueueServiceClient: { fromConnectionString: () => new FakeQueueService(new FakeQueueClient()) } };

const envelope = (id, kind, n, attempt) => contract.envelope(id, kind, { n }, attempt);

// ---- contract: Storage Queue -------------------------------------------------

contract.queue(test, () => {
  const queueClient = new FakeQueueClient();
  const service = new FakeQueueService(queueClient);
  return {
    sink: new parity.azure.StorageQueueSink({ queueName: 'q', sdk: fakeSq, client: service }),
    source: new parity.azure.StorageQueueSource({ queueName: 'q', sdk: fakeSq, client: service})
  };
}, { name: 'Azure Storage Queue' });

// ---- Storage Queue specifics -------------------------------------------------

test('StorageQueue: attributes and id survive the text wrapper; attempt is the dequeue count', async () => {
  const queueClient = new FakeQueueClient();
  const service = new FakeQueueService(queueClient);
  const sink = new parity.azure.StorageQueueSink({ queueName: 'q', sdk: fakeSq, client: service });
  const source = new parity.azure.StorageQueueSource({ queueName: 'q', sdk: fakeSq, client: service});

  await sink.publish([envelope('idem-1', 'orders', 1)]);
  const [m] = await source.receive(5);
  assert.equal(m.id, 'idem-1');
  assert.equal(m.attributes['x-job-kind'], 'orders');
  assert.equal(m.attributes['idempotency-key'], 'idem-1');
  assert.equal(m.body, envelope('idem-1', 'orders', 1).body);
  assert.equal(m.attempt, 1);
  assert.deepEqual(Object.keys(m.ackToken).sort(), ['messageId', 'popReceipt', 'text']);

  await source.nack([m.ackToken], 0);
  const [again] = await source.receive(5);
  assert.equal(again.attempt, 2, 'a redelivery is the second dequeue');
  await source.ack([again.ackToken]);
  assert.deepEqual((await source.receive(5)), []);
});

test('StorageQueue: send failures map to retry (transient) or dead (permanent)', async () => {
  const queueClient = new FakeQueueClient();
  const service = new FakeQueueService(queueClient);
  const sink = new parity.azure.StorageQueueSink({ queueName: 'q', sdk: fakeSq, client: service });
  const bad = envelope('b', 'k', 2);
  const busy = envelope('c', 'k', 3);
  queueClient.failTexts.set(parity.internals.wrap(bad), Object.assign(new Error('QueueDisabled'), { code: 'QueueDisabled' }));
  queueClient.failTexts.set(parity.internals.wrap(busy), Object.assign(new Error('ServerBusy'), { code: 'ServerBusy' }));
  const outcomes = await sink.publish([envelope('a', 'k', 1), bad, busy]);
  assert.deepEqual(outcomes.map((o) => o.id), ['a', 'b', 'c'], 'input order');
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'dead', 'retry']);
});

test('StorageQueue: needs a queueName and a client or connectionString; missing SDK is reported', () => {
  assert.throws(() => new parity.azure.StorageQueueSink({ sdk: fakeSq, client: new FakeQueueService(new FakeQueueClient()) }), /"queueName"/);
  assert.throws(() => new parity.azure.StorageQueueSink({ queueName: 'q', sdk: fakeSq }), /"client" or a "connectionString"/);
  assert.throws(() => new parity.azure.StorageQueueSink({ queueName: 'q', sdk: undefined }), /@azure\/storage-queue is not installed/);
});

// ---- Event Hubs --------------------------------------------------------------

test('EventHubSink: one batch per ordering key, body and properties carried', async () => {
  const producer = new FakeEventHubProducer();
  const sink = new parity.azure.EventHubSink({ sdk: fakeEh, client: producer });
  const outcomes = await sink.publish([
    envelope('a', 'odd', 1), envelope('b', 'even', 2), envelope('c', 'odd', 3)
  ]);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'ok', 'ok']);
  assert.deepEqual(outcomes.map((o) => o.id), ['a', 'b', 'c']);
  // Two partition keys → two batches; 'odd' carries a and c.
  assert.equal(producer.batches.length, 2);
  const odd = producer.batches.find((b) => b.partitionKey === 'odd');
  assert.deepEqual(odd.events.map((e) => e.properties['idempotency-key']), ['a', 'c']);
  assert.equal(odd.events[0].body, envelope('a', 'odd', 1).body);
});

test('EventHubSink: a permanent fault isolates to the bad event; a transient one retries the group', async () => {
  const producer = new FakeEventHubProducer();
  producer.failIds.set('b', Object.assign(new Error('MessageSizeExceeded'), { code: 'MessageSizeExceeded' }));
  const sink = new parity.azure.EventHubSink({ sdk: fakeEh, client: producer });
  // a, b, c share ordering key 'k' → one group; b is a permanent fault.
  const outcomes = await sink.publish([envelope('a', 'k', 1), envelope('b', 'k', 2), envelope('c', 'k', 3)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'dead', 'ok']);

  const busy = new FakeEventHubProducer();
  busy.failIds.set('a', Object.assign(new Error('ServerBusy'), { code: 'ServerBusy' }));
  const sink2 = new parity.azure.EventHubSink({ sdk: fakeEh, client: busy });
  const out2 = await sink2.publish([envelope('a', 'k', 1), envelope('d', 'k', 4)]);
  assert.deepEqual(out2.map((o) => o.status), ['retry', 'retry'], 'transient fault retries the whole group');
});

test('EventHubSink: needs a client or connectionString; missing SDK is reported', () => {
  assert.throws(() => new parity.azure.EventHubSink({ sdk: fakeEh }), /"client" or a "connectionString"/);
  assert.throws(() => new parity.azure.EventHubSink({ sdk: undefined, connectionString: 'X' }), /@azure\/event-hubs is not installed/);
});
