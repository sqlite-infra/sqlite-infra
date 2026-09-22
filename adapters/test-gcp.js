'use strict';

// Tests for the Google Cloud adapters against fake @google-cloud SDKs. They
// prove the interface behaviour and outcome mapping without talking to GCP,
// running the shared contract suite from better-sqlite3/lib/contract.js plus
// GCS/Pub/Sub specifics. Mirrors adapters/test.js.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Readable } = require('node:stream');

const contract = require('../lib/contract');
const gcp = require('./gcp');

// ---- fake Google Cloud Storage --------------------------------------------

function notFound404() {
  const err = new Error('Not Found');
  err.code = 404;
  return err;
}

class FakeStorage {
  constructor() { this.objects = new Map(); this.calls = []; }
  bucket(name) {
    const self = this;
    return {
      file(key) {
        return {
          async save(body, opts) {
            self.calls.push({ op: 'save', key, opts });
            self.objects.set(key, {
              body: Buffer.from(body),
              contentType: opts && opts.contentType,
              contentEncoding: opts && opts.metadata && opts.metadata.contentEncoding,
              metadata: (opts && opts.metadata && opts.metadata.metadata) || {}
            });
          },
          async exists() { return [self.objects.has(key)]; },
          async getMetadata() {
            const o = self.objects.get(key);
            if (!o) throw notFound404();
            return [{ size: String(o.body.length), contentType: o.contentType, contentEncoding: o.contentEncoding, metadata: o.metadata }];
          },
          createReadStream() {
            const o = self.objects.get(key);
            return Readable.from([o ? o.body : Buffer.alloc(0)]);
          },
          async delete(opts) {
            self.calls.push({ op: 'delete', key });
            if (!self.objects.has(key) && !(opts && opts.ignoreNotFound)) throw notFound404();
            self.objects.delete(key);
          }
        };
      },
      async getFiles({ prefix } = {}) {
        const files = [...self.objects.entries()]
          .filter(([k]) => k.startsWith(prefix || ''))
          .map(([k, o]) => ({ name: k, metadata: { size: String(o.body.length) } }));
        return [files];
      }
    };
  }
}

const fakeStorageSdk = { Storage: FakeStorage };

// ---- fake Pub/Sub ----------------------------------------------------------

// One in-memory queue shared by a publisher fake and a subscriber fake, with a
// visibility model: a pulled message is invisible until acked or its ack
// deadline lapses; modifyAckDeadline(0) makes it immediately visible again.
class FakeQueue {
  constructor() { this.messages = []; this.seq = 0; this.tok = 0; }
  push(msg) {
    this.seq += 1;
    this.messages.push({ messageId: `mid-${this.seq}`, data: msg.data, attributes: msg.attributes || {}, orderingKey: msg.orderingKey, visibleAt: 0, deliveries: 0, ackId: null });
    return `mid-${this.seq}`;
  }
  pull(max) {
    const now = Date.now();
    const out = [];
    for (const m of this.messages) {
      if (out.length >= max) break;
      if (m.visibleAt > now) continue;
      m.deliveries += 1;
      m.ackId = `ack-${++this.tok}`;
      m.visibleAt = now + 30000;
      out.push({ ackId: m.ackId, deliveryAttempt: m.deliveries, message: { messageId: m.messageId, data: m.data, attributes: m.attributes } });
    }
    return out;
  }
  ack(ackIds) { this.messages = this.messages.filter((m) => !ackIds.includes(m.ackId)); }
  modify(ackIds, seconds) {
    const at = Date.now() + seconds * 1000;
    for (const m of this.messages) if (ackIds.includes(m.ackId)) m.visibleAt = at;
  }
}

class FakePubSub {
  constructor(queue) { this.queue = queue; this.calls = []; }
  topic(name, opts) {
    const self = this;
    return {
      async publishMessage(msg) {
        self.calls.push({ name, opts, msg });
        return self.queue.push(msg);
      }
    };
  }
}
const fakePubsubSdk = (queue) => ({ PubSub: class { constructor() { return new FakePubSub(queue); } } });

class FakeSubscriberClient {
  constructor(queue) { this.queue = queue; this.calls = []; }
  async pull({ subscription, maxMessages }) {
    this.calls.push({ op: 'pull', subscription, maxMessages });
    return [{ receivedMessages: this.queue.pull(maxMessages) }];
  }
  async acknowledge({ ackIds }) { this.calls.push({ op: 'ack', ackIds }); this.queue.ack(ackIds); }
  async modifyAckDeadline({ ackIds, ackDeadlineSeconds }) {
    this.calls.push({ op: 'modify', ackIds, ackDeadlineSeconds });
    this.queue.modify(ackIds, ackDeadlineSeconds);
  }
}
const fakeSubscriberSdk = (queue) => ({ v1: { SubscriberClient: class { constructor() { return new FakeSubscriberClient(queue); } } } });

// ---- contract --------------------------------------------------------------

contract.objectStore(
  test,
  () => new gcp.ObjectStore({ bucket: 'b', prefix: 'pre', sdk: fakeStorageSdk, client: new FakeStorage() }),
  { name: 'GCS ObjectStore' }
);

contract.queue(
  test,
  () => {
    const queue = new FakeQueue();
    return {
      sink: new gcp.QueueSink({ topic: 't', sdk: {}, client: new FakePubSub(queue) }),
      source: new gcp.QueueSource({ subscription: 's', sdk: {}, client: new FakeSubscriberClient(queue) })
    };
  },
  { name: 'Pub/Sub' }
);

const envelope = (id, kind, n, attempt) => contract.envelope(id, kind, { n }, attempt);

// ---- GCS specifics ---------------------------------------------------------

test('GCS ObjectStore: put records sha256/contentType/encoding; get and head miss cleanly', async () => {
  const client = new FakeStorage();
  const store = new gcp.ObjectStore({ bucket: 'b', prefix: '/pre/', sdk: fakeStorageSdk, client });
  await store.put('a/b.gz', Buffer.from('x'), { sha256: 'deadbeef', contentType: 'application/gzip', contentEncoding: 'gzip', size: 1 });
  const saved = client.objects.get('pre/a/b.gz');
  assert.equal(saved.contentType, 'application/gzip');
  assert.equal(saved.contentEncoding, 'gzip');
  assert.deepEqual(saved.metadata, { sha256: 'deadbeef' });
  const head = await store.head('a/b.gz');
  assert.equal(head.size, 1);
  assert.equal(head.meta.sha256, 'deadbeef');
  assert.equal(head.meta.contentType, 'application/gzip');
  assert.equal(await store.head('missing'), null);
  await assert.rejects(store.get('missing'), (err) => err.code === 'ENOENT');
  assert.throws(() => new gcp.ObjectStore({ sdk: fakeStorageSdk, client }), /"bucket"/);
});

test('GCS ObjectStore: list strips the prefix and sorts ascending', async () => {
  const client = new FakeStorage();
  const store = new gcp.ObjectStore({ bucket: 'b', prefix: 'p', sdk: fakeStorageSdk, client });
  for (const n of [3, 1, 2]) await store.put(`cs/${n}`, Buffer.from('x'.repeat(n)));
  await store.put('other/z', Buffer.from('zz'));
  assert.deepEqual(await store.list('cs/'), [{ key: 'cs/1', size: 1 }, { key: 'cs/2', size: 2 }, { key: 'cs/3', size: 3 }]);
});

// ---- Pub/Sub specifics -----------------------------------------------------

test('Pub/Sub QueueSink: ordering key defaults to kind; attributes carry the idempotency key', async () => {
  const queue = new FakeQueue();
  const client = new FakePubSub(queue);
  const sink = new gcp.QueueSink({ topic: 't', sdk: {}, client });
  const outcomes = await sink.publish([envelope('id-1', 'orders', 1), envelope('id-2', 'orders', 2)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'ok']);
  assert.deepEqual(outcomes.map((o) => o.id), ['id-1', 'id-2']);
  assert.equal(client.calls[0].msg.orderingKey, 'orders');
  assert.equal(client.calls[0].opts.messageOrdering, true);
  assert.equal(client.calls[0].msg.attributes['idempotency-key'], 'id-1');

  const source = new gcp.QueueSource({ subscription: 's', sdk: {}, client: new FakeSubscriberClient(queue) });
  const got = await source.receive(10);
  assert.equal(got.length, 2);
  assert.equal(got[0].id, 'id-1');
  assert.equal(got[0].attributes['x-job-kind'], 'orders');
  assert.equal(got[0].body, envelope('id-1', 'orders', 1).body);
});

test('Pub/Sub QueueSink: transient errors retry, permanent errors dead-letter', async () => {
  const transient = { async publishMessage() { const e = new Error('backend'); e.code = 14; throw e; } };   // UNAVAILABLE
  const permanent = { async publishMessage() { const e = new Error('bad'); e.code = 3; e.name = 'INVALID_ARGUMENT'; throw e; } };
  const sinkT = new gcp.QueueSink({ topic: 't', sdk: {}, client: { topic: () => transient } });
  const sinkP = new gcp.QueueSink({ topic: 't', sdk: {}, client: { topic: () => permanent } });
  assert.equal((await sinkT.publish([envelope('a', 'k', 1)]))[0].status, 'retry');
  assert.equal((await sinkP.publish([envelope('a', 'k', 1)]))[0].status, 'dead');

  assert.equal(gcp.internals.isTransient({ code: 8 }), true);          // RESOURCE_EXHAUSTED
  assert.equal(gcp.internals.isTransient({ code: 3 }), false);         // INVALID_ARGUMENT
  assert.equal(gcp.internals.isTransient({ message: 'DEADLINE_EXCEEDED' }), true);
  assert.equal(gcp.internals.isTransient(new Error('nope')), false);
});

test('Pub/Sub QueueSource: nack with a delay holds the message that long', async () => {
  const queue = new FakeQueue();
  const source = new gcp.QueueSource({ subscription: 's', sdk: {}, client: new FakeSubscriberClient(queue) });
  await new gcp.QueueSink({ topic: 't', sdk: {}, client: new FakePubSub(queue) }).publish([envelope('a', 'k', 1)]);
  const [m] = await source.receive(10);
  await source.nack([m.ackToken], 60000);   // 60s backoff -> not immediately visible
  assert.deepEqual(await source.receive(10), []);
  await source.nack([m.ackToken], 0);        // now redeliver
  const [again] = await source.receive(10);
  assert.equal(again.id, 'a');
  await source.ack([again.ackToken]);
  assert.deepEqual(await source.receive(10), []);
  await source.ack([]);   // empty is a no-op
});

test('a missing SDK is reported with the package to install', () => {
  assert.throws(() => new gcp.ObjectStore({ bucket: 'b', sdk: undefined }), /@google-cloud\/storage is not installed/);
  assert.throws(() => new gcp.QueueSink({ topic: 't', sdk: undefined }), /@google-cloud\/pubsub is not installed/);
  assert.throws(() => new gcp.QueueSource({ subscription: 's', sdk: undefined }), /@google-cloud\/pubsub is not installed/);
});
