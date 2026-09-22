'use strict';

// Tests for lib/consumer.js: exactly-once handling of an at-least-once queue,
// retry with backoff, dead-lettering, the queue -> memory queue -> consumer round
// trip, and lifecycle.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const Database = require('../');
const { Consumer, Queue } = Database;
const { MemoryQueue } = Database.interfaces;

const until = async (predicate, ms = 4000, label = 'condition') => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 10));
  }
};

function publish(queue, id, payload, kind = 'k') {
  return queue.publish([{
    id, kind, attempt: 1,
    body: JSON.stringify({ id, kind, payload }),
    attributes: { 'idempotency-key': id, 'x-job-kind': kind, 'x-job-attempt': '1' },
    orderingKey: kind
  }]);
}

const fast = { pollMs: 10, backoffMs: 5, maxBackoffMs: 10 };

test('validates its arguments', () => {
  const db = new Database(':memory:');
  const q = new MemoryQueue();
  assert.throws(() => new Consumer({}, { source: q, handler() {} }), /Expected first argument to be a Database/);
  assert.throws(() => new Consumer(db, { source: q }), /"handler"/);
  assert.throws(() => new Consumer(db, { source: {}, handler() {} }), /receive\(\), ack\(\) and nack\(\)/);
  assert.throws(() => new Consumer(db, { source: q, handler() {}, attempts: 0 }), /"attempts"/);
  db.close();

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-'));
  const file = path.join(dir, 'ro.db');
  new Database(file).close();
  const ro = new Database(file, { readonly: true });
  assert.throws(() => new Consumer(ro, { source: q, handler() {} }), /readonly/);
  ro.close();
});

test('handles each message once inside a transaction; duplicates are acked without running', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE seen (id TEXT PRIMARY KEY, n INTEGER)');
  const insert = db.prepare('INSERT INTO seen (id, n) VALUES (?, ?)');
  const q = new MemoryQueue();
  let runs = 0;
  const consumer = new Consumer(db, Object.assign({
    source: q,
    handler(envelope, message) {
      runs += 1;
      assert.equal(message.attributes['x-job-kind'], 'k');
      insert.run(envelope.id, envelope.payload.n);
    }
  }, fast));
  const duplicates = [];
  consumer.on('duplicate', (m) => duplicates.push(m.id));

  await publish(q, 'm1', { n: 1 });
  await publish(q, 'm2', { n: 2 });
  await publish(q, 'm1', { n: 1 });      // redelivered by the queue
  await until(() => q.acked.length === 3, 4000, 'three acks');

  assert.equal(runs, 2, 'the handler ran once per distinct message');
  assert.deepEqual(duplicates, ['m1']);
  assert.deepEqual(db.prepare('SELECT id, n FROM seen ORDER BY id').all(), [{ id: 'm1', n: 1 }, { id: 'm2', n: 2 }]);
  assert.equal(consumer.dead().length, 0);
  await consumer.drain();
  db.close();
});

test('a throwing handler rolls back, is retried with backoff, then dead-lettered with its body', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id TEXT PRIMARY KEY)');
  const q = new MemoryQueue();
  let attempts = 0;
  const consumer = new Consumer(db, Object.assign({
    source: q,
    attempts: 3,
    handler(envelope) {
      attempts += 1;
      db.prepare('INSERT INTO t (id) VALUES (?)').run(envelope.id);   // must be rolled back
      throw new Error(`boom ${attempts}`);
    }
  }, fast));
  const retries = [];
  const dead = [];
  consumer.on('retry', (m, err, delay) => retries.push([err.message, delay]));
  consumer.on('dead', (m, err) => dead.push(err.message));

  await publish(q, 'bad', { x: 1 });
  await until(() => dead.length === 1, 5000, 'dead letter');

  assert.equal(attempts, 3);
  assert.equal(retries.length, 2);
  assert.ok(retries.every(([, delay]) => Number.isInteger(delay) && delay >= 0));
  assert.deepEqual(dead, ['boom 3']);
  assert.equal(db.prepare('SELECT count(*) n FROM t').get().n, 0, 'handler writes were rolled back');
  assert.equal(q.acked.length, 1, 'a dead-lettered message is acked so the queue stops redelivering');

  const [row] = consumer.dead();
  assert.equal(row.id, 'bad');
  assert.equal(row.kind, 'k');
  assert.deepEqual(row.payload.payload, { x: 1 });
  assert.equal(row.attempts, 3);
  assert.match(row.last_error, /boom 3/);

  // Redelivery of a dead message is a duplicate: acked, not rerun.
  await publish(q, 'bad', { x: 1 });
  await until(() => q.acked.length === 2, 2000, 'dup ack');
  assert.equal(attempts, 3);
  await consumer.drain();
  db.close();
});

test('a non-JSON body or an async handler is dead-lettered at once', async () => {
  const db = new Database(':memory:');
  const q = new MemoryQueue();
  const consumer = new Consumer(db, Object.assign({ source: q, async handler() {} }, fast));
  const dead = [];
  consumer.on('dead', (m, err) => dead.push([m.id, err.message]));
  await q.publish([{ id: 'raw', kind: 'k', attempt: 1, body: 'not json', attributes: {}, orderingKey: 'k' }]);
  await publish(q, 'async', {});
  await until(() => dead.length === 2, 3000, 'two dead');
  dead.sort();
  assert.match(dead[0][1], /must be synchronous/);
  assert.match(dead[1][1], /not JSON/);
  assert.equal(consumer.dead().length, 2);
  await consumer.drain();
  db.close();
});

test('queue -> memory queue -> consumer round trip carries the envelope and the idempotency key', async () => {
  const producer = new Database(':memory:');
  const receiver = new Database(':memory:');
  receiver.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)');
  const q = new MemoryQueue();

  const jobs = new Queue(producer, { routes: { 'order.placed': q }, pollMs: 10 });
  const consumer = new Consumer(receiver, Object.assign({
    source: q,
    handler(envelope, message) {
      assert.equal(message.id, message.attributes['idempotency-key']);
      assert.equal(envelope.kind, 'order.placed');
      receiver.prepare('INSERT INTO orders (id, status) VALUES (?, ?)').run(envelope.payload.orderId, 'placed');
    }
  }, fast));

  for (let i = 1; i <= 20; i += 1) jobs.enqueue('order.placed', { orderId: i });
  await jobs.drain();
  await until(() => receiver.prepare('SELECT count(*) n FROM orders').get().n === 20, 5000, '20 orders');
  assert.equal(consumer.dead().length, 0);
  await consumer.drain();
  producer.close();
  receiver.close();
});

test('a failing receive() emits stalled and keeps going; stop() ends the loop; purge() shrinks the window', async () => {
  const db = new Database(':memory:');
  const q = new MemoryQueue();
  let fail = 2;
  const source = {
    receive: (n) => (fail-- > 0 ? Promise.reject(new Error('queue down')) : q.receive(n)),
    ack: (t) => q.ack(t),
    nack: (t, d) => q.nack(t, d)
  };
  const consumer = new Consumer(db, Object.assign({ source, handler() {} }, fast));
  const stalled = [];
  consumer.on('stalled', (err) => stalled.push(err.message));
  await publish(q, 'x', {});
  await until(() => q.acked.length === 1, 3000, 'ack after recovery');
  assert.deepEqual(stalled, ['queue down', 'queue down']);
  assert.equal(consumer.running, true);
  await consumer.drain();
  assert.equal(consumer.running, false);

  assert.equal(consumer.purge(60000), 0, 'nothing old enough to purge yet');
  assert.equal(consumer.purge(0), 1, 'the handled marker is purged');
  assert.throws(() => consumer.purge(-1), /non-negative/);
  db.close();
});

test('two consumers on one file never run the handler twice for the same message', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-'));
  const file = path.join(dir, 'shared.db');
  const a = new Database(file);
  a.pragma('journal_mode = WAL');
  a.exec('CREATE TABLE hits (id TEXT, by TEXT)');
  const b = new Database(file);
  const q = new MemoryQueue();

  // The same message is handed to both consumers at once, as a queue with
  // an expired visibility timeout would do.
  const message = { id: 'once', body: JSON.stringify({ id: 'once', kind: 'k', payload: {} }), attributes: {}, ackToken: 't' };
  const fakeSource = (received) => ({ receive: async () => received.splice(0), ack: async () => {}, nack: async () => {} });
  const ia = new Consumer(a, { source: fakeSource([message]), handler: () => a.prepare('INSERT INTO hits VALUES (?, ?)').run('once', 'a'), autoStart: false, pollMs: 10 });
  const ib = new Consumer(b, { source: fakeSource([message]), handler: () => b.prepare('INSERT INTO hits VALUES (?, ?)').run('once', 'b'), autoStart: false, pollMs: 10 });
  ia.start(); ib.start();
  await until(() => a.prepare('SELECT count(*) n FROM hits').get().n >= 1, 3000, 'one hit');
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(a.prepare('SELECT count(*) n FROM hits').get().n, 1, 'exactly one consumer ran the handler');
  await ia.drain(); await ib.drain();
  a.close(); b.close();
});

test('a locked database is transient: the message is nacked, no attempt is spent, and the loop survives', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-'));
  const file = path.join(dir, 'locked.db');
  const a = new Database(file, { timeout: 0 });
  a.pragma('journal_mode = WAL');
  a.exec('CREATE TABLE hits (id)');
  const q = new MemoryQueue();
  await publish(q, 'x', {});
  const events = [];
  const consumer = new Consumer(a, Object.assign({
    source: q, attempts: 2, autoStart: false,
    handler() { a.prepare('INSERT INTO hits VALUES (1)').run(); }
  }, fast));
  for (const name of ['retry', 'dead', 'handled']) consumer.on(name, () => events.push(name));
  consumer.on('stalled', (err) => events.push(`stalled:${err.code}`));

  const b = new Database(file);
  b.exec('BEGIN IMMEDIATE');
  consumer.start();
  await new Promise((r) => setTimeout(r, 150));
  assert.ok(events.length > 0 && events.every((e) => e === 'stalled:SQLITE_BUSY'), events.join(','));
  assert.equal(consumer.running, true);
  assert.equal(consumer.dead().length, 0, 'lock contention must not dead-letter');
  assert.equal(a.prepare(`SELECT count(*) n FROM ${Consumer.TABLE}`).get().n, 0, 'no attempt recorded');

  b.exec('COMMIT');
  await until(() => events.includes('handled'), 3000, 'handled after unlock');
  assert.equal(a.prepare('SELECT count(*) n FROM hits').get().n, 1);
  assert.equal(q.acked.length, 1);
  await consumer.drain();
  a.close(); b.close();
});
