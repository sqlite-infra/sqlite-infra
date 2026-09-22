'use strict';

// Tests for lib/interfaces.js and lib/contract.js

const { test } = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../');
const contract = require('../lib/contract');
const { MemoryObjectStore, MemoryQueue, HttpSink, assertObjectStore, assertMessageSink, assertMessageSource } = Database.interfaces;

// drain() delivers what is due and stops; jobs waiting out a backoff are not
// due, so tests that exercise retries wait for the queue to empty instead.
const until = async (predicate, ms = 4000, label = 'condition') => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 10));
  }
};

contract.objectStore(test, () => new MemoryObjectStore(), { name: 'MemoryObjectStore' });
contract.queue(test, () => { const q = new MemoryQueue(); return { sink: q, source: q }; }, { name: 'MemoryQueue' });

test('interface validators name what is missing', () => {
  assert.throws(() => assertObjectStore({ put() {} }, 'store'), /ObjectStore with put\(\), get\(\), head\(\), list\(\) and delete\(\)/);
  assert.throws(() => assertMessageSink({}), /publish\(envelopes\)/);
  assert.throws(() => assertMessageSource({ receive() {} }), /receive\(\), ack\(\) and nack\(\)/);
  assert.equal(assertObjectStore(new MemoryObjectStore()) instanceof MemoryObjectStore, true);
});

test('a queue routes jobs to a MessageSink, batching one publish per route per tick', async () => {
  const db = new Database(':memory:');
  const queue = new MemoryQueue();
  const calls = [];
  const spy = { publish: (envelopes) => { calls.push(envelopes.length); return queue.publish(envelopes); } };
  const jobs = new Database.Queue(db, { routes: { audit: spy, '*': { sink: queue } }, pollMs: 20, concurrency: 16, autoStart: false });

  db.transaction(() => {
    for (let i = 0; i < 5; i += 1) jobs.enqueue('audit', { i });
    jobs.enqueue('other', { x: 1 });
  })();
  assert.equal(jobs.pending(), 6);

  const delivered = [];
  jobs.on('delivered', (job) => delivered.push(job.kind));
  jobs.start();
  await jobs.drain();

  assert.equal(jobs.pending(), 0);
  assert.equal(delivered.length, 6);
  assert.deepEqual(calls, [5], 'all five audit jobs claimed in one tick go out in one publish()');
  assert.equal(queue.published.length, 6);
  const audit = queue.published.filter((e) => e.kind === 'audit');
  assert.deepEqual(audit.map((e) => JSON.parse(e.body).payload.i), [0, 1, 2, 3, 4]);
  assert.match(audit[0].id, /^(memory|[0-9a-f]{16}):\d+$/);
  assert.equal(audit[0].attributes['idempotency-key'], audit[0].id);
  assert.equal(audit[0].orderingKey, 'audit');
  db.close();
});

test('sink outcomes drive retry and dead-letter; a rejected publish() retries the whole batch', async () => {
  const db = new Database(':memory:');
  const queue = new MemoryQueue();
  const jobs = new Database.Queue(db, { routes: queue, pollMs: 10, backoffMs: 10, maxBackoffMs: 20, attempts: 3, autoStart: false });
  const events = { retry: [], dead: [], delivered: [] };
  for (const name of Object.keys(events)) jobs.on(name, (job) => events[name].push(job.id));

  queue.failNext(1, 'retry', 'throttled');
  queue.failNext(1, 'dead', 'message too large');
  const a = jobs.enqueue('k', { a: 1 });
  const b = jobs.enqueue('k', { b: 1 });
  jobs.start();
  await until(() => jobs.pending() === 0, 4000, 'first queue empty');
  jobs.stop();

  assert.deepEqual(events.dead, [b]);
  assert.deepEqual(events.delivered, [a]);
  assert.equal(events.retry.length, 1);
  assert.match(jobs.dead()[0].last_error, /too large/);

  // A sink whose publish() rejects: every job in the batch is retried.
  let failures = 2;
  const flaky = { publish: (envelopes) => (failures-- > 0 ? Promise.reject(new Error('network down')) : queue.publish(envelopes)) };
  const jobs2 = new Database.Queue(db, { routes: flaky, pollMs: 10, backoffMs: 10, maxBackoffMs: 20, autoStart: false });
  const retries = [];
  jobs2.on('retry', (_job, err) => retries.push(err.message));
  jobs2.enqueue('k', {});
  jobs2.enqueue('k', {});
  jobs2.start();
  await until(() => jobs2.pending() === 0, 4000, 'flaky queue empty');
  jobs2.stop();
  assert.ok(retries.length >= 2 && retries.every((m) => m === 'network down'));

  // A sink that answers only for the first envelope of a batch: the other
  // job is retried (and delivered alone next tick), not lost.
  const forgetful = { publish: async (envelopes) => [{ id: envelopes[0].id, status: 'ok' }] };
  const jobs3 = new Database.Queue(db, { routes: forgetful, pollMs: 10, backoffMs: 10, maxBackoffMs: 20, autoStart: false });
  const retries3 = [];
  const delivered3 = [];
  jobs3.on('retry', (_job, err) => retries3.push(err.message));
  jobs3.on('delivered', (job) => delivered3.push(job.id));
  jobs3.enqueue('k', { n: 1 }); jobs3.enqueue('k', { n: 2 });
  jobs3.start();
  await until(() => jobs3.pending() === 0, 4000, 'forgetful queue empty');
  jobs3.stop();
  assert.deepEqual(retries3, ['sink returned no outcome']);
  assert.equal(delivered3.length, 2);
  db.close();
});
