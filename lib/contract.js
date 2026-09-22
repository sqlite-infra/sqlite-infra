'use strict';

// Contract suite for the cloud interface shapes in adapters/util.js. An adapter package runs
// these against its own implementation (backed by a fake client or an
// emulator) to prove it behaves the way the sync, consumer and queue
// assume. The in-memory implementations pass them too (test-interfaces.js).
//
//   const contract = require('better-sqlite3/lib/contract');
//   contract.objectStore(test, () => new ObjectStore({ bucket: 'b', client: fake }));
//   contract.queue(test, () => { const q = new MemoryQueue(); return { sink: q, source: q }; });
//
// `test` is node:test's test function (or anything with the same shape).

const assert = require('assert/strict');
const { Readable } = require('stream');

async function drain(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function objectStore(test, factory, options = {}) {
  const name = options.name || 'ObjectStore';

  test(`${name}: put then get round-trips bytes from a Buffer and from a stream`, async () => {
    const store = await factory();
    const small = Buffer.from('hello, bucket');
    await store.put('contract/a.txt', small, { size: small.length, contentType: 'text/plain' });
    assert.deepEqual(await drain(await store.get('contract/a.txt')), small);

    const big = Buffer.alloc(3 * 1024 * 1024 + 17, 7);
    await store.put('contract/big.bin', Readable.from([big.subarray(0, 1024 * 1024), big.subarray(1024 * 1024)]), { size: big.length });
    assert.deepEqual(await drain(await store.get('contract/big.bin')), big);
  });

  test(`${name}: head reports size and preserves sha256 metadata; null for a missing key`, async () => {
    const store = await factory();
    const body = Buffer.from('with meta');
    await store.put('contract/meta', body, { size: body.length, sha256: 'abc123', contentType: 'application/octet-stream' });
    const head = await store.head('contract/meta');
    assert.ok(head, 'head() must return an object for an existing key');
    assert.equal(head.size, body.length);
    assert.equal(head.meta.sha256, 'abc123');
    assert.equal(await store.head('contract/absent'), null);
  });

  test(`${name}: get of a missing key rejects with code ENOENT`, async () => {
    const store = await factory();
    await assert.rejects(store.get('contract/missing'), (err) => err.code === 'ENOENT');
  });

  test(`${name}: list returns only the prefix, sorted ascending by key, with sizes`, async () => {
    const store = await factory();
    await store.put('contract/list/000000000002.x', Buffer.from('bb'), { size: 2 });
    await store.put('contract/list/000000000010.x', Buffer.from('cccc'), { size: 4 });
    await store.put('contract/list/000000000001.x', Buffer.from('a'), { size: 1 });
    await store.put('contract/other/000000000001.x', Buffer.from('zz'), { size: 2 });
    const listed = await store.list('contract/list/');
    assert.deepEqual(listed.map((o) => o.key), [
      'contract/list/000000000001.x', 'contract/list/000000000002.x', 'contract/list/000000000010.x'
    ]);
    assert.deepEqual(listed.map((o) => o.size), [1, 2, 4]);
  });

  test(`${name}: put overwrites and delete removes`, async () => {
    const store = await factory();
    await store.put('contract/over', Buffer.from('one'), { size: 3 });
    await store.put('contract/over', Buffer.from('two!'), { size: 4 });
    assert.equal((await drain(await store.get('contract/over'))).toString(), 'two!');
    await store.delete('contract/over');
    assert.equal(await store.head('contract/over'), null);
    await store.delete('contract/over');   // idempotent
  });
}

function envelope(id, kind, payload, attempt = 1) {
  return {
    id,
    kind,
    attempt,
    body: JSON.stringify({ id, kind, payload, attempt }),
    attributes: { 'idempotency-key': id, 'x-job-kind': kind, 'x-job-attempt': String(attempt) },
    orderingKey: kind
  };
}

function queue(test, factory, options = {}) {
  const name = options.name || 'Queue';
  // Some sources may take a moment to expose a published message.
  const receiveAll = async (source, n, max = 10) => {
    const out = [];
    for (let i = 0; i < 50 && out.length < n; i += 1) {
      const got = await source.receive(max);
      out.push(...got);
      if (out.length < n) await new Promise((r) => setTimeout(r, 10));
    }
    return out;
  };

  test(`${name}: publish returns one 'ok' outcome per envelope, in order`, async () => {
    const { sink } = await factory();
    const envelopes = [envelope('c:1', 'a', { n: 1 }), envelope('c:2', 'b', { n: 2 }), envelope('c:3', 'a', { n: 3 })];
    const outcomes = await sink.publish(envelopes);
    assert.equal(outcomes.length, 3);
    assert.deepEqual(outcomes.map((o) => o.id), ['c:1', 'c:2', 'c:3']);
    for (const o of outcomes) assert.equal(o.status, 'ok');
  });

  test(`${name}: receive delivers body and attributes; ack removes; nack redelivers`, async () => {
    const { sink, source } = await factory();
    const sent = envelope('c:10', 'k', { hello: 'world' }, 2);
    await sink.publish([sent]);

    const [first] = await receiveAll(source, 1);
    assert.ok(first, 'a published message must be receivable');
    assert.equal(first.body, sent.body);
    assert.equal(first.attributes['x-job-kind'], 'k');
    assert.equal(first.attributes['idempotency-key'], 'c:10');
    assert.equal(typeof first.id, 'string');
    assert.notEqual(first.ackToken, undefined);

    await source.nack([first.ackToken], 0);
    const [again] = await receiveAll(source, 1);
    assert.ok(again, 'a nacked message must come back');
    assert.equal(again.body, sent.body);

    await source.ack([again.ackToken]);
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(await source.receive(10), []);
  });

  test(`${name}: receive honours the batch limit`, async () => {
    const { sink, source } = await factory();
    await sink.publish([1, 2, 3, 4, 5].map((n) => envelope(`c:b${n}`, 'k', { n })));
    const got = await source.receive(2);
    assert.ok(got.length <= 2, `asked for 2, got ${got.length}`);
    const rest = await receiveAll(source, 5 - got.length);
    assert.equal(got.length + rest.length, 5);
    await source.ack([...got, ...rest].map((m) => m.ackToken));
  });
}

module.exports = { objectStore, queue, envelope, drain };
