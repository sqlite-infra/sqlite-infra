'use strict';

// Tests for the Redis Streams adapter against a fake `redis` client that models
// a stream + consumer group (XADD / XREADGROUP / XACK / XAUTOCLAIM / XCLAIM /
// XGROUPCREATE). No live Redis — same fake-client standard as the other
// adapters.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const contract = require('../lib/contract');
const redis = require('./redis');

// ---- fake redis (node-redis v4 shapes) -----------------------------------

class FakeRedis {
  constructor() {
    this.isOpen = true;
    this.streams = new Map();   // key -> { entries:[{id,fields}], seq }
    this.groups = new Map();    // `${key}\0${group}` -> { pending:Map(id->{consumer,deliveryTime}), read:Set }
    this.calls = [];
  }
  async connect() { this.isOpen = true; }
  _stream(key) { if (!this.streams.has(key)) this.streams.set(key, { entries: [], seq: 0 }); return this.streams.get(key); }
  _group(key, group) { return this.groups.get(`${key}\0${group}`); }

  async xAdd(key, id, message, options) {
    this.calls.push({ cmd: 'xAdd', key, message, options });
    const s = this._stream(key);
    s.seq += 1;
    const entryId = `${Date.now()}-${s.seq}`;
    s.entries.push({ id: entryId, fields: Object.assign({}, message) });
    if (options && options.TRIM && options.TRIM.threshold && s.entries.length > options.TRIM.threshold) {
      s.entries.splice(0, s.entries.length - options.TRIM.threshold);
    }
    return entryId;
  }
  async xGroupCreate(key, group, id, options) {
    if (options && options.MKSTREAM) this._stream(key);
    const k = `${key}\0${group}`;
    if (this.groups.has(k)) throw new Error('BUSYGROUP Consumer Group name already exists');
    this.groups.set(k, { pending: new Map(), read: new Set() });
    return 'OK';
  }
  async xReadGroup(group, consumer, streams, options) {
    const arr = Array.isArray(streams) ? streams : [streams];
    const count = (options && options.COUNT) || Infinity;
    const out = [];
    for (const { key } of arr) {
      const s = this._stream(key);
      const g = this._group(key, group);
      if (!g) continue;
      const messages = [];
      for (const entry of s.entries) {
        if (messages.length >= count) break;
        if (g.read.has(entry.id)) continue;   // '>' returns only never-delivered entries
        g.read.add(entry.id);
        g.pending.set(entry.id, { consumer, deliveryTime: Date.now() });
        messages.push({ id: entry.id, message: Object.assign({}, entry.fields) });
      }
      if (messages.length) out.push({ name: key, messages });
    }
    return out.length ? out : null;
  }
  async xAck(key, group, ids) {
    const list = Array.isArray(ids) ? ids : [ids];
    const g = this._group(key, group);
    if (!g) return 0;
    let n = 0;
    for (const id of list) if (g.pending.delete(id)) n += 1;
    return n;
  }
  async xAutoClaim(key, group, consumer, minIdle, start, options) {
    const g = this._group(key, group);
    if (!g) return { nextCursor: '0', messages: [] };
    const s = this._stream(key);
    const now = Date.now();
    const count = (options && options.COUNT) || Infinity;
    const messages = [];
    for (const [id, p] of g.pending) {
      if (messages.length >= count) break;
      if (now - p.deliveryTime >= minIdle) {
        p.consumer = consumer;
        p.deliveryTime = now;
        const entry = s.entries.find((e) => e.id === id);
        messages.push(entry ? { id, message: Object.assign({}, entry.fields) } : null);
      }
    }
    return { nextCursor: '0', messages };
  }
  async xClaim(key, group, consumer, minIdle, ids, options) {
    const list = Array.isArray(ids) ? ids : [ids];
    const g = this._group(key, group);
    if (!g) return [];
    const now = Date.now();
    const claimed = [];
    for (const id of list) {
      const p = g.pending.get(id);
      if (!p) continue;
      p.consumer = consumer;
      p.deliveryTime = now - ((options && options.IDLE) || 0);   // set idle to IDLE
      claimed.push(id);
    }
    return claimed;   // JUSTID
  }
}

const fakeRedis = { createClient: () => new FakeRedis() };
const envelope = (id, kind, n) => contract.envelope(id, kind, { n });

// ---- contract ------------------------------------------------------------

contract.queue(test, () => {
  const client = new FakeRedis();
  return {
    sink: new redis.redis.StreamSink({ stream: 's', client, sdk: fakeRedis }),
    source: new redis.redis.StreamSource({ stream: 's', group: 'g', consumer: 'c1', client, sdk: fakeRedis, blockMs: 0, minIdleMs: 30000 })
  };
}, { name: 'Redis Streams' });

// ---- specifics -----------------------------------------------------------

test('XADD maps envelope fields and receive unmaps id and attributes', async () => {
  const client = new FakeRedis();
  const sink = new redis.redis.StreamSink({ stream: 's', client, sdk: fakeRedis });
  const source = new redis.redis.StreamSource({ stream: 's', group: 'g', consumer: 'c1', client, sdk: fakeRedis });
  await sink.publish([envelope('idem-9', 'k', 1)]);
  const add = client.calls.find((c) => c.cmd === 'xAdd');
  assert.equal(add.message.id, 'idem-9');
  assert.equal(add.message.kind, 'k');
  assert.equal(add.message.orderingKey, 'k');
  assert.equal(typeof add.message.body, 'string');
  assert.equal(JSON.parse(add.message.attrs)['x-job-kind'], 'k');

  const [m] = await source.receive(10);
  assert.equal(m.id, 'idem-9');
  assert.equal(m.attributes['idempotency-key'], 'idem-9');
  assert.equal(m.attributes['x-job-kind'], 'k');
  assert.ok(/^\d+-\d+$/.test(m.ackToken), 'ackToken is the stream entry id');
});

test('ack removes from pending; a nacked message is reclaimed on a later receive', async () => {
  const client = new FakeRedis();
  const sink = new redis.redis.StreamSink({ stream: 's', client, sdk: fakeRedis });
  const source = new redis.redis.StreamSource({ stream: 's', group: 'g', consumer: 'c1', client, sdk: fakeRedis, minIdleMs: 30000 });
  await sink.publish([envelope('a', 'k', 1), envelope('b', 'k', 2)]);

  const first = await source.receive(10);
  assert.equal(first.length, 2);
  const g = client._group('s', 'g');
  assert.equal(g.pending.size, 2, 'both delivered are pending');

  await source.ack([first[0].ackToken]);
  assert.equal(g.pending.size, 1, 'ack removed one from pending');

  // The remaining un-acked one is not re-read by '>' but comes back via reclaim.
  await source.nack([first[1].ackToken], 0);
  const again = await source.receive(10);
  assert.deepEqual(again.map((m) => m.ackToken), [first[1].ackToken]);
  await source.ack([again[0].ackToken]);
  assert.equal((await source.receive(10)).length, 0);
});

test('nack delay maps to reclaim idle: not eligible before the delay elapses', async () => {
  const client = new FakeRedis();
  const sink = new redis.redis.StreamSink({ stream: 's', client, sdk: fakeRedis });
  const source = new redis.redis.StreamSource({ stream: 's', group: 'g', consumer: 'c1', client, sdk: fakeRedis, minIdleMs: 30000 });
  await sink.publish([envelope('a', 'k', 1)]);
  const [m] = await source.receive(10);
  await source.nack([m.ackToken], 60000);   // wants ~60s before reclaim -> idle set below threshold
  assert.equal((await source.receive(10)).length, 0, 'not yet reclaimable');
});

test('the consumer group is auto-created and BUSYGROUP is ignored', async () => {
  const client = new FakeRedis();
  const s1 = new redis.redis.StreamSource({ stream: 's', group: 'g', consumer: 'c1', client, sdk: fakeRedis });
  const s2 = new redis.redis.StreamSource({ stream: 's', group: 'g', consumer: 'c2', client, sdk: fakeRedis });
  await s1.receive(1);          // creates the group
  await assert.doesNotReject(s2.receive(1));   // second create hits BUSYGROUP, swallowed
  assert.ok(client._group('s', 'g'));
});

test('publish outcome mapping: transient -> retry, permanent -> dead', async () => {
  const client = new FakeRedis();
  client.xAdd = async (key, id, message) => {
    if (message.id === 't') throw Object.assign(new Error('Connection is closed'), { code: 'ECONNRESET' });
    if (message.id === 'd') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    return '1-1';
  };
  const sink = new redis.redis.StreamSink({ stream: 's', client, sdk: fakeRedis });
  const out = await sink.publish([envelope('ok', 'k', 1), envelope('t', 'k', 2), envelope('d', 'k', 3)]);
  assert.deepEqual(out.map((o) => o.status), ['ok', 'retry', 'dead']);
});

test('validation and a missing SDK are reported', () => {
  assert.throws(() => new redis.redis.StreamSink({ client: new FakeRedis(), sdk: fakeRedis }), /"stream"/);
  assert.throws(() => new redis.redis.StreamSource({ stream: 's', client: new FakeRedis(), sdk: fakeRedis }), /"group"/);
  assert.throws(() => new redis.redis.StreamSink({ stream: 's', sdk: undefined }), /redis is not installed/);
});
