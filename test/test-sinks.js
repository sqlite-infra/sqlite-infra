'use strict';

// Tests for lib/sinks.js — the keepalive delivery backends, focusing on the
// StatsD/DogStatsD metrics sink added for metric collectors, and the URL
// scheme dispatch in createSink().

const test = require('node:test');
const assert = require('node:assert/strict');
const dgram = require('node:dgram');

const { createSink, createStatsdSink, createNoopSink, createMemorySink } = require('../lib/sinks');
const dictionary = require('../lib/dictionary');

const M = dictionary.metrics;

function udpServer() {
  return new Promise((resolve) => {
    const packets = [];
    const server = dgram.createSocket('udp4');
    server.on('message', (m) => packets.push(m.toString()));
    server.bind(0, '127.0.0.1', () => resolve({ server, port: server.address().port, packets }));
  });
}

const waitFor = async (predicate, ms = 2000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
};

test('createSink dispatches by URL scheme', () => {
  assert.equal(createSink('noop://x').send, createNoopSink().send);   // both noop
  assert.ok(createSink('memory://x').sent, 'memory sink has a sent array');
  assert.ok(typeof createSink('statsd://127.0.0.1:8125').send === 'function');
  assert.ok(typeof createSink('udp://127.0.0.1:8125').send === 'function');
  assert.ok(typeof createSink('https://collector/x').send === 'function');
  assert.equal(createSink('ftp://nope'), null, 'unknown scheme is null');
  assert.equal(createSink('not a url'), null, 'unparseable is null');
});

test('memory sink collects events', () => {
  const r = createMemorySink();
  r.send([{ type: 'open' }, { type: 'close' }]);
  assert.deepEqual(r.sent.map((e) => e.type), ['open', 'close']);
});

test('statsd sink emits DogStatsD lines for keepalive events', async () => {
  const { server, port, packets } = await udpServer();
  const r = createStatsdSink(new URL(`statsd://127.0.0.1:${port}`));
  r.send([
    { type: 'heartbeat', service: 'svc', host: 'h1', instance: 'i1', connections: 3, rss: 12345678, errors: { SQLITE_BUSY: 2 }, dropped: 1 },
    { type: 'slow', service: 'svc', host: 'h1', instance: 'i1', ms: 42 }
  ]);
  await waitFor(() => packets.length > 0);
  const text = packets.join('\n');
  const p = M.prefix;
  assert.match(text, new RegExp(`${p}\\.${M.suffixes.connections}:3\\|g\\|#${M.tagKeys.service}:svc,${M.tagKeys.host}:h1,${M.tagKeys.instance}:i1`));
  assert.match(text, new RegExp(`${p}\\.${M.suffixes.rss}:12345678\\|g`));
  assert.match(text, new RegExp(`${p}\\.${M.suffixes.errors}:2\\|c\\|.*${M.tagKeys.code}:SQLITE_BUSY`));
  assert.match(text, new RegExp(`${p}\\.${M.suffixes.dropped}:1\\|c`));
  assert.match(text, new RegExp(`${p}\\.${M.suffixes.slow}:42\\|ms`));
  assert.match(text, new RegExp(`${p}\\.${M.suffixes.lifecycle}:1\\|c\\|.*${M.tagKeys.event}:heartbeat`));
  r.close();
  server.close();
});

test('statsd sink sanitizes tag values and honours a custom prefix', async () => {
  const { server, port, packets } = await udpServer();
  const r = createStatsdSink(new URL(`statsd://127.0.0.1:${port}?prefix=myapp`));
  r.send([{ type: 'open', service: 'sv:c|x', host: 'h,1', instance: 'i#1', connections: 1 }]);
  await waitFor(() => packets.length > 0);
  const text = packets.join('\n');
  assert.match(text, /myapp\.connections:1\|g/);
  assert.match(text, /service:sv_c_x,host:h_1,instance:i_1/, 'reserved characters replaced');
  r.close();
  server.close();
});

test('statsd sink never throws and stays silent after close()', () => {
  const r = createStatsdSink(new URL('statsd://127.0.0.1:1'));   // nothing listening
  assert.doesNotThrow(() => r.send([{ type: 'open', service: 's', host: 'h', instance: 'i', connections: 1 }]));
  r.close();
  assert.doesNotThrow(() => r.send([{ type: 'open' }]));   // no-op after close
});
