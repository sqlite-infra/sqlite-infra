'use strict';

// Tests for lib/presence.js: the generic reporting agent must never read the
// environment. The region stamped on events comes from the consumer (deps or
// configure({ region })) and is otherwise a placeholder.

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { createAgent, REGION_PLACEHOLDER } = require('../lib/presence');

function memorySink() {
  return { sent: [], send(events) { this.sent.push(...events); }, close() {} };
}

function make(deps = {}) {
  return createAgent(Object.assign({
    BATCH_MS: 5, DEFAULT_INTERVAL: 1000, MIN_INTERVAL: 1000, DEFAULT_QUEUE: 100,
    defaultService: () => 'test', libVersion: () => 'test'
  }, deps));
}

test('presence.js source never reads process.env', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'lib', 'presence.js'), 'utf8');
  const code = src.split('\n').filter((l) => !l.trim().startsWith('//')).join('\n');
  assert.ok(!/process\.env/.test(code), 'lib/presence.js must not touch process.env');
});

test('region defaults to the placeholder, ignoring AWS_REGION in the environment', () => {
  const saved = process.env.AWS_REGION;
  process.env.AWS_REGION = 'eu-west-1';
  try {
    const sink = memorySink();
    const agent = make();
    agent.configure({ sink });
    agent.report('ping', {});
    agent.flush();
    assert.equal(sink.sent.length, 1);
    assert.equal(sink.sent[0].region, REGION_PLACEHOLDER);
  } finally {
    if (saved === undefined) delete process.env.AWS_REGION; else process.env.AWS_REGION = saved;
  }
});

test('region passed by the consumer via deps is stamped on events', () => {
  const sink = memorySink();
  const agent = make({ region: 'consumer-region' });
  agent.configure({ sink });
  agent.report('ping', {});
  agent.flush();
  assert.equal(sink.sent[0].region, 'consumer-region');
});

test('configure({ region }) overrides the deps region', () => {
  const sink = memorySink();
  const agent = make({ region: 'deps-region' });
  agent.configure({ sink, region: 'configured-region' });
  agent.report('ping', {});
  agent.flush();
  assert.equal(sink.sent[0].region, 'configured-region');
  assert.equal(agent.config.region, 'configured-region');
});

test('ensureConfigured uses the consumer resolveConfig and takes no environment', () => {
  const sink = memorySink();
  let calls = 0;
  const agent = make({ resolveConfig: (...args) => { calls += 1; assert.equal(args.length, 0); return { sink, region: 'resolved' }; } });
  assert.equal(agent.ensureConfigured(), true);
  assert.equal(calls, 1);
  agent.report('ping', {});
  agent.flush();
  assert.equal(sink.sent[0].region, 'resolved');
  assert.equal(agent.ensureConfigured(), true, 'second call is a cached hit');
  assert.equal(calls, 1);
});

test('ensureConfigured stays dark without resolveConfig', () => {
  const agent = make();
  assert.equal(agent.ensureConfigured(), false);
  assert.equal(agent.config, null);
});
