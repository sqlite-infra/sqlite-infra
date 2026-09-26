'use strict';

// Tests for the Kafka sink adapter against a fake kafkajs. No live Kafka; the
// fake records producer.connect()/send() calls, same standard as the other
// adapter tests.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const contract = require('../lib/contract');
const kafka = require('./kafka');

function fakeKafka(producer) {
  return { Kafka: class { constructor(config) { this.config = config; } producer() { return producer; } } };
}

function makeProducer(behaviour = {}) {
  return {
    connects: 0,
    sends: [],
    async connect() { this.connects += 1; },
    async disconnect() { this.disconnected = true; },
    async send(payload) {
      this.sends.push(payload);
      if (behaviour.throwFor && behaviour.throwFor(payload)) throw behaviour.error;
      return [{ topicName: payload.topic, partition: 0 }];
    }
  };
}

const envelope = (id, kind, n) => contract.envelope(id, kind, { n });

test('publish sends one Kafka message per envelope with key/value/headers and topic', async () => {
  const producer = makeProducer();
  const sink = new kafka.kafka.Sink({ topic: 'events', sdk: fakeKafka(producer), client: undefined, producer });
  const envelopes = [envelope('a', 'k1', 1), envelope('b', 'k2', 2)];
  const outcomes = await sink.publish(envelopes);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'ok']);
  assert.deepEqual(outcomes.map((o) => o.id), ['a', 'b']);
  assert.equal(producer.sends.length, 1);
  const sent = producer.sends[0];
  assert.equal(sent.topic, 'events');
  assert.deepEqual(sent.messages.map((m) => m.key), ['k1', 'k2']);
  assert.equal(sent.messages[0].value, envelope('a', 'k1', 1).body);
  assert.equal(sent.messages[0].headers['x-job-kind'], 'k1');
  assert.equal(sent.messages[0].headers['idempotency-key'], 'a');
});

test('key falls back to kind then the default when orderingKey is absent', async () => {
  const producer = makeProducer();
  const sink = new kafka.kafka.Sink({ topic: 't', sdk: fakeKafka(producer), producer });
  await sink.publish([{ id: 'x', kind: '', attempt: 1, body: '{}', attributes: {}, orderingKey: '' }]);
  assert.equal(producer.sends[0].messages[0].key, 'default');
});

test('producer connects once across multiple publishes', async () => {
  const producer = makeProducer();
  const sink = new kafka.kafka.Sink({ topic: 't', sdk: fakeKafka(producer), producer });
  await sink.publish([envelope('a', 'k', 1)]);
  await sink.publish([envelope('b', 'k', 2)]);
  await Promise.all([sink.publish([envelope('c', 'k', 3)]), sink.publish([envelope('d', 'k', 4)])]);
  assert.equal(producer.connects, 1);
});

test('a transient send error retries the whole batch', async () => {
  const error = Object.assign(new Error('The broker is not available'), { name: 'KafkaJSBrokerNotFound', retriable: true });
  const producer = makeProducer({ throwFor: () => true, error });
  const sink = new kafka.kafka.Sink({ topic: 't', sdk: fakeKafka(producer), producer });
  const outcomes = await sink.publish([envelope('a', 'k', 1), envelope('b', 'k', 2)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['retry', 'retry']);
  assert.equal(outcomes[0].error, error);
});

test('a permanent send error isolates the batch to the bad message (dead)', async () => {
  // Fails only for the message whose payload carries n:2; permanent (not retriable).
  const error = Object.assign(new Error('Message too large'), { type: 'MESSAGE_TOO_LARGE' });
  const producer = makeProducer({
    throwFor: (payload) => payload.messages.some((m) => m.value.includes('"n":2')),
    error
  });
  const sink = new kafka.kafka.Sink({ topic: 't', sdk: fakeKafka(producer), producer });
  const outcomes = await sink.publish([envelope('a', 'k', 1), envelope('b', 'k', 2), envelope('c', 'k', 3)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'dead', 'ok']);
  assert.equal(outcomes[1].error, error);
  // one batch send, then three isolated single sends
  assert.equal(producer.sends.length, 4);
});

test('isTransient classifies retriable vs permanent', () => {
  assert.equal(kafka.internals.isTransient({ retriable: true }), true);
  assert.equal(kafka.internals.isTransient({ type: 'LEADER_NOT_AVAILABLE' }), true);
  assert.equal(kafka.internals.isTransient(new Error('Request timed out')), true);
  assert.equal(kafka.internals.isTransient({ type: 'TOPIC_AUTHORIZATION_FAILED' }), false);
  assert.equal(kafka.internals.isTransient(null), false);
});

test('disconnect() closes the producer', async () => {
  const producer = makeProducer();
  const sink = new kafka.kafka.Sink({ topic: 't', sdk: fakeKafka(producer), producer });
  await sink.publish([envelope('a', 'k', 1)]);
  await kafka.internals.disconnect(sink);
  assert.equal(producer.disconnected, true);
});

test('a missing SDK is reported with the package to install', () => {
  assert.throws(() => new kafka.kafka.Sink({ topic: 't', sdk: undefined }), /kafkajs is not installed/);
});

test('a missing topic throws', () => {
  assert.throws(() => new kafka.kafka.Sink({ sdk: fakeKafka(makeProducer()) }), /"topic"/);
});
