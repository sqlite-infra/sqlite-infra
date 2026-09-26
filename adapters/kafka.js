'use strict';

// Apache Kafka integration for better-sqlite's cloud interfaces, via kafkajs.
//
//   kafka.Sink   a MessageSink on a Kafka topic (producer.send)
//
// Sink only, on purpose. Kafka's consumer model is a long-lived push
// subscription with offset commits and rebalancing, which does not map onto
// the pull-based MessageSource contract (receive / ack / nack) the queue and
// consumer expect. This mirrors the AWS Kinesis and EventBridge adapters,
// which are also sink-only. A host that wants to consume from Kafka runs a
// kafkajs consumer directly and calls Database.Consumer with its own source.
//
// Outcome mapping follows the shape contract: a transient failure
// (broker unavailable, timeout, leader election, a retriable error) is
// 'retry'; a permanent one (bad config, message too large, authorization) is
// 'dead'. When a whole batch send throws a permanent error, it is retried one
// message at a time so a single bad message cannot dead-letter its neighbours.

const { outcomes: OUTCOME, queue: queueNames, fallbacks: FALLBACKS } = require('../lib/dictionary');

function loadSdk(name, injected) {
  if (injected) return injected;
  try {
    return require(name);
  } catch (err) {
    const wrapped = new Error(`better-sqlite-adapters: ${name} is not installed. Add it as a dependency or pass { sdk } to the constructor.`);
    wrapped.cause = err;
    throw wrapped;
  }
}

// Errors a retry may fix: transport, timeouts, leadership/coordination churn,
// and anything kafkajs flags retriable. Everything else is treated as the
// sender's fault (dead).
const TRANSIENT = /Timeout|Timed[ _]?Out|Connection|Network|BrokerNotFound|BROKER_NOT_AVAILABLE|LeaderNotAvailable|LEADER_NOT_AVAILABLE|NotEnoughReplicas|NOT_ENOUGH_REPLICAS|NotLeaderForPartition|NOT_LEADER|Coordinator|COORDINATOR|NumberOfRetriesExceeded|Retriable|Rebalance|UnknownTopicOrPartition|UNKNOWN_TOPIC_OR_PARTITION/i;

function isTransient(err) {
  if (!err) return false;
  if (err.retriable === true) return true;
  const s = `${err.name || ''} ${err.type || ''} ${err.code || ''} ${err.message || ''}`;
  return TRANSIENT.test(s);
}

class Sink {
  constructor(options = {}) {
    if (typeof options.topic !== 'string' || !options.topic) throw new TypeError('kafka.Sink needs a "topic"');
    this.sdk = loadSdk('kafkajs', options.sdk);
    const client = options.client || new this.sdk.Kafka(options.clientConfig || { brokers: options.brokers });
    this.topic = options.topic;
    this.producer = options.producer || client.producer();
    this._connected = null;   // memoized connect() promise, so connect runs once
  }

  _connect() {
    if (!this._connected) {
      this._connected = Promise.resolve()
        .then(() => this.producer.connect())
        .catch((err) => { this._connected = null; throw err; });
    }
    return this._connected;
  }

  async publish(envelopes) {
    try {
      await this._connect();
    } catch (err) {
      // Could not connect: nothing was sent, so every envelope may be retried.
      const status = isTransient(err) ? OUTCOME.retry : OUTCOME.retry;
      return envelopes.map((e) => ({ id: e.id, status, error: err }));
    }
    return this._send(envelopes);
  }

  async _send(batch) {
    try {
      await this.producer.send({
        topic: this.topic,
        messages: batch.map((e) => ({
          key: e.orderingKey || e.kind || FALLBACKS.name,
          value: e.body,
          headers: e.attributes || {}
        }))
      });
      return batch.map((e) => ({ id: e.id, status: OUTCOME.ok }));
    } catch (err) {
      // Permanent error on a multi-message batch: isolate to find the culprit.
      if (!isTransient(err) && batch.length > 1) {
        const out = [];
        for (const e of batch) out.push(...await this._send([e]));
        return out;
      }
      const status = isTransient(err) ? OUTCOME.retry : OUTCOME.dead;
      return batch.map((e) => ({ id: e.id, status, error: err }));
    }
  }
}

// Best-effort producer shutdown; safe to call more than once.
async function disconnect(sink) {
  if (sink && sink.producer && typeof sink.producer.disconnect === 'function') {
    await sink.producer.disconnect();
    sink._connected = null;
  }
}

module.exports = {
  kafka: { Sink },
  internals: { isTransient, TRANSIENT, disconnect, attributeNames: queueNames.attributes }
};
