'use strict';

// better-sqlite-adapters (Azure parity): two more Azure-backed MessageSink /
// MessageSource implementations, to bring Azure closer to the AWS surface.
// Merged into the `azure` namespace by adapters/index.js.
//
//   EventHubSink        a MessageSink on Azure Event Hubs (@azure/event-hubs),
//                       the streaming counterpart to AWS Kinesis
//   StorageQueueSink    a MessageSink on Azure Queue Storage (@azure/storage-queue)
//   StorageQueueSource  a MessageSource on Azure Queue Storage
//
// Same structure and outcome mapping as adapters/azure.js (transient -> retry,
// sender fault -> dead, batch isolation on a sender fault). isTransient is
// reused from adapters/azure.js so classification stays identical across the
// Azure adapters.
//
// Queue Storage messages carry only text, so the envelope's attributes and id
// are wrapped into the message body as JSON and unwrapped on receive; this is
// what lets the interface's attributes survive the round trip (the contract
// suite checks it).

const { outcomes: OUTCOME, queue: queueNames, fallbacks: FALLBACKS } = require('../lib/dictionary');
const { isTransient, TRANSIENT } = require('./azure').internals;

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

// ---- Azure Event Hubs -----------------------------------------------------

// A batch shares one partition key, so envelopes are grouped by ordering key
// and each group is sent as one batch; a group that fails as a sender fault is
// re-sent one event at a time so a single bad event cannot fail its neighbours.
class EventHubSink {
  constructor(options = {}) {
    this.sdk = loadSdk('@azure/event-hubs', options.sdk);
    if (options.client) {
      this.client = options.client;
    } else {
      if (typeof options.connectionString !== 'string' || !options.connectionString) {
        throw new TypeError('EventHubSink needs a "client" or a "connectionString"');
      }
      this.client = new this.sdk.EventHubProducerClient(options.connectionString, options.eventHubName);
    }
  }

  async publish(envelopes) {
    const groups = new Map();          // partitionKey -> envelopes (input order kept)
    for (const e of envelopes) {
      const key = e.orderingKey || e.kind || FALLBACKS.name;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(e);
    }
    const outcomes = new Map();
    const send = async (partitionKey, group) => {
      try {
        await this.client.sendBatch(
          group.map((e) => ({ body: e.body, properties: Object.assign({}, e.attributes) })),
          { partitionKey }
        );
        for (const e of group) outcomes.set(e.id, { id: e.id, status: OUTCOME.ok });
      } catch (err) {
        if (!isTransient(err) && group.length > 1) {
          for (const e of group) await send(partitionKey, [e]);
          return;
        }
        const status = isTransient(err) ? OUTCOME.retry : OUTCOME.dead;
        for (const e of group) outcomes.set(e.id, { id: e.id, status, error: err });
      }
    };
    for (const [key, group] of groups) await send(key, group);
    return envelopes.map((e) => outcomes.get(e.id) || { id: e.id, status: OUTCOME.retry, error: 'no result for entry' });
  }
}

// ---- Azure Queue Storage --------------------------------------------------

function queueClientFor(sdk, options, what) {
  let client = options.client;
  if (!client) {
    if (typeof options.connectionString !== 'string' || !options.connectionString) {
      throw new TypeError(`${what} needs a "client" or a "connectionString"`);
    }
    client = sdk.QueueServiceClient.fromConnectionString(options.connectionString);
  }
  // Accept either a QueueServiceClient (resolve the named queue) or a QueueClient.
  return typeof client.getQueueClient === 'function' ? client.getQueueClient(options.queueName) : client;
}

// Queue Storage has no per-message attributes, so the whole envelope shape is
// carried in the message text and reconstructed on receive.
function wrap(envelope) {
  return JSON.stringify({
    body: envelope.body,
    attributes: envelope.attributes || {},
    id: envelope.id,
    orderingKey: envelope.orderingKey
  });
}

function unwrap(text) {
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === 'object' && 'body' in parsed) return parsed;
  } catch { /* not a wrapped message; treat the raw text as the body */ }
  return { body: text, attributes: {} };
}

class StorageQueueSink {
  constructor(options = {}) {
    if (typeof options.queueName !== 'string' || !options.queueName) throw new TypeError('StorageQueueSink needs a "queueName"');
    this.sdk = loadSdk('@azure/storage-queue', options.sdk);
    this.queueClient = queueClientFor(this.sdk, options, 'StorageQueueSink');
    this.visibilityTimeoutSec = options.visibilityTimeoutSec;
  }

  publish(envelopes) {
    const send = this.visibilityTimeoutSec ? { visibilityTimeout: this.visibilityTimeoutSec } : {};
    return Promise.all(envelopes.map(async (e) => {
      try {
        await this.queueClient.sendMessage(wrap(e), send);
        return { id: e.id, status: OUTCOME.ok };
      } catch (err) {
        return { id: e.id, status: isTransient(err) ? OUTCOME.retry : OUTCOME.dead, error: err };
      }
    }));
  }
}

class StorageQueueSource {
  constructor(options = {}) {
    if (typeof options.queueName !== 'string' || !options.queueName) throw new TypeError('StorageQueueSource needs a "queueName"');
    this.sdk = loadSdk('@azure/storage-queue', options.sdk);
    this.queueClient = queueClientFor(this.sdk, options, 'StorageQueueSource');
    this.visibilityTimeoutSec = options.visibilityTimeoutSec;
  }

  async receive(max) {
    const opts = { numberOfMessages: Math.max(1, max || 1) };
    if (this.visibilityTimeoutSec !== undefined) opts.visibilityTimeout = this.visibilityTimeoutSec;
    const res = await this.queueClient.receiveMessages(opts);
    const items = (res && res.receivedMessageItems) || [];
    return items.map((m) => {
      const wrapped = unwrap(m.messageText);
      return {
        id: wrapped.id || m.messageId,
        body: wrapped.body,
        attributes: wrapped.attributes || {},
        // popReceipt is required (with messageId) to delete or extend the lease;
        // the text is kept so nack can rewrite the message on updateMessage.
        ackToken: { messageId: m.messageId, popReceipt: m.popReceipt, text: m.messageText },
        attempt: m.dequeueCount
      };
    });
  }

  async ack(tokens) {
    for (const t of tokens) await this.queueClient.deleteMessage(t.messageId, t.popReceipt);
  }

  async nack(tokens, delayMs = 0) {
    // updateMessage returns a fresh popReceipt and sets the visibility timeout,
    // so a positive delay holds the message that long before redelivery.
    const seconds = Math.max(0, Math.ceil((delayMs || 0) / 1000));
    for (const t of tokens) await this.queueClient.updateMessage(t.messageId, t.popReceipt, t.text, seconds);
  }
}

module.exports = {
  azure: { EventHubSink, StorageQueueSink, StorageQueueSource },
  internals: { isTransient, TRANSIENT, wrap, unwrap }
};
