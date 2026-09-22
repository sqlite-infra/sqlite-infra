'use strict';

// better-sqlite-adapters: AWS-backed implementations of better-sqlite's cloud
// interfaces (see better-sqlite3/lib/interfaces.js for the shapes and the contract).
//
//   ObjectStore   an ObjectStore on S3 (single PUT or multipart, SSE passthrough)
//   QueueSink     a MessageSink on SQS (SendMessageBatch, FIFO fields when the URL ends in .fifo)
//   TopicSink     a MessageSink on SNS (PublishBatch)
//   EventBusSink  a MessageSink on EventBridge (PutEvents)
//   StreamSink    a MessageSink on Kinesis Data Streams (PutRecords)
//   QueueSource   a MessageSource on SQS (ReceiveMessage / DeleteMessageBatch / ChangeMessageVisibilityBatch)
//
// Outcome mapping follows the interfaces contract: a per-entry failure the
// service marks as the sender's fault (bad request, too large) is 'dead';
// throttling, internal errors and transport failures are 'retry'. When a
// whole batch call throws a client-side validation error, the batch is
// republished one envelope at a time so a single bad message cannot
// dead-letter its neighbours.

const { Readable } = require('stream');
const { outcomes: OUTCOME, store: storeNames, queue: queueNames, fallbacks: FALLBACKS } = require('../lib/dictionary');

const MiB = 1024 * 1024;
const S3_MIN_PART = 5 * MiB;
const SQS_BATCH = 10;
const SNS_BATCH = 10;
const EVENTBRIDGE_BATCH = 10;
const KINESIS_BATCH = 500;
const SQS_MAX_VISIBILITY_S = 12 * 60 * 60;

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

function notFound(key) {
  const err = new Error(`No such object: ${key}`);
  err.code = storeNames.errors.notFound;
  err.key = key;
  return err;
}

function is404(err) {
  const status = err && err.$metadata && err.$metadata.httpStatusCode;
  return status === 404 || (err && (err.name === 'NoSuchKey' || err.name === 'NotFound'));
}

// SDK v3 marks errors with $fault: 'client' | 'server'. A client fault that
// is not throttling is one no retry will fix.
const TRANSIENT = /Throttl|TooManyRequests|RequestLimitExceeded|ServiceUnavailable|Timeout|InternalError|InternalFailure|KMS\.?Throttling|ProvisionedThroughputExceeded|RequestTimeout|SlowDown/i;

function isSenderFault(err) {
  if (!err) return false;
  const name = String(err.name || err.Code || err.code || '');
  if (TRANSIENT.test(name)) return false;
  return err.$fault === 'client';
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function collect(body) {
  if (Buffer.isBuffer(body)) return body;
  if (typeof body === 'string') return Buffer.from(body);
  if (body instanceof Uint8Array) return Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  const chunks = [];
  for await (const c of body) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
  return Buffer.concat(chunks);
}

function toReadable(body) {
  if (!body) return Readable.from([]);
  if (typeof body.pipe === 'function') return body;
  if (typeof body[Symbol.asyncIterator] === 'function') return Readable.from(body);
  if (typeof body.transformToByteArray === 'function') {
    return Readable.from((async function* gen() { yield Buffer.from(await body.transformToByteArray()); })());
  }
  return Readable.from([Buffer.from(body)]);
}

// Yields Buffers of exactly `size` bytes (except the last) from any body.
async function* parts(body, size) {
  let pending = [];
  let pendingBytes = 0;
  const source = Buffer.isBuffer(body) || typeof body === 'string' || body instanceof Uint8Array ? [Buffer.from(body)] : body;
  for await (const raw of source) {
    let c = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
    while (pendingBytes + c.length >= size) {
      const take = size - pendingBytes;
      pending.push(c.subarray(0, take));
      yield Buffer.concat(pending);
      pending = [];
      pendingBytes = 0;
      c = c.subarray(take);
    }
    if (c.length) { pending.push(c); pendingBytes += c.length; }
  }
  if (pendingBytes) yield Buffer.concat(pending);
}

// Message attributes in the SQS/SNS shape.
function toMessageAttributes(attributes) {
  const out = {};
  for (const [name, value] of Object.entries(attributes || {})) {
    if (value === undefined || value === null) continue;
    out[name] = { DataType: 'String', StringValue: String(value) };
  }
  return Object.keys(out).length ? out : undefined;
}

function fromMessageAttributes(attributes) {
  const out = {};
  for (const [name, attr] of Object.entries(attributes || {})) {
    if (attr && attr.StringValue !== undefined) out[name] = attr.StringValue;
  }
  return out;
}

// Runs `send(batch)` for each chunk, isolating a thrown client-side
// validation error down to the single envelope that caused it.
async function publishInChunks(envelopes, size, send) {
  const outcomes = new Map();
  const run = async (batch) => {
    let results;
    try {
      results = await send(batch);
    } catch (err) {
      if (isSenderFault(err) && batch.length > 1) {
        for (const envelope of batch) await run([envelope]);
        return;
      }
      const status = isSenderFault(err) ? OUTCOME.dead : OUTCOME.retry;
      for (const envelope of batch) outcomes.set(envelope.id, { id: envelope.id, status, error: err });
      return;
    }
    for (const outcome of results) outcomes.set(outcome.id, outcome);
  };
  for (const batch of chunk(envelopes, size)) await run(batch);
  return envelopes.map((e) => outcomes.get(e.id) || { id: e.id, status: OUTCOME.retry, error: 'no result for entry' });
}

// ---- S3 ------------------------------------------------------------------------

class ObjectStore {
  constructor(options = {}) {
    if (typeof options.bucket !== 'string' || !options.bucket) throw new TypeError('ObjectStore needs a "bucket"');
    this.sdk = loadSdk('@aws-sdk/client-s3', options.sdk);
    this.client = options.client || new this.sdk.S3Client(options.clientConfig || {});
    this.bucket = options.bucket;
    this.prefix = options.prefix ? String(options.prefix).replace(/^\/+|\/+$/g, '') : '';
    this.partSize = options.partSize || 8 * MiB;
    if (!Number.isInteger(this.partSize) || this.partSize < S3_MIN_PART) throw new TypeError(`"partSize" must be an integer of at least ${S3_MIN_PART} bytes`);
    this.sse = options.sse || null;               // 'AES256' | 'aws:kms'
    this.kmsKeyId = options.kmsKeyId || null;
    this.storageClass = options.storageClass || null;
  }

  _key(key) { return this.prefix ? `${this.prefix}/${key}` : key; }
  _unkey(full) { return this.prefix ? full.slice(this.prefix.length + 1) : full; }

  _common(key, meta) {
    const out = { Bucket: this.bucket, Key: this._key(key) };
    if (meta.contentType) out.ContentType = meta.contentType;
    if (meta.contentEncoding) out.ContentEncoding = meta.contentEncoding;
    if (meta.sha256) out.Metadata = { sha256: meta.sha256 };
    if (this.sse) out.ServerSideEncryption = this.sse;
    if (this.sse === 'aws:kms' && this.kmsKeyId) out.SSEKMSKeyId = this.kmsKeyId;
    if (this.storageClass) out.StorageClass = this.storageClass;
    return out;
  }

  async put(key, body, meta = {}) {
    const common = this._common(key, meta);
    const size = Buffer.isBuffer(body) ? body.length : typeof body === 'string' ? Buffer.byteLength(body) : meta.size;
    if (size !== undefined && size <= this.partSize) {
      const bytes = await collect(body);
      await this.client.send(new this.sdk.PutObjectCommand(Object.assign({}, common, { Body: bytes, ContentLength: bytes.length })));
      return;
    }
    // Unknown size or large: multipart. Parts are exactly partSize except the last.
    const { UploadId } = await this.client.send(new this.sdk.CreateMultipartUploadCommand(common));
    const uploaded = [];
    try {
      let number = 0;
      for await (const part of parts(body, this.partSize)) {
        number += 1;
        const { ETag } = await this.client.send(new this.sdk.UploadPartCommand({
          Bucket: common.Bucket, Key: common.Key, UploadId, PartNumber: number, Body: part, ContentLength: part.length
        }));
        uploaded.push({ ETag, PartNumber: number });
      }
      if (uploaded.length === 0) {
        // Empty body: multipart cannot complete with zero parts.
        await this.client.send(new this.sdk.AbortMultipartUploadCommand({ Bucket: common.Bucket, Key: common.Key, UploadId }));
        await this.client.send(new this.sdk.PutObjectCommand(Object.assign({}, common, { Body: Buffer.alloc(0), ContentLength: 0 })));
        return;
      }
      await this.client.send(new this.sdk.CompleteMultipartUploadCommand({
        Bucket: common.Bucket, Key: common.Key, UploadId, MultipartUpload: { Parts: uploaded }
      }));
    } catch (err) {
      await this.client.send(new this.sdk.AbortMultipartUploadCommand({ Bucket: common.Bucket, Key: common.Key, UploadId })).catch(() => {});
      throw err;
    }
  }

  async get(key) {
    try {
      const res = await this.client.send(new this.sdk.GetObjectCommand({ Bucket: this.bucket, Key: this._key(key) }));
      return toReadable(res.Body);
    } catch (err) {
      if (is404(err)) throw notFound(key);
      throw err;
    }
  }

  async head(key) {
    try {
      const res = await this.client.send(new this.sdk.HeadObjectCommand({ Bucket: this.bucket, Key: this._key(key) }));
      const meta = Object.assign({}, res.Metadata || {});
      if (res.ContentType) meta.contentType = res.ContentType;
      return { size: Number(res.ContentLength), meta };
    } catch (err) {
      if (is404(err)) return null;
      throw err;
    }
  }

  async list(prefix = '') {
    const out = [];
    let ContinuationToken;
    do {
      const res = await this.client.send(new this.sdk.ListObjectsV2Command({
        Bucket: this.bucket, Prefix: this._key(prefix), ContinuationToken
      }));
      for (const o of res.Contents || []) out.push({ key: this._unkey(o.Key), size: Number(o.Size) });
      ContinuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
    } while (ContinuationToken);
    return out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key) {
    await this.client.send(new this.sdk.DeleteObjectCommand({ Bucket: this.bucket, Key: this._key(key) }));
  }
}

// ---- SQS ------------------------------------------------------------------------

function isFifo(name) { return /\.fifo$/.test(name); }

// SQS FIFO deduplication ids allow [a-zA-Z0-9!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~], max 128.
function dedupeId(id) { return String(id).replace(/[^a-zA-Z0-9!-/:-@[-`{-~]/g, '_').slice(0, 128); }

function batchOutcomes(batch, res) {
  const byEntry = new Map(batch.map((e, i) => [`m${i}`, e]));
  const outcomes = [];
  for (const ok of (res && res.Successful) || []) {
    const e = byEntry.get(ok.Id);
    if (e) outcomes.push({ id: e.id, status: OUTCOME.ok });
  }
  for (const failed of (res && res.Failed) || []) {
    const e = byEntry.get(failed.Id);
    if (!e) continue;
    const transient = TRANSIENT.test(String(failed.Code || ''));
    outcomes.push({
      id: e.id,
      status: failed.SenderFault && !transient ? OUTCOME.dead : OUTCOME.retry,
      error: `${failed.Code || 'Failed'}: ${failed.Message || ''}`.trim()
    });
  }
  return outcomes;
}

class QueueSink {
  constructor(options = {}) {
    if (typeof options.queueUrl !== 'string' || !options.queueUrl) throw new TypeError('QueueSink needs a "queueUrl"');
    this.sdk = loadSdk('@aws-sdk/client-sqs', options.sdk);
    this.client = options.client || new this.sdk.SQSClient(options.clientConfig || {});
    this.queueUrl = options.queueUrl;
    this.fifo = isFifo(options.queueUrl);
    this.delaySeconds = options.delaySeconds;
  }

  publish(envelopes) {
    return publishInChunks(envelopes, SQS_BATCH, async (batch) => {
      const Entries = batch.map((e, i) => {
        const entry = { Id: `m${i}`, MessageBody: e.body, MessageAttributes: toMessageAttributes(e.attributes) };
        if (this.fifo) {
          entry.MessageGroupId = e.orderingKey || e.kind || FALLBACKS.name;
          entry.MessageDeduplicationId = dedupeId(e.id);
        } else if (this.delaySeconds) {
          entry.DelaySeconds = this.delaySeconds;
        }
        return entry;
      });
      const res = await this.client.send(new this.sdk.SendMessageBatchCommand({ QueueUrl: this.queueUrl, Entries }));
      return batchOutcomes(batch, res);
    });
  }
}

class QueueSource {
  constructor(options = {}) {
    if (typeof options.queueUrl !== 'string' || !options.queueUrl) throw new TypeError('QueueSource needs a "queueUrl"');
    this.sdk = loadSdk('@aws-sdk/client-sqs', options.sdk);
    this.client = options.client || new this.sdk.SQSClient(options.clientConfig || {});
    this.queueUrl = options.queueUrl;
    this.waitTimeSeconds = options.waitTimeSeconds === undefined ? 20 : options.waitTimeSeconds;
    this.visibilityTimeout = options.visibilityTimeout;
  }

  async receive(max) {
    const input = {
      QueueUrl: this.queueUrl,
      MaxNumberOfMessages: Math.max(1, Math.min(SQS_BATCH, max || SQS_BATCH)),
      WaitTimeSeconds: this.waitTimeSeconds,
      MessageAttributeNames: ['All'],
      MessageSystemAttributeNames: ['ApproximateReceiveCount']
    };
    if (this.visibilityTimeout !== undefined) input.VisibilityTimeout = this.visibilityTimeout;
    const res = await this.client.send(new this.sdk.ReceiveMessageCommand(input));
    return (res.Messages || []).map((m) => {
      const attributes = fromMessageAttributes(m.MessageAttributes);
      const count = m.Attributes && Number(m.Attributes.ApproximateReceiveCount);
      return {
        id: attributes[queueNames.attributes.idempotencyKey] || m.MessageId,
        body: m.Body,
        attributes,
        ackToken: m.ReceiptHandle,
        attempt: Number.isFinite(count) ? count : undefined
      };
    });
  }

  async ack(tokens) {
    for (const batch of chunk(tokens, SQS_BATCH)) {
      await this.client.send(new this.sdk.DeleteMessageBatchCommand({
        QueueUrl: this.queueUrl,
        Entries: batch.map((ReceiptHandle, i) => ({ Id: `d${i}`, ReceiptHandle }))
      }));
    }
  }

  async nack(tokens, delayMs = 0) {
    const VisibilityTimeout = Math.min(SQS_MAX_VISIBILITY_S, Math.max(0, Math.ceil((delayMs || 0) / 1000)));
    for (const batch of chunk(tokens, SQS_BATCH)) {
      await this.client.send(new this.sdk.ChangeMessageVisibilityBatchCommand({
        QueueUrl: this.queueUrl,
        Entries: batch.map((ReceiptHandle, i) => ({ Id: `v${i}`, ReceiptHandle, VisibilityTimeout }))
      }));
    }
  }
}

// ---- SNS ------------------------------------------------------------------------

class TopicSink {
  constructor(options = {}) {
    if (typeof options.topicArn !== 'string' || !options.topicArn) throw new TypeError('TopicSink needs a "topicArn"');
    this.sdk = loadSdk('@aws-sdk/client-sns', options.sdk);
    this.client = options.client || new this.sdk.SNSClient(options.clientConfig || {});
    this.topicArn = options.topicArn;
    this.fifo = isFifo(options.topicArn);
  }

  publish(envelopes) {
    return publishInChunks(envelopes, SNS_BATCH, async (batch) => {
      const PublishBatchRequestEntries = batch.map((e, i) => {
        const entry = { Id: `m${i}`, Message: e.body, MessageAttributes: toMessageAttributes(e.attributes) };
        if (this.fifo) {
          entry.MessageGroupId = e.orderingKey || e.kind || FALLBACKS.name;
          entry.MessageDeduplicationId = dedupeId(e.id);
        }
        return entry;
      });
      const res = await this.client.send(new this.sdk.PublishBatchCommand({ TopicArn: this.topicArn, PublishBatchRequestEntries }));
      return batchOutcomes(batch, res);
    });
  }
}

// ---- EventBridge ------------------------------------------------------------------

class EventBusSink {
  constructor(options = {}) {
    this.sdk = loadSdk('@aws-sdk/client-eventbridge', options.sdk);
    this.client = options.client || new this.sdk.EventBridgeClient(options.clientConfig || {});
    this.eventBusName = options.eventBusName || FALLBACKS.name;
    this.source = options.source || 'better-sqlite';
  }

  publish(envelopes) {
    return publishInChunks(envelopes, EVENTBRIDGE_BATCH, async (batch) => {
      const Entries = batch.map((e) => ({
        EventBusName: this.eventBusName,
        Source: this.source,
        DetailType: e.kind,
        Detail: e.body,
        Resources: [e.id]
      }));
      const res = await this.client.send(new this.sdk.PutEventsCommand({ Entries }));
      const results = (res && res.Entries) || [];
      return batch.map((e, i) => {
        const r = results[i] || {};
        if (!r.ErrorCode) return { id: e.id, status: OUTCOME.ok };
        return { id: e.id, status: TRANSIENT.test(r.ErrorCode) ? OUTCOME.retry : OUTCOME.dead, error: `${r.ErrorCode}: ${r.ErrorMessage || ''}`.trim() };
      });
    });
  }
}

// ---- Kinesis ------------------------------------------------------------------------

class StreamSink {
  constructor(options = {}) {
    if (typeof options.streamName !== 'string' || !options.streamName) throw new TypeError('StreamSink needs a "streamName"');
    this.sdk = loadSdk('@aws-sdk/client-kinesis', options.sdk);
    this.client = options.client || new this.sdk.KinesisClient(options.clientConfig || {});
    this.streamName = options.streamName;
  }

  publish(envelopes) {
    return publishInChunks(envelopes, KINESIS_BATCH, async (batch) => {
      const Records = batch.map((e) => ({ Data: Buffer.from(e.body), PartitionKey: e.orderingKey || e.kind || e.id }));
      const res = await this.client.send(new this.sdk.PutRecordsCommand({ StreamName: this.streamName, Records }));
      const results = (res && res.Records) || [];
      return batch.map((e, i) => {
        const r = results[i] || {};
        if (!r.ErrorCode) return { id: e.id, status: OUTCOME.ok };
        // Kinesis reports only throughput and internal errors per record.
        return { id: e.id, status: OUTCOME.retry, error: `${r.ErrorCode}: ${r.ErrorMessage || ''}`.trim() };
      });
    });
  }
}

const { configDir, configFile, resolvedRegion, regionFromFile } = require('./config');

// Other providers live in their own submodules and are exposed under a
// namespace each, so `adapters.ObjectStore` stays the AWS/S3 store (the
// original surface) while `adapters.gcp.ObjectStore` and
// `adapters.azure.ObjectStore` reach the others. Each satisfies the same
// interfaces from better-sqlite3/lib/interfaces.js and passes the same
// contract suite (adapters/test-gcp.js, adapters/test-azure.js).
const gcp = require('./gcp');
// Azure: the Blob + Service Bus adapters, plus the Event Hubs and Queue Storage
// parity adapters, merged into one namespace.
const azure = Object.assign({}, require('./azure'), require('./parity').azure);
// Non-cloud message brokers, each under its own namespace.
const kafka = require('./kafka').kafka;
const redis = require('./redis').redis;

module.exports = {
  configDir,
  configFile,
  resolvedRegion,
  regionFromFile,
  ObjectStore,
  QueueSink,
  QueueSource,
  TopicSink,
  EventBusSink,
  StreamSink,
  gcp,
  azure,
  kafka,
  redis,
  // exposed for tests and for hosts writing their own adapters
  internals: { publishInChunks, isSenderFault, parts, toMessageAttributes, fromMessageAttributes, dedupeId, TRANSIENT }
};
