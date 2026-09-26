'use strict';

// Tests for better-sqlite-adapters against fake SDK modules and clients. They
// prove the exact inputs handed to each SDK command and the outcome mapping;
// they do not talk to AWS. The fakes model S3 and SQS closely enough to run
// the contract suite from better-sqlite3/lib/contract.js.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Readable } = require('node:stream');

const contract = require('../lib/contract');
const adapters = require('./');

// ---- fake SDK ----------------------------------------------------------------

function command(name) {
  return class { constructor(input) { this.input = input; this.name = name; } };
}

const fakeS3 = {
  S3Client: class { constructor(config) { this.config = config; } },
  PutObjectCommand: command('PutObject'),
  GetObjectCommand: command('GetObject'),
  HeadObjectCommand: command('HeadObject'),
  ListObjectsV2Command: command('ListObjectsV2'),
  DeleteObjectCommand: command('DeleteObject'),
  CreateMultipartUploadCommand: command('CreateMultipartUpload'),
  UploadPartCommand: command('UploadPart'),
  CompleteMultipartUploadCommand: command('CompleteMultipartUpload'),
  AbortMultipartUploadCommand: command('AbortMultipartUpload')
};

const fakeSqs = {
  SQSClient: class {},
  SendMessageBatchCommand: command('SendMessageBatch'),
  ReceiveMessageCommand: command('ReceiveMessage'),
  DeleteMessageBatchCommand: command('DeleteMessageBatch'),
  ChangeMessageVisibilityBatchCommand: command('ChangeMessageVisibilityBatch')
};

const fakeSns = { SNSClient: class {}, PublishBatchCommand: command('PublishBatch') };
const fakeEventBridge = { EventBridgeClient: class {}, PutEventsCommand: command('PutEvents') };
const fakeKinesis = { KinesisClient: class {}, PutRecordsCommand: command('PutRecords') };

function notFoundError() {
  const err = new Error('NoSuchKey');
  err.name = 'NoSuchKey';
  err.$fault = 'client';
  err.$metadata = { httpStatusCode: 404 };
  return err;
}

// An S3 that stores bytes in a Map, including a multipart upload state machine.
class FakeS3Client {
  constructor() { this.objects = new Map(); this.uploads = new Map(); this.calls = []; }
  async send(cmd) {
    this.calls.push(cmd);
    const i = cmd.input;
    switch (cmd.name) {
      case 'PutObject':
        this.objects.set(i.Key, { body: Buffer.from(i.Body), meta: i.Metadata || {}, contentType: i.ContentType, sse: i.ServerSideEncryption });
        return {};
      case 'GetObject': {
        const o = this.objects.get(i.Key);
        if (!o) throw notFoundError();
        return { Body: Readable.from([o.body]) };
      }
      case 'HeadObject': {
        const o = this.objects.get(i.Key);
        if (!o) { const e = notFoundError(); e.name = 'NotFound'; throw e; }
        return { ContentLength: o.body.length, Metadata: o.meta, ContentType: o.contentType };
      }
      case 'ListObjectsV2': {
        const all = [...this.objects.keys()].filter((k) => k.startsWith(i.Prefix)).sort().reverse();   // unsorted on purpose
        const page = 2;
        const start = i.ContinuationToken ? Number(i.ContinuationToken) : 0;
        const slice = all.slice(start, start + page);
        return {
          Contents: slice.map((Key) => ({ Key, Size: this.objects.get(Key).body.length })),
          IsTruncated: start + page < all.length,
          NextContinuationToken: String(start + page)
        };
      }
      case 'DeleteObject': this.objects.delete(i.Key); return {};
      case 'CreateMultipartUpload': {
        const id = `u${this.uploads.size + 1}`;
        this.uploads.set(id, { key: i.Key, parts: new Map(), meta: i.Metadata || {}, contentType: i.ContentType });
        return { UploadId: id };
      }
      case 'UploadPart': {
        const u = this.uploads.get(i.UploadId);
        u.parts.set(i.PartNumber, Buffer.from(i.Body));
        return { ETag: `"etag-${i.PartNumber}"` };
      }
      case 'CompleteMultipartUpload': {
        const u = this.uploads.get(i.UploadId);
        const ordered = i.MultipartUpload.Parts.sort((a, b) => a.PartNumber - b.PartNumber).map((p) => u.parts.get(p.PartNumber));
        this.objects.set(u.key, { body: Buffer.concat(ordered), meta: u.meta, contentType: u.contentType });
        this.uploads.delete(i.UploadId);
        return {};
      }
      case 'AbortMultipartUpload': this.uploads.delete(i.UploadId); return {};
      default: throw new Error(`unexpected ${cmd.name}`);
    }
  }
}

// An SQS queue with visibility timeouts and receive counts.
class FakeSqsClient {
  constructor() { this.messages = []; this.calls = []; this.seq = 0; this.failIds = new Map(); }
  async send(cmd) {
    this.calls.push(cmd);
    const i = cmd.input;
    switch (cmd.name) {
      case 'SendMessageBatch': {
        const Successful = []; const Failed = [];
        for (const e of i.Entries) {
          const failure = this.failIds.get(e.MessageBody);
          if (failure) { Failed.push(Object.assign({ Id: e.Id }, failure)); continue; }
          this.seq += 1;
          this.messages.push({ MessageId: `mid-${this.seq}`, Body: e.MessageBody, MessageAttributes: e.MessageAttributes, ReceiptHandle: null, visibleAt: 0, receives: 0 });
          Successful.push({ Id: e.Id, MessageId: `mid-${this.seq}` });
        }
        return { Successful, Failed };
      }
      case 'ReceiveMessage': {
        const now = Date.now();
        const out = [];
        for (const m of this.messages) {
          if (out.length >= i.MaxNumberOfMessages) break;
          if (m.visibleAt > now) continue;
          m.receives += 1;
          m.ReceiptHandle = `rh-${m.MessageId}-${m.receives}`;
          m.visibleAt = now + 30000;
          out.push({ MessageId: m.MessageId, Body: m.Body, MessageAttributes: m.MessageAttributes, ReceiptHandle: m.ReceiptHandle, Attributes: { ApproximateReceiveCount: String(m.receives) } });
        }
        return { Messages: out };
      }
      case 'DeleteMessageBatch':
        this.messages = this.messages.filter((m) => !i.Entries.some((e) => e.ReceiptHandle === m.ReceiptHandle));
        return { Successful: i.Entries.map((e) => ({ Id: e.Id })), Failed: [] };
      case 'ChangeMessageVisibilityBatch':
        for (const e of i.Entries) {
          const m = this.messages.find((x) => x.ReceiptHandle === e.ReceiptHandle);
          if (m) m.visibleAt = Date.now() + e.VisibilityTimeout * 1000;
        }
        return { Successful: i.Entries.map((e) => ({ Id: e.Id })), Failed: [] };
      default: throw new Error(`unexpected ${cmd.name}`);
    }
  }
}

const bigPart = 5 * 1024 * 1024;   // the S3 minimum, to keep the tests fast

// ---- contract ------------------------------------------------------------------

contract.objectStore(test, () => new adapters.ObjectStore({ bucket: 'b', prefix: 'pre', sdk: fakeS3, client: new FakeS3Client(), partSize: bigPart }), { name: 'ObjectStore' });
contract.queue(test, () => {
  const client = new FakeSqsClient();
  return {
    sink: new adapters.QueueSink({ queueUrl: 'https://sqs.eu-west-1.amazonaws.com/1/q', sdk: fakeSqs, client }),
    source: new adapters.QueueSource({ queueUrl: 'https://sqs.eu-west-1.amazonaws.com/1/q', sdk: fakeSqs, client, waitTimeSeconds: 0 })
  };
}, { name: 'QueueSink+QueueSource' });

// ---- S3 specifics ----------------------------------------------------------------

test('ObjectStore: small bodies are one PutObject with metadata, encryption and content type', async () => {
  const client = new FakeS3Client();
  const store = new adapters.ObjectStore({ bucket: 'b', prefix: '/pre/', sdk: fakeS3, client, sse: 'aws:kms', kmsKeyId: 'k1', partSize: bigPart });
  await store.put('a/b.gz', Buffer.from('x'), { sha256: 'deadbeef', contentType: 'application/gzip', contentEncoding: 'gzip', size: 1 });
  const put = client.calls.find((c) => c.name === 'PutObject');
  assert.equal(put.input.Bucket, 'b');
  assert.equal(put.input.Key, 'pre/a/b.gz');
  assert.deepEqual(put.input.Metadata, { sha256: 'deadbeef' });
  assert.equal(put.input.ContentType, 'application/gzip');
  assert.equal(put.input.ContentEncoding, 'gzip');
  assert.equal(put.input.ServerSideEncryption, 'aws:kms');
  assert.equal(put.input.SSEKMSKeyId, 'k1');
  assert.equal(put.input.ContentLength, 1);
  assert.deepEqual((await store.head('a/b.gz')).meta.sha256, 'deadbeef');
  assert.deepEqual(await store.list('a/'), [{ key: 'a/b.gz', size: 1 }]);
  assert.throws(() => new adapters.ObjectStore({ bucket: 'b', sdk: fakeS3, client, partSize: 1024 }), /at least/);
  assert.throws(() => new adapters.ObjectStore({ sdk: fakeS3, client }), /"bucket"/);
});

test('ObjectStore: a stream of unknown size or a large body goes multipart with exact part sizes', async () => {
  const client = new FakeS3Client();
  const store = new adapters.ObjectStore({ bucket: 'b', sdk: fakeS3, client, partSize: bigPart });
  const total = 2 * bigPart + 12345;
  const body = Buffer.alloc(total);
  for (let i = 0; i < total; i += 4096) body[i] = i & 255;
  // Chunks of odd sizes to exercise re-chunking.
  const chunks = [];
  for (let off = 0; off < total; off += 700001) chunks.push(body.subarray(off, Math.min(total, off + 700001)));
  await store.put('big', Readable.from(chunks), { sha256: 'h' });

  const names = client.calls.map((c) => c.name);
  assert.deepEqual(names.filter((n) => n !== 'UploadPart'), ['CreateMultipartUpload', 'CompleteMultipartUpload']);
  const partSizes = client.calls.filter((c) => c.name === 'UploadPart').map((c) => c.input.ContentLength);
  assert.deepEqual(partSizes, [bigPart, bigPart, 12345]);
  assert.deepEqual(client.calls.filter((c) => c.name === 'UploadPart').map((c) => c.input.PartNumber), [1, 2, 3]);
  assert.deepEqual(client.objects.get('big').body, body);
  assert.deepEqual(client.objects.get('big').meta, { sha256: 'h' });

  // Known size above the threshold from a Buffer also goes multipart.
  client.calls.length = 0;
  await store.put('big2', body, { size: total });
  assert.equal(client.calls.filter((c) => c.name === 'UploadPart').length, 3);

  // A failing part aborts the upload.
  const failing = new FakeS3Client();
  failing.send = async function send(cmd) {
    if (cmd.name === 'UploadPart' && cmd.input.PartNumber === 2) throw new Error('part failed');
    return FakeS3Client.prototype.send.call(this, cmd);
  };
  const store2 = new adapters.ObjectStore({ bucket: 'b', sdk: fakeS3, client: failing, partSize: bigPart });
  await assert.rejects(store2.put('x', Readable.from([body]), {}), /part failed/);
  assert.equal(failing.calls.some((c) => c.name === 'AbortMultipartUpload'), true);
  assert.equal(failing.uploads.size, 0);
});

test('ObjectStore: list follows continuation tokens and strips the prefix', async () => {
  const client = new FakeS3Client();
  const store = new adapters.ObjectStore({ bucket: 'b', prefix: 'p', sdk: fakeS3, client, partSize: bigPart });
  for (const n of [3, 1, 5, 2, 4]) await store.put(`cs/${String(n).padStart(3, '0')}`, Buffer.from('x'.repeat(n)));
  const listed = await store.list('cs/');
  assert.deepEqual(listed.map((o) => o.key), ['cs/001', 'cs/002', 'cs/003', 'cs/004', 'cs/005']);
  assert.deepEqual(listed.map((o) => o.size), [1, 2, 3, 4, 5]);
  assert.ok(client.calls.filter((c) => c.name === 'ListObjectsV2').length >= 3, 'paginated');
});

// ---- SQS specifics --------------------------------------------------------------

const envelope = (id, kind, n) => contract.envelope(id, kind, { n });

test('QueueSink: batches of 10, string attributes, FIFO fields from the queue URL, SenderFault -> dead', async () => {
  const client = new FakeSqsClient();
  const sink = new adapters.QueueSink({ queueUrl: 'https://sqs.eu-west-1.amazonaws.com/1/q.fifo', sdk: fakeSqs, client });
  assert.equal(sink.fifo, true);
  const envelopes = [];
  for (let i = 0; i < 23; i += 1) envelopes.push(envelope(`db:${i}`, i % 2 ? 'odd' : 'even', i));
  client.failIds.set(envelopes[4].body, { SenderFault: true, Code: 'InvalidMessageContents', Message: 'bad chars' });
  client.failIds.set(envelopes[7].body, { SenderFault: false, Code: 'InternalError', Message: 'try later' });
  client.failIds.set(envelopes[9].body, { SenderFault: true, Code: 'ThrottlingException', Message: 'slow down' });

  const outcomes = await sink.publish(envelopes);
  assert.equal(outcomes.length, 23);
  assert.deepEqual(outcomes.map((o) => o.id), envelopes.map((e) => e.id), 'same order as input');
  assert.equal(outcomes[4].status, 'dead');
  assert.match(String(outcomes[4].error), /InvalidMessageContents/);
  assert.equal(outcomes[7].status, 'retry');
  assert.equal(outcomes[9].status, 'retry', 'throttling is transient even when flagged SenderFault');
  assert.equal(outcomes.filter((o) => o.status === 'ok').length, 20);

  const sends = client.calls.filter((c) => c.name === 'SendMessageBatch');
  assert.deepEqual(sends.map((c) => c.input.Entries.length), [10, 10, 3]);
  const first = sends[0].input.Entries[0];
  assert.equal(first.MessageBody, envelopes[0].body);
  assert.equal(first.MessageGroupId, 'even');
  assert.equal(first.MessageDeduplicationId, 'db:0');
  assert.deepEqual(first.MessageAttributes['x-job-kind'], { DataType: 'String', StringValue: 'even' });
  assert.equal(first.DelaySeconds, undefined);

  const standard = new adapters.QueueSink({ queueUrl: 'https://sqs/1/q', sdk: fakeSqs, client: new FakeSqsClient(), delaySeconds: 5 });
  await standard.publish([envelope('a', 'k', 1)]);
  const entry = standard.client.calls[0].input.Entries[0];
  assert.equal(entry.MessageGroupId, undefined);
  assert.equal(entry.DelaySeconds, 5);
});

test('a thrown client-side error isolates the batch to the bad envelope; server errors retry the batch', async () => {
  const sent = [];
  const validation = Object.assign(new Error('Batch request too long'), { name: 'BatchRequestTooLong', $fault: 'client' });
  const client = {
    async send(cmd) {
      sent.push(cmd.input.Entries.map((e) => e.MessageBody));
      if (cmd.input.Entries.some((e) => e.MessageBody.includes('"n":2'))) throw validation;
      return { Successful: cmd.input.Entries.map((e) => ({ Id: e.Id })), Failed: [] };
    }
  };
  const sink = new adapters.QueueSink({ queueUrl: 'https://sqs/1/q', sdk: fakeSqs, client });
  const envelopes = [envelope('a', 'k', 1), envelope('b', 'k', 2), envelope('c', 'k', 3)];
  const outcomes = await sink.publish(envelopes);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'dead', 'ok']);
  assert.equal(outcomes[1].error, validation);
  assert.equal(sent.length, 4, 'one batch call, then one call per envelope');

  const server = Object.assign(new Error('Service unavailable'), { name: 'ServiceUnavailable', $fault: 'server' });
  const down = { async send() { throw server; } };
  const sink2 = new adapters.QueueSink({ queueUrl: 'https://sqs/1/q', sdk: fakeSqs, client: down });
  const out2 = await sink2.publish(envelopes);
  assert.deepEqual(out2.map((o) => o.status), ['retry', 'retry', 'retry']);
  assert.equal(out2[0].error, server);

  assert.equal(adapters.internals.isSenderFault({ $fault: 'client', name: 'ThrottlingException' }), false);
  assert.equal(adapters.internals.isSenderFault({ $fault: 'client', name: 'ValidationException' }), true);
  assert.equal(adapters.internals.isSenderFault(new Error('plain')), false);
});

test('QueueSource: receive maps messages, ack deletes, nack sets visibility in whole seconds, capped', async () => {
  const client = new FakeSqsClient();
  const sink = new adapters.QueueSink({ queueUrl: 'https://sqs/1/q', sdk: fakeSqs, client });
  const source = new adapters.QueueSource({ queueUrl: 'https://sqs/1/q', sdk: fakeSqs, client, waitTimeSeconds: 3, visibilityTimeout: 45 });
  await sink.publish([envelope('idem-1', 'k', 1), envelope('idem-2', 'k', 2)]);

  const got = await source.receive(25);
  const receive = client.calls.find((c) => c.name === 'ReceiveMessage').input;
  assert.equal(receive.MaxNumberOfMessages, 10, 'capped at the SQS maximum');
  assert.equal(receive.WaitTimeSeconds, 3);
  assert.equal(receive.VisibilityTimeout, 45);
  assert.deepEqual(receive.MessageAttributeNames, ['All']);
  assert.equal(got.length, 2);
  assert.equal(got[0].id, 'idem-1', 'the idempotency key, not the SQS MessageId, identifies a message');
  assert.equal(got[0].attributes['x-job-kind'], 'k');
  assert.equal(got[0].attempt, 1);
  assert.match(String(got[0].ackToken), /^rh-/);

  await source.nack([got[0].ackToken], 2500);
  const vis = client.calls.find((c) => c.name === 'ChangeMessageVisibilityBatch').input;
  assert.equal(vis.Entries[0].VisibilityTimeout, 3);
  await source.nack([got[1].ackToken], 10 ** 9);
  const capped = client.calls.filter((c) => c.name === 'ChangeMessageVisibilityBatch').pop().input;
  assert.equal(capped.Entries[0].VisibilityTimeout, 43200);

  await source.ack([got[0].ackToken, got[1].ackToken]);
  const del = client.calls.find((c) => c.name === 'DeleteMessageBatch').input;
  assert.equal(del.Entries.length, 2);
  assert.equal(client.messages.length, 0);
});

// ---- SNS / EventBridge / Kinesis ----------------------------------------------------

test('TopicSink: PublishBatch entries with FIFO fields and Failed mapping', async () => {
  const calls = [];
  const client = {
    async send(cmd) {
      calls.push(cmd);
      const entries = cmd.input.PublishBatchRequestEntries;
      return {
        Successful: entries.slice(1).map((e) => ({ Id: e.Id })),
        Failed: [{ Id: entries[0].Id, SenderFault: true, Code: 'InvalidParameter', Message: 'too big' }]
      };
    }
  };
  const sink = new adapters.TopicSink({ topicArn: 'arn:aws:sns:eu-west-1:1:topic.fifo', sdk: fakeSns, client });
  const outcomes = await sink.publish([envelope('a', 'k', 1), envelope('b', 'k', 2)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['dead', 'ok']);
  const entry = calls[0].input.PublishBatchRequestEntries[0];
  assert.equal(calls[0].input.TopicArn, 'arn:aws:sns:eu-west-1:1:topic.fifo');
  assert.equal(entry.Message, envelope('a', 'k', 1).body);
  assert.equal(entry.MessageGroupId, 'k');
  assert.equal(entry.MessageDeduplicationId, 'a');
  assert.equal(entry.MessageAttributes['idempotency-key'].StringValue, 'a');
});

test('EventBusSink: PutEvents entries and per-entry error codes', async () => {
  const calls = [];
  const client = {
    async send(cmd) {
      calls.push(cmd);
      return {
        FailedEntryCount: 2,
        Entries: cmd.input.Entries.map((e, i) => (i === 1 ? { ErrorCode: 'ThrottlingException', ErrorMessage: 'slow' }
          : i === 2 ? { ErrorCode: 'MalformedDetail', ErrorMessage: 'not json' } : { EventId: `ev${i}` }))
      };
    }
  };
  const sink = new adapters.EventBusSink({ eventBusName: 'apps', source: 'orders', sdk: fakeEventBridge, client });
  const outcomes = await sink.publish([envelope('a', 'order.placed', 1), envelope('b', 'order.placed', 2), envelope('c', 'order.placed', 3)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['ok', 'retry', 'dead']);
  const entry = calls[0].input.Entries[0];
  assert.deepEqual(entry, { EventBusName: 'apps', Source: 'orders', DetailType: 'order.placed', Detail: envelope('a', 'order.placed', 1).body, Resources: ['a'] });
  const defaults = new adapters.EventBusSink({ sdk: fakeEventBridge, client });
  assert.equal(defaults.eventBusName, 'default');
  assert.equal(defaults.source, 'better-sqlite');
});

test('StreamSink: PutRecords with the ordering key as partition key; throughput errors retry', async () => {
  const calls = [];
  const client = {
    async send(cmd) {
      calls.push(cmd);
      return { Records: cmd.input.Records.map((r, i) => (i === 0 ? { ErrorCode: 'ProvisionedThroughputExceededException', ErrorMessage: 'x' } : { SequenceNumber: String(i) })) };
    }
  };
  const sink = new adapters.StreamSink({ streamName: 's', sdk: fakeKinesis, client });
  const outcomes = await sink.publish([envelope('a', 'k1', 1), envelope('b', 'k2', 2)]);
  assert.deepEqual(outcomes.map((o) => o.status), ['retry', 'ok']);
  assert.equal(calls[0].input.StreamName, 's');
  assert.deepEqual(calls[0].input.Records.map((r) => r.PartitionKey), ['k1', 'k2']);
  assert.equal(calls[0].input.Records[0].Data.toString(), envelope('a', 'k1', 1).body);
});

test('a missing SDK is reported with the package to install', () => {
  assert.throws(() => new adapters.QueueSink({ queueUrl: 'https://q', sdk: undefined }), /@aws-sdk\/client-sqs is not installed/);
});

test('helpers: parts() re-chunks exactly, dedupeId sanitizes', async () => {
  const chunks = [];
  for await (const p of adapters.internals.parts(Readable.from([Buffer.alloc(3, 1), Buffer.alloc(5, 2), Buffer.alloc(1, 3)]), 4)) chunks.push(p);
  assert.deepEqual(chunks.map((c) => c.length), [4, 4, 1]);
  assert.deepEqual(Buffer.concat(chunks), Buffer.concat([Buffer.alloc(3, 1), Buffer.alloc(5, 2), Buffer.alloc(1, 3)]));
  assert.equal(adapters.internals.dedupeId('abc:12'), 'abc:12');
  assert.equal(adapters.internals.dedupeId('a bé'), 'a_b_');
  assert.equal(adapters.internals.dedupeId('x'.repeat(200)).length, 128);
});
