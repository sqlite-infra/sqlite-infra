// Types for better-sqlite-adapters. The interface shapes are declared in ./util.d.ts.

/// <reference types="node" />

import { Readable } from 'stream';

export interface Client {
  send(command: unknown): Promise<any>;
}

interface CommonOptions {
  /** An SDK client instance. Default: constructed from the SDK with `clientConfig`. */
  client?: Client;
  /** Passed to the SDK client constructor when `client` is not given. */
  clientConfig?: Record<string, unknown>;
  /** The SDK module object (for tests or a bundled copy). Default: `require()`d lazily. */
  sdk?: any;
}

export interface ObjectStoreOptions extends CommonOptions {
  bucket: string;
  /** Key prefix inside the bucket. */
  prefix?: string;
  /** Multipart part size in bytes; also the single-PUT threshold. Default 8 MiB, minimum 5 MiB. */
  partSize?: number;
  /** Server-side encryption: 'AES256' or 'aws:kms'. */
  sse?: 'AES256' | 'aws:kms';
  /** KMS key for 'aws:kms'. */
  kmsKeyId?: string;
  storageClass?: string;
}

export interface PutMeta {
  size?: number;
  sha256?: string;
  contentType?: string;
  contentEncoding?: string;
  [key: string]: unknown;
}

export class ObjectStore {
  constructor(options: ObjectStoreOptions);
  readonly bucket: string;
  readonly prefix: string;
  readonly partSize: number;
  put(key: string, body: Buffer | string | Readable, meta?: PutMeta): Promise<void>;
  get(key: string): Promise<Readable>;
  head(key: string): Promise<{ size: number; meta: Record<string, unknown> } | null>;
  list(prefix?: string): Promise<Array<{ key: string; size: number }>>;
  delete(key: string): Promise<void>;
}

export interface Envelope {
  id: string;
  kind: string;
  attempt: number;
  body: string;
  attributes: Record<string, string>;
  orderingKey: string;
}

export interface Outcome {
  id: string;
  status: 'ok' | 'retry' | 'dead';
  error?: Error | string;
}

export interface Message {
  id: string;
  body: string;
  attributes: Record<string, string>;
  ackToken: unknown;
  attempt?: number;
}

export interface QueueSinkOptions extends CommonOptions {
  queueUrl: string;
  /** Standard queues only: DelaySeconds on every message. */
  delaySeconds?: number;
}

/** SendMessageBatch. FIFO queues (URL ending in .fifo) get MessageGroupId = orderingKey and MessageDeduplicationId = envelope id. */
export class QueueSink {
  constructor(options: QueueSinkOptions);
  readonly queueUrl: string;
  readonly fifo: boolean;
  publish(envelopes: Envelope[]): Promise<Outcome[]>;
}

export interface QueueSourceOptions extends CommonOptions {
  queueUrl: string;
  /** Long-poll wait. Default 20. */
  waitTimeSeconds?: number;
  /** Override the queue's visibility timeout for received messages. */
  visibilityTimeout?: number;
}

/** ReceiveMessage / DeleteMessageBatch / ChangeMessageVisibilityBatch. */
export class QueueSource {
  constructor(options: QueueSourceOptions);
  receive(max: number): Promise<Message[]>;
  ack(tokens: unknown[]): Promise<void>;
  nack(tokens: unknown[], delayMs: number): Promise<void>;
}

export interface TopicSinkOptions extends CommonOptions {
  topicArn: string;
}

/** PublishBatch. FIFO topics get MessageGroupId and MessageDeduplicationId like QueueSink. */
export class TopicSink {
  constructor(options: TopicSinkOptions);
  publish(envelopes: Envelope[]): Promise<Outcome[]>;
}

export interface EventBusSinkOptions extends CommonOptions {
  /** Default 'default'. */
  eventBusName?: string;
  /** The event `source` field. Default 'better-sqlite'. */
  source?: string;
}

/** PutEvents: DetailType = kind, Detail = body, Resources = [envelope id]. */
export class EventBusSink {
  constructor(options?: EventBusSinkOptions);
  publish(envelopes: Envelope[]): Promise<Outcome[]>;
}

export interface StreamSinkOptions extends CommonOptions {
  streamName: string;
}

/** PutRecords: PartitionKey = orderingKey. */
export class StreamSink {
  constructor(options: StreamSinkOptions);
  publish(envelopes: Envelope[]): Promise<Outcome[]>;
}

/** Apache Kafka integration (kafkajs). Sink only — Kafka's consumer model doesn't fit the pull-based source contract. */
export namespace kafka {
  interface SinkOptions { topic: string; brokers?: string[]; producer?: unknown; client?: unknown; clientConfig?: Record<string, unknown>; sdk?: any }
  /** MessageSink on Kafka; key = orderingKey, value = body, headers = attributes. */
  class Sink implements MessageSinkLike { constructor(options: SinkOptions); publish(envelopes: Envelope[]): Promise<Outcome[]>; }
}

/** Redis Streams integration (node-redis v4) with a consumer group. */
export namespace redis {
  interface StreamSinkOptions { stream: string; maxLen?: number; client?: unknown; clientConfig?: Record<string, unknown>; sdk?: any }
  interface StreamSourceOptions { stream: string; group: string; consumer?: string; blockMs?: number; minIdleMs?: number; createGroup?: boolean; client?: unknown; clientConfig?: Record<string, unknown>; sdk?: any }
  /** MessageSink on a Redis stream (XADD). */
  class StreamSink implements MessageSinkLike { constructor(options: StreamSinkOptions); publish(envelopes: Envelope[]): Promise<Outcome[]>; }
  /** MessageSource on a Redis stream consumer group (XREADGROUP/XACK/XAUTOCLAIM). */
  class StreamSource implements MessageSourceLike {
    constructor(options: StreamSourceOptions);
    receive(max: number): Promise<Message[]>;
    ack(tokens: unknown[]): Promise<void>;
    nack(tokens: unknown[], delayMs: number): Promise<void>;
  }
}

/** Region the client resolved (env or IMDS), falling back to AWS_REGION / AWS_DEFAULT_REGION. Never throws. */
export function resolvedRegion(client?: { config?: { region?: unknown } } | null, env?: NodeJS.ProcessEnv): Promise<string | null>;
/** Region from AWS_REGION / AWS_DEFAULT_REGION, or null. No file is read. */
export function regionFromEnv(env?: NodeJS.ProcessEnv): string | null;

export const internals: {
  publishInChunks(envelopes: Envelope[], size: number, send: (batch: Envelope[]) => Promise<Outcome[]>): Promise<Outcome[]>;
  isSenderFault(err: unknown): boolean;
  parts(body: Buffer | string | AsyncIterable<Buffer | Uint8Array>, size: number): AsyncGenerator<Buffer>;
  toMessageAttributes(attributes: Record<string, string>): Record<string, { DataType: string; StringValue: string }> | undefined;
  fromMessageAttributes(attributes: unknown): Record<string, string>;
  dedupeId(id: string): string;
  TRANSIENT: RegExp;
};

/** An ObjectStore/MessageSink/MessageSource, matching ./util.d.ts. */
interface ObjectStoreLike {
  put(key: string, body: Buffer | string | Readable, meta?: PutMeta): Promise<void>;
  get(key: string): Promise<Readable>;
  head(key: string): Promise<{ size: number; meta: Record<string, unknown> } | null>;
  list(prefix?: string): Promise<Array<{ key: string; size: number }>>;
  delete(key: string): Promise<void>;
}
interface MessageSinkLike { publish(envelopes: Envelope[]): Promise<Outcome[]>; }
interface MessageSourceLike {
  receive(max: number): Promise<Message[]>;
  ack(tokens: unknown[]): Promise<void>;
  nack(tokens: unknown[], delayMs: number): Promise<void>;
}

/** Google Cloud adapters: Cloud Storage and Pub/Sub. */
export namespace gcp {
  interface ObjectStoreOptions extends CommonOptions { bucket: string; prefix?: string }
  interface QueueSinkOptions extends CommonOptions { topic: string }
  interface QueueSourceOptions extends CommonOptions { subscription: string }
  /** Cloud Storage (@google-cloud/storage). */
  class ObjectStore implements ObjectStoreLike {
    constructor(options: ObjectStoreOptions);
    put(key: string, body: Buffer | string | Readable, meta?: PutMeta): Promise<void>;
    get(key: string): Promise<Readable>;
    head(key: string): Promise<{ size: number; meta: Record<string, unknown> } | null>;
    list(prefix?: string): Promise<Array<{ key: string; size: number }>>;
    delete(key: string): Promise<void>;
  }
  /** Pub/Sub publish (@google-cloud/pubsub), ordering key = orderingKey || kind. */
  class QueueSink implements MessageSinkLike { constructor(options: QueueSinkOptions); publish(envelopes: Envelope[]): Promise<Outcome[]>; }
  /** Pub/Sub pull (@google-cloud/pubsub v1.SubscriberClient). */
  class QueueSource implements MessageSourceLike {
    constructor(options: QueueSourceOptions);
    receive(max: number): Promise<Message[]>;
    ack(tokens: unknown[]): Promise<void>;
    nack(tokens: unknown[], delayMs: number): Promise<void>;
  }
}

/** Microsoft Azure adapters: Blob Storage and Service Bus. */
export namespace azure {
  interface CommonAzureOptions { client?: unknown; connectionString?: string; sdk?: any }
  interface ObjectStoreOptions extends CommonAzureOptions { container: string; prefix?: string }
  interface QueueSinkOptions extends CommonAzureOptions { queueName: string }
  interface QueueSourceOptions extends CommonAzureOptions { queueName: string; maxWaitTimeMs?: number }
  /** Blob Storage (@azure/storage-blob). */
  class ObjectStore implements ObjectStoreLike {
    constructor(options: ObjectStoreOptions);
    put(key: string, body: Buffer | string | Readable, meta?: PutMeta): Promise<void>;
    get(key: string): Promise<Readable>;
    head(key: string): Promise<{ size: number; meta: Record<string, unknown> } | null>;
    list(prefix?: string): Promise<Array<{ key: string; size: number }>>;
    delete(key: string): Promise<void>;
  }
  /** Service Bus send (@azure/service-bus); sessionId = orderingKey. */
  class QueueSink implements MessageSinkLike { constructor(options: QueueSinkOptions); publish(envelopes: Envelope[]): Promise<Outcome[]>; }
  /** Service Bus peek-lock receiver (@azure/service-bus). */
  class QueueSource implements MessageSourceLike {
    constructor(options: QueueSourceOptions);
    receive(max: number): Promise<Message[]>;
    ack(tokens: unknown[]): Promise<void>;
    nack(tokens: unknown[], delayMs: number): Promise<void>;
  }

  interface EventHubSinkOptions extends CommonAzureOptions { eventHubName?: string }
  /** Event Hubs producer (@azure/event-hubs); partitionKey = orderingKey. */
  class EventHubSink implements MessageSinkLike { constructor(options: EventHubSinkOptions); publish(envelopes: Envelope[]): Promise<Outcome[]>; }

  interface StorageQueueOptions extends CommonAzureOptions { queueName: string; visibilityTimeoutSec?: number }
  /** Queue Storage send (@azure/storage-queue); the envelope is JSON-wrapped so attributes survive. */
  class StorageQueueSink implements MessageSinkLike { constructor(options: StorageQueueOptions); publish(envelopes: Envelope[]): Promise<Outcome[]>; }
  /** Queue Storage receiver (@azure/storage-queue); nack sets the visibility timeout as backoff. */
  class StorageQueueSource implements MessageSourceLike {
    constructor(options: StorageQueueOptions);
    receive(max: number): Promise<Message[]>;
    ack(tokens: unknown[]): Promise<void>;
    nack(tokens: unknown[], delayMs: number): Promise<void>;
  }
}
