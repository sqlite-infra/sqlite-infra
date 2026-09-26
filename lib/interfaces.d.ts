// Types for the cloud interface bundle. The shapes and validators are
// declared in adapters/util.d.ts (see adapters/util.js for the contract) and
// aliased here so this module stays the one bundle for tests and adapters.

/// <reference types="node" />

import { Readable } from 'stream';
import util = require('../adapters/util');

declare namespace interfaces {
  type PutMeta = util.PutMeta;
  type ObjectHead = util.ObjectHead;
  type ObjectEntry = util.ObjectEntry;
  type ObjectStore = util.ObjectStore;
  type Envelope = util.Envelope;
  type Outcome = util.Outcome;
  type MessageSink = util.MessageSink;
  type Message = util.Message;
  type MessageSource = util.MessageSource;

  const assertObjectStore: typeof util.assertObjectStore;
  const assertMessageSink: typeof util.assertMessageSink;
  const assertMessageSource: typeof util.assertMessageSource;
  /** Collects a Buffer, string or Readable into one Buffer. */
  function collect(body: Buffer | string | Readable | Uint8Array): Promise<Buffer>;
  /** An Error with `code: 'ENOENT'` for `key`. */
  function notFound(key: string): Error & { code: 'ENOENT'; key: string };
  /** Maps an HTTP status to an outcome: 2xx ok, 5xx/408/425/429 retry, other 4xx dead. */
  function outcomeForStatus(id: string, status: number): Outcome;

  class MemoryObjectStore implements ObjectStore {
    readonly objects: Map<string, { body: Buffer; meta: PutMeta }>;
    put(key: string, body: Buffer | string | Readable, meta?: PutMeta): Promise<void>;
    get(key: string): Promise<Readable>;
    head(key: string): Promise<ObjectHead | null>;
    list(prefix?: string): Promise<ObjectEntry[]>;
    delete(key: string): Promise<void>;
    /** Test helper: stored bytes, or undefined. */
    bytes(key: string): Buffer | undefined;
  }

  interface MemoryQueueOptions {
    /** Most messages one receive() returns. Default 10. */
    maxBatch?: number;
  }

  /** An in-process at-least-once queue that is both a sink and a source. */
  class MemoryQueue implements MessageSink, MessageSource {
    constructor(options?: MemoryQueueOptions);
    readonly published: Envelope[];
    readonly acked: Message[];
    /** Make the next `n` published envelopes report `status`. */
    failNext(n: number, status?: 'retry' | 'dead', error?: string): void;
    publish(envelopes: Envelope[]): Promise<Outcome[]>;
    receive(max?: number): Promise<Message[]>;
    ack(tokens: unknown[]): Promise<void>;
    nack(tokens: unknown[], delayMs?: number): Promise<void>;
  }

  interface HttpSinkOptions {
    /** Per-request timeout in ms. Default 10000. */
    timeoutMs?: number;
  }

  /** One POST per envelope, attributes as headers. */
  class HttpSink implements MessageSink {
    constructor(url: string | URL, options?: HttpSinkOptions);
    readonly url: URL;
    readonly timeoutMs: number;
    publish(envelopes: Envelope[]): Promise<Outcome[]>;
  }
}

export = interfaces;
