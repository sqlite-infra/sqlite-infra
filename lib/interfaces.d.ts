// Types for the cloud interfaces. See lib/interfaces.js for the contract.

/// <reference types="node" />

import { Readable } from 'stream';

declare namespace interfaces {
  interface PutMeta {
    /** Byte length of the body when known; lets adapters choose a single PUT or multipart. */
    size?: number;
    /** Hex SHA-256 of the body as stored. Adapters persist it as object metadata; restore verifies it. */
    sha256?: string;
    contentType?: string;
    contentEncoding?: string;
    [key: string]: unknown;
  }

  interface ObjectHead {
    size: number;
    meta: { sha256?: string; [key: string]: unknown };
  }

  interface ObjectEntry {
    key: string;
    size: number;
  }

  /** A key space of bytes; MemoryObjectStore is the in-memory implementation. */
  interface ObjectStore {
    put(key: string, body: Buffer | string | Readable, meta?: PutMeta): Promise<void>;
    /** Rejects with `code: 'ENOENT'` for a missing key. */
    get(key: string): Promise<Readable>;
    head(key: string): Promise<ObjectHead | null>;
    /** Every key under `prefix`, sorted ascending. */
    list(prefix: string): Promise<ObjectEntry[]>;
    delete(key: string): Promise<void>;
  }

  /** What the queue hands a sink for one job. `id` is the idempotency key. */
  interface Envelope {
    id: string;
    kind: string;
    attempt: number;
    /** JSON text: `{ id, kind, payload, attempt, enqueuedAt, source }` from the queue. */
    body: string;
    attributes: Record<string, string>;
    orderingKey: string;
  }

  interface Outcome {
    id: string;
    /** 'retry' when the service may accept the envelope later; 'dead' when no retry can fix it. */
    status: 'ok' | 'retry' | 'dead';
    error?: Error | string;
  }

  /** A queue, topic, stream or HTTP endpoint that accepts envelopes. */
  interface MessageSink {
    /** One outcome per envelope, in the same order. A rejected promise retries the whole batch. */
    publish(envelopes: Envelope[]): Promise<Outcome[]>;
  }

  interface Message {
    id: string;
    body: string;
    attributes: Record<string, string>;
    /** Opaque; passed back to ack() / nack(). */
    ackToken: unknown;
    /** Delivery count when the service reports it. */
    attempt?: number;
  }

  /** A queue subscription that is read with receive / ack / nack. */
  interface MessageSource {
    receive(max: number): Promise<Message[]>;
    ack(tokens: unknown[]): Promise<void>;
    nack(tokens: unknown[], delayMs: number): Promise<void>;
  }

  function assertObjectStore<T extends ObjectStore>(store: T, what?: string): T;
  function assertMessageSink<T extends MessageSink>(sink: T, what?: string): T;
  /**
   * Resolves a delivery target to a sink: a URL string or `{ url, timeoutMs? }`
   * becomes an HttpSink; a sink or `{ sink }` is validated and returned as is.
   * `what` names the option in error messages; `options.timeoutMs` is the
   * HttpSink default when the spec gives none.
   */
  function toMessageSink(spec: string | MessageSink | { url: string; timeoutMs?: number } | { sink: MessageSink }, what?: string, options?: HttpSinkOptions): MessageSink;
  function assertMessageSource<T extends MessageSource>(source: T, what?: string): T;
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
