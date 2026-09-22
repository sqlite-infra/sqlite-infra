// Types for the cloud interface shapes and their validators. See
// adapters/util.js for the contract.

/// <reference types="node" />

import { Readable } from 'stream';

declare namespace util {
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

  /** A key space of bytes; MemoryObjectStore (lib/memory.js) is the in-memory implementation. */
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

  /** True when `value` is an object with every method in `methods`. */
  function hasMethods(value: unknown, methods: string[]): boolean;
  function assertObjectStore<T extends ObjectStore>(store: T, what?: string): T;
  function assertMessageSink<T extends MessageSink>(sink: T, what?: string): T;
  function assertMessageSource<T extends MessageSource>(source: T, what?: string): T;
}

export = util;
