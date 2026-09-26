// Types for the consumer. See lib/consumer.js for the contract.

import { EventEmitter } from 'events';
import Database = require('..');
import shapes = require('../adapters/util');

declare namespace Consumer {
  /**
   * Runs inside a transaction with the deduplication marker. Must be
   * synchronous; a returned promise dead-letters the message.
   */
  type Handler = (payload: any, message: shapes.Message) => void;

  interface Options {
    source: shapes.MessageSource;
    handler: Handler;
    /** Messages per receive(). Default 10. */
    maxBatch?: number;
    /** Idle wait when the queue is empty or failing, in ms. Default 1000. */
    pollMs?: number;
    /** Handler failures before a message is dead-lettered. Default 8. */
    attempts?: number;
    /** Base nack delay in ms; doubles per attempt with jitter. Default 500. */
    backoffMs?: number;
    /** Nack delay ceiling in ms. Default 60000. */
    maxBackoffMs?: number;
    /** Start consuming on construction. Default true. */
    autoStart?: boolean;
  }

  interface DeadMessage {
    id: string;
    kind: string | null;
    /** The body parsed as JSON, or null when it was not JSON. */
    payload: unknown;
    body: string;
    attributes: Record<string, string>;
    attempts: number;
    last_error: string | null;
    failed_at: number;
    received_at: number;
  }

  interface Events {
    handled: [message: shapes.Message, payload: unknown];
    duplicate: [message: shapes.Message];
    retry: [message: shapes.Message, error: Error, delayMs: number];
    dead: [message: shapes.Message, error: Error];
    /** receive(), ack() or nack() failed, or the source returned a malformed message. */
    stalled: [error: Error];
  }
}

declare class Consumer extends EventEmitter {
  static readonly TABLE: string;
  /** Thrown by the consumer for failures no retry fixes; throw it from a handler for the same effect. */
  static PermanentError: typeof Error;

  constructor(db: Database.Database, options: Consumer.Options);

  readonly db: Database.Database;
  readonly running: boolean;
  /** Messages handled by this instance. */
  readonly handled: number;
  handler: Consumer.Handler;

  start(): this;
  /** Ends the loop after the receive in progress. */
  stop(): this;
  /** stop(), then resolves when the loop has exited. */
  drain(): Promise<void>;

  dead(): Consumer.DeadMessage[];
  /** Deletes handled markers older than `olderThanMs`; shrinks the deduplication window. */
  purge(olderThanMs: number): number;

  on<E extends keyof Consumer.Events>(event: E, listener: (...args: Consumer.Events[E]) => void): this;
  once<E extends keyof Consumer.Events>(event: E, listener: (...args: Consumer.Events[E]) => void): this;
  off<E extends keyof Consumer.Events>(event: E, listener: (...args: Consumer.Events[E]) => void): this;
}

export = Consumer;
