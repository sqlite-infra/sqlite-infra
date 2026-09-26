// Types for the transactional queue. See lib/queue.js for the contract.

import { EventEmitter } from 'events';
import Database = require('..');
import shapes = require('../adapters/util');

declare namespace Queue {
  interface Route {
    url: string;
    /** Per-route request timeout; falls back to the queue's `timeoutMs`. */
    timeoutMs?: number;
    /** Per-route attempt budget; falls back to the queue's `attempts`. */
    attempts?: number;
  }

  interface SinkRoute {
    /** Anything with publish(envelopes): a queue adapter, HttpSink, MemoryQueue. */
    sink: shapes.MessageSink;
    /** Per-route attempt budget; falls back to the queue's `attempts`. */
    attempts?: number;
  }

  /**
   * A single URL or sink for every kind, or a map of kind -> route with `'*'`
   * as fallback. A URL string is delivered by HttpSink, one POST per job.
   */
  type Routes = string | shapes.MessageSink | { [kind: string]: string | Route | SinkRoute | shapes.MessageSink };

  interface Options {
    routes: Routes;
    /** Max requests in flight at once. Default 4. */
    concurrency?: number;
    /** Idle poll period in ms. Default 500. */
    pollMs?: number;
    /** Max jobs claimed per poll. Default 16. */
    batch?: number;
    /** Request timeout in ms. Default 10000. */
    timeoutMs?: number;
    /** Attempts before a job is dead-lettered. Default 8. */
    attempts?: number;
    /** Base backoff in ms; doubles per attempt with jitter. Default 500. */
    backoffMs?: number;
    /** Backoff ceiling in ms. Default 60000. */
    maxBackoffMs?: number;
    /** How long a claim is held before another process may take the job. Default timeoutMs + 5000. */
    leaseMs?: number;
    /** Start dispatching on construction. Default true. */
    autoStart?: boolean;
  }

  interface EnqueueOptions {
    /** Do not dispatch before this time. */
    runAt?: Date | number;
    /** Do not dispatch for this many ms. Ignored when `runAt` is given. */
    delayMs?: number;
  }

  /** A claimed job row as passed to events. `payload` is still the JSON string here. */
  interface Job {
    id: number;
    kind: string;
    payload: string;
    attempts: number;
    created_at: number;
  }

  /** A dead-lettered row as returned by dead(), with the payload parsed. */
  interface DeadJob {
    id: number;
    kind: string;
    payload: unknown;
    attempts: number;
    last_error: string | null;
    failed_at: number;
    created_at: number;
  }

  /** The JSON body delivered to a route (the `body` of an shapes.Envelope). */
  interface Envelope {
    id: number;
    kind: string;
    payload: unknown;
    attempt: number;
    enqueuedAt: number;
    source: { db: string; host?: string; pid?: number; instance?: string; service?: string; node?: string; lib?: string };
  }

  interface Events {
    delivered: [job: Job];
    retry: [job: Job, error: Error, delayMs: number];
    dead: [job: Job, error: Error];
    /** The connection was busy or closed while the dispatcher needed it; it will try again. */
    stalled: [error: Error];
  }
}

declare class Queue extends EventEmitter {
  /** Name of the table jobs live in. */
  static readonly TABLE: string;

  /** Creates the job table if needed and (unless `autoStart: false`) starts dispatching. Throws for a readonly database. */
  constructor(db: Database.Database, options: Queue.Options);

  readonly db: Database.Database;
  readonly running: boolean;
  readonly inflight: number;

  /**
   * Stores a job and returns its id. Synchronous, on the caller's connection:
   * inside db.transaction() it commits with the caller's writes or not at all.
   * Throws a TypeError for an unknown kind or an unserializable payload.
   */
  enqueue(kind: string, payload: unknown, options?: Queue.EnqueueOptions): number;

  start(): this;
  /** Stops claiming jobs; in-flight requests finish on their own. */
  stop(): this;
  /** Dispatches everything due, waits for in-flight requests, then stops. */
  drain(): Promise<void>;

  /** Jobs queued or retrying (not dead-lettered). */
  pending(): number;
  dead(): Queue.DeadJob[];
  /** Requeues a dead-lettered job with a fresh attempt budget. */
  retry(id: number): boolean;
  /** Deletes dead-lettered jobs that failed more than `olderThanMs` ago. Returns the count. */
  purge(olderThanMs?: number): number;

  on<E extends keyof Queue.Events>(event: E, listener: (...args: Queue.Events[E]) => void): this;
  once<E extends keyof Queue.Events>(event: E, listener: (...args: Queue.Events[E]) => void): this;
  off<E extends keyof Queue.Events>(event: E, listener: (...args: Queue.Events[E]) => void): this;
}

export = Queue;
