// Types for the sync. See lib/sync.js for the contract.

import { EventEmitter } from 'events';
import Database = require('..');
import interfaces = require('./interfaces');

declare namespace Sync {
  interface Options {
    store: interfaces.ObjectStore;
    /** Key prefix; the database id (a hash) is appended. Default ''. */
    prefix?: string;
    /** Gzip snapshots and row exports. Default true. */
    gzip?: boolean;
    /** Record changesets. Default: whenever the engine has sessions; `true` throws where it does not. */
    changesets?: boolean;
    /** start(): tick period in ms. Required for start(). */
    everyMs?: number;
    /** start(): take a snapshot after this many changesets. */
    snapshotEvery?: number;
    /** How long one process holds the tick lease. Default 600000. */
    leaseMs?: number;
    /** Call start() from the constructor. Default false. */
    autoStart?: boolean;
  }

  interface SnapshotInfo {
    key: string;
    generation: number;
    size: number;
    sha256: string;
    at: string;
    gzip: boolean;
  }

  interface ChangesetInfo {
    key: string;
    generation: number;
    seq: number;
    size: number;
    sha256: string;
  }

  interface RowExportOptions {
    /** SQL after WHERE, written by the caller. */
    where?: string;
    params?: unknown[];
    columns?: string[];
    /** Applied to the encoded row; return null to drop it. */
    transform?: (row: Record<string, unknown>) => Record<string, unknown> | null | undefined;
    /** false: no `dt=YYYY-MM-DD/` segment in the key. */
    partition?: boolean;
  }

  interface RowExportInfo {
    key: string;
    schemaKey: string;
    table: string;
    rows: number;
    size: number;
    sha256: string;
    at: string;
    gzip: boolean;
  }

  interface Manifest {
    version: 1;
    dbId: string;
    generation: number;
    seq: number;
    snapshot: SnapshotInfo | null;
    updatedAt: string;
    lib: string;
  }

  interface Keys {
    manifest: string;
    snapshots: string;
    changesets: string;
    exports: string;
  }

  interface Events {
    snapshot: [info: SnapshotInfo];
    changeset: [info: ChangesetInfo];
    export: [info: RowExportInfo];
    /** Sessions have no record of the current schema; start() will snapshot. */
    'schema-changed': [];
    /** A scheduled tick failed. */
    error: [error: Error];
  }
}

declare class Sync extends EventEmitter {
  static readonly TABLES: { readonly SYNC: string; readonly PENDING: string };

  constructor(db: Database.Database, options: Sync.Options);

  readonly db: Database.Database;
  readonly store: interfaces.ObjectStore;
  readonly dbId: string;
  readonly base: string;
  readonly gzip: boolean;
  readonly changesets: boolean;
  readonly running: boolean;
  readonly generation: number;
  readonly seq: number;
  readonly schemaChanged: boolean;

  keys(): Sync.Keys;
  /** Changesets cut but not yet uploaded. */
  pending(): number;
  /** User tables the session extension cannot record (no explicit PRIMARY KEY). */
  unsupportedTables(): string[];

  snapshot(): Promise<Sync.SnapshotInfo>;
  /** Null when nothing changed and nothing was pending. */
  changeset(): Promise<Sync.ChangesetInfo | null>;
  exportRows(table: string, options?: Sync.RowExportOptions): Promise<Sync.RowExportInfo>;
  manifest(): Promise<Sync.Manifest | null>;

  start(): this;
  stop(): this;
  /** Waits for the operation in progress, releases the lease, stops. */
  drain(): Promise<void>;
  /** stop() and close the sessions. */
  close(): void;

  on<E extends keyof Sync.Events>(event: E, listener: (...args: Sync.Events[E]) => void): this;
  once<E extends keyof Sync.Events>(event: E, listener: (...args: Sync.Events[E]) => void): this;
  off<E extends keyof Sync.Events>(event: E, listener: (...args: Sync.Events[E]) => void): this;
}

export = Sync;
