// Types for import. See lib/importer.js for the contract.

import Database = require('..');
import shapes = require('../adapters/util');

declare namespace importer {
  type ConflictResolution = 'abort' | 'replace' | 'omit' | ((conflictType: number) => number);

  interface RestoreOptions {
    store: shapes.ObjectStore;
    /** The sync's prefix. Default ''. */
    prefix?: string;
    /** The database id in the manifest key (`Sync#dbId`). */
    dbId: string;
    /** Destination path, replaced atomically on success. */
    to: string;
    /** Default 'abort'. */
    onConflict?: ConflictResolution;
    /** Stop after this changeset sequence number. */
    upToSeq?: number;
    /** Default true. false skips hash checks and emits a warning. */
    verify?: boolean;
  }

  interface RestoreResult {
    generation: number;
    seq: number;
    snapshotKey: string;
    applied: string[];
    to: string;
  }

  interface LoadOptions {
    store: shapes.ObjectStore;
    /** Object to read; `.gz` keys are gunzipped. */
    key: string;
    table: string;
    /** Default from the key: `.csv` is CSV, anything else NDJSON. */
    format?: 'ndjson' | 'csv';
    /** Default 'insert'. */
    mode?: 'insert' | 'replace' | 'upsert';
    /** Rows per transaction. Default 500. */
    batch?: number;
    /** CSV without a header row: column names in order. */
    columns?: string[];
    /** Before validation; return null to drop the row. */
    transform?: (row: Record<string, unknown>, line: number) => Record<string, unknown> | null | undefined;
    /** Continue where a previous run of this key stopped. Default true. */
    resume?: boolean;
    /** CSV: field text meaning NULL. Default ''. */
    nullValue?: string;
    /** Default 16 MiB. */
    maxLineBytes?: number;
  }

  interface LoadResult {
    /** Rows written by this call. */
    rows: number;
    /** Lines skipped because a previous run had committed them. */
    skipped: number;
    /** Lines read in total. */
    lines: number;
    done: boolean;
    /** True when the key had already been fully loaded. */
    resumed?: boolean;
  }

  /** Newest generation of `dbId`: verified snapshot plus its changesets, written to `to`. */
  function restore(options: RestoreOptions): Promise<RestoreResult>;
  /** NDJSON or CSV rows from one object into an existing table, resumably. */
  function load(db: Database.Database, options: LoadOptions): Promise<LoadResult>;
  const TABLE: string;
}

export = importer;
