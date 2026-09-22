'use strict';

// Dictionary: the one place this library spells its strings.
// 
// const { dictionary: d } = require('better-sqlite3');
//
// Two tables are sources in their own right and are only re-exported here:
// lib/codes.js (SQLite result code names, generated from sqlite3.h) feeds
// `errors` and `sqliteCodes`. Everything else is defined below and nowhere
// else; test-dictionary.js fails if a module spells one of these literals
// itself. The objects are deeply frozen, so a host cannot redefine a name
// for the rest of the process by accident.
//
// This module must stay a leaf: it may import lib/codes.js and nothing that
// imports it back, or a require cycle hands a module an empty object.
//
// Sections:
//   env            environment variables the library reads
//   defaults       what applies when an option or variable is not set
//   tables         tables the library creates in the host's database
//   events         EventEmitter event names per module; keepalive event types
//   queue          route fallback key; envelope attribute names (HTTP header names)
//   outcomes       a MessageSink's verdict per envelope
//   importer       load modes, formats, conflict resolutions, error codes
//   store          ObjectStore error codes
//   keys           the layout of a Sync's keys under its prefix
//   fallbacks      the "default" identifier a provider uses when given no name
//   contentTypes   media types the library writes; contentEncodings
//   http           header names the library sends
//   changeset      the framing magic of a changeset object
//   collision      the runtime driver-collision check's modes and package.json keys
//   errors         SQLite result code names a host is likely to branch on
//
// `find(value)` reverses the lookup: it returns the dotted path(s) of an
// exact string, which is handy when a log line shows the wire value and
// the reader wants its name.

const { NAMES } = require('./codes');

function deepFreeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const key of Object.keys(value)) deepFreeze(value[key]);
  }
  return value;
}

const tables = {
  prefix: '_better_sqlite_',          // every table below starts with this; sessions skip them
  queue: '_better_sqlite_outbox',
  consumer: '_better_sqlite_inbox',
  syncState: '_better_sqlite_sync',
  syncPending: '_better_sqlite_sync_pending',
  importer: '_better_sqlite_import',
  migrations: '_better_sqlite_migrations',
  schedules: '_better_sqlite_schedules',
  audit: '_better_sqlite_audit',
  locks: '_better_sqlite_locks',
  kv: '_better_sqlite_kv'
};

const events = {
  // Types on the `type` field of a keepalive event (lib/keepalive.js).
  keepalive: {
    install: 'install',
    register: 'register',
    open: 'open',
    heartbeat: 'heartbeat',
    slow: 'slow',
    close: 'close',
    deregister: 'deregister'
  },
  // Database.Queue is an EventEmitter.
  queue: {
    delivered: 'delivered',   // (job)
    retry: 'retry',           // (job, err, delayMs)
    dead: 'dead',             // (job, err)
    stalled: 'stalled'        // (err): the database went away or a claim failed
  },
  // Database.Consumer is an EventEmitter.
  consumer: {
    handled: 'handled',       // (message, payload)
    duplicate: 'duplicate',   // (message): already seen, acked without running
    retry: 'retry',           // (message, err, delayMs)
    dead: 'dead',             // (message, err)
    stalled: 'stalled'        // (err)
  },
  // Sync (lib/sync.js) is an EventEmitter.
  sync: {
    snapshot: 'snapshot',            // (info)
    changeset: 'changeset',          // (info)
    export: 'export',                // (result)
    schemaChanged: 'schema-changed', // ()
    error: 'error'                   // (err): a scheduled tick failed; attach a listener
  }
};

const queue = {
  // Route key that receives every kind without a route of its own.
  anyRoute: '*',
  // Envelope attributes. HttpSink sends them as request headers under these
  // exact names (HTTP header names are case-insensitive); queue sinks send
  // them as message attributes.
  attributes: {
    idempotencyKey: 'idempotency-key',
    kind: 'x-job-kind',
    attempt: 'x-job-attempt'
  }
};

// One per envelope from a MessageSink, and how HttpSink maps responses.
const outcomes = {
  ok: 'ok',          // 2xx: job deleted
  retry: 'retry',    // 5xx, 408, 425, 429, timeout, network error: backoff
  dead: 'dead'       // any other 4xx: dead-lettered at once
};

const importer = {
  modes: {
    insert: 'insert',
    replace: 'replace',
    upsert: 'upsert'
  },
  formats: {
    ndjson: 'ndjson',
    csv: 'csv'
  },
  onConflict: {
    abort: 'abort',
    replace: 'replace',
    omit: 'omit'
  },
  errors: {
    integrity: 'EINTEGRITY'   // a snapshot or changeset did not match its recorded hash
  }
};

const store = {
  errors: {
    notFound: 'ENOENT'        // get() of a missing key
  }
};

// Layout under `<prefix>/<dbId>/`. Directories end in '/'.
const keys = {
  manifest: 'manifest.json',
  snapshots: 'snapshots/',
  changesets: 'changesets/',
  exports: 'exports/',
  schema: 'schema.json',           // sidecar next to a table's row exports
  partition: 'dt=',                // `exports/<table>/dt=<YYYY-MM-DD>/`
  extensions: {
    sqlite: '.sqlite',             // a snapshot; `.gz` follows when gzipped
    changeset: '.changeset',
    ndjson: '.ndjson',             // a row export; `.gz` follows when gzipped
    gzip: '.gz'
  }
};

// The literal "default": the identifier a provider falls back to when none is
// given. One word so no provider spells it itself - a FIFO message group or
// event bus with none set (adapters/), and whatever a future adapter names the
// same way.
const fallbacks = {
  name: 'default'
};

const contentTypes = {
  gzip: 'application/gzip',              // gzipped snapshot
  sqlite: 'application/vnd.sqlite3',     // raw snapshot
  binary: 'application/octet-stream',    // changeset
  ndjson: 'application/x-ndjson',        // row export
  json: 'application/json'               // manifest, schema sidecar, HTTP bodies
};

const contentEncodings = {
  gzip: 'gzip'
};

const http = {
  headers: {
    contentType: 'content-type',
    contentLength: 'content-length'
  }
};

const changeset = {
  magic: 'BSQC'
};

// Metric names the StatsD/DogStatsD keepalive sink emits (lib/sinks.js),
// under `prefix`, with `tagKeys` naming the dimensions on each line.
const metrics = {
  prefix: 'better_sqlite',
  suffixes: {
    connections: 'connections',   // gauge
    rss: 'rss',                    // gauge (bytes)
    errors: 'errors',             // counter, tagged by code
    slow: 'statement_ms',         // timing (ms)
    lifecycle: 'lifecycle',       // counter, tagged by event
    dropped: 'events_dropped'     // counter
  },
  tagKeys: { service: 'service', host: 'host', instance: 'instance', code: 'code', event: 'event' }
};

const collision = {
  modes: {
    error: 'error',
    warn: 'warn',
    off: 'off'
  },
  // Consumer package.json: `"better-sqlite3": { "collisions": "warn", "allow": ["sqlite3"] }`.
  // The key is this package's name; lib/collision.js reads it from package.json.
  fields: {
    mode: 'collisions',
    allow: 'allow'
  }
};

// SQLite result code names a host is likely to branch on. The full table is
// lib/codes.js; these are the ones the README and the tests reach for.
const errors = {
  ok: 'SQLITE_OK',
  error: 'SQLITE_ERROR',
  busy: 'SQLITE_BUSY',
  locked: 'SQLITE_LOCKED',
  readonly: 'SQLITE_READONLY',
  ioError: 'SQLITE_IOERR',
  corrupt: 'SQLITE_CORRUPT',
  full: 'SQLITE_FULL',
  cantOpen: 'SQLITE_CANTOPEN',
  schema: 'SQLITE_SCHEMA',
  tooBig: 'SQLITE_TOOBIG',
  constraint: 'SQLITE_CONSTRAINT',
  mismatch: 'SQLITE_MISMATCH',
  misuse: 'SQLITE_MISUSE',
  auth: 'SQLITE_AUTH',
  range: 'SQLITE_RANGE',
  notADatabase: 'SQLITE_NOTADB',
  // Constraint kinds, the ones a data-entry path usually catches.
  check: 'SQLITE_CONSTRAINT_CHECK',
  foreignKey: 'SQLITE_CONSTRAINT_FOREIGNKEY',
  notNull: 'SQLITE_CONSTRAINT_NOTNULL',
  primaryKey: 'SQLITE_CONSTRAINT_PRIMARYKEY',
  unique: 'SQLITE_CONSTRAINT_UNIQUE',
  trigger: 'SQLITE_CONSTRAINT_TRIGGER',
  // What `err.code` is when the engine reports a code lib/codes.js has never seen.
  unknown: 'UNKNOWN_SQLITE_ERROR'
};

const env = {
  // Keepalive (lib/keepalive.js). Read once, when the first connection opens.
  keepaliveInterval: 'BETTER_SQLITE_KEEPALIVE_INTERVAL',
  keepaliveQueue: 'BETTER_SQLITE_KEEPALIVE_QUEUE',
  keepaliveSlowMs: 'BETTER_SQLITE_KEEPALIVE_SLOW_MS',
  keepaliveService: 'BETTER_SQLITE_KEEPALIVE_SERVICE',
  keepaliveEndpoint: 'BETTER_SQLITE_KEEPALIVE_ENDPOINT',
  // Runtime driver-collision check (lib/collision.js).
  collision: 'BETTER_SQLITE_COLLISION',
  // Region for the `region` field, read by lib/region.js. These are the standard
  // variables the SDK reads, in this precedence order. No file is consulted.
  region: 'AWS_REGION',
  regionDefault: 'AWS_DEFAULT_REGION'
};

const defaults = {
  database: {
    timeoutMs: 5000        // busy_timeout, as in better-sqlite3
  },
  queue: {
    concurrency: 4,
    pollMs: 500,
    batch: 16,
    timeoutMs: 10000,
    attempts: 8,
    backoffMs: 500,
    maxBackoffMs: 60000,
    leaseExtraMs: 5000     // leaseMs defaults to timeoutMs + this
  },
  consumer: {
    maxBatch: 10,
    pollMs: 1000,
    attempts: 8,
    backoffMs: 500,
    maxBackoffMs: 60000
  },
  sync: {
    gzip: true,
    leaseMs: 10 * 60 * 1000
  },
  importer: {
    batch: 500,
    maxLineBytes: 16 * 1024 * 1024,
    maxObjectBytes: 64 * 1024 * 1024 * 1024,
    maxManifestBytes: 16 * 1024 * 1024
  },
  keepalive: {
    // Where keepalive events go when `env.keepaliveEndpoint` is unset.
    endpoint: 'http://127.0.0.1:4318',
    intervalMs: 30000,
    minIntervalMs: 1000,
    queue: 256,
    slowMs: 0,
    sqlMaxChars: 300,      // a slow statement's SQL is cut here before sending
    httpTimeoutMs: 5000,   // an HTTP sink gives up on a report after this
    // A remote collector needs DNS + TCP + TLS before the first byte moves,
    // unlike the loopback default. Unref'ing the socket the instant it is
    // assigned (before any of that runs) means a short-lived process can
    // exit mid-handshake and silently drop the report. Hold the socket
    // ref'd for this long - or until the response/error lands, whichever
    // is first - so a real round trip gets a fair chance; after that it is
    // unref'd exactly as before and the send is abandoned like any other
    // best-effort report.
    sendGraceMs: 1000
  },
  collision: 'error'
};

const dictionary = deepFreeze({
  env,
  defaults,
  tables,
  events,
  queue,
  outcomes,
  importer,
  store,
  keys,
  fallbacks,
  contentTypes,
  contentEncodings,
  http,
  changeset,
  metrics,
  collision,
  errors
});

// Dotted paths of every leaf equal to `value`, e.g.
// find('retry') -> ['events.queue.retry', 'events.consumer.retry', 'outcomes.retry'].
function find(value, node = dictionary, path = []) {
  const out = [];
  for (const key of Object.keys(node)) {
    const child = node[key];
    if (child && typeof child === 'object') out.push(...find(value, child, path.concat(key)));
    else if (child === value) out.push(path.concat(key).join('.'));
  }
  return out;
}

// Every SQLite result code name, for a host that wants the full table under
// the same import. Read-only view of lib/codes.js.
const sqliteCodes = Object.freeze(Object.values(NAMES));

module.exports = Object.freeze(Object.assign({}, dictionary, { find, sqliteCodes }));
