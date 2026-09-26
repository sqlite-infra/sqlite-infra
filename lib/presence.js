'use strict';

// Generic reporting agent: a self-contained transport and lifecycle mechanism
// for batched, best-effort reporting. It owns the bounded queue, the batched
// flush, the unref'd heartbeat timer, the sink, the WeakRef registry for the
// resources it tracks, the identity block stamped on every event, configuration
// and error containment.
//
// It holds no domain knowledge of its own: the caller decides which events
// exist, what each one reports, and when to emit them, driving the agent through
// the methods it returns (report, beginBeat/endBeat, sweep, flush,
// ensureConfigured, safe) and the shared state on the agent object.
//
// Three invariants hold everywhere below. 1: never break the caller — every
// entry point swallows its own errors and warns at most once per process. 2:
// never keep the process alive — every timer and socket is unref'd. 3: never
// block — events go through a bounded queue that drops the oldest rather than
// growing without limit.

const { validateSchema } = require('../adapters/util');

// Stamped as `region` on every event when no consumer supplied one. This
// module never reads the environment: the region string is passed in by the
// consumer (via `deps.region` or configure({ region })) or stays a placeholder.
const REGION_PLACEHOLDER = 'placeholder';

// Built by the caller, which injects the transport constants and the
// configuration helpers via `deps` (batch/interval/queue defaults,
// defaultService/libVersion, an optional no-argument `resolveConfig` that
// returns the consumer's default configure() options or null, an optional
// `region` string, and an optional `createSink` factory for turning a
// configured `endpoint` string into a sink — this module has no concept of
// what a sink is or how to build one from a URL; it only knows the
// `{send, close}` shape). A caller that only ever supplies an explicit `sink`
// in configure() can omit createSink. Nothing here touches process.env:
// whatever the consumer wants read from the environment, it reads itself and
// hands over through resolveConfig / region.
function createAgent(deps) {
  const {
    BATCH_MS, DEFAULT_INTERVAL, MIN_INTERVAL, DEFAULT_QUEUE,
    defaultService, libVersion, resolveConfig, createSink
  } = deps;
  const defaultRegion = typeof deps.region === 'string' && deps.region.trim()
    ? deps.region.trim() : REGION_PLACEHOLDER;

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
    beatTimer: null,
    flushTimer: null,
    warned: false,
    startedAt: 0
  };

  // ---- error containment -------------------------------------------------

  // This agent deliberately lies about its own health rather than propagate a
  // failure into the caller's path; everywhere else in this package the goal is
  // the opposite: report faithfully. The caller wraps its own hooks with this
  // same guard.
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
        `better-sqlite3 presence agent disabled after an internal error: ${err && err.message}`,
        'BetterSqlitePresenceWarning'
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
    const fromConsumer = resolveConfig ? resolveConfig() : null;
    if (!fromConsumer) return false;
    configure(fromConsumer);
    return Boolean(agent.config);
  }

  // ---- queue and flush ---------------------------------------------------

  // Enqueue one event of `type` carrying `fields`, stamped with the identity
  // block. The caller reports every event through this.
  function report(type, fields) {
    const max = agent.config.queue;
    if (agent.queue.length >= max) {
      // Drop the oldest: the freshest picture of the process is worth more
      // to an operator than the stalest, and the drop count is reported so
      // the loss is visible rather than silent.
      agent.queue.shift();
      agent.dropped += 1;
    }
    agent.queue.push(Object.assign({ type }, identity(), fields));
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

  // ---- heartbeat timer ---------------------------------------------------

  // Start the unref'd heartbeat interval that calls `tick`. Returns true if it
  // actually started (false if already running), so the domain layer knows
  // whether to emit its register event. The unref is the single most important
  // line in the subsystem: without it a heartbeat keeps the event loop alive
  // and every consumer's test run hangs.
  function beginBeat(tick) {
    if (agent.started) return false;
    agent.started = true;
    agent.startedAt = Date.now();
    agent.beatTimer = setInterval(tick, agent.config.interval);
    agent.beatTimer.unref();
    return true;
  }

  function endBeat() {
    if (agent.beatTimer) { clearInterval(agent.beatTimer); agent.beatTimer = null; }
    agent.started = false;
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

  // ---- connection registry -----------------------------------------------

  // Drops WeakRefs whose Database has been collected, so a caller who never
  // calls close() cannot inflate the connection count forever. This is why
  // the registry holds WeakRefs and not the objects themselves.
  function sweep() {
    for (const ref of agent.live) {
      if (ref.deref() === undefined) agent.live.delete(ref);
    }
    return agent.live.size;
  }

  // ---- identity ----------------------------------------------------------

  // The block stamped onto every event. Only `region` is reported; instance,
  // service, host, pid, node, lib and context are deliberately not sent. The
  // region is whatever the consumer passed (configure({ region }) wins over
  // deps.region), never something read from the environment here.
  function identity() {
    if (!agent.identity) {
      agent.identity = {
        // Resolved once per configure() and cached.
        region: agent.config && agent.config.region ? agent.config.region : defaultRegion
      };
    }
    return agent.identity;
  }

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
    region: {
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
    const validation = validateSchema(options, configSchema);
    if (!validation.valid) {
      warnOnce(new Error(`Invalid presence options: ${validation.errors.map(e => `${e.field}: ${e.error}`).join('; ')}`));
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
      region: typeof options.region === 'string' && options.region.trim()
        ? options.region.trim() : defaultRegion,
      context: options.context && typeof options.context === 'object'
        ? Object.assign({}, options.context) : null
    };
    const sink = options.sink || (config.endpoint && createSink ? createSink(config.endpoint) : null);
    if (!sink) { agent.config = null; agent.SLOW = false; return agent; }

    agent.config = config;
    agent.sink = sink;
    agent.identity = null;
    agent.SLOW = config.slowMs > 0;
    // A connection may already be open when a host configures late. start() is
    // supplied by the caller.
    if (agent.live.size > 0) agent.start();
    return agent;
  });

  return Object.assign(agent, {
    safe, ensureConfigured, configure, report, flush,
    scheduleFlush, beginBeat, endBeat, teardown, identity, sweep
  });
}

module.exports = { createAgent, REGION_PLACEHOLDER };
