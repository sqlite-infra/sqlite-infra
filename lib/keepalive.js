'use strict';

// Keepalive: presence and health signal for the internal platform.
//
// This is an internal package, used by our own servicesy.
// The platform keeps the servers and services that support a running process
// asleep until something needs them. Holding a live SQLite connection is that
// signal: 
// it means the process has state worth protecting and work that can be taken
// off it. So a connection announces itself here, the platform wakes what
// supports it and hands heavy tasks to the services meant for them, and
// when heartbeats stop the platform scales that support back down. Without
// this signal every service would have to be wired in by hand, and the ones
// that were not would run unsupported.
//
// This file is the keepalive domain: it decides which events exist,
// what each one reports, the hooks the rest of the library calls, and the
// heartbeat metrics. The generic transport underneath it (the bounded queue,
// the batched flush, the unref'd timer, the sink, the connection registry, the
// identity block and configuration) lives in lib/presence.js; this module
// drives that agent and owns the process-wide singleton.
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

const path = require('path');
const dictionary = require('./dictionary');
const util = require('../adapters/util');
const { databaseId } = require('./identity');
const { createAgent, REGION_PLACEHOLDER } = require('./presence');
const { createSink } = require('./sinks');

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

// Configuration is environment-only, so the platform can retarget or kill it
// without a code change or a redeploy of the caller. Keepalive is opt-in: with
// no endpoint named in the environment this returns null, ensureConfigured()
// reports "not configured", and the subsystem stays completely dark - no timer
// starts and no packet (not even the on-import install event) is ever sent. A
// collector is contacted only when the operator explicitly points at one via
// BETTER_SQLITE_KEEPALIVE_ENDPOINT; there is no built-in default target.
function readEnv(env) {
  const endpoint = String(env[ENV.keepaliveEndpoint] || '').trim();
  if (!endpoint) return null;
  const interval = Number(env[ENV.keepaliveInterval]);
  const queue = Number(env[ENV.keepaliveQueue]);
  const slowMs = Number(env[ENV.keepaliveSlowMs]);
  const service = String(env[ENV.keepaliveService] || '').trim();
  return {
    endpoint,
    interval: Number.isFinite(interval) && interval >= MIN_INTERVAL ? interval : DEFAULT_INTERVAL,
    queue: Number.isFinite(queue) && queue > 0 ? Math.floor(queue) : DEFAULT_QUEUE,
    slowMs: Number.isFinite(slowMs) && slowMs > 0 ? slowMs : 0,
    service: service || defaultService()
  };
}

// Assemble the process-wide keepalive agent: a generic reporting agent from
// lib/presence.js, plus the keepalive fields defined here. The configuration
// layer above (constants and environment parsing) is injected into the agent as
// `deps`; the below drives it through report()/beginBeat()/sweep(). The
// environment is read here, on the consumer side, and handed to the agent
// already parsed: lib/presence.js itself never touches process.env. The
// `region` stamped on events is a placeholder unless a host passes its own
// string through configure({ region }).
function build() {
  const agent = createAgent({
    BATCH_MS, DEFAULT_INTERVAL, MIN_INTERVAL, DEFAULT_QUEUE,
    defaultService, libVersion,
    resolveConfig: () => readEnv(process.env),
    region: REGION_PLACEHOLDER,
    createSink
  });

  // Keepalive-specific state, updated by the hooks and drained by the
  // heartbeat. The transport counters (queue, dropped, connections) live on the
  // agent itself.
  agent.errors = new Map();   // SQLite result-code name -> count since last beat
  agent.slowSeen = 0;         // slow statements this window (reported and dropped)
  agent.slowSent = 0;

  // ---- lifecycle events --------------------------------------------------

  function start() {
    // beginBeat() starts the unref'd heartbeat and reports whether it actually
    // started, so a redundant open does not emit a second register.
    if (agent.beginBeat(beat)) {
      agent.report(EVENT.register, { connections: agent.sweep() });
    }
  }

  function stop() {
    if (!agent.started) return;
    // Best-effort farewell so the collector can scale support down promptly
    // after a clean shutdown. Reported before the timer is torn down and sent
    // by the synchronous flush() below; the sink's socket is unref'd, so a
    // farewell in flight never holds the process open (see the header).
    agent.report(EVENT.deregister, { connections: agent.sweep(), uptimeMs: Date.now() - agent.startedAt });
    agent.endBeat();
    agent.flush();
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

    agent.report(EVENT.heartbeat, {
      connections: agent.sweep(),
      uptimeMs: Date.now() - agent.startedAt,
      rss: process.memoryUsage.rss(),
      errors,
      slowSeen,
      dropped
    });
    agent.flush();
  }

  // ---- hooks called from the rest of the library -------------------------

  const onOpen = agent.safe(function onOpen(db, state) {
    if (!agent.ensureConfigured()) return;
    const ref = new WeakRef(db);
    state.keepaliveRef = ref;
    agent.live.add(ref);
    if (!agent.started) start();
    else agent.report(EVENT.open, { connections: agent.sweep(), db: databaseId(db.name, db.memory), readonly: db.readonly });
  });

  const onClose = agent.safe(function onClose(db, state) {
    if (!agent.config) return;
    if (state.keepaliveRef) {
      agent.live.delete(state.keepaliveRef);
      state.keepaliveRef = null;
    }
    if (agent.sweep() === 0) stop();
    else agent.report(EVENT.close, { connections: agent.live.size, db: databaseId(db.name, db.memory) });
  });

  // Called from adapters/util.js for every error SQLite itself raises, which is
  // the one chokepoint every failing query already passes through.
  const onError = agent.safe(function onError(code) {
    if (!agent.config || !agent.started) return;
    agent.errors.set(code, (agent.errors.get(code) || 0) + 1);
  });

  // Called only when agent.SLOW is true; the caller checks the flag so a
  // disabled agent costs one property read per statement and no closure.
  const onStatement = agent.safe(function onStatement(sql, ms) {
    if (!agent.config || !agent.started || ms < agent.config.slowMs) return;
    agent.slowSeen += 1;
    if (agent.slowSent >= SLOW_PER_WINDOW) return;
    agent.slowSent += 1;
    agent.report(EVENT.slow, { ms: Math.round(ms), sql: String(sql).slice(0, SQL_MAX) });
  });

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
  const reset = agent.safe(function reset() {
    agent.teardown();
    agent.live.clear();
    agent.errors.clear();
    agent.slowSeen = 0;
    agent.slowSent = 0;
    agent.warned = false;
    agent.resolved = false;
    agent.config = null;
    agent.identity = null;
    agent.SLOW = false;
  });

  function describe() {
    return {
      config: agent.config ? util.describeConfig(agent.config) : { status: 'not configured' },
      state: util.describeState(agent)
    };
  }

  Object.assign(agent, {
    onOpen, onClose, onError, onStatement,
    start, stop, snapshot, reset, describe
  });

  // Best-effort farewell so the platform can scale support down promptly after
  // a clean exit. 'beforeExit' does not fire on process.exit() or on a signal,
  // which is why the collector cannot rely on it (see the header). This listener
  // is the only thing requiring this module registers.
  process.on('beforeExit', () => { try { if (agent.started) stop(); } catch { /* ignore */ } });

  // On import, if - and only if - the operator has opted in by naming an
  // endpoint, announce once that the driver is now loaded in this process,
  // before any connection opens, so the collector can expect the keepalive
  // stream (and the errors) that follow. With no endpoint configured,
  // ensureConfigured() returns false and this sends nothing at all: import is
  // silent and networkless by default. Wrapped so importing the package can
  // never fail here.
  try {
    if (agent.ensureConfigured()) {
      agent.report(EVENT.install, { connections: 0 });
      agent.flush();
    }
  } catch { /* import must never fail because of keepalive */ }

  return agent;
}

// There is one agent per process (see KEY), even when two copies of this
// package land in one dependency tree.
module.exports = globalThis[KEY] || (globalThis[KEY] = build());
