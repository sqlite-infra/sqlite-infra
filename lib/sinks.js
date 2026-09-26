'use strict';

// A sink is `{ send(events), close() }`. Three rules, and they are the
// reason this file is separate from the agent: `send` must not throw, must
// not block, and must not keep the event loop alive. A collector that is
// down, slow, or misconfigured is not this process's problem.
//
// These sinks are one-way. Nothing here reads from the socket or interprets
// a response; an HTTP reply is drained only so the socket can be released.
//
// The signal is deliberately lossy. Sockets are unref'd, so if the only
// thing left in the event loop is an in-flight report, the process exits and
// the report is dropped. That is the correct trade: the collector infers
// liveness from missed heartbeats (see lib/keepalive.js), never from a
// delivered goodbye.
//
// Note: distinct from the durable MessageSink/HttpSink in lib/http-sink.js,
// which is ref'd and must keep the process up until delivery settles. These
// keepalive sinks are the opposite: fire-and-forget and lossy by design.

const noop = () => {};
const { defaults, contentTypes, http: httpNames, metrics: METRICS } = require('./dictionary');

function createNoopSink() {
  return { send: noop, close: noop };
}

// Collects into an array instead of sending. Used by the test suite and by
// hosts that want to forward events through their own client.
function createMemorySink() {
  const sent = [];
  return {
    sent,
    send(events) { for (const event of events) sent.push(event); },
    close: noop
  };
}

function createHttpSink(url) {
  const lib = url.protocol === 'https:' ? require('https') : require('http');
  let closed = false;

  return {
    send(events) {
      if (closed) return;
      const body = Buffer.from(JSON.stringify({ events }));
      let req;
      try {
        req = lib.request({
          protocol: url.protocol,
          hostname: url.hostname,
          port: url.port || undefined,
          path: (url.pathname || '/') + (url.search || ''),
          method: 'POST',
          headers: {
            [httpNames.headers.contentType]: contentTypes.json,
            [httpNames.headers.contentLength]: String(body.length)
          }
        });
      } catch {
        return; // malformed endpoint; nothing to report and nobody to tell
      }
      req.on('error', noop);
      // A remote collector needs DNS + TCP + TLS before anything can arrive,
      // so unref'ing the instant the socket is assigned can exit the process
      // mid-handshake and drop the report (loopback is fast enough that this
      // never showed up there). Keep the socket ref'd for a short, bounded
      // grace window - or until the response/error lands, whichever is
      // first - then unref it either way so a down or slow collector still
      // can never hold the process open.
      let graceTimer = null;
      const releaseSocket = (s) => { if (graceTimer) { clearTimeout(graceTimer); graceTimer = null; } s.unref(); };
      req.on('socket', (s) => {
        graceTimer = setTimeout(() => { graceTimer = null; s.unref(); }, defaults.keepalive.sendGraceMs);
        req.once('response', () => releaseSocket(s));
        req.once('error', () => releaseSocket(s));
      });
      req.setTimeout(defaults.keepalive.httpTimeoutMs, () => req.destroy());
      // Drain the response so the socket can be released.
      req.on('response', (res) => res.resume());
      try { req.end(body); } catch { /* destroyed mid-write */ }
    },
    close() { closed = true; }
  };
}

// StatsD / DogStatsD sink. Translates keepalive events into metrics over
// UDP, so a process's presence, connection count, error rate and slow
// statements land in StatsD, Telegraf, the Datadog agent or any DogStatsD
// listener without the host writing any code — tags are the DogStatsD `#k:v,k:v`
// form, which plain StatsD ignores. Same three rules as every sink: send
// never throws, never blocks and never keeps the event loop alive (the socket
// is unref'd), so a down collector cannot affect the process.
function sanitizeTag(value) {
  // StatsD tags cannot contain '|', ',', ':' or '#'; keep them printable.
  return String(value == null ? '' : value).replace(/[|,:#\s]/g, '_');
}

function createStatsdSink(url) {
  const dgram = require('dgram');
  const host = url.hostname || '127.0.0.1';
  const port = Number(url.port) || 8125;
  const prefix = url.searchParams.get('prefix') || METRICS.prefix;
  const S = METRICS.suffixes;
  const T = METRICS.tagKeys;
  let socket = null;
  let closed = false;

  const ensure = () => {
    if (!socket && !closed) { socket = dgram.createSocket('udp4'); socket.on('error', noop); socket.unref(); }
    return socket;
  };

  const linesFor = (e) => {
    const base = `#${T.service}:${sanitizeTag(e.service)},${T.host}:${sanitizeTag(e.host)},${T.instance}:${sanitizeTag(e.instance)}`;
    const out = [];
    if (typeof e.connections === 'number') out.push(`${prefix}.${S.connections}:${e.connections}|g|${base}`);
    if (typeof e.rss === 'number') out.push(`${prefix}.${S.rss}:${Math.round(e.rss)}|g|${base}`);
    if (typeof e.dropped === 'number' && e.dropped > 0) out.push(`${prefix}.${S.dropped}:${e.dropped}|c|${base}`);
    if (e.errors && typeof e.errors === 'object') {
      for (const [code, count] of Object.entries(e.errors)) out.push(`${prefix}.${S.errors}:${count}|c|${base},${T.code}:${sanitizeTag(code)}`);
    }
    if (e.type === 'slow' && typeof e.ms === 'number') out.push(`${prefix}.${S.slow}:${Math.round(e.ms)}|ms|${base}`);
    out.push(`${prefix}.${S.lifecycle}:1|c|${base},${T.event}:${sanitizeTag(e.type)}`);
    return out;
  };

  return {
    send(events) {
      if (closed) return;
      const sock = ensure();
      if (!sock) return;
      const lines = [];
      for (const e of events) lines.push(...linesFor(e));
      if (lines.length === 0) return;
      const payload = Buffer.from(lines.join('\n'));
      try { sock.send(payload, port, host, noop); } catch { /* fire and forget */ }
    },
    close() {
      closed = true;
      if (socket) { try { socket.close(); } catch { /* already closed */ } socket = null; }
    }
  };
}

function createSink(endpoint) {
  let url;
  try {
    url = new URL(endpoint);
  } catch {
    return null;
  }
  switch (url.protocol) {
    case 'http:':
    case 'https:':
      return createHttpSink(url);
    case 'statsd:':
    case 'udp:':
      return createStatsdSink(url);
    case 'noop:':
      return createNoopSink();
    case 'memory:':
      return createMemorySink();
    default:
      return null;
  }
}

module.exports = {
  createSink,
  createNoopSink,
  createMemorySink,
  createStatsdSink
};
