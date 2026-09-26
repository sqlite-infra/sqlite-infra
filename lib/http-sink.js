'use strict';

// HttpSink: a MessageSink that delivers each envelope as one HTTP POST.
// One of the concrete implementations of the shapes in adapters/util.js, and
// the only one that touches the network. HTTP has no batch, so envelopes go
// out concurrently and their outcomes are collected in order.

const http = require('http');
const https = require('https');
const { outcomes, contentTypes, http: httpNames } = require('./dictionary');

// Status codes that mean "try again later" even though they are 4xx.
const RETRYABLE_4XX = new Set([408, 425, 429]);

// Maps an HTTP status to an outcome: 2xx ok, 5xx/408/425/429 retry, other 4xx dead.
function outcomeForStatus(id, status) {
  if (status >= 200 && status < 300) return { id, status: outcomes.ok };
  const permanent = status >= 400 && status < 500 && !RETRYABLE_4XX.has(status);
  return { id, status: permanent ? outcomes.dead : outcomes.retry, error: `HTTP ${status}` };
}

// One HTTP POST. Resolves { status } or rejects on network error/timeout.
// The socket is ref'd (the default): while a delivery is in flight the
// process must stay up, unlike the keepalive sink (lib/sinks.js).
function post(url, body, headers, timeoutMs) {
  const lib = url.protocol === 'https:' ? https : http;
  return new Promise((resolve, reject) => {
    const req = lib.request({
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port || undefined,
      path: (url.pathname || '/') + (url.search || ''),
      method: 'POST',
      headers: Object.assign({
        [httpNames.headers.contentType]: contentTypes.json,
        [httpNames.headers.contentLength]: String(body.length)
      }, headers)
    });
    let settled = false;
    const done = (fn, value) => { if (!settled) { settled = true; fn(value); } };
    req.setTimeout(timeoutMs, () => { req.destroy(new Error(`timed out after ${timeoutMs}ms`)); });
    req.on('error', (err) => done(reject, err));
    req.on('response', (res) => {
      // Drain so the socket is reusable; the body is not interpreted.
      res.resume();
      res.on('end', () => done(resolve, { status: res.statusCode }));
      res.on('error', (err) => done(reject, err));
    });
    req.end(body);
  });
}

// Delivers each envelope as one POST with the attributes as headers.
class HttpSink {
  constructor(url, options = {}) {
    const parsed = typeof url === 'string' ? new URL(url) : url;
    if (!(parsed instanceof URL)) throw new TypeError('Expected a URL string or URL');
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new TypeError(`HttpSink needs an http: or https: URL, got ${parsed.protocol}`);
    }
    this.url = parsed;
    this.timeoutMs = options.timeoutMs || 10000;
  }

  publish(envelopes) {
    return Promise.all(envelopes.map((envelope) =>
      post(this.url, Buffer.from(envelope.body), envelope.attributes || {}, this.timeoutMs)
        .then(({ status }) => outcomeForStatus(envelope.id, status),
          (err) => ({ id: envelope.id, status: outcomes.retry, error: err }))));
  }
}

module.exports = { HttpSink, outcomeForStatus };
