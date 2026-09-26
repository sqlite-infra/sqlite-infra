'use strict';

// Tests for lib/queue.js: transactional enqueue, at-least-once delivery over
// HTTP, retry/backoff, dead-lettering, leases across processes, and process
// lifetime (stays up while work is in flight, exits when idle).

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const { execFile } = require('node:child_process');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');

const Database = require('../index.js');
const { Queue } = Database;

// A scriptable collector: `respond(req) -> status` decides each answer, and
// every request is recorded with its parsed body and headers.
function collector(respond = () => 200) {
  const received = [];
  let inflight = 0;
  let maxInflight = 0;
  const server = http.createServer((req, res) => {
    inflight += 1;
    maxInflight = Math.max(maxInflight, inflight);
    let raw = '';
    req.on('data', (c) => { raw += c; });
    req.on('end', () => {
      const entry = { url: req.url, headers: req.headers, body: JSON.parse(raw) };
      received.push(entry);
      const answer = respond(entry, received.length);
      const finish = (status) => { inflight -= 1; res.writeHead(status).end(); };
      if (answer && typeof answer.then === 'function') answer.then(finish);
      else if (answer === 'hang') { /* never answer; client times out */ }
      else finish(answer);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({
      url: `http://127.0.0.1:${server.address().port}/jobs`,
      received,
      maxInflight: () => maxInflight,
      close: () => new Promise((r) => server.close(r))
    }));
  });
}

const until = async (predicate, ms = 4000, label = 'condition') => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 15));
  }
};

const fastOpts = (routes, extra = {}) => Object.assign({ routes, pollMs: 20, backoffMs: 20, maxBackoffMs: 60, timeoutMs: 400 }, extra);

// ---- construction --------------------------------------------------------

test('validates its arguments and refuses a readonly database', () => {
  const db = new Database(':memory:');
  assert.throws(() => new Queue({}, { routes: 'http://x' }), /Expected first argument to be a Database/);
  assert.throws(() => new Queue(db, { routes: {} }), /routes/);
  assert.throws(() => new Queue(db, { routes: 'ftp://x' }), /http: or https:/);
  assert.throws(() => new Queue(db, { routes: 'http://x', concurrency: 0 }), /"concurrency"/);
  db.close();

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'queue-'));
  const file = path.join(dir, 'ro.db');
  new Database(file).close();
  const ro = new Database(file, { readonly: true });
  assert.throws(() => new Queue(ro, { routes: 'http://x' }), /readonly/);
  ro.close();
});

// ---- producer side -------------------------------------------------------

test('enqueue fails fast on things no retry could fix', () => {
  const db = new Database(':memory:');
  const jobs = new Queue(db, { routes: { enrich: 'http://127.0.0.1:1/x' }, autoStart: false });
  assert.throws(() => jobs.enqueue('unknown', {}), /No route for job kind "unknown"/);
  assert.throws(() => jobs.enqueue('enrich'), /Expected a payload/);
  const circular = {}; circular.self = circular;
  assert.throws(() => jobs.enqueue('enrich', circular), /not JSON-serializable/);
  assert.throws(() => jobs.enqueue('enrich', { big: 1n }), /not JSON-serializable/);
  assert.throws(() => jobs.enqueue('enrich', {}, { delayMs: -1 }), /"delayMs"/);
  assert.strictEqual(jobs.pending(), 0, 'nothing may be stored when enqueue throws');
  db.close();
});

test('a job enqueued in a rolled-back transaction does not exist', () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)');
  const jobs = new Queue(db, { routes: 'http://127.0.0.1:1/x', autoStart: false });
  const insert = db.prepare('INSERT INTO orders (total) VALUES (?)');

  const place = db.transaction((total) => {
    const { lastInsertRowid } = insert.run(total);
    jobs.enqueue('enrich', { orderId: Number(lastInsertRowid) });
    if (total < 0) throw new Error('negative total');
  });

  place(10);
  assert.throws(() => place(-1), /negative total/);

  assert.strictEqual(db.prepare('SELECT count(*) n FROM orders').get().n, 1);
  assert.strictEqual(jobs.pending(), 1, 'the job for the rolled-back order must not exist');
  db.close();
});

// ---- delivery ------------------------------------------------------------

test('delivers a job as a JSON envelope with an idempotency key, then deletes it', async () => {
  const c = await collector();
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts({ enrich: c.url }));
  const delivered = [];
  jobs.on('delivered', (j) => delivered.push(j.id));

  const id = jobs.enqueue('enrich', { orderId: 7, tags: ['a', 'b'] });
  await until(() => delivered.length === 1, 4000, 'delivery');

  assert.strictEqual(c.received.length, 1);
  const { body, headers } = c.received[0];
  assert.strictEqual(body.id, id);
  assert.strictEqual(body.kind, 'enrich');
  assert.deepStrictEqual(body.payload, { orderId: 7, tags: ['a', 'b'] });
  assert.strictEqual(body.attempt, 1);
  assert.strictEqual(typeof body.enqueuedAt, 'number');
  assert.strictEqual(body.source.db, 'memory');
  assert.strictEqual(headers['content-type'], 'application/json');
  assert.strictEqual(headers['idempotency-key'], `memory:${id}`);
  assert.strictEqual(headers['x-job-kind'], 'enrich');
  assert.strictEqual(jobs.pending(), 0, 'a delivered job is removed');

  jobs.stop(); db.close(); await c.close();
});

test('routes by kind with "*" as the fallback', async () => {
  const a = await collector();
  const b = await collector();
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts({ report: a.url, '*': b.url }));
  let delivered = 0;
  jobs.on('delivered', () => { delivered += 1; });

  jobs.enqueue('report', { r: 1 });
  jobs.enqueue('notify', { n: 1 });
  jobs.enqueue('anything', { x: 1 });
  await until(() => delivered === 3, 4000, '3 deliveries');

  assert.deepStrictEqual(a.received.map((r) => r.body.kind), ['report']);
  assert.deepStrictEqual(b.received.map((r) => r.body.kind).sort(), ['anything', 'notify']);
  jobs.stop(); db.close(); await a.close(); await b.close();
});

test('retries on 5xx with the attempt number rising, then succeeds', async () => {
  const c = await collector((_req, n) => (n < 3 ? 503 : 200));
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url));
  const retries = [];
  let delivered = 0;
  jobs.on('retry', (j, err, delay) => retries.push({ attempt: j.attempts, err: err.message, delay }));
  jobs.on('delivered', () => { delivered += 1; });

  jobs.enqueue('k', { v: 1 });
  await until(() => delivered === 1, 6000, 'eventual delivery');

  assert.deepStrictEqual(c.received.map((r) => r.body.attempt), [1, 2, 3]);
  assert.deepStrictEqual(retries.map((r) => r.err), ['HTTP 503', 'HTTP 503']);
  assert.ok(retries.every((r) => r.delay >= 10 && r.delay <= 120), `backoff within bounds: ${JSON.stringify(retries)}`);
  // The same row presents the same key on every attempt.
  assert.strictEqual(new Set(c.received.map((r) => r.headers['idempotency-key'])).size, 1);
  jobs.stop(); db.close(); await c.close();
});

test('a 4xx dead-letters immediately; retry() gives it a fresh budget', async () => {
  let fail = true;
  const c = await collector(() => (fail ? 422 : 200));
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url));
  const dead = [];
  let delivered = 0;
  jobs.on('dead', (j, err) => dead.push({ id: j.id, err: err.message }));
  jobs.on('delivered', () => { delivered += 1; });

  const id = jobs.enqueue('k', { bad: true });
  await until(() => dead.length === 1, 4000, 'dead-letter');

  assert.strictEqual(c.received.length, 1, 'a bad request is not retried');
  assert.deepStrictEqual(dead, [{ id, err: 'HTTP 422' }]);
  const rows = jobs.dead();
  assert.strictEqual(rows.length, 1);
  assert.strictEqual(rows[0].last_error, 'HTTP 422');
  assert.deepStrictEqual(rows[0].payload, { bad: true });
  assert.strictEqual(jobs.pending(), 0, 'dead jobs are not pending');

  fail = false;
  assert.strictEqual(jobs.retry(id), true);
  assert.strictEqual(jobs.retry(id), false, 'a live job cannot be retried again');
  await until(() => delivered === 1, 4000, 'delivery after retry');
  assert.strictEqual(c.received[1].body.attempt, 1, 'retry() resets the attempt count');
  assert.strictEqual(jobs.dead().length, 0);

  jobs.stop(); db.close(); await c.close();
});

test('429 and 408 are retried, not dead-lettered', async () => {
  const c = await collector((_req, n) => [429, 408, 200][n - 1]);
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url));
  let delivered = 0;
  jobs.on('delivered', () => { delivered += 1; });
  jobs.enqueue('k', null);
  await until(() => delivered === 1, 6000, 'delivery');
  assert.strictEqual(c.received.length, 3);
  assert.strictEqual(jobs.dead().length, 0);
  jobs.stop(); db.close(); await c.close();
});

test('exhausting attempts against a refused connection dead-letters with the network error', async () => {
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts('http://127.0.0.1:1/x', { attempts: 3 }));
  const dead = [];
  jobs.on('dead', (j, err) => dead.push({ attempts: j.attempts, code: err.code }));
  jobs.enqueue('k', {});
  await until(() => dead.length === 1, 6000, 'dead after 3 attempts');
  assert.strictEqual(dead[0].attempts, 3);
  assert.match(String(dead[0].code), /ECONNREFUSED|EADDRNOTAVAIL|EACCES/);
  assert.strictEqual(jobs.dead()[0].attempts, 3);
  jobs.stop(); db.close();
});

test('a hung service times out and the job is retried', async () => {
  const c = await collector((_req, n) => (n === 1 ? 'hang' : 200));
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url, { timeoutMs: 150 }));
  const retries = [];
  let delivered = 0;
  jobs.on('retry', (_j, err) => retries.push(err.message));
  jobs.on('delivered', () => { delivered += 1; });
  jobs.enqueue('k', {});
  await until(() => delivered === 1, 6000, 'delivery after timeout');
  assert.match(retries[0], /timed out after 150ms/);
  jobs.stop(); db.close(); await c.close();
});

// ---- scheduling and concurrency -----------------------------------------

test('runAt / delayMs hold a job until it is due', async () => {
  const c = await collector();
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url));
  let delivered = 0;
  jobs.on('delivered', () => { delivered += 1; });

  const t0 = Date.now();
  jobs.enqueue('k', { which: 'later' }, { delayMs: 300 });
  jobs.enqueue('k', { which: 'now' });
  jobs.enqueue('k', { which: 'date' }, { runAt: new Date(t0 + 300) });

  await until(() => delivered === 1, 2000, 'first delivery');
  assert.strictEqual(c.received[0].body.payload.which, 'now');
  assert.strictEqual(jobs.pending(), 2);

  await until(() => delivered === 3, 3000, 'delayed deliveries');
  const late = c.received.slice(1);
  assert.ok(late.every((r) => Date.now() - t0 >= 280), 'delayed jobs must not run early');
  jobs.stop(); db.close(); await c.close();
});

test('never exceeds the configured concurrency', async () => {
  const c = await collector(() => new Promise((r) => setTimeout(() => r(200), 60)));
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url, { concurrency: 3 }));
  let delivered = 0;
  jobs.on('delivered', () => { delivered += 1; });
  for (let i = 0; i < 12; i++) jobs.enqueue('k', { i });
  await until(() => delivered === 12, 8000, '12 deliveries');
  assert.strictEqual(c.maxInflight(), 3);
  jobs.stop(); db.close(); await c.close();
});

test('a lease held by another process is respected until it expires', async () => {
  const c = await collector();
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url, { leaseMs: 400 }), { autoStart: false });
  jobs.stop();

  // Simulate a sibling process that claimed the job and then died.
  const id = jobs.enqueue('k', {});
  db.prepare(`UPDATE ${Queue.TABLE} SET locked_until = ?, attempts = 1 WHERE id = ?`).run(Date.now() + 250, id);

  let delivered = 0;
  jobs.on('delivered', () => { delivered += 1; });
  const t0 = Date.now();
  jobs.start();

  await new Promise((r) => setTimeout(r, 120));
  assert.strictEqual(c.received.length, 0, 'must not steal a live lease');

  await until(() => delivered === 1, 3000, 'delivery after lease expiry');
  assert.ok(Date.now() - t0 >= 240, 'delivered only after the lease expired');
  assert.strictEqual(c.received[0].body.attempt, 2, 'the dead sibling\'s attempt still counts');
  jobs.stop(); db.close(); await c.close();
});

// ---- shutdown and lifetime ----------------------------------------------

test('drain() delivers what is due, waits for in-flight work, then stops', async () => {
  const c = await collector(() => new Promise((r) => setTimeout(() => r(200), 40)));
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url, { concurrency: 2 }), { autoStart: false });
  jobs.stop();
  for (let i = 0; i < 5; i++) jobs.enqueue('k', { i });
  jobs.enqueue('k', { later: true }, { delayMs: 60000 });

  await jobs.drain();
  assert.strictEqual(c.received.length, 5);
  assert.strictEqual(jobs.pending(), 1, 'the far-future job stays queued');
  assert.strictEqual(jobs.running, false);
  assert.strictEqual(jobs.inflight, 0);
  db.close(); await c.close();
});

test('closing the database stops the queue without throwing', async () => {
  const c = await collector();
  const db = new Database(':memory:');
  const jobs = new Queue(db, fastOpts(c.url));
  const stalled = [];
  jobs.on('stalled', (e) => stalled.push(e.message));
  jobs.enqueue('k', {});
  await until(() => c.received.length === 1, 3000, 'delivery');
  db.close();
  await new Promise((r) => setTimeout(r, 100));
  assert.strictEqual(jobs.running, false);
  assert.strictEqual(stalled.length, 0, `no stalls expected: ${stalled}`);
  await c.close();
});

test('a process stays alive to deliver due work, then exits once idle', async () => {
  const c = await collector();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'queue-exit-'));
  const file = path.join(dir, 'app.db');
  const script = path.join(dir, 'child.js');
  fs.writeFileSync(script, `
    const Database = require(${JSON.stringify(path.join(__dirname, '..', 'index.js'))});
    const db = new Database(${JSON.stringify(file)});
    const jobs = new Database.Queue(db, { routes: ${JSON.stringify(c.url)}, pollMs: 20 });
    jobs.enqueue('k', { from: 'child' });
    // No close(), no drain(): the queue alone decides when the process may exit.
  `);
  const t0 = Date.now();
  const code = await new Promise((resolve, reject) => {
    const child = execFile(process.execPath, [script], { timeout: 10000 }, (err) => {
      if (err && err.killed) return reject(new Error('process hung: queue held it open while idle'));
      resolve(child.exitCode);
    });
  });
  assert.strictEqual(code, 0);
  assert.strictEqual(c.received.length, 1, 'the child must have delivered before exiting');
  assert.strictEqual(c.received[0].body.payload.from, 'child');
  assert.ok(Date.now() - t0 < 5000);

  // Durability: a job that was never dispatched is still there for the next process.
  const db = new Database(file);
  assert.strictEqual(new Queue(db, { routes: c.url, autoStart: false }).pending(), 0);
  db.close();
  await c.close();
});

test('jobs left behind by a dead process are delivered by the next one', async () => {
  const c = await collector();
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'queue-durable-')), 'app.db');

  // First "process": enqueue with the dispatcher off, then close. Nothing sent.
  let db = new Database(file);
  new Queue(db, { routes: c.url, autoStart: false }).enqueue('k', { survived: true });
  db.close();
  assert.strictEqual(c.received.length, 0);

  // Second "process": attaches and finds the work.
  db = new Database(file);
  const jobs = new Queue(db, fastOpts(c.url));
  await until(() => c.received.length === 1, 3000, 'delivery by the second process');
  assert.strictEqual(c.received[0].body.payload.survived, true);
  assert.strictEqual(c.received[0].body.source.db.length, 16, 'a file database is identified by hash');
  assert.ok(!JSON.stringify(c.received[0]).includes(file), 'the file path must never be sent');
  jobs.stop(); db.close(); await c.close();
});

test('purge() removes old dead-lettered jobs only', async () => {
  const db = new Database(':memory:');
  const jobs = new Queue(db, { routes: 'http://127.0.0.1:1/x', autoStart: false });
  const a = jobs.enqueue('k', {});
  const b = jobs.enqueue('k', {});
  jobs.enqueue('k', {});
  db.prepare(`UPDATE ${Queue.TABLE} SET failed_at = ? WHERE id = ?`).run(Date.now() - 10000, a);
  db.prepare(`UPDATE ${Queue.TABLE} SET failed_at = ? WHERE id = ?`).run(Date.now(), b);
  assert.strictEqual(jobs.purge(5000), 1);
  assert.strictEqual(jobs.dead().length, 1);
  assert.strictEqual(jobs.pending(), 1);
  db.close();
});
