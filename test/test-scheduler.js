'use strict';

// Tests for lib/scheduler.js — recurring/delayed jobs on top of a Queue.
// Time is driven explicitly by passing `now` to tick(), so the tests are
// deterministic and do not sleep. The Queue is a fake that records enqueues.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
const { Scheduler, internals } = require('../lib/scheduler');

function fakeQueue() {
  return { enqueued: [], enqueue(kind, payload, options) { this.enqueued.push({ kind, payload, options }); return this.enqueued.length; } };
}

test('every() fires when due and reschedules by the interval', () => {
  const db = new Database(':memory:');
  const q = fakeQueue();
  const s = new Scheduler(db, { queue: q, autoStart: false });
  const now = 1_000_000;
  s.every(1000, 'beat', { n: 1 }, { name: 'beat', startAt: now });

  assert.equal(s.tick(now - 1), 0, 'not due yet');
  assert.equal(s.tick(now), 1, 'due now');
  assert.deepEqual(q.enqueued[0], { kind: 'beat', payload: { n: 1 }, options: {} });
  assert.equal(s.tick(now), 0, 'rescheduled past now');
  assert.equal(s.tick(now + 1000), 1, 'due again after the interval');
  assert.equal(q.enqueued.length, 2);
  db.close();
});

test('at() fires once then removes itself', () => {
  const db = new Database(':memory:');
  const q = fakeQueue();
  const s = new Scheduler(db, { queue: q, autoStart: false });
  const when = 5_000_000;
  const name = s.at(when, 'welcome', { id: 7 });
  assert.equal(s.tick(when - 1), 0);
  assert.equal(s.tick(when), 1);
  assert.equal(q.enqueued[0].kind, 'welcome');
  assert.equal(s.get(name), undefined, 'one-shot removed after firing');
  assert.equal(s.tick(when + 10_000), 0);
  db.close();
});

test('cron() computes the next UTC occurrence and fires at that minute', () => {
  const db = new Database(':memory:');
  const q = fakeQueue();
  const s = new Scheduler(db, { queue: q, autoStart: false });
  // 2026-01-01T00:02:30Z → next */5 minute mark is 00:05:00Z.
  const from = Date.parse('2026-01-01T00:02:30Z');
  const name = s.cron('*/5 * * * *', 'rollup', {}, { name: 'r' });
  const row = s.get(name);
  const expected = Date.parse('2026-01-01T00:05:00Z');
  // next_run was computed from Date.now(); assert the parser directly for the known instant:
  assert.equal(internals.cronNext(internals.parseCron('*/5 * * * *'), from), expected);

  // Force a known next_run and fire it.
  db.prepare(`UPDATE ${Scheduler.TABLE} SET next_run = ? WHERE name = ?`).run(expected, name);
  assert.equal(s.tick(expected - 1), 0);
  assert.equal(s.tick(expected), 1);
  assert.equal(q.enqueued[0].kind, 'rollup');
  // Rescheduled to the following 5-minute mark.
  assert.equal(s.get(name).next_run, Date.parse('2026-01-01T00:10:00Z'));
  db.close();
});

test('cron parser handles lists, ranges, steps, dow, and rejects bad input', () => {
  const { parseCron, cronNext } = internals;
  // 30 8 * * 1-5  → weekdays 08:30 UTC. From Sat 2026-01-03 → Mon 2026-01-05 08:30.
  const wk = parseCron('30 8 * * 1-5');
  assert.equal(cronNext(wk, Date.parse('2026-01-03T12:00:00Z')), Date.parse('2026-01-05T08:30:00Z'));
  // day-of-month OR day-of-week (both restricted): 0 0 1 * 0 → the 1st or any Sunday.
  const either = parseCron('0 0 1 * 0');
  assert.equal(cronNext(either, Date.parse('2026-01-01T12:00:00Z')), Date.parse('2026-01-04T00:00:00Z')); // first Sunday
  assert.throws(() => parseCron('* * * *'), /5-field/);
  assert.throws(() => parseCron('99 * * * *'), /out of range/);
  assert.throws(() => parseCron('*/0 * * * *'), /Invalid step/);
});

test('schedules persist and a second Scheduler on the same db sees them', () => {
  const db = new Database(':memory:');
  const s1 = new Scheduler(db, { queue: fakeQueue(), autoStart: false });
  s1.every(60_000, 'a', { x: 1 }, { name: 'a' });
  s1.cron('0 * * * *', 'b', { y: 2 }, { name: 'b' });
  const s2 = new Scheduler(db, { queue: fakeQueue(), autoStart: false });
  const names = s2.list().map((r) => r.name).sort();
  assert.deepEqual(names, ['a', 'b']);
  assert.deepEqual(s2.get('a').payload, { x: 1 });
  assert.equal(s2.remove('a'), true);
  assert.equal(s2.remove('a'), false);
  assert.equal(s1.get('a'), undefined, 'removal is visible on the other instance');
  db.close();
});

test('an enqueue failure emits error and does not stop the tick', () => {
  const db = new Database(':memory:');
  const q = { calls: 0, enqueue(kind) { this.calls += 1; if (kind === 'bad') throw new Error('boom'); return 1; } };
  const s = new Scheduler(db, { queue: q, autoStart: false });
  const now = 2_000_000;
  s.every(1000, 'bad', {}, { name: 'bad', startAt: now });
  s.every(1000, 'good', {}, { name: 'good', startAt: now });
  const errors = [];
  s.on('error', (e) => errors.push(e));
  const fired = s.tick(now);
  assert.equal(q.calls, 2, 'both schedules attempted');
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /boom/);
  assert.equal(fired, 1, 'only the successful one counts as fired');
  db.close();
});

test('validates its arguments and the queue option', () => {
  const db = new Database(':memory:');
  assert.throws(() => new Scheduler(db, {}), /queue/);
  assert.throws(() => new Scheduler({}, { queue: fakeQueue() }), /Database/);
  const s = new Scheduler(db, { queue: fakeQueue(), autoStart: false });
  assert.throws(() => s.every(0, 'k', {}), /positive integer/);
  assert.throws(() => s.every(1000, '', {}), /non-empty string/);
  assert.throws(() => s.at(new Date(), '', {}), /non-empty string/);
  db.close();
});

test('start()/stop() manage the tick timer and it is unref-d (process still exits)', () => {
  const db = new Database(':memory:');
  const s = new Scheduler(db, { queue: fakeQueue(), tickMs: 10 });   // autoStart defaults true
  assert.equal(s.running, true);
  s.stop();
  assert.equal(s.running, false);
  s.start();
  assert.equal(s.running, true);
  s.stop();
  db.close();
});
