'use strict';

// Tests for lib/validate.js — the standalone row/schema validator exposed as
// Database.validator.

const test = require('node:test');
const assert = require('node:assert/strict');

const { validator, ValidationError } = require('../lib/validate');

test('required and type errors are reported, in spec-field order', () => {
  const v = validator({
    id: { type: 'integer', required: true },
    name: { type: 'string', required: true },
    age: { type: 'integer' }
  });
  const r = v.validate({ age: 'not a number' });
  assert.equal(r.valid, false);
  assert.deepEqual(r.errors.map((e) => e.field), ['id', 'name', 'age']);
  assert.match(r.errors[0].message, /required/);
  assert.match(r.errors[2].message, /must be a integer/);
});

test('coerce turns strings into numbers, integers, booleans, dates and json', () => {
  const v = validator({
    n: { type: 'number', coerce: true },
    i: { type: 'integer', coerce: true },
    b: { type: 'boolean', coerce: true },
    d: { type: 'date', coerce: true },
    j: { type: 'json', coerce: true }
  });
  const r = v.validate({ n: '3.5', i: '42', b: 'true', d: '2026-01-02T00:00:00Z', j: '{"a":1}' });
  assert.equal(r.valid, true);
  assert.equal(r.value.n, 3.5);
  assert.equal(r.value.i, 42);
  assert.equal(r.value.b, true);
  assert.ok(r.value.d instanceof Date);
  assert.equal(r.value.d.getUTCFullYear(), 2026);
  assert.deepEqual(r.value.j, { a: 1 });

  // non-numeric string and bad json fail
  assert.equal(validator({ n: { type: 'number', coerce: true } }).validate({ n: 'x' }).valid, false);
  assert.equal(validator({ j: { type: 'json', coerce: true } }).validate({ j: '{bad' }).valid, false);
  // boolean coercions
  assert.equal(v.validate({ b: '0' }).value.b, false);
  assert.equal(v.validate({ b: 1 }).value.b, true);
});

test('min/max apply to numbers and to string/array length', () => {
  const v = validator({
    score: { type: 'number', min: 0, max: 100 },
    name: { type: 'string', min: 2, max: 5 },
    tags: { type: 'json', min: 1, max: 3 }
  });
  assert.equal(v.validate({ score: 50, name: 'abc', tags: ['a', 'b'] }).valid, true);
  const r = v.validate({ score: 200, name: 'x', tags: [] });
  assert.equal(r.valid, false);
  assert.deepEqual(r.errors.map((e) => e.field).sort(), ['name', 'score', 'tags']);
  assert.match(v.validate({ score: -1, name: 'ok', tags: ['a'] }).errors[0].message, /at least 0/);
});

test('pattern and enum', () => {
  const v = validator({
    email: { type: 'string', pattern: /@/ },
    role: { type: 'string', enum: ['admin', 'user'] }
  });
  assert.equal(v.validate({ email: 'a@b', role: 'admin' }).valid, true);
  assert.equal(v.validate({ email: 'nope', role: 'admin' }).valid, false);
  assert.equal(v.validate({ email: 'a@b', role: 'root' }).valid, false);
  // string pattern also accepted
  assert.equal(validator({ x: { type: 'string', pattern: '^\\d+$' } }).validate({ x: '123' }).valid, true);
});

test('default value and default factory apply when absent', () => {
  let n = 0;
  const v = validator({
    active: { type: 'boolean', default: false },
    tags: { type: 'json', default: () => [] },
    seq: { type: 'integer', default: () => ++n }
  });
  const a = v.validate({});
  assert.equal(a.value.active, false);
  assert.deepEqual(a.value.tags, []);
  assert.equal(a.value.seq, 1);
  const b = v.validate({});
  assert.equal(b.value.seq, 2, 'factory runs each time');
  assert.notEqual(a.value.tags, v.validate({}).value.tags, 'factory produces a fresh array');
});

test('custom validate returning a message fails with that message', () => {
  const v = validator({
    even: { type: 'integer', validate: (x) => (x % 2 === 0 ? true : 'must be even') },
    ok: { type: 'integer', validate: (x) => x > 0 }
  });
  assert.equal(v.validate({ even: 4, ok: 1 }).valid, true);
  assert.equal(v.validate({ even: 3, ok: 1 }).errors[0].message, 'must be even');
  assert.match(v.validate({ even: 2, ok: -1 }).errors[0].message, /invalid/);
});

test('assert() throws ValidationError with .errors; returns coerced value when valid', () => {
  const v = validator({ id: { type: 'integer', required: true, coerce: true } });
  assert.throws(() => v.assert({}), (err) => {
    assert.ok(err instanceof ValidationError);
    assert.equal(err.errors[0].field, 'id');
    return true;
  });
  assert.deepEqual(v.assert({ id: '7' }), { id: 7 });
});

test('isValid and coerce (best-effort, never fails)', () => {
  const v = validator({ n: { type: 'number', coerce: true, default: 0 } });
  assert.equal(v.isValid({ n: 5 }), true);
  assert.equal(v.isValid({ n: 'x' }), false);
  // coerce() applies defaults and coercions without throwing
  assert.deepEqual(v.coerce({}), { n: 0 });
  assert.equal(v.coerce({ n: '9' }).n, 9);
});

test('strict rejects unknown fields; stripUnknown drops them; default keeps them', () => {
  const spec = { id: { type: 'integer' } };
  assert.deepEqual(validator(spec).validate({ id: 1, extra: 'x' }).value, { id: 1, extra: 'x' });
  assert.deepEqual(validator(spec, { stripUnknown: true }).validate({ id: 1, extra: 'x' }).value, { id: 1 });
  const strict = validator(spec, { strict: true }).validate({ id: 1, extra: 'x' });
  assert.equal(strict.valid, false);
  assert.equal(strict.errors.find((e) => e.field === 'extra').message, 'is not allowed');
});

test('null is required-sensitive; optional null is kept', () => {
  assert.equal(validator({ a: { type: 'string', required: true } }).validate({ a: null }).valid, false);
  const r = validator({ a: { type: 'string' } }).validate({ a: null });
  assert.equal(r.valid, true);
  assert.equal(r.value.a, null);
});

test('a non-object row is reported, not thrown', () => {
  const v = validator({ id: { type: 'integer' } });
  const r = v.validate(42);
  assert.equal(r.valid, false);
  assert.doesNotThrow(() => v.isValid(null));
});
