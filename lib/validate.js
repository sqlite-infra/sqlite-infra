'use strict';

// A small, dependency-free row/schema validator, exposed as Database.validator.
// Pure (no database needed), so it is equally at home validating rows in an
// ingest pipeline, request payloads, or a model's writes.
//
//   const v = Database.validator({
//     id:    { type: 'integer', required: true },
//     email: { type: 'string', required: true, pattern: /@/ },
//     tags:  { type: 'json', coerce: true, default: () => [] },
//     active:{ type: 'boolean', coerce: true, default: false },
//   });
//   v.validate(row)  -> { valid, errors: [{ field, message }], value }
//   v.assert(row)    -> coerced row, or throws ValidationError
//   v.isValid(row)   -> boolean
//   v.coerce(row)    -> best-effort coerced row (no failing)
//
// A rule is { type, required?, default?, min?, max?, pattern?, enum?, coerce?,
// validate? }. Errors are reported in spec-field order (then unknown fields),
// so the output is deterministic.

const TYPES = new Set(['string', 'number', 'integer', 'boolean', 'date', 'json', 'any']);

class ValidationError extends Error {
  constructor(errors) {
    const summary = errors.map((e) => `${e.field}: ${e.message}`).join('; ');
    super(`Validation failed: ${summary}`);
    this.name = 'ValidationError';
    this.errors = errors;
    if (typeof Error.captureStackTrace === 'function') Error.captureStackTrace(this, ValidationError);
  }
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

// Coerces `value` toward `type`. Returns { ok, value }.
function coerceValue(type, value) {
  switch (type) {
    case 'string':
      if (typeof value === 'string') return { ok: true, value };
      if (value === null || value === undefined) return { ok: false };
      if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return { ok: true, value: String(value) };
      return { ok: false };
    case 'number': {
      if (typeof value === 'number') return Number.isNaN(value) ? { ok: false } : { ok: true, value };
      if (typeof value === 'string' && value.trim() !== '') {
        const n = Number(value);
        return Number.isNaN(n) ? { ok: false } : { ok: true, value: n };
      }
      if (typeof value === 'bigint') return { ok: true, value: Number(value) };
      return { ok: false };
    }
    case 'integer': {
      const c = coerceValue('number', value);
      if (!c.ok) return { ok: false };
      return Number.isInteger(c.value) ? { ok: true, value: c.value } : { ok: false };
    }
    case 'boolean':
      if (typeof value === 'boolean') return { ok: true, value };
      if (value === 1 || value === '1' || value === 'true') return { ok: true, value: true };
      if (value === 0 || value === '0' || value === 'false') return { ok: true, value: false };
      return { ok: false };
    case 'date': {
      if (value instanceof Date) return Number.isNaN(value.getTime()) ? { ok: false } : { ok: true, value };
      if (typeof value === 'number') { const d = new Date(value); return Number.isNaN(d.getTime()) ? { ok: false } : { ok: true, value: d }; }
      if (typeof value === 'string') { const d = new Date(value); return Number.isNaN(d.getTime()) ? { ok: false } : { ok: true, value: d }; }
      return { ok: false };
    }
    case 'json':
      if (typeof value === 'string') { try { return { ok: true, value: JSON.parse(value) }; } catch { return { ok: false }; } }
      if (value !== null && typeof value === 'object') return { ok: true, value };
      return { ok: false };
    case 'any':
    default:
      return { ok: true, value };
  }
}

// True when `value` already matches `type` (no coercion).
function checkType(type, value) {
  switch (type) {
    case 'string': return typeof value === 'string';
    case 'number': return typeof value === 'number' && !Number.isNaN(value);
    case 'integer': return Number.isInteger(value);
    case 'boolean': return typeof value === 'boolean';
    case 'date': return value instanceof Date && !Number.isNaN(value.getTime());
    case 'json': return value !== null && typeof value === 'object';
    case 'any': default: return true;
  }
}

// The measure min/max compares: numeric value for numbers/dates, length for
// strings and arrays.
function measure(type, value) {
  if (type === 'date' && value instanceof Date) return value.getTime();
  if (typeof value === 'number') return value;
  if (typeof value === 'string' || Array.isArray(value)) return value.length;
  return null;
}

function applyDefault(rule) {
  return typeof rule.default === 'function' ? rule.default() : rule.default;
}

function validator(spec, options = {}) {
  if (!isPlainObject(spec)) throw new TypeError('Expected the validator spec to be an object');
  const stripUnknown = Boolean(options.stripUnknown);
  const strict = Boolean(options.strict);
  const fields = Object.keys(spec);
  for (const field of fields) {
    const rule = spec[field];
    if (!isPlainObject(rule)) throw new TypeError(`Rule for "${field}" must be an object`);
    if (rule.type !== undefined && !TYPES.has(rule.type)) throw new TypeError(`Unknown type "${rule.type}" for field "${field}"`);
  }

  // Runs the whole spec over `row`. When `failing` is false (coerce mode),
  // errors are collected but never stop coercion. Returns { errors, value }.
  function run(row, { collectErrors }) {
    if (!isPlainObject(row)) {
      return { errors: [{ field: '', message: 'expected an object' }], value: row };
    }
    const errors = [];
    const out = {};
    const fail = (field, message) => { if (collectErrors) errors.push({ field, message }); };

    for (const field of fields) {
      const rule = spec[field];
      const type = rule.type || 'any';
      let value = row[field];
      const present = Object.prototype.hasOwnProperty.call(row, field) && value !== undefined;

      if (!present) {
        if ('default' in rule) { value = applyDefault(rule); }
        else {
          if (rule.required) fail(field, 'is required');
          continue;   // absent optional field: leave it out
        }
      }

      if (value === null) {
        if (rule.required) fail(field, 'is required');
        else out[field] = null;
        continue;
      }

      // Coerce if asked and not already the right type.
      if (rule.coerce && !checkType(type, value)) {
        const c = coerceValue(type, value);
        if (c.ok) value = c.value;
        else { fail(field, `could not be coerced to ${type}`); out[field] = value; continue; }
      }

      if (!checkType(type, value)) { fail(field, `must be a ${type}`); out[field] = value; continue; }

      const m = measure(type, value);
      if (rule.min !== undefined && m !== null && m < rule.min) fail(field, `must be at least ${rule.min}`);
      if (rule.max !== undefined && m !== null && m > rule.max) fail(field, `must be at most ${rule.max}`);

      if (rule.pattern !== undefined) {
        const re = rule.pattern instanceof RegExp ? rule.pattern : new RegExp(rule.pattern);
        if (!re.test(String(value))) fail(field, 'does not match the required pattern');
      }

      if (rule.enum !== undefined && !rule.enum.includes(value)) {
        fail(field, `must be one of: ${rule.enum.join(', ')}`);
      }

      if (typeof rule.validate === 'function') {
        const r = rule.validate(value, row);
        if (r === false) fail(field, 'is invalid');
        else if (typeof r === 'string') fail(field, r);
      }

      out[field] = value;
    }

    // Unknown fields.
    for (const field of Object.keys(row)) {
      if (Object.prototype.hasOwnProperty.call(spec, field)) continue;
      if (strict) fail(field, 'is not allowed');
      if (!stripUnknown) out[field] = row[field];
    }

    return { errors, value: out };
  }

  return {
    validate(row) {
      const { errors, value } = run(row, { collectErrors: true });
      return { valid: errors.length === 0, errors, value };
    },
    assert(row) {
      const { errors, value } = run(row, { collectErrors: true });
      if (errors.length) throw new ValidationError(errors);
      return value;
    },
    isValid(row) {
      return run(row, { collectErrors: true }).errors.length === 0;
    },
    coerce(row) {
      return run(row, { collectErrors: false }).value;
    }
  };
}

module.exports = { validator, ValidationError };
