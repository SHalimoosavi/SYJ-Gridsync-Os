'use strict';

const { ValidationError } = require('./errors');

/** Asserts `value` is a finite number (rejects NaN, Infinity, strings, null, undefined). */
function assertFiniteNumber(value, fieldName) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new ValidationError(`Field "${fieldName}" must be a finite number, got: ${JSON.stringify(value)}`, {
      field: fieldName,
      value,
    });
  }
  return value;
}

/** Asserts `value` is a non-empty string. */
function assertNonEmptyString(value, fieldName) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new ValidationError(`Field "${fieldName}" must be a non-empty string, got: ${JSON.stringify(value)}`, {
      field: fieldName,
      value,
    });
  }
  return value;
}

/** Asserts `value` is one of `allowed`. */
function assertOneOf(value, allowed, fieldName) {
  if (!allowed.includes(value)) {
    throw new ValidationError(`Field "${fieldName}" must be one of [${allowed.join(', ')}], got: ${JSON.stringify(value)}`, {
      field: fieldName,
      value,
      allowed,
    });
  }
  return value;
}

/** Asserts a numeric value lies within [min, max] inclusive. */
function assertInRange(value, min, max, fieldName) {
  assertFiniteNumber(value, fieldName);
  if (value < min || value > max) {
    throw new ValidationError(`Field "${fieldName}" must be within [${min}, ${max}], got: ${value}`, {
      field: fieldName,
      value,
      min,
      max,
    });
  }
  return value;
}

/** Clamp a number into [min, max] without throwing -- used for safe command shaping, not input validation. */
function clamp(value, min, max) {
  if (!Number.isFinite(value)) return min;
  return Math.min(Math.max(value, min), max);
}

/** Asserts a plain object (not array, not null). */
function assertPlainObject(value, fieldName) {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new ValidationError(`Field "${fieldName}" must be a plain object, got: ${JSON.stringify(value)}`, {
      field: fieldName,
      value,
    });
  }
  return value;
}

module.exports = {
  assertFiniteNumber,
  assertNonEmptyString,
  assertOneOf,
  assertInRange,
  assertPlainObject,
  clamp,
};
