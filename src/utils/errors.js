'use strict';

/**
 * Base class for all GridSync-OS domain errors.
 * Every domain error carries a machine-readable `code` so callers can
 * branch on failure type without string-matching `.message`.
 */
class GridSyncError extends Error {
  constructor(message, code, details = {}) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.details = details;
    this.timestamp = Date.now();
    if (Error.captureStackTrace) Error.captureStackTrace(this, this.constructor);
  }
}

/** Malformed or out-of-range input from an adapter or caller. */
class ValidationError extends GridSyncError {
  constructor(message, details = {}) {
    super(message, 'VALIDATION_ERROR', details);
  }
}

/** A command failed grid-constraint checks (voltage/frequency/SoC/rate limits). */
class ConstraintViolationError extends GridSyncError {
  constructor(message, details = {}) {
    super(message, 'CONSTRAINT_VIOLATION', details);
  }
}

/** Circuit breaker is open (stale telemetry, or trip threshold exceeded) and blocked a command. */
class CircuitOpenError extends GridSyncError {
  constructor(message, details = {}) {
    super(message, 'CIRCUIT_OPEN', details);
  }
}

/** Storage/persistence layer failure (disk full, corrupt WAL entry, etc). */
class StorageError extends GridSyncError {
  constructor(message, details = {}) {
    super(message, 'STORAGE_ERROR', details);
  }
}

/** Adapter (transport) failure — connection drop, malformed frame, timeout. */
class AdapterError extends GridSyncError {
  constructor(message, details = {}) {
    super(message, 'ADAPTER_ERROR', details);
  }
}

module.exports = {
  GridSyncError,
  ValidationError,
  ConstraintViolationError,
  CircuitOpenError,
  StorageError,
  AdapterError,
};
