'use strict';

const { assertNonEmptyString } = require('../utils/validation');

/**
 * Per-device circuit breaker keyed on telemetry freshness, plus one global
 * breaker keyed on ingestion error rate. This is the primary "data
 * integrity" safety net requested: if we haven't heard from a device
 * recently, we do not trust our picture of its state well enough to send
 * it a command (e.g. discharging a battery based on stale SoC is unsafe).
 *
 * Deliberately synchronous and side-effect-free on read (`isOpen`) so it
 * can be checked cheaply on every single command attempt without adding
 * I/O latency to the hot path.
 */
class CircuitBreaker {
  constructor({ staleTelemetryMs, maxErrorsPerWindow, errorWindowMs, logger }) {
    this.staleTelemetryMs = staleTelemetryMs;
    this.maxErrorsPerWindow = maxErrorsPerWindow;
    this.errorWindowMs = errorWindowMs;
    this.logger = logger ? logger.child('circuit-breaker') : null;

    /** @type {Map<string, number>} deviceId -> last telemetry timestamp (ms) */
    this._lastSeen = new Map();
    /** @type {number[]} timestamps of recent ingestion errors, pruned each check */
    this._errorTimestamps = [];
    this._globalTripped = false;
    this._globalTrippedAt = null;
  }

  /** Call every time a valid telemetry point is accepted for a device. */
  recordTelemetry(deviceId, timestamp = Date.now()) {
    assertNonEmptyString(deviceId, 'deviceId');
    this._lastSeen.set(deviceId, timestamp);
  }

  /** Call every time ingestion rejects/errors on a point (feeds the global breaker). */
  recordError(timestamp = Date.now()) {
    this._errorTimestamps.push(timestamp);
    this._pruneErrors(timestamp);
    if (this._errorTimestamps.length > this.maxErrorsPerWindow && !this._globalTripped) {
      this._globalTripped = true;
      this._globalTrippedAt = timestamp;
      if (this.logger) {
        this.logger.error('global circuit breaker TRIPPED: ingestion error rate exceeded threshold', {
          count: this._errorTimestamps.length,
          windowMs: this.errorWindowMs,
        });
      }
    }
  }

  _pruneErrors(now) {
    const cutoff = now - this.errorWindowMs;
    while (this._errorTimestamps.length && this._errorTimestamps[0] < cutoff) {
      this._errorTimestamps.shift();
    }
    // Auto-reset the global breaker once the error rate has subsided.
    if (this._globalTripped && this._errorTimestamps.length <= this.maxErrorsPerWindow / 2) {
      this._globalTripped = false;
      if (this.logger) this.logger.info('global circuit breaker reset: error rate normalized');
    }
  }

  /**
   * @returns {{open: boolean, reason?: string}} whether commands to this
   * device should currently be blocked.
   */
  isOpen(deviceId, now = Date.now()) {
    if (this._globalTripped) {
      return { open: true, reason: 'GLOBAL_ERROR_RATE_TRIPPED' };
    }
    const lastSeen = this._lastSeen.get(deviceId);
    if (lastSeen === undefined) {
      return { open: true, reason: 'NO_TELEMETRY_EVER_RECEIVED' };
    }
    const age = now - lastSeen;
    if (age > this.staleTelemetryMs) {
      return { open: true, reason: 'STALE_TELEMETRY', ageMs: age, thresholdMs: this.staleTelemetryMs };
    }
    return { open: false };
  }

  getStatus() {
    return {
      globalTripped: this._globalTripped,
      trackedDevices: this._lastSeen.size,
      recentErrorCount: this._errorTimestamps.length,
    };
  }
}

module.exports = { CircuitBreaker };
