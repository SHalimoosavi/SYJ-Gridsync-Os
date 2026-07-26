'use strict';

const { AdapterError } = require('../utils/errors');

/**
 * CommandDispatcher pulls commands out of the durable CommandQueue and
 * attempts delivery. Every single attempt -- including retries -- is
 * re-checked against the circuit breaker and constraint validator using
 * the *current* device state, not whatever was true when the command was
 * first created. This matters: telemetry may have gone stale, or SoC may
 * have changed, in the time between enqueue and a retry attempt.
 *
 * Concurrency guards:
 *  - `_pumping` is a mutex so overlapping 'ready' events (queue.enqueue
 *    firing while a drain loop is already mid-flight) don't spin up two
 *    concurrent drain loops.
 *  - `_inFlight` is a per-commandId guard so a retry timer firing at the
 *    same moment a manual `_pump()` picks up the same command cannot result
 *    in two simultaneous sendCommand() calls for one commandId.
 */
class CommandDispatcher {
  constructor({ queue, circuitBreaker, constraintValidator, resolveAdapter, getLatestState, config, logger }) {
    this.queue = queue;
    this.circuitBreaker = circuitBreaker;
    this.constraintValidator = constraintValidator;
    this.resolveAdapter = resolveAdapter;
    this.getLatestState = getLatestState;
    this.config = config;
    this.logger = logger.child('command-dispatcher');

    this._pumping = false;
    this._inFlight = new Set();
    this._retryTimers = new Set();
    this._stopped = false;
  }

  start() {
    this.queue.on('ready', () => this._pump());
    this._pump(); // in case recover() already populated the queue before start()
  }

  stop() {
    this._stopped = true;
    for (const timer of this._retryTimers) clearTimeout(timer);
    this._retryTimers.clear();
  }

  async _pump() {
    if (this._pumping || this._stopped) return;
    this._pumping = true;
    try {
      let record = this.queue.takeNextPending();
      while (record) {
        // eslint-disable-next-line no-await-in-loop
        await this._handleOne(record);
        if (this._stopped) break;
        record = this.queue.takeNextPending();
      }
    } finally {
      this._pumping = false;
    }
  }

  async _handleOne(record) {
    const { commandId } = record;
    if (this._inFlight.has(commandId)) return;
    this._inFlight.add(commandId);

    try {
      const breakerCheck = this.circuitBreaker.isOpen(record.deviceId);
      if (breakerCheck.open) {
        this.logger.warn('command blocked by circuit breaker', {
          commandId,
          deviceId: record.deviceId,
          reason: breakerCheck.reason,
        });
        const outcome = await this.queue.markFailedAttempt(
          commandId,
          `CIRCUIT_OPEN:${breakerCheck.reason}`,
          { maxAttempts: this.config.commandQueue.maxAttempts },
        );
        if (outcome === 'RETRY_SCHEDULED') this._scheduleRetryWake(record.attempts);
        return;
      }

      let shaped;
      try {
        const latestState = this.getLatestState(record.deviceId);
        shaped = this.constraintValidator.validateCommand(record, latestState);
      } catch (err) {
        // A constraint violation is a hard authorization failure, not a
        // transient fault -- retrying it would just fail identically, so we
        // fail it immediately (maxAttempts override = 1) instead of
        // burning the normal retry budget.
        this.logger.error('command rejected by constraint validator', { commandId, err });
        await this.queue.markFailedAttempt(commandId, `CONSTRAINT_VIOLATION:${err.message}`, { maxAttempts: 1 });
        return;
      }

      await this.queue.markDispatching(commandId);
      const adapter = this.resolveAdapter(record.deviceId);
      if (!adapter) {
        throw new AdapterError('No adapter registered for device', { deviceId: record.deviceId });
      }
      await adapter.sendCommand(shaped);
      await this.queue.markAcked(commandId);
      this.logger.info('command dispatched and acknowledged', {
        commandId,
        type: record.type,
        deviceId: record.deviceId,
        value: shaped.value,
      });
    } catch (err) {
      this.logger.warn('command dispatch attempt failed', { commandId, err: err.message });
      const outcome = await this.queue.markFailedAttempt(commandId, err.message, {
        maxAttempts: this.config.commandQueue.maxAttempts,
      });
      if (outcome === 'RETRY_SCHEDULED') this._scheduleRetryWake(record.attempts);
    } finally {
      this._inFlight.delete(commandId);
    }
  }

  _scheduleRetryWake(attemptNumber) {
    if (this._stopped) return;
    const { baseRetryDelayMs, maxRetryDelayMs } = this.config.commandQueue;
    const delay = Math.min(maxRetryDelayMs, baseRetryDelayMs * 2 ** Math.max(0, attemptNumber - 1));
    const timer = setTimeout(() => {
      this._retryTimers.delete(timer);
      this._pump();
    }, delay);
    timer.unref?.();
    this._retryTimers.add(timer);
  }
}

module.exports = { CommandDispatcher };
