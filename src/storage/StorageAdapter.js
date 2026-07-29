'use strict';

/**
 * Storage interface. Two responsibilities, kept in one contract so callers
 * don't need to know which backend is active:
 *
 *  1. Time-series telemetry (append-mostly, read for dashboards/analytics)
 *  2. Command write-ahead-log (durability guarantee: a command is only
 *     considered "safely queued" once `appendCommandEvent` for its CREATED
 *     event has resolved -- see CommandQueue)
 */
class StorageAdapter {
  // eslint-disable-next-line class-methods-use-this
  async init() {
    throw new Error('StorageAdapter.init() not implemented');
  }

  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async appendTelemetry(_point) {
    throw new Error('StorageAdapter.appendTelemetry() not implemented');
  }

  /**
   * Appends one immutable event to the command WAL. `record` must include
   * at minimum: {commandId, event, status, attempts, ts}.
   */
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async appendCommandEvent(_record) {
    throw new Error('StorageAdapter.appendCommandEvent() not implemented');
  }

  /**
   * Used on startup to recover any commands that were durably queued but
   * never reached a terminal state (ACKED/FAILED) before the process
   * exited -- e.g. a crash mid-dispatch. Returns the latest known record
   * per commandId, for records still in a non-terminal status.
   * @returns {Promise<object[]>}
   */
  // eslint-disable-next-line class-methods-use-this
  async loadPendingCommands() {
    throw new Error('StorageAdapter.loadPendingCommands() not implemented');
  }

  /**
   * Returns the most recent telemetry points for a device, newest first,
   * capped at `limit`. Used by the monitoring dashboard/API -- not on any
   * ingestion hot path.
   * @returns {Promise<object[]>}
   */
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async queryTelemetry(_deviceId, _limit) {
    throw new Error('StorageAdapter.queryTelemetry() not implemented');
  }

  /**
   * Returns the most recent command records (one per commandId, latest
   * known status), newest first, capped at `limit`.
   * @returns {Promise<object[]>}
   */
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async queryCommandHistory(_limit) {
    throw new Error('StorageAdapter.queryCommandHistory() not implemented');
  }

  /**
   * Appends one immutable event to the alarm WAL. `record` must include at
   * minimum: {alarmId, event, status, deviceId, type, ts}.
   */
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async appendAlarmEvent(_record) {
    throw new Error('StorageAdapter.appendAlarmEvent() not implemented');
  }

  /**
   * Returns the most recent alarm records (one per alarmId, latest known
   * state), newest first, capped at `limit`.
   * @returns {Promise<object[]>}
   */
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async queryAlarmHistory(_limit) {
    throw new Error('StorageAdapter.queryAlarmHistory() not implemented');
  }

  // eslint-disable-next-line class-methods-use-this
  async close() {
    throw new Error('StorageAdapter.close() not implemented');
  }
}

module.exports = { StorageAdapter };
