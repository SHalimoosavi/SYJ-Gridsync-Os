'use strict';

const crypto = require('node:crypto');

const ALARM_TYPES = ['OVER_VOLTAGE', 'UNDER_VOLTAGE', 'HIGH_FREQUENCY', 'LOW_SOC', 'DEVICE_OFFLINE', 'COMM_TIMEOUT'];

/**
 * AlarmEngine tracks currently-active alarms per (deviceId, type) and
 * decides when to trigger/clear them. It is deliberately independent from
 * StateMachine/ConstraintValidator: the state machine decides *control*
 * actions (auto-curtailment), this decides *notification* records for
 * operators. Both read the same gridConstraints so their thresholds agree,
 * but AlarmEngine needs OVER/UNDER granularity that the constraint
 * validator's generic "OUT_OF_RANGE" violation doesn't carry, so it
 * evaluates raw values directly rather than reusing violations.
 *
 * State (the active-alarm map) lives in memory for fast, synchronous
 * decisions; the orchestrator is responsible for persisting every
 * triggered/cleared/acknowledged event to durable storage so alarm
 * history survives a restart -- this class itself does no I/O.
 */
class AlarmEngine {
  constructor({ gridConstraints, commTimeoutMs, staleTelemetryMs, logger }) {
    this.gridConstraints = gridConstraints;
    this.commTimeoutMs = commTimeoutMs;
    this.staleTelemetryMs = staleTelemetryMs;
    this.logger = logger ? logger.child('alarm-engine') : null;
    /** @type {Map<string, object>} `${deviceId}:${type}` -> active alarm record */
    this._active = new Map();
  }

  static key(deviceId, type) {
    return `${deviceId}:${type}`;
  }

  _trigger(deviceId, type, severity, value, threshold, message) {
    const key = AlarmEngine.key(deviceId, type);
    if (this._active.has(key)) return null; // already active -- no duplicate alarms
    const alarm = {
      alarmId: crypto.randomUUID(),
      deviceId,
      type,
      severity,
      status: 'ACTIVE',
      value,
      threshold,
      message,
      triggeredAt: Date.now(),
      clearedAt: null,
      acknowledged: false,
      acknowledgedBy: null,
      acknowledgedAt: null,
    };
    this._active.set(key, alarm);
    if (this.logger) this.logger.warn('alarm triggered', { deviceId, type, severity, value });
    return { ...alarm, event: 'TRIGGERED' };
  }

  _clear(deviceId, type) {
    const key = AlarmEngine.key(deviceId, type);
    const alarm = this._active.get(key);
    if (!alarm) return null;
    this._active.delete(key);
    const cleared = { ...alarm, status: 'CLEARED', clearedAt: Date.now() };
    if (this.logger) this.logger.info('alarm cleared', { deviceId, type });
    return { ...cleared, event: 'CLEARED' };
  }

  /**
   * @param {object} point - canonical telemetry point (see Normalizer)
   * @returns {object[]} alarm lifecycle events (TRIGGERED/CLEARED) to persist
   */
  evaluateTelemetry(point) {
    const events = [];
    const { voltage, frequency, soc } = point.metrics;
    const c = this.gridConstraints;
    const deviceId = point.deviceId;

    if (voltage !== undefined) {
      events.push(
        voltage > c.voltage.max
          ? this._trigger(deviceId, 'OVER_VOLTAGE', 'CRITICAL', voltage, c.voltage.max, `Voltage ${voltage}V exceeds max ${c.voltage.max}V`)
          : this._clear(deviceId, 'OVER_VOLTAGE'),
      );
      events.push(
        voltage < c.voltage.min
          ? this._trigger(deviceId, 'UNDER_VOLTAGE', 'CRITICAL', voltage, c.voltage.min, `Voltage ${voltage}V below min ${c.voltage.min}V`)
          : this._clear(deviceId, 'UNDER_VOLTAGE'),
      );
    }

    if (frequency !== undefined) {
      events.push(
        frequency > c.frequency.max
          ? this._trigger(deviceId, 'HIGH_FREQUENCY', 'WARNING', frequency, c.frequency.max, `Frequency ${frequency}Hz exceeds max ${c.frequency.max}Hz`)
          : this._clear(deviceId, 'HIGH_FREQUENCY'),
      );
    }

    if (soc !== undefined) {
      events.push(
        soc < c.soc.min
          ? this._trigger(deviceId, 'LOW_SOC', 'WARNING', soc, c.soc.min, `State of charge ${Math.round(soc * 100)}% below min ${Math.round(c.soc.min * 100)}%`)
          : this._clear(deviceId, 'LOW_SOC'),
      );
    }

    return events.filter(Boolean);
  }

  /**
   * Call periodically for every known device -- staleness is an absence of
   * events, so it can't be detected by evaluateTelemetry alone.
   * @returns {object[]} alarm lifecycle events to persist
   */
  evaluateStaleness(deviceId, lastSeenTs, now = Date.now()) {
    const events = [];
    const age = now - lastSeenTs;

    events.push(
      age > this.staleTelemetryMs
        ? this._trigger(deviceId, 'DEVICE_OFFLINE', 'CRITICAL', age, this.staleTelemetryMs, `No telemetry from ${deviceId} for ${Math.round(age / 1000)}s`)
        : this._clear(deviceId, 'DEVICE_OFFLINE'),
    );
    events.push(
      age > this.commTimeoutMs
        ? this._trigger(deviceId, 'COMM_TIMEOUT', 'WARNING', age, this.commTimeoutMs, `${deviceId} has not reported in ${Math.round(age / 1000)}s`)
        : this._clear(deviceId, 'COMM_TIMEOUT'),
    );

    return events.filter(Boolean);
  }

  /** @returns {object|null} the acknowledged alarm (event: 'ACKNOWLEDGED'), or null if not currently active. */
  acknowledge(alarmId, byUsername) {
    for (const alarm of this._active.values()) {
      if (alarm.alarmId === alarmId) {
        alarm.acknowledged = true;
        alarm.acknowledgedBy = byUsername;
        alarm.acknowledgedAt = Date.now();
        return { ...alarm, event: 'ACKNOWLEDGED' };
      }
    }
    return null;
  }

  /** Acknowledges every currently-active alarm for a device (used by the RESET command). */
  acknowledgeAllForDevice(deviceId, byUsername) {
    const events = [];
    for (const alarm of this._active.values()) {
      if (alarm.deviceId === deviceId && !alarm.acknowledged) {
        alarm.acknowledged = true;
        alarm.acknowledgedBy = byUsername;
        alarm.acknowledgedAt = Date.now();
        events.push({ ...alarm, event: 'ACKNOWLEDGED' });
      }
    }
    return events;
  }

  listActive() {
    return [...this._active.values()].map((a) => ({ ...a }));
  }
}

module.exports = { AlarmEngine, ALARM_TYPES };
