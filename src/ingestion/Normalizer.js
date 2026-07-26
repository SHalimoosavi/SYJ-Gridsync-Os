'use strict';

const { assertFiniteNumber, assertNonEmptyString, assertOneOf } = require('../utils/validation');
const { ValidationError } = require('../utils/errors');

const KNOWN_DEVICE_TYPES = ['INVERTER', 'METER', 'BATTERY'];
const KNOWN_PROTOCOLS = ['MQTT', 'MODBUS', 'DNP3'];

/**
 * Canonical telemetry point shape (this is the contract the whole engine
 * downstream relies on -- it never sees protocol-specific fields again):
 *
 * {
 *   deviceId: string,
 *   protocol: 'MQTT'|'MODBUS'|'DNP3',
 *   deviceType: 'INVERTER'|'METER'|'BATTERY',
 *   timestamp: number (ms epoch),
 *   metrics: { voltage?, frequency?, powerKw?, soc? }
 * }
 */
class Normalizer {
  /**
   * @param {any} raw - protocol-specific payload
   * @param {{protocol: string, deviceId?: string}} meta
   * @returns {object} canonical telemetry point
   * @throws {ValidationError} on any malformed/missing/out-of-type field
   */
  static normalize(raw, meta) {
    if (raw === null || typeof raw !== 'object') {
      throw new ValidationError('Raw telemetry payload must be an object', { raw, meta });
    }
    assertOneOf(meta?.protocol, KNOWN_PROTOCOLS, 'meta.protocol');

    // deviceId may arrive either inside the payload itself, or only via
    // transport metadata (e.g. derived from an MQTT topic segment, as
    // MqttAdapter does). Both are legitimate sources -- payload wins if
    // both are present. If neither is present, this throws.
    const deviceId = assertNonEmptyString(raw.deviceId || meta.deviceId, 'deviceId');
    const deviceType = assertOneOf(raw.deviceType, KNOWN_DEVICE_TYPES, 'deviceType');
    const timestamp = Number.isFinite(raw.timestamp) ? raw.timestamp : Date.now();

    const metrics = {};
    // Each metric is optional at the schema level (not every device reports
    // every field) but must be a finite number if present -- never silently
    // coerce a bad value into 0, which would be indistinguishable from a
    // real zero reading and could mask a sensor fault.
    for (const key of ['voltage', 'frequency', 'powerKw', 'soc']) {
      if (raw[key] !== undefined) {
        metrics[key] = assertFiniteNumber(raw[key], `metrics.${key}`);
      }
    }

    if (Object.keys(metrics).length === 0) {
      throw new ValidationError('Telemetry point has no recognized metrics', { deviceId, raw });
    }

    return {
      deviceId,
      protocol: meta.protocol,
      deviceType,
      timestamp,
      metrics,
    };
  }
}

module.exports = { Normalizer, KNOWN_DEVICE_TYPES, KNOWN_PROTOCOLS };
