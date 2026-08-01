'use strict';

const fsp = require('node:fs/promises');
const path = require('node:path');
const { assertNonEmptyString, assertOneOf } = require('../utils/validation');
const { StorageError, ValidationError } = require('../utils/errors');

const STATUSES = ['ENABLED', 'DISABLED', 'REMOVED'];
const EDITABLE_FIELDS = ['name', 'location', 'notes', 'firmwareVersion'];

/**
 * DeviceRegistry: explicit metadata (name, location, firmware version) and
 * lifecycle control (enabled/disabled/removed) layered on top of the
 * telemetry-driven device discovery that already exists elsewhere in the
 * system. This is deliberately an overlay, not a gatekeeper: a device that
 * has never been explicitly registered can still send telemetry and get
 * auto-registered on first contact (see `ensureRegistered`) -- nothing
 * about the existing "just plug in a device and it shows up" behavior
 * changes unless an operator explicitly disables or removes it.
 *
 * Performance note: `get`/`list`/`isBlocked`/`canReceiveCommands` are all
 * synchronous, in-memory Map lookups -- these are called on the ingestion
 * and command-dispatch hot paths, which must never touch disk per-call.
 * The Map is the source of truth for reads; writes update it in-memory
 * *and* persist to disk (write-through), serialized through the same
 * chained-promise pattern used by UserStore/FileWalStorage so concurrent
 * writes can't corrupt the file.
 *
 * "Remove" is a soft-delete (status -> REMOVED), not a file deletion:
 * hard-deleting the record would make a removed device indistinguishable
 * from one that was simply never seen before, and its very next telemetry
 * point would silently re-register it -- defeating the point of removing
 * it. A REMOVED device can be brought back via the enable endpoint.
 */
class DeviceRegistry {
  constructor({ dataDir, logger }) {
    this.filePath = path.join(dataDir, 'devices.json');
    this.logger = logger ? logger.child('device-registry') : null;
    /** @type {Map<string, object>} deviceId -> record; in-memory source of truth for reads */
    this._records = new Map();
    this._writeChain = Promise.resolve();
  }

  async init() {
    let content;
    try {
      content = await fsp.readFile(this.filePath, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return; // no file yet -- start empty, created on first write
      throw new StorageError('Failed to read device registry', { cause: err.message });
    }
    let arr;
    try {
      arr = JSON.parse(content || '[]');
    } catch (err) {
      throw new StorageError('Device registry file is corrupt', { cause: err.message });
    }
    for (const record of arr) {
      if (record && record.deviceId) this._records.set(record.deviceId, record);
    }
  }

  _persist() {
    const snapshot = JSON.stringify([...this._records.values()], null, 2);
    const prevChain = this._writeChain;
    const runPromise = prevChain.then(() => fsp.writeFile(this.filePath, snapshot));
    this._writeChain = runPromise.then(() => undefined, () => undefined);
    return runPromise.catch((err) => {
      throw new StorageError('Device registry write failed', { cause: err.message });
    });
  }

  // ---- Synchronous reads (in-memory, hot-path safe) ----

  get(deviceId) {
    return this._records.get(deviceId) || null;
  }

  list({ includeRemoved = false } = {}) {
    return [...this._records.values()].filter((r) => includeRemoved || r.status !== 'REMOVED');
  }

  /** @returns {boolean} true if this device was explicitly removed -- its telemetry should be dropped. */
  isBlocked(deviceId) {
    const r = this._records.get(deviceId);
    return !!r && r.status === 'REMOVED';
  }

  /** @returns {boolean} true if commands may be dispatched to this device (no record = not blocked by the registry -- other checks like the circuit breaker still apply). */
  canReceiveCommands(deviceId) {
    const r = this._records.get(deviceId);
    return !r || r.status === 'ENABLED';
  }

  // ---- Async writes (persist then update in-memory) ----

  /** Idempotent auto-registration on first telemetry contact. Never overwrites an existing record. */
  async ensureRegistered(deviceId, defaults = {}) {
    if (this._records.has(deviceId)) return this._records.get(deviceId);
    const now = Date.now();
    const record = {
      deviceId,
      name: defaults.name || deviceId,
      location: null,
      notes: null,
      firmwareVersion: null,
      status: 'ENABLED',
      registeredAt: now,
      updatedAt: now,
      autoRegistered: true,
    };
    this._records.set(deviceId, record);
    await this._persist();
    return record;
  }

  /** Explicit operator registration. Throws if a record already exists in any status (use setStatus to restore a REMOVED device). */
  async register({ deviceId, name, location, notes, firmwareVersion }) {
    assertNonEmptyString(deviceId, 'deviceId');
    if (this._records.has(deviceId)) {
      throw new ValidationError(`Device "${deviceId}" is already registered`, { deviceId });
    }
    const now = Date.now();
    const record = {
      deviceId,
      name: name || deviceId,
      location: location || null,
      notes: notes || null,
      firmwareVersion: firmwareVersion || null,
      status: 'ENABLED',
      registeredAt: now,
      updatedAt: now,
      autoRegistered: false,
    };
    this._records.set(deviceId, record);
    await this._persist();
    return record;
  }

  async update(deviceId, patch) {
    const record = this._records.get(deviceId);
    if (!record) throw new ValidationError(`Unknown device: ${deviceId}`, { deviceId });
    if (record.status === 'REMOVED') {
      throw new ValidationError(`Device "${deviceId}" is removed; re-enable it before editing`, { deviceId });
    }
    for (const field of EDITABLE_FIELDS) {
      if (patch[field] !== undefined) record[field] = patch[field];
    }
    record.updatedAt = Date.now();
    await this._persist();
    return record;
  }

  async setStatus(deviceId, status) {
    assertOneOf(status, STATUSES, 'status');
    const record = this._records.get(deviceId);
    if (!record) throw new ValidationError(`Unknown device: ${deviceId}`, { deviceId });
    record.status = status;
    record.updatedAt = Date.now();
    await this._persist();
    return record;
  }

  /** Waits for any in-flight write to finish. Call before tearing down the process/data directory -- this write chain is independent of FileWalStorage's. */
  async close() {
    await this._writeChain;
  }
}

module.exports = { DeviceRegistry, STATUSES };
