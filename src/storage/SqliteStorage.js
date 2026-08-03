'use strict';

const path = require('node:path');
const fsp = require('node:fs/promises');
const { StorageAdapter } = require('./StorageAdapter');
const { StorageError } = require('../utils/errors');
const { assertPlainObject, assertNonEmptyString } = require('../utils/validation');

/**
 * SqliteStorage: uses Node's built-in `node:sqlite` module (no native
 * compilation step, no npm package) when available. This is preferable to
 * FileWalStorage for larger deployments (TimescaleDB-style querying of
 * telemetry history) but is gated behind a feature check because
 * `node:sqlite` is still experimental and only exists on newer Node
 * releases -- so this module must degrade cleanly rather than crash the
 * whole process if it isn't present.
 *
 * For real production time-series scale, swap this for a
 * TimescaleStorage.js implementing the same StorageAdapter interface
 * against Postgres/TimescaleDB -- the rest of GridSync-OS is unaffected,
 * since it only ever depends on the StorageAdapter contract.
 */
class SqliteStorage extends StorageAdapter {
  constructor({ dataDir, logger }) {
    super();
    this.dataDir = dataDir;
    this.dbFile = path.join(dataDir, 'gridsync.db');
    this.logger = logger ? logger.child('storage:sqlite') : null;
    this.db = null;
  }

  static isSupported() {
    try {
      // eslint-disable-next-line global-require
      require('node:sqlite');
      return true;
    } catch {
      return false;
    }
  }

  async init() {
    let DatabaseSync;
    try {
      // eslint-disable-next-line global-require
      ({ DatabaseSync } = require('node:sqlite'));
    } catch (err) {
      throw new StorageError(
        'node:sqlite is not available on this Node runtime. Use storage driver "file-wal" instead, or upgrade Node.',
        { cause: err.message },
      );
    }
    await fsp.mkdir(this.dataDir, { recursive: true });
    this.db = new DatabaseSync(this.dbFile);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS telemetry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT NOT NULL,
        protocol TEXT NOT NULL,
        device_type TEXT NOT NULL,
        ts INTEGER NOT NULL,
        metrics_json TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_telemetry_device_ts ON telemetry(device_id, ts);

      CREATE TABLE IF NOT EXISTS command_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        command_id TEXT NOT NULL,
        event TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        payload_json TEXT NOT NULL,
        ts INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_cmd_events_command_id ON command_events(command_id);

      CREATE TABLE IF NOT EXISTS alarm_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alarm_id TEXT NOT NULL,
        device_id TEXT NOT NULL,
        event TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        ts INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_alarm_events_alarm_id ON alarm_events(alarm_id);
    `);

    this._insertTelemetryStmt = this.db.prepare(
      'INSERT INTO telemetry (device_id, protocol, device_type, ts, metrics_json) VALUES (?, ?, ?, ?, ?)',
    );
    this._insertCommandEventStmt = this.db.prepare(
      'INSERT INTO command_events (command_id, event, status, attempts, payload_json, ts) VALUES (?, ?, ?, ?, ?, ?)',
    );
    this._insertAlarmEventStmt = this.db.prepare(
      'INSERT INTO alarm_events (alarm_id, device_id, event, status, payload_json, ts) VALUES (?, ?, ?, ?, ?, ?)',
    );
  }

  async appendTelemetry(point) {
    assertPlainObject(point, 'point');
    try {
      this._insertTelemetryStmt.run(
        point.deviceId,
        point.protocol,
        point.deviceType,
        point.timestamp,
        JSON.stringify(point.metrics),
      );
    } catch (err) {
      throw new StorageError('SQLite telemetry insert failed', { cause: err.message });
    }
  }

  async appendCommandEvent(record) {
    assertPlainObject(record, 'record');
    if (!record.commandId) throw new StorageError('appendCommandEvent requires record.commandId', { record });
    try {
      this._insertCommandEventStmt.run(
        record.commandId,
        record.event,
        record.status,
        record.attempts || 0,
        JSON.stringify(record),
        record.ts || Date.now(),
      );
    } catch (err) {
      throw new StorageError('SQLite command event insert failed', { cause: err.message });
    }
  }

  async appendAlarmEvent(record) {
    assertPlainObject(record, 'record');
    if (!record.alarmId) throw new StorageError('appendAlarmEvent requires record.alarmId', { record });
    try {
      this._insertAlarmEventStmt.run(
        record.alarmId,
        record.deviceId,
        record.event,
        record.status,
        JSON.stringify(record),
        record.ts || Date.now(),
      );
    } catch (err) {
      throw new StorageError('SQLite alarm event insert failed', { cause: err.message });
    }
  }

  async loadPendingCommands() {
    try {
      // Latest event per command_id, via max(id) grouping.
      const rows = this.db
        .prepare(
          `SELECT ce.* FROM command_events ce
           INNER JOIN (SELECT command_id, MAX(id) AS max_id FROM command_events GROUP BY command_id) latest
           ON ce.command_id = latest.command_id AND ce.id = latest.max_id
           WHERE ce.status NOT IN ('ACKED', 'FAILED')`,
        )
        .all();
      return rows.map((r) => JSON.parse(r.payload_json));
    } catch (err) {
      throw new StorageError('SQLite pending-commands query failed', { cause: err.message });
    }
  }

  async queryTelemetry(deviceId, limit, filters = {}) {
    assertNonEmptyString(deviceId, 'deviceId');
    try {
      let sql = 'SELECT * FROM telemetry WHERE device_id = ?';
      const params = [deviceId];
      if (filters.startTime !== undefined) {
        sql += ' AND ts >= ?';
        params.push(filters.startTime);
      }
      if (filters.endTime !== undefined) {
        sql += ' AND ts <= ?';
        params.push(filters.endTime);
      }
      sql += ' ORDER BY ts DESC LIMIT ?';
      params.push(limit);
      const rows = this.db.prepare(sql).all(...params);
      return rows.map((r) => ({
        deviceId: r.device_id,
        protocol: r.protocol,
        deviceType: r.device_type,
        timestamp: r.ts,
        metrics: JSON.parse(r.metrics_json),
      }));
    } catch (err) {
      throw new StorageError('SQLite telemetry query failed', { cause: err.message });
    }
  }

  async queryCommandHistory(limit, filters = {}) {
    try {
      let sql = `SELECT ce.* FROM command_events ce
           INNER JOIN (SELECT command_id, MAX(id) AS max_id FROM command_events GROUP BY command_id) latest
           ON ce.command_id = latest.command_id AND ce.id = latest.max_id`;
      const params = [];
      const where = [];
      if (filters.deviceId) {
        where.push(`json_extract(ce.payload_json, '$.deviceId') = ?`);
        params.push(filters.deviceId);
      }
      if (filters.status) {
        where.push('ce.status = ?');
        params.push(filters.status);
      }
      if (filters.startTime !== undefined) {
        where.push('ce.ts >= ?');
        params.push(filters.startTime);
      }
      if (filters.endTime !== undefined) {
        where.push('ce.ts <= ?');
        params.push(filters.endTime);
      }
      if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
      sql += ' ORDER BY ce.ts DESC LIMIT ?';
      params.push(limit);
      const rows = this.db.prepare(sql).all(...params);
      return rows.map((r) => JSON.parse(r.payload_json));
    } catch (err) {
      throw new StorageError('SQLite command history query failed', { cause: err.message });
    }
  }

  async queryAlarmHistory(limit, filters = {}) {
    try {
      let sql = `SELECT ae.* FROM alarm_events ae
           INNER JOIN (SELECT alarm_id, MAX(id) AS max_id FROM alarm_events GROUP BY alarm_id) latest
           ON ae.alarm_id = latest.alarm_id AND ae.id = latest.max_id`;
      const params = [];
      const where = [];
      if (filters.deviceId) {
        where.push('ae.device_id = ?');
        params.push(filters.deviceId);
      }
      if (filters.status) {
        where.push('ae.status = ?');
        params.push(filters.status);
      }
      if (filters.startTime !== undefined) {
        where.push('ae.ts >= ?');
        params.push(filters.startTime);
      }
      if (filters.endTime !== undefined) {
        where.push('ae.ts <= ?');
        params.push(filters.endTime);
      }
      if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
      sql += ' ORDER BY ae.ts DESC LIMIT ?';
      params.push(limit);
      const rows = this.db.prepare(sql).all(...params);
      return rows.map((r) => JSON.parse(r.payload_json));
    } catch (err) {
      throw new StorageError('SQLite alarm history query failed', { cause: err.message });
    }
  }

  async close() {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}

module.exports = { SqliteStorage };
