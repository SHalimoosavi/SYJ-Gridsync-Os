'use strict';

const fsp = require('node:fs/promises');
const path = require('node:path');
const { StorageAdapter } = require('./StorageAdapter');
const { StorageError } = require('../utils/errors');
const { assertPlainObject, assertNonEmptyString } = require('../utils/validation');

/**
 * FileWalStorage: append-only JSONL files, zero external dependencies.
 * Chosen as the default backend because it has zero native build
 * requirements and behaves identically across every Node version you might
 * find across different Termux installs.
 *
 * Race-condition guard: concurrent calls to `fs.appendFile` on the same
 * path are NOT guaranteed to interleave safely at the OS level under all
 * conditions across platforms, and even where they are, out-of-order
 * completion would break the WAL's "later line wins" recovery semantics.
 * All writes to a given file are therefore serialized through a promise
 * chain (`_enqueue`) so writes for that file always land in call order,
 * one at a time -- while a slow write to one file never blocks a write to
 * another, since each has its own independent chain.
 *
 * Commands and alarms share the exact same shape of need -- an id-keyed
 * event log where "the latest line for a given id wins" on recovery/query
 * -- so both are implemented via the same generalized helpers
 * (`_readLatestStates` / `_appendIdKeyedEvent` / `_compactIdKeyedLog`)
 * rather than two near-identical copies of the same logic.
 */
class FileWalStorage extends StorageAdapter {
  constructor({ dataDir, compactionIntervalMs, maxWalLinesBeforeCompaction, logger }) {
    super();
    this.dataDir = dataDir;
    this.telemetryFile = path.join(dataDir, 'telemetry.jsonl');
    this.commandsFile = path.join(dataDir, 'commands.jsonl');
    this.alarmsFile = path.join(dataDir, 'alarms.jsonl');
    this.compactionIntervalMs = compactionIntervalMs;
    this.maxWalLinesBeforeCompaction = maxWalLinesBeforeCompaction;
    this.logger = logger ? logger.child('storage:file-wal') : null;

    this._telemetryChain = null;
    this._commandChain = null;
    this._alarmChain = null;
    this._commandLineCount = 0;
    this._alarmLineCount = 0;
    this._compactionTimer = null;
  }

  async init() {
    await fsp.mkdir(this.dataDir, { recursive: true });
    for (const file of [this.telemetryFile, this.commandsFile, this.alarmsFile]) {
      try {
        await fsp.access(file);
      } catch {
        await fsp.writeFile(file, '');
      }
    }
    this._commandLineCount = await this._countLines(this.commandsFile);
    this._alarmLineCount = await this._countLines(this.alarmsFile);

    this._compactionTimer = setInterval(() => {
      this._compactIdKeyedLog('_commandChain', this.commandsFile, 'commandId', 'command').catch((err) => {
        if (this.logger) this.logger.error('scheduled command WAL compaction failed', { err });
      });
      this._compactIdKeyedLog('_alarmChain', this.alarmsFile, 'alarmId', 'alarm').catch((err) => {
        if (this.logger) this.logger.error('scheduled alarm WAL compaction failed', { err });
      });
    }, this.compactionIntervalMs);
    this._compactionTimer.unref?.();
  }

  async _countLines(file) {
    try {
      const content = await fsp.readFile(file, 'utf8');
      return content.split('\n').filter(Boolean).length;
    } catch {
      return 0;
    }
  }

  async appendTelemetry(point) {
    assertPlainObject(point, 'point');
    return this._enqueue('_telemetryChain', () =>
      fsp.appendFile(this.telemetryFile, `${JSON.stringify(point)}\n`, 'utf8'));
  }

  async appendCommandEvent(record) {
    const result = await this._appendIdKeyedEvent('_commandChain', this.commandsFile, record, 'commandId');
    this._commandLineCount += 1;
    if (this._commandLineCount >= this.maxWalLinesBeforeCompaction) {
      this._compactIdKeyedLog('_commandChain', this.commandsFile, 'commandId', 'command').catch((err) => {
        if (this.logger) this.logger.error('threshold command compaction failed', { err });
      });
    }
    return result;
  }

  async appendAlarmEvent(record) {
    const result = await this._appendIdKeyedEvent('_alarmChain', this.alarmsFile, record, 'alarmId');
    this._alarmLineCount += 1;
    if (this._alarmLineCount >= this.maxWalLinesBeforeCompaction) {
      this._compactIdKeyedLog('_alarmChain', this.alarmsFile, 'alarmId', 'alarm').catch((err) => {
        if (this.logger) this.logger.error('threshold alarm compaction failed', { err });
      });
    }
    return result;
  }

  async _appendIdKeyedEvent(chainKey, file, record, idField) {
    assertPlainObject(record, 'record');
    if (!record[idField]) {
      throw new StorageError(`Event record requires .${idField}`, { record });
    }
    const line = `${JSON.stringify({ ...record, ts: record.ts || Date.now() })}\n`;
    return this._enqueue(chainKey, () => fsp.appendFile(file, line, 'utf8'));
  }

  async loadPendingCommands() {
    const latest = await this._readLatestStates(this.commandsFile, 'commandId');
    return Object.values(latest).filter((r) => r.status !== 'ACKED' && r.status !== 'FAILED');
  }

  async queryTelemetry(deviceId, limit, filters = {}) {
    assertNonEmptyString(deviceId, 'deviceId');
    let content;
    try {
      content = await fsp.readFile(this.telemetryFile, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return [];
      throw new StorageError('Failed to read telemetry log', { cause: err.message });
    }
    let matches = [];
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        const point = JSON.parse(line);
        if (point.deviceId === deviceId) matches.push(point);
      } catch {
        // A single corrupted line must never break a query -- skip and continue.
        if (this.logger) this.logger.warn('skipping corrupt telemetry line during query');
      }
    }
    matches = this._filterByTimeRange(matches, (p) => p.timestamp, filters);
    // Sort by timestamp explicitly rather than trusting file-append order
    // to equal chronological order -- concurrent writes for the same
    // device are not guaranteed to land in temporal order (see the
    // orchestrator's _handlePoint docblock for a real case that violated
    // this assumption).
    matches.sort((a, b) => a.timestamp - b.timestamp);
    return matches.slice(-limit).reverse();
  }

  async queryCommandHistory(limit, filters = {}) {
    const latest = await this._readLatestStates(this.commandsFile, 'commandId');
    let all = Object.values(latest);
    if (filters.deviceId) all = all.filter((r) => r.deviceId === filters.deviceId);
    if (filters.status) all = all.filter((r) => r.status === filters.status);
    all = this._filterByTimeRange(all, (r) => r.ts || 0, filters);
    all.sort((a, b) => (b.ts || 0) - (a.ts || 0));
    return all.slice(0, limit);
  }

  async queryAlarmHistory(limit, filters = {}) {
    const latest = await this._readLatestStates(this.alarmsFile, 'alarmId');
    let all = Object.values(latest);
    if (filters.deviceId) all = all.filter((r) => r.deviceId === filters.deviceId);
    if (filters.status) all = all.filter((r) => r.status === filters.status);
    all = this._filterByTimeRange(all, (r) => r.ts || 0, filters);
    all.sort((a, b) => (b.ts || 0) - (a.ts || 0));
    return all.slice(0, limit);
  }

  /**
   * Filters `items` to those whose timestamp (via `getTimestamp`) falls
   * within [startTime, endTime] inclusive. Either bound may be omitted.
   * No-ops (returns `items` unchanged) if neither bound is present, to
   * avoid an unnecessary array copy on the (default) unfiltered path.
   */
  _filterByTimeRange(items, getTimestamp, { startTime, endTime } = {}) {
    if (startTime === undefined && endTime === undefined) return items;
    return items.filter((item) => {
      const ts = getTimestamp(item);
      if (startTime !== undefined && ts < startTime) return false;
      if (endTime !== undefined && ts > endTime) return false;
      return true;
    });
  }

  /** Reads an id-keyed WAL file and returns the latest record per id ("later line wins"). */
  async _readLatestStates(file, idField) {
    let content;
    try {
      content = await fsp.readFile(file, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return {};
      throw new StorageError(`Failed to read WAL: ${file}`, { cause: err.message });
    }
    const latest = {};
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        const rec = JSON.parse(line);
        if (!rec[idField]) continue;
        latest[rec[idField]] = rec; // later lines overwrite earlier entries: "latest state wins"
      } catch {
        // A single corrupted line (e.g. a partial write from a killed
        // process) must never prevent recovery of every other record.
        if (this.logger) this.logger.warn('skipping corrupt WAL line during scan', { file });
      }
    }
    return latest;
  }

  async _compactIdKeyedLog(chainKey, file, idField, label) {
    return this._enqueue(chainKey, async () => {
      const latest = await this._readLatestStates(file, idField);
      const lines = Object.values(latest).map((r) => JSON.stringify(r)).join('\n');
      const tmpFile = `${file}.tmp`;
      await fsp.writeFile(tmpFile, lines ? `${lines}\n` : '');
      await fsp.rename(tmpFile, file); // atomic on POSIX filesystems
      const count = Object.keys(latest).length;
      if (chainKey === '_commandChain') this._commandLineCount = count;
      if (chainKey === '_alarmChain') this._alarmLineCount = count;
      if (this.logger) this.logger.info(`${label} WAL compacted`, { records: count });
    });
  }

  /** Serializes all writes to a given logical file through a chained promise. */
  _enqueue(chainKey, task) {
    const prevChain = this[chainKey] || Promise.resolve();
    const runPromise = prevChain.then(() => task());
    // The stored chain must never itself become a rejected promise -- that
    // would permanently wedge every future write behind a dead chain link.
    this[chainKey] = runPromise.then(() => undefined, () => undefined);
    return runPromise.catch((err) => {
      throw err instanceof StorageError ? err : new StorageError('Storage write failed', { cause: err.message });
    });
  }

  async close() {
    if (this._compactionTimer) {
      clearInterval(this._compactionTimer);
      this._compactionTimer = null;
    }
    await Promise.all([this._telemetryChain, this._commandChain, this._alarmChain].filter(Boolean));
  }
}

module.exports = { FileWalStorage };
