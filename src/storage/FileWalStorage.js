'use strict';

const fsp = require('node:fs/promises');
const path = require('node:path');
const { StorageAdapter } = require('./StorageAdapter');
const { StorageError } = require('../utils/errors');
const { assertPlainObject } = require('../utils/validation');

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
 * one at a time -- while a slow write to the *commands* file never blocks
 * a *telemetry* write, since each file has its own independent chain.
 */
class FileWalStorage extends StorageAdapter {
  constructor({ dataDir, compactionIntervalMs, maxWalLinesBeforeCompaction, logger }) {
    super();
    this.dataDir = dataDir;
    this.telemetryFile = path.join(dataDir, 'telemetry.jsonl');
    this.commandsFile = path.join(dataDir, 'commands.jsonl');
    this.compactionIntervalMs = compactionIntervalMs;
    this.maxWalLinesBeforeCompaction = maxWalLinesBeforeCompaction;
    this.logger = logger ? logger.child('storage:file-wal') : null;

    this._telemetryChain = null;
    this._commandChain = null;
    this._commandLineCount = 0;
    this._compactionTimer = null;
  }

  async init() {
    await fsp.mkdir(this.dataDir, { recursive: true });
    for (const file of [this.telemetryFile, this.commandsFile]) {
      try {
        await fsp.access(file);
      } catch {
        await fsp.writeFile(file, '');
      }
    }
    try {
      const content = await fsp.readFile(this.commandsFile, 'utf8');
      this._commandLineCount = content.split('\n').filter(Boolean).length;
    } catch {
      this._commandLineCount = 0;
    }

    this._compactionTimer = setInterval(() => {
      this._compactCommands().catch((err) => {
        if (this.logger) this.logger.error('scheduled compaction failed', { err });
      });
    }, this.compactionIntervalMs);
    this._compactionTimer.unref?.();
  }

  async appendTelemetry(point) {
    assertPlainObject(point, 'point');
    return this._enqueue('_telemetryChain', () =>
      fsp.appendFile(this.telemetryFile, `${JSON.stringify(point)}\n`, 'utf8'));
  }

  async appendCommandEvent(record) {
    assertPlainObject(record, 'record');
    if (!record.commandId) {
      throw new StorageError('appendCommandEvent requires record.commandId', { record });
    }
    const line = `${JSON.stringify({ ...record, ts: record.ts || Date.now() })}\n`;
    const result = await this._enqueue('_commandChain', () => fsp.appendFile(this.commandsFile, line, 'utf8'));
    this._commandLineCount += 1;
    if (this._commandLineCount >= this.maxWalLinesBeforeCompaction) {
      this._compactCommands().catch((err) => {
        if (this.logger) this.logger.error('threshold compaction failed', { err });
      });
    }
    return result;
  }

  async loadPendingCommands() {
    const latest = await this._readLatestCommandStates();
    return Object.values(latest).filter((r) => r.status !== 'ACKED' && r.status !== 'FAILED');
  }

  async _readLatestCommandStates() {
    let content;
    try {
      content = await fsp.readFile(this.commandsFile, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return {};
      throw new StorageError('Failed to read command WAL', { cause: err.message });
    }
    const latest = {};
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        const rec = JSON.parse(line);
        if (!rec.commandId) continue;
        latest[rec.commandId] = rec; // later lines overwrite earlier entries: "latest state wins"
      } catch {
        // A single corrupted line (e.g. a partial write from a killed
        // process) must never prevent recovery of every other command.
        if (this.logger) this.logger.warn('skipping corrupt WAL line during recovery scan');
      }
    }
    return latest;
  }

  async _compactCommands() {
    return this._enqueue('_commandChain', async () => {
      const latest = await this._readLatestCommandStates();
      const lines = Object.values(latest).map((r) => JSON.stringify(r)).join('\n');
      const tmpFile = `${this.commandsFile}.tmp`;
      await fsp.writeFile(tmpFile, lines ? `${lines}\n` : '');
      await fsp.rename(tmpFile, this.commandsFile); // atomic on POSIX filesystems
      this._commandLineCount = Object.keys(latest).length;
      if (this.logger) this.logger.info('command WAL compacted', { records: this._commandLineCount });
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
    await Promise.all([this._telemetryChain, this._commandChain].filter(Boolean));
  }
}

module.exports = { FileWalStorage };
