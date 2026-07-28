'use strict';

const crypto = require('node:crypto');
const EventEmitter = require('node:events');
const { assertPlainObject } = require('../utils/validation');
const { StorageError } = require('../utils/errors');

const TERMINAL_STATUSES = new Set(['ACKED', 'FAILED']);

/**
 * CommandQueue is the data-integrity backbone: "command signals must never
 * be lost during network drops."
 *
 * The guarantee is structural, not best-effort: `enqueue()` does not
 * resolve until the command's CREATED event has been durably written to
 * the storage WAL. Only after that write succeeds is the command handed to
 * the dispatcher for actual network transmission. If the process crashes
 * or a network drop happens *after* this point, `recover()` on the next
 * startup will find the record (status still non-terminal) and re-queue it
 * for dispatch -- so at-least-once delivery is preserved across restarts.
 *
 * Commands are idempotency-keyed by `commandId` (UUID), so a device-side
 * handler that has already processed a retried command can safely ignore
 * the duplicate.
 *
 * Emits 'ready' whenever a command becomes available for dispatch, so the
 * CommandDispatcher can be purely event-driven rather than polling (kinder
 * to battery on a mobile/Termux host).
 */
class CommandQueue extends EventEmitter {
  constructor({ storage, logger }) {
    super();
    this.storage = storage;
    this.logger = logger.child('command-queue');
    /** @type {Map<string, object>} commandId -> current record */
    this._records = new Map();
    /** @type {string[]} FIFO order of commandIds awaiting dispatch */
    this._pendingOrder = [];
  }

  /** Re-hydrates any non-terminal commands left over from a prior process run. */
  async recover() {
    let pending;
    try {
      pending = await this.storage.loadPendingCommands();
    } catch (err) {
      this.logger.error('command recovery failed -- continuing with empty queue', { err });
      return 0;
    }
    for (const record of pending) {
      if (!record.commandId) continue;
      // Recovered commands re-enter as PENDING regardless of their last
      // known status (e.g. DISPATCHING) -- we cannot know whether the
      // in-flight send actually reached the device before the crash, and
      // re-sending a curtail/discharge command is safe (idempotent at the
      // device via commandId) whereas silently dropping it is not.
      const rehydrated = { ...record, status: 'PENDING' };
      this._records.set(record.commandId, rehydrated);
      this._pendingOrder.push(record.commandId);
    }
    if (pending.length > 0) {
      this.logger.warn('recovered in-flight commands from prior run', { count: pending.length });
      this.emit('ready');
    }
    return pending.length;
  }

  /**
   * Durably enqueues a new command. Resolves only after the WAL write
   * completes -- this is the "never lost" guarantee's write-ahead half.
   * @param {{type: string, deviceId: string, value: number, reason?: string}} command
   * @returns {Promise<object>} the persisted command record
   */
  async enqueue(command) {
    assertPlainObject(command, 'command');
    const commandId = crypto.randomUUID();
    const record = {
      commandId,
      type: command.type,
      deviceId: command.deviceId,
      value: command.value,
      reason: command.reason || null,
      issuedBy: command.issuedBy || 'SYSTEM', // e.g. a username, or 'SYSTEM' for auto-generated FSM effects
      status: 'PENDING',
      attempts: 0,
      createdAt: Date.now(),
    };

    try {
      await this.storage.appendCommandEvent({ ...record, event: 'CREATED' });
    } catch (err) {
      // If we cannot durably record the command, we must not pretend it was
      // queued -- surface the failure rather than silently dropping intent.
      throw err instanceof StorageError ? err : new StorageError('Failed to persist command', { cause: err.message });
    }

    this._records.set(commandId, record);
    this._pendingOrder.push(commandId);
    this.emit('ready');
    return record;
  }

  /**
   * Dequeues the next command eligible for dispatch and hands ownership to
   * the caller (the dispatcher must then call markDispatching/markAcked/
   * markFailedAttempt to transition it -- it will not be re-offered by this
   * method until markFailedAttempt re-enqueues it for retry).
   */
  takeNextPending() {
    while (this._pendingOrder.length > 0) {
      const id = this._pendingOrder.shift();
      const record = this._records.get(id);
      if (!record || TERMINAL_STATUSES.has(record.status)) {
        continue; // stale entry (already terminal), skip
      }
      if (record.status === 'PENDING') return record;
      // status is DISPATCHING (already owned elsewhere) -- shouldn't normally
      // be in the order array, but skip defensively rather than reprocess.
    }
    return null;
  }

  async markDispatching(commandId) {
    const record = this._records.get(commandId);
    if (!record) return;
    record.status = 'DISPATCHING';
    await this._persist(record, 'DISPATCHING');
  }

  async markAcked(commandId) {
    const record = this._records.get(commandId);
    if (!record) return;
    record.status = 'ACKED';
    record.acknowledgedAt = Date.now();
    await this._persist(record, 'ACKED');
    this._records.delete(commandId);
  }

  async markFailedAttempt(commandId, reason, { maxAttempts }) {
    const record = this._records.get(commandId);
    if (!record) return null;
    record.attempts += 1;
    record.lastError = reason;

    if (record.attempts >= maxAttempts) {
      record.status = 'FAILED';
      await this._persist(record, 'FAILED');
      this._records.delete(commandId);
      this.logger.error('command permanently failed after max attempts', {
        commandId,
        attempts: record.attempts,
        reason,
      });
      return 'FAILED';
    }

    record.status = 'PENDING'; // eligible for retry
    await this._persist(record, 'RETRY_SCHEDULED');
    this._pendingOrder.push(commandId);
    return 'RETRY_SCHEDULED';
  }

  async _persist(record, event) {
    try {
      await this.storage.appendCommandEvent({ ...record, event });
    } catch (err) {
      // A WAL write failure here means our on-disk record of this command's
      // status is now behind reality. We log loudly rather than throw,
      // because throwing here would abort dispatch bookkeeping mid-flight
      // and risk double-processing; the in-memory record remains
      // authoritative for this process's lifetime.
      this.logger.error('failed to persist command status transition', { commandId: record.commandId, event, err });
    }
  }

  /** Non-destructive snapshot of currently live (non-terminal) commands, for monitoring/API use. */
  listPending() {
    return [...this._records.values()].map((r) => ({ ...r }));
  }

  size() {
    return this._pendingOrder.length;
  }
}

module.exports = { CommandQueue };
