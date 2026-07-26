'use strict';

const { RingBuffer } = require('../utils/RingBuffer');
const { Normalizer } = require('./Normalizer');
const { ValidationError } = require('../utils/errors');

/**
 * IngestionManager is the boundary between "protocol adapters" and
 * "everything else". It owns one bounded RingBuffer per adapter, applies
 * backpressure when a buffer fills, and drains buffers in small batches so
 * a high-velocity burst on one adapter can never starve the event loop (and
 * therefore command dispatch, health checks, or other adapters).
 *
 * Concurrency guard: `_draining` is a per-adapter mutex flag. Without it, a
 * fast producer (adapter emitting 'data' repeatedly) combined with the
 * self-scheduling setImmediate drain loop could end up with two drain
 * cycles interleaved, which would violate FIFO ordering guarantees and
 * could double-process items under certain buffer states. The flag makes
 * "start a new drain loop" idempotent.
 */
class IngestionManager {
  constructor({ config, logger, onPoint, onInvalid }) {
    this.config = config;
    this.logger = logger.child('ingestion');
    this.onPoint = typeof onPoint === 'function' ? onPoint : () => {};
    this.onInvalid = typeof onInvalid === 'function' ? onInvalid : () => {};

    /** @type {Map<string, {adapter: object, buffer: RingBuffer, draining: boolean}>} */
    this._adapters = new Map();

    this.metrics = {
      received: 0,
      normalized: 0,
      invalid: 0,
      dropped: 0,
    };
  }

  registerAdapter(adapterId, adapter) {
    if (this._adapters.has(adapterId)) {
      throw new Error(`Adapter "${adapterId}" already registered`);
    }
    const buffer = new RingBuffer(this.config.ingestion.maxBufferSize);
    const entry = { adapter, buffer, draining: false, paused: false };
    this._adapters.set(adapterId, entry);

    adapter.on('data', (raw, meta) => {
      this._enqueue(adapterId, raw, meta);
    });
    adapter.on('error', (err) => {
      this.logger.warn('adapter reported error', { adapterId, err: err.message, code: err.code });
    });

    return entry;
  }

  _enqueue(adapterId, raw, meta) {
    const entry = this._adapters.get(adapterId);
    if (!entry) return; // defensive: adapter emitted after teardown

    this.metrics.received += 1;
    const evicted = entry.buffer.push({ raw, meta, receivedAt: Date.now() });

    if (evicted) {
      this.metrics.dropped += 1;
      this.logger.warn('buffer full, dropped oldest point', { adapterId, bufferSize: entry.buffer.size });
    }

    // Backpressure: if we're at/near capacity, ask the adapter to pause
    // producing (if it supports it). This is best-effort -- adapters that
    // can't pause (e.g. a poll-based simulator mid-tick) fall back to the
    // drop-oldest policy above, which still bounds memory.
    if (entry.buffer.isFull && !entry.paused) {
      entry.paused = true;
      entry.adapter.pause();
      this.logger.warn('backpressure: pausing adapter', { adapterId });
    }

    this._scheduleDrain(adapterId);
  }

  _scheduleDrain(adapterId) {
    const entry = this._adapters.get(adapterId);
    if (!entry || entry.draining) return; // mutex: a drain loop is already running
    entry.draining = true;
    setImmediate(() => this._drain(adapterId));
  }

  _drain(adapterId) {
    const entry = this._adapters.get(adapterId);
    if (!entry) return;

    const batch = entry.buffer.drain(this.config.ingestion.batchSize);
    for (const item of batch) {
      this._processOne(adapterId, item);
    }

    // Resume the adapter once the buffer has drained below the low-water
    // mark, so we don't thrash pause/resume on every single item near capacity.
    const lowWaterMark = Math.floor(this.config.ingestion.maxBufferSize * 0.5);
    if (entry.paused && entry.buffer.size <= lowWaterMark) {
      entry.paused = false;
      entry.adapter.resume();
      this.logger.info('backpressure released: resuming adapter', { adapterId });
    }

    if (entry.buffer.isEmpty) {
      entry.draining = false; // idle until next _enqueue wakes us up
    } else {
      // More work queued -- yield to the event loop first so timers, I/O
      // callbacks, and other adapters' drains get a turn.
      setImmediate(() => this._drain(adapterId));
    }
  }

  _processOne(adapterId, item) {
    try {
      const point = Normalizer.normalize(item.raw, item.meta);
      this.metrics.normalized += 1;
      this.onPoint(point, adapterId);
    } catch (err) {
      this.metrics.invalid += 1;
      if (err instanceof ValidationError) {
        this.logger.debug('discarding invalid telemetry point', { adapterId, reason: err.message });
      } else {
        this.logger.error('unexpected error normalizing point', { adapterId, err });
      }
      this.onInvalid(err, item);
    }
  }

  getMetrics() {
    const perAdapter = {};
    for (const [id, entry] of this._adapters) {
      perAdapter[id] = { bufferSize: entry.buffer.size, paused: entry.paused };
    }
    return { ...this.metrics, perAdapter };
  }
}

module.exports = { IngestionManager };
