'use strict';

/**
 * Fixed-capacity circular buffer. Deliberately avoids Array.shift() (O(n))
 * so that push/drain stay O(1) even under high-velocity telemetry bursts.
 *
 * Overflow policy is drop-oldest: once full, pushing evicts the oldest
 * queued item. Callers should check `wasFull` return value to count drops.
 */
class RingBuffer {
  constructor(capacity) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('RingBuffer capacity must be a positive integer');
    }
    this.capacity = capacity;
    this._store = new Array(capacity);
    this._head = 0; // index of oldest item
    this._count = 0;
  }

  get size() {
    return this._count;
  }

  get isFull() {
    return this._count === this.capacity;
  }

  get isEmpty() {
    return this._count === 0;
  }

  /**
   * @returns {boolean} true if the buffer was already full and an item was evicted
   */
  push(item) {
    const wasFull = this.isFull;
    const tail = (this._head + this._count) % this.capacity;
    this._store[tail] = item;
    if (wasFull) {
      this._head = (this._head + 1) % this.capacity; // evict oldest
    } else {
      this._count += 1;
    }
    return wasFull;
  }

  /** Removes and returns up to `n` oldest items, in FIFO order. */
  drain(n) {
    const take = Math.min(n, this._count);
    const out = new Array(take);
    for (let i = 0; i < take; i += 1) {
      out[i] = this._store[this._head];
      this._store[this._head] = undefined; // avoid holding references (memory leak guard)
      this._head = (this._head + 1) % this.capacity;
    }
    this._count -= take;
    return out;
  }
}

module.exports = { RingBuffer };
