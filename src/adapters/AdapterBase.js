'use strict';

const EventEmitter = require('node:events');

/**
 * AdapterBase defines the contract every protocol adapter must satisfy.
 * This is what makes the ingestion layer protocol-agnostic: the
 * IngestionManager only ever talks to this interface, never to
 * MQTT/Modbus/DNP3 specifics directly.
 *
 * Events emitted:
 *   'data'  -> (rawPayload: any, meta: {protocol, deviceId?})   raw point, not yet normalized
 *   'error' -> (err: Error)                                      never throws synchronously
 *   'connected' -> ()
 *   'disconnected' -> (reason?: string)
 */
class AdapterBase extends EventEmitter {
  constructor(protocolName, logger) {
    super();
    this.protocolName = protocolName;
    this.logger = logger ? logger.child(`adapter:${protocolName}`) : null;
    this.connected = false;
    this.paused = false;

    // Defensive net: an adapter subclass emitting 'error' with zero listeners
    // would otherwise crash the whole process (EventEmitter default behavior).
    // We guarantee at least a swallow-and-log handler exists.
    this.on('error', (err) => {
      if (this.logger) this.logger.error('adapter error', { err });
    });
  }

  // eslint-disable-next-line class-methods-use-this
  async connect() {
    throw new Error(`${this.constructor.name} must implement connect()`);
  }

  // eslint-disable-next-line class-methods-use-this
  async disconnect() {
    throw new Error(`${this.constructor.name} must implement disconnect()`);
  }

  /** Backpressure hook: called by IngestionManager when its buffer is full. */
  pause() {
    this.paused = true;
  }

  /** Backpressure hook: called by IngestionManager when buffer has room again. */
  resume() {
    this.paused = false;
  }

  /**
   * Send an outbound command to the physical device.
   * Must resolve/reject -- must never throw synchronously, and must never
   * leave a dangling unhandled promise if the transport errors.
   */
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async sendCommand(_command) {
    throw new Error(`${this.constructor.name} must implement sendCommand()`);
  }
}

module.exports = { AdapterBase };
