'use strict';

const { AdapterBase } = require('./AdapterBase');
const { AdapterError } = require('../utils/errors');

/**
 * ModbusAdapter — normalizes Modbus register reads into the canonical
 * telemetry schema.
 *
 * IMPORTANT (production note): real Modbus/TCP polling requires a dedicated
 * client (e.g. `jsmodbus` or `modbus-serial`) talking to each inverter's
 * IP:port and a register map per device model. That is fleet-specific and
 * out of scope for a generic middleware core. This adapter ships in
 * "simulated" mode by default (deterministic synthetic readings) so the
 * ingestion/engine/storage pipeline is fully testable without hardware.
 *
 * To go live: implement `_pollOnce()` to read real registers and call
 * `this._emitReading(deviceId, registers)` with the decoded values --
 * everything downstream (normalizer, engine, storage) is unchanged.
 */
class ModbusAdapter extends AdapterBase {
  constructor({ deviceIds = ['inverter-01'], pollIntervalMs = 1000, logger, simulate = true } = {}) {
    super('MODBUS', logger);
    this.deviceIds = deviceIds;
    this.pollIntervalMs = Math.max(50, pollIntervalMs);
    this.simulate = simulate;
    this._pollTimer = null;
    this._tick = 0;
  }

  async connect() {
    if (!this.simulate) {
      throw new AdapterError('Live Modbus polling is not implemented in this adapter -- see class docblock.', {
        protocol: 'MODBUS',
      });
    }
    this.connected = true;
    this._pollTimer = setInterval(() => this._pollOnce(), this.pollIntervalMs);
    this._pollTimer.unref?.();
    this.emit('connected');
  }

  async disconnect() {
    if (this._pollTimer) {
      clearInterval(this._pollTimer);
      this._pollTimer = null;
    }
    this.connected = false;
    this.emit('disconnected', 'manual');
  }

  _pollOnce() {
    if (this.paused) return; // backpressure: skip this poll cycle entirely
    this._tick += 1;
    for (const deviceId of this.deviceIds) {
      try {
        const reading = this._simulateReading(deviceId);
        this.emit('data', reading, { protocol: 'MODBUS', deviceId });
      } catch (err) {
        this.emit('error', new AdapterError('Modbus simulated read failed', { cause: err.message, deviceId }));
      }
    }
  }

  _simulateReading(deviceId) {
    // Deterministic-ish synthetic waveform so self-tests can assert on shape,
    // with a small pseudo-random jitter for realism.
    const base = 230 + Math.sin(this._tick / 5) * 3;
    const jitter = (Math.random() - 0.5) * 1.5;
    return {
      deviceType: 'INVERTER',
      voltage: Number((base + jitter).toFixed(2)),
      frequency: Number((50 + Math.sin(this._tick / 20) * 0.05).toFixed(3)),
      powerKw: Number((Math.max(0, 5 + Math.sin(this._tick / 8) * 4)).toFixed(2)),
      soc: Number(Math.min(1, Math.max(0, 0.5 + Math.sin(this._tick / 50) * 0.3)).toFixed(3)),
      timestamp: Date.now(),
    };
  }

  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async sendCommand(command) {
    // Simulated actuation: in production this would write to a holding
    // register via the Modbus client. We resolve immediately to model a
    // successful write.
    return Promise.resolve({ acked: true, commandId: command.commandId });
  }
}

module.exports = { ModbusAdapter };
