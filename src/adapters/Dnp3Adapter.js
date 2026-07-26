'use strict';

const { AdapterBase } = require('./AdapterBase');
const { AdapterError } = require('../utils/errors');

/**
 * Dnp3Adapter — normalizes DNP3 outstation data (typically from utility-side
 * meters/RTUs) into the canonical telemetry schema.
 *
 * IMPORTANT (production note): there is no mature, actively-maintained pure-JS
 * DNP3 master stack. Real deployments almost always front DNP3 devices with a
 * dedicated protocol gateway (e.g. a hardened C/C++ DNP3 master such as
 * opendnp3, or a commercial protocol converter) that republishes normalized
 * points over MQTT or a local REST/gRPC endpoint -- which GridSync-OS then
 * consumes via MqttAdapter. This adapter ships in "simulated" mode so the
 * rest of the pipeline is exercised end-to-end without that infrastructure.
 *
 * To go live: point `_pollOnce()` at your DNP3 gateway's output and call
 * `this.emit('data', reading, {protocol: 'DNP3', deviceId})` with decoded
 * values -- the downstream contract is identical to every other adapter.
 */
class Dnp3Adapter extends AdapterBase {
  constructor({ deviceIds = ['meter-01'], pollIntervalMs = 2000, logger, simulate = true } = {}) {
    super('DNP3', logger);
    this.deviceIds = deviceIds;
    this.pollIntervalMs = Math.max(50, pollIntervalMs);
    this.simulate = simulate;
    this._pollTimer = null;
    this._tick = 0;
  }

  async connect() {
    if (!this.simulate) {
      throw new AdapterError('Live DNP3 polling requires an external gateway -- see class docblock.', {
        protocol: 'DNP3',
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
    if (this.paused) return;
    this._tick += 1;
    for (const deviceId of this.deviceIds) {
      try {
        const reading = this._simulateReading(deviceId);
        this.emit('data', reading, { protocol: 'DNP3', deviceId });
      } catch (err) {
        this.emit('error', new AdapterError('DNP3 simulated read failed', { cause: err.message, deviceId }));
      }
    }
  }

  _simulateReading(deviceId) {
    const base = 230 + Math.cos(this._tick / 7) * 2;
    return {
      deviceType: 'METER',
      voltage: Number(base.toFixed(2)),
      frequency: Number((50 + Math.cos(this._tick / 25) * 0.03).toFixed(3)),
      powerKw: Number((3 + Math.cos(this._tick / 10) * 2).toFixed(2)),
      timestamp: Date.now(),
    };
  }

  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async sendCommand(command) {
    return Promise.resolve({ acked: true, commandId: command.commandId });
  }
}

module.exports = { Dnp3Adapter };
