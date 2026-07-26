'use strict';

const { AdapterBase } = require('./AdapterBase');
const { AdapterError } = require('../utils/errors');

/**
 * MqttAdapter: ingests telemetry published by meters/inverters onto an MQTT
 * broker, and publishes outbound command topics. Uses the pure-JS `mqtt`
 * package (no native bindings) so it installs cleanly under Termux.
 *
 * Topic convention (adjust to your fleet):
 *   Inbound telemetry: gridsync/telemetry/<deviceId>
 *   Outbound commands: gridsync/commands/<deviceId>
 *
 * Reconnect-failure logging is throttled: the 1st failure logs at `error`
 * with full detail, every 10th failure after that logs a concise `warn`,
 * and everything in between is counted but silent. This is enforced by
 * NOT emitting the generic 'error' event on connection failures -- doing
 * so would re-trigger AdapterBase's default error-level auto-logger on
 * every single reconnect tick, which is exactly the spam this prevents.
 * The counter resets to 0 on a successful connection.
 */
class MqttAdapter extends AdapterBase {
  constructor({ url, topicPrefix, reconnectPeriodMs, connectTimeoutMs, maxReconnectAttempts, logger, mqttLib }) {
    super('MQTT', logger);
    this.url = url;
    this.topicPrefix = topicPrefix;
    this.reconnectPeriodMs = reconnectPeriodMs;
    this.connectTimeoutMs = connectTimeoutMs;
    // 0 (or falsy) = unlimited reconnect attempts, the original behavior.
    this.maxReconnectAttempts = maxReconnectAttempts || 0;
    // Injected so the self-test suite can supply a fake MQTT client without a real broker.
    this._mqttLib = mqttLib || null;
    this.client = null;
    this._boundHandlers = null; // for clean listener teardown on reconnect/disconnect

    this._consecutiveErrorCount = 0;
    this._gaveUp = false; // true once maxReconnectAttempts is exceeded and we've stopped retrying
  }

  async connect() {
    const mqttLib = this._mqttLib || safeRequireMqtt();
    if (!mqttLib) {
      throw new AdapterError('mqtt package is not installed. Run `npm install` first.', { protocol: 'MQTT' });
    }

    this._gaveUp = false;

    return new Promise((resolve, reject) => {
      let settled = false;
      const client = mqttLib.connect(this.url, {
        reconnectPeriod: this.reconnectPeriodMs,
        connectTimeout: this.connectTimeoutMs,
      });
      this.client = client;

      const onConnect = () => {
        this.connected = true;
        this._consecutiveErrorCount = 0; // reset throttle counter on successful connection
        client.subscribe(this.topicPrefix, (err) => {
          if (err) {
            this.emit('error', new AdapterError('MQTT subscribe failed', { cause: err.message }));
            return;
          }
          this.emit('connected');
          if (!settled) {
            settled = true;
            resolve();
          }
        });
      };

      const onMessage = (topic, payloadBuffer) => {
        if (this.paused) return; // backpressure: drop at the transport edge, buffer owns the rest
        try {
          const deviceId = topic.split('/').pop();
          const text = payloadBuffer.toString('utf8');
          const parsed = JSON.parse(text);
          this.emit('data', parsed, { protocol: 'MQTT', deviceId, topic });
        } catch (err) {
          // A malformed message must never crash the process or stall the broker connection.
          this.emit('error', new AdapterError('Failed to parse MQTT payload', { cause: err.message, topic }));
        }
      };

      const onClose = () => {
        if (this._gaveUp) return; // already handled via the give-up path below
        this.connected = false;
        this.emit('disconnected', 'mqtt-close');
      };

      const onErr = (err) => {
        if (this._gaveUp) return; // stopped reconnecting -- ignore trailing events from teardown

        this._consecutiveErrorCount += 1;
        const count = this._consecutiveErrorCount;

        if (count === 1) {
          if (this.logger) {
            this.logger.error('MQTT connection failed', { cause: err.message, url: this.url });
          }
        } else if (count % 10 === 0) {
          if (this.logger) {
            this.logger.warn(`MQTT still disconnected after ${count} attempts`, { cause: err.message, url: this.url });
          }
        }
        // NOTE: no this.emit('error', ...) here on purpose -- see class docblock.

        if (this.maxReconnectAttempts > 0 && count >= this.maxReconnectAttempts) {
          if (this.logger) {
            this.logger.error(`MQTT giving up after ${count} failed attempts -- reconnection stopped`, {
              url: this.url,
            });
          }
          this._gaveUp = true;
          this._teardownListeners();
          this.connected = false;
          try {
            client.end(true);
          } catch {
            // client may already be in a bad state during teardown -- nothing more to do.
          }
          this.client = null; // guards disconnect() against calling .end() twice on the same client
          this.emit('disconnected', 'max-reconnect-attempts-exceeded');
        }

        if (!settled) {
          settled = true;
          reject(new AdapterError('MQTT initial connection failed', { cause: err.message }));
        }
      };

      this._boundHandlers = { onConnect, onMessage, onClose, onErr };
      client.on('connect', onConnect);
      client.on('message', onMessage);
      client.on('close', onClose);
      client.on('error', onErr);

      // Guard against a broker that never responds at all.
      setTimeout(() => {
        if (!settled) {
          settled = true;
          reject(new AdapterError('MQTT connection timed out', { url: this.url }));
        }
      }, this.connectTimeoutMs + 1000).unref();
    });
  }

  /** Removes all listeners we attached in connect() -- shared by disconnect() and the give-up path. */
  _teardownListeners() {
    if (!this.client || !this._boundHandlers) return;
    const { onConnect, onMessage, onClose, onErr } = this._boundHandlers;
    if (onConnect) this.client.removeListener('connect', onConnect);
    if (onMessage) this.client.removeListener('message', onMessage);
    if (onClose) this.client.removeListener('close', onClose);
    if (onErr) this.client.removeListener('error', onErr);
  }

  async disconnect() {
    if (!this.client) return; // already torn down (e.g. via the give-up path) -- nothing to do
    this._teardownListeners();

    await new Promise((resolve) => {
      this.client.end(false, {}, () => resolve());
    });
    this.connected = false;
    this.client = null;
  }

  async sendCommand(command) {
    if (!this.client || !this.connected) {
      throw new AdapterError('Cannot send command: MQTT client not connected', { commandId: command.commandId });
    }
    const topic = `gridsync/commands/${command.deviceId}`;
    return new Promise((resolve, reject) => {
      this.client.publish(topic, JSON.stringify(command), { qos: 1 }, (err) => {
        if (err) {
          reject(new AdapterError('MQTT publish failed', { cause: err.message, commandId: command.commandId }));
        } else {
          resolve();
        }
      });
    });
  }

  /** Observability helper -- lets callers/heartbeats report reconnect health without relying on log volume. */
  getReconnectStatus() {
    return {
      connected: this.connected,
      consecutiveErrorCount: this._consecutiveErrorCount,
      maxReconnectAttempts: this.maxReconnectAttempts,
      gaveUp: this._gaveUp,
    };
  }
}

function safeRequireMqtt() {
  try {
    // eslint-disable-next-line global-require
    return require('mqtt');
  } catch {
    return null;
  }
}

module.exports = { MqttAdapter };
