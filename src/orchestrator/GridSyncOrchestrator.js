'use strict';

const { IngestionManager } = require('../ingestion/IngestionManager');
const { StateMachine } = require('../engine/StateMachine');
const { ConstraintValidator } = require('../engine/ConstraintValidator');
const { CircuitBreaker } = require('../engine/CircuitBreaker');
const { CommandQueue } = require('../commands/CommandQueue');
const { CommandDispatcher } = require('../commands/CommandDispatcher');
const { FileWalStorage } = require('../storage/FileWalStorage');
const { SqliteStorage } = require('../storage/SqliteStorage');
const { ApiServer } = require('../api/ApiServer');
const { assertPlainObject, assertNonEmptyString } = require('../utils/validation');

/**
 * GridSyncOrchestrator is the composition root. It owns no protocol or
 * storage details itself -- it just wires the pieces (ingestion -> state
 * machine -> command queue -> dispatcher -> adapters/storage) and keeps the
 * small pieces of shared state (latest telemetry per device, FSM state per
 * device, deviceId -> adapterId routing) that legitimately need to live
 * somewhere central.
 */
class GridSyncOrchestrator {
  constructor({ config, logger }) {
    this.config = config;
    this.logger = logger.child('orchestrator');

    this.storage = this._buildStorage();
    this.circuitBreaker = new CircuitBreaker({ ...config.circuitBreaker, logger: this.logger });
    this.constraintValidator = new ConstraintValidator(config.gridConstraints);
    this.stateMachine = new StateMachine(this.constraintValidator);
    this.commandQueue = new CommandQueue({ storage: this.storage, logger: this.logger });

    /** @type {Map<string, object>} adapterId -> adapter instance */
    this.adapters = new Map();
    /** @type {Map<string, string>} deviceId -> adapterId that last reported for it */
    this._deviceAdapterRouting = new Map();
    /** @type {Map<string, object>} deviceId -> latest normalized telemetry point */
    this._latestPoints = new Map();
    /** @type {Map<string, object>} deviceId -> current FSM state */
    this._deviceStates = new Map();

    this.ingestionManager = new IngestionManager({
      config,
      logger: this.logger,
      onPoint: (point, adapterId) => {
        this._handlePoint(point, adapterId).catch((err) => {
          this.logger.error('unhandled error while processing telemetry point (point dropped)', {
            deviceId: point?.deviceId,
            err,
          });
        });
      },
      onInvalid: () => {
        this.circuitBreaker.recordError();
      },
    });

    this.commandDispatcher = new CommandDispatcher({
      queue: this.commandQueue,
      circuitBreaker: this.circuitBreaker,
      constraintValidator: this.constraintValidator,
      resolveAdapter: (deviceId) => this._resolveAdapterForDevice(deviceId),
      getLatestState: (deviceId) => this._latestPoints.get(deviceId) || null,
      config,
      logger: this.logger,
    });

    this.apiServer = config.api.enabled
      ? new ApiServer({ orchestrator: this, config, logger: this.logger })
      : null;

    this._started = false;
  }

  _buildStorage() {
    const driver = this.config.storage.driver;
    if (driver === 'sqlite') {
      if (!SqliteStorage.isSupported()) {
        this.logger?.warn?.('sqlite storage requested but node:sqlite unavailable -- falling back to file-wal');
        return new FileWalStorage({ ...this.config.storage, logger: this.logger });
      }
      return new SqliteStorage({ dataDir: this.config.storage.dataDir, logger: this.logger });
    }
    return new FileWalStorage({ ...this.config.storage, logger: this.logger });
  }

  /** Registers a protocol adapter under a caller-chosen id (e.g. "mqtt-main", "modbus-fleet-a"). */
  registerAdapter(adapterId, adapter) {
    assertNonEmptyString(adapterId, 'adapterId');
    assertPlainObject(adapter, 'adapter');
    this.adapters.set(adapterId, adapter);
    this.ingestionManager.registerAdapter(adapterId, adapter);
  }

  _resolveAdapterForDevice(deviceId) {
    const adapterId = this._deviceAdapterRouting.get(deviceId);
    if (!adapterId) return null;
    return this.adapters.get(adapterId) || null;
  }

  async _handlePoint(point, adapterId) {
    this._deviceAdapterRouting.set(point.deviceId, adapterId);
    this.circuitBreaker.recordTelemetry(point.deviceId, point.timestamp);
    this._latestPoints.set(point.deviceId, point);

    try {
      await this.storage.appendTelemetry(point);
    } catch (err) {
      // A storage hiccup must not stop the safety-critical control loop --
      // log loudly and keep processing state transitions in memory.
      this.logger.error('failed to persist telemetry point (continuing)', { deviceId: point.deviceId, err });
    }

    const prevState = this._deviceStates.get(point.deviceId) || StateMachine.initialDeviceState(point.deviceId);
    const { newState, effects } = this.stateMachine.transition(prevState, point);
    this._deviceStates.set(point.deviceId, newState);

    for (const effect of effects) {
      // eslint-disable-next-line no-await-in-loop
      await this.commandQueue.enqueue(effect);
    }
  }

  /** Manually issue a command (e.g. operator-initiated curtailment/discharge), same durability guarantees as auto-generated ones. */
  async issueManualCommand({ type, deviceId, value, reason }) {
    assertNonEmptyString(deviceId, 'deviceId');
    return this.commandQueue.enqueue({ type, deviceId, value, reason: reason || 'MANUAL' });
  }

  /** All known devices with their current FSM mode and latest telemetry -- used by the API/dashboard. */
  listDevices() {
    const devices = [];
    for (const [deviceId, state] of this._deviceStates) {
      devices.push({
        deviceId,
        mode: state.mode,
        consecutiveViolations: state.consecutiveViolations,
        lastPoint: this._latestPoints.get(deviceId) || null,
      });
    }
    return devices;
  }

  /** Full detail for one device, or null if it's never reported telemetry. */
  getDeviceDetail(deviceId) {
    const state = this._deviceStates.get(deviceId);
    if (!state) return null;
    return {
      deviceId,
      mode: state.mode,
      consecutiveViolations: state.consecutiveViolations,
      violations: state.violations,
      lastPoint: this._latestPoints.get(deviceId) || null,
      adapterId: this._deviceAdapterRouting.get(deviceId) || null,
    };
  }

  async start() {
    if (this._started) return;
    await this.storage.init();
    const recoveredCount = await this.commandQueue.recover();
    this.commandDispatcher.start();

    for (const [adapterId, adapter] of this.adapters) {
      // eslint-disable-next-line no-await-in-loop
      await adapter.connect().catch((err) => {
        // A single adapter failing to connect must not prevent the rest of
        // the fleet (or the control loop itself) from starting.
        this.logger.error('adapter failed to connect at startup', { adapterId, err });
      });
    }

    if (this.apiServer) {
      await this.apiServer.start().catch((err) => {
        // A failed-to-bind API server (e.g. port already in use) is a
        // visibility problem, not a safety one -- log it and keep running
        // the actual control loop rather than aborting startup entirely.
        this.logger.error('API server failed to start (control loop continues without it)', { err });
      });
    }

    this._started = true;
    this.logger.info('GridSync-OS orchestrator started', {
      adapters: [...this.adapters.keys()],
      recoveredCommands: recoveredCount,
    });
  }

  async stop() {
    if (!this._started) return;
    this.commandDispatcher.stop();
    if (this.apiServer) {
      await this.apiServer.stop().catch((err) => {
        this.logger.error('API server failed to stop cleanly', { err });
      });
    }
    for (const [adapterId, adapter] of this.adapters) {
      // eslint-disable-next-line no-await-in-loop
      await adapter.disconnect().catch((err) => {
        this.logger.error('adapter failed to disconnect cleanly', { adapterId, err });
      });
    }
    await this.storage.close();
    this._started = false;
    this.logger.info('GridSync-OS orchestrator stopped');
  }

  getSnapshot() {
    return {
      ingestion: this.ingestionManager.getMetrics(),
      circuitBreaker: this.circuitBreaker.getStatus(),
      pendingCommands: this.commandQueue.size(),
      devices: [...this._deviceStates.values()].map((s) => ({
        deviceId: s.deviceId,
        mode: s.mode,
        consecutiveViolations: s.consecutiveViolations,
      })),
    };
  }
}

module.exports = { GridSyncOrchestrator };
