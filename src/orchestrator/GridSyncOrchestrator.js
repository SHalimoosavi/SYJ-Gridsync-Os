'use strict';

const { IngestionManager } = require('../ingestion/IngestionManager');
const { StateMachine } = require('../engine/StateMachine');
const { ConstraintValidator } = require('../engine/ConstraintValidator');
const { CircuitBreaker } = require('../engine/CircuitBreaker');
const { AlarmEngine } = require('../engine/AlarmEngine');
const { CommandQueue } = require('../commands/CommandQueue');
const { CommandDispatcher } = require('../commands/CommandDispatcher');
const { FileWalStorage } = require('../storage/FileWalStorage');
const { SqliteStorage } = require('../storage/SqliteStorage');
const { ApiServer } = require('../api/ApiServer');
const { UserStore } = require('../auth/UserStore');
const { DeviceRegistry } = require('../devices/DeviceRegistry');
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
    this.userStore = new UserStore({ dataDir: config.storage.dataDir, logger: this.logger });
    this.deviceRegistry = new DeviceRegistry({ dataDir: config.storage.dataDir, logger: this.logger });
    this._blockedTelemetryCount = 0;
    this.circuitBreaker = new CircuitBreaker({ ...config.circuitBreaker, logger: this.logger });
    this.alarmEngine = new AlarmEngine({
      gridConstraints: config.gridConstraints,
      commTimeoutMs: config.alarms.commTimeoutMs,
      staleTelemetryMs: config.circuitBreaker.staleTelemetryMs,
      logger: this.logger,
    });
    this._alarmStalenessTimer = null;
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
      deviceRegistry: this.deviceRegistry,
      resolveAdapter: (deviceId) => this._resolveAdapterForDevice(deviceId),
      getLatestState: (deviceId) => this._latestPoints.get(deviceId) || null,
      config,
      logger: this.logger,
    });

    this.apiServer = config.api.enabled
      ? new ApiServer({ orchestrator: this, userStore: this.userStore, config, logger: this.logger })
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
    if (this.deviceRegistry.isBlocked(point.deviceId)) {
      this._blockedTelemetryCount += 1;
      this.logger.debug('dropping telemetry from a removed device', { deviceId: point.deviceId });
      return;
    }
    // Fire-and-forget: registration is metadata bookkeeping, not a
    // prerequisite for processing this point. Awaiting it here would add a
    // one-time delay on a device's first-ever point (its registry write)
    // relative to that same device's subsequent points -- since
    // _handlePoint calls for a batch run concurrently (fire-and-forget from
    // IngestionManager), that delay could let later points "overtake" the
    // first one, corrupting the temporal order the state machine and
    // telemetry history both depend on. Found via a real test failure, not
    // by inspection -- see queryTelemetry's explicit timestamp sort below
    // for the second, independent layer of defense against this class of bug.
    this.deviceRegistry.ensureRegistered(point.deviceId, { name: point.deviceId }).catch((err) => {
      this.logger.error('failed to auto-register device (continuing)', { deviceId: point.deviceId, err });
    });

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

    const alarmEvents = this.alarmEngine.evaluateTelemetry(point);
    await this._persistAlarmEvents(alarmEvents);

    for (const effect of effects) {
      // eslint-disable-next-line no-await-in-loop
      await this.commandQueue.enqueue(effect);
    }
  }

  async _persistAlarmEvents(events) {
    for (const alarm of events) {
      try {
        // eslint-disable-next-line no-await-in-loop
        await this.storage.appendAlarmEvent(alarm);
      } catch (err) {
        this.logger.error('failed to persist alarm event (continuing)', { alarmId: alarm.alarmId, err });
      }
    }
  }

  /** Manually issue a command (e.g. operator-initiated curtailment/discharge), same durability guarantees as auto-generated ones. */
  async issueManualCommand({ type, deviceId, value, reason, issuedBy }) {
    assertNonEmptyString(deviceId, 'deviceId');
    const record = await this.commandQueue.enqueue({ type, deviceId, value, reason: reason || 'MANUAL', issuedBy });
    if (type === 'RESET') {
      const acknowledged = this.alarmEngine.acknowledgeAllForDevice(deviceId, issuedBy || 'SYSTEM');
      await this._persistAlarmEvents(acknowledged);
    }
    return record;
  }

  /** Currently active (unresolved) alarms across the whole fleet. */
  listActiveAlarms() {
    return this.alarmEngine.listActive();
  }

  /** Acknowledge one specific active alarm. Returns null if it's not currently active (already cleared, or never existed). */
  async acknowledgeAlarm(alarmId, byUsername) {
    const event = this.alarmEngine.acknowledge(alarmId, byUsername);
    if (event) await this.storage.appendAlarmEvent(event);
    return event;
  }

  async _sweepStaleness() {
    const now = Date.now();
    for (const { deviceId, lastSeen } of this.circuitBreaker.listTrackedDevices()) {
      const events = this.alarmEngine.evaluateStaleness(deviceId, lastSeen, now);
      // eslint-disable-next-line no-await-in-loop
      await this._persistAlarmEvents(events);
    }
  }

  /** All known devices with their current FSM mode, latest telemetry, and registry metadata -- used by the API/dashboard. */
  listDevices() {
    const devices = [];
    for (const [deviceId, state] of this._deviceStates) {
      const registryRecord = this.deviceRegistry.get(deviceId);
      devices.push({
        deviceId,
        mode: state.mode,
        consecutiveViolations: state.consecutiveViolations,
        lastPoint: this._latestPoints.get(deviceId) || null,
        name: registryRecord?.name || deviceId,
        location: registryRecord?.location || null,
        firmwareVersion: registryRecord?.firmwareVersion || null,
        status: registryRecord?.status || 'ENABLED',
      });
    }
    return devices;
  }

  /** Full detail for one device, or null if it's never reported telemetry. */
  getDeviceDetail(deviceId) {
    const state = this._deviceStates.get(deviceId);
    if (!state) return null;
    const registryRecord = this.deviceRegistry.get(deviceId);
    return {
      deviceId,
      mode: state.mode,
      consecutiveViolations: state.consecutiveViolations,
      violations: state.violations,
      lastPoint: this._latestPoints.get(deviceId) || null,
      adapterId: this._deviceAdapterRouting.get(deviceId) || null,
      name: registryRecord?.name || deviceId,
      location: registryRecord?.location || null,
      notes: registryRecord?.notes || null,
      firmwareVersion: registryRecord?.firmwareVersion || null,
      status: registryRecord?.status || 'ENABLED',
    };
  }

  /** Raw registry view (includes devices pre-provisioned but never yet connected) -- distinct from listDevices(), which is telemetry-derived. */
  listRegisteredDevices({ includeRemoved = false } = {}) {
    return this.deviceRegistry.list({ includeRemoved });
  }

  async registerDevice({ deviceId, name, location, notes, firmwareVersion }) {
    return this.deviceRegistry.register({ deviceId, name, location, notes, firmwareVersion });
  }

  async updateDeviceMetadata(deviceId, patch) {
    return this.deviceRegistry.update(deviceId, patch);
  }

  async setDeviceStatus(deviceId, status) {
    return this.deviceRegistry.setStatus(deviceId, status);
  }

  /** Removes a device from active tracking (soft-delete in the registry; future telemetry from it is dropped). */
  async removeDevice(deviceId) {
    const record = await this.deviceRegistry.setStatus(deviceId, 'REMOVED');
    this._deviceStates.delete(deviceId);
    this._latestPoints.delete(deviceId);
    this._deviceAdapterRouting.delete(deviceId);
    return record;
  }

  async _bootstrapAdminIfConfigured() {
    const { bootstrapAdminUsername, bootstrapAdminPassword } = this.config.auth;
    if (!bootstrapAdminUsername || !bootstrapAdminPassword) return;
    const existingCount = await this.userStore.count();
    if (existingCount > 0) return; // never auto-create once any account exists
    try {
      await this.userStore.createUser({ username: bootstrapAdminUsername, password: bootstrapAdminPassword, role: 'ADMIN' });
      this.logger.info('bootstrap admin account created', { username: bootstrapAdminUsername.toLowerCase() });
    } catch (err) {
      this.logger.error('failed to create bootstrap admin account', { err });
    }
  }

  async start() {
    if (this._started) return;
    await this.storage.init();
    await this.userStore.init();
    await this.deviceRegistry.init();
    await this._bootstrapAdminIfConfigured();
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

    this._alarmStalenessTimer = setInterval(() => {
      this._sweepStaleness().catch((err) => {
        this.logger.error('staleness sweep failed', { err });
      });
    }, this.config.alarms.stalenessCheckIntervalMs);
    this._alarmStalenessTimer.unref?.();

    this._started = true;
    this.logger.info('GridSync-OS orchestrator started', {
      adapters: [...this.adapters.keys()],
      recoveredCommands: recoveredCount,
    });
  }

  async stop() {
    if (!this._started) return;
    if (this._alarmStalenessTimer) {
      clearInterval(this._alarmStalenessTimer);
      this._alarmStalenessTimer = null;
    }
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
    await this.deviceRegistry.close();
    await this.userStore.close();
    this._started = false;
    this.logger.info('GridSync-OS orchestrator stopped');
  }

  getSnapshot() {
    return {
      ingestion: this.ingestionManager.getMetrics(),
      circuitBreaker: this.circuitBreaker.getStatus(),
      pendingCommands: this.commandQueue.size(),
      activeAlarms: this.alarmEngine.listActive().length,
      registeredDevices: this.deviceRegistry.list().length,
      blockedTelemetry: this._blockedTelemetryCount,
      devices: [...this._deviceStates.values()].map((s) => ({
        deviceId: s.deviceId,
        mode: s.mode,
        consecutiveViolations: s.consecutiveViolations,
      })),
    };
  }
}

module.exports = { GridSyncOrchestrator };
