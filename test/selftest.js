'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const os = require('node:os');
const path = require('node:path');
const fsp = require('node:fs/promises');
const EventEmitter = require('node:events');

const { Logger } = require('../src/utils/logger');
const baseConfig = require('../src/config');
const { RingBuffer } = require('../src/utils/RingBuffer');
const { Normalizer } = require('../src/ingestion/Normalizer');
const { IngestionManager } = require('../src/ingestion/IngestionManager');
const { ConstraintValidator } = require('../src/engine/ConstraintValidator');
const { CircuitBreaker } = require('../src/engine/CircuitBreaker');
const { StateMachine, MODES } = require('../src/engine/StateMachine');
const { AlarmEngine } = require('../src/engine/AlarmEngine');
const { DeviceRegistry } = require('../src/devices/DeviceRegistry');
const { CommandQueue } = require('../src/commands/CommandQueue');
const { CommandDispatcher } = require('../src/commands/CommandDispatcher');
const { FileWalStorage } = require('../src/storage/FileWalStorage');
const { SqliteStorage } = require('../src/storage/SqliteStorage');
const { AdapterBase } = require('../src/adapters/AdapterBase');
const { MqttAdapter } = require('../src/adapters/MqttAdapter');
const { GridSyncOrchestrator } = require('../src/orchestrator/GridSyncOrchestrator');
const { ValidationError } = require('../src/utils/errors');
const { Router } = require('../src/api/Router');
const { hashPassword, verifyPassword } = require('../src/auth/PasswordHasher');
const Jwt = require('../src/auth/Jwt');
const { UserStore } = require('../src/auth/UserStore');

const quietLogger = new Logger('selftest', 'error'); // suppress info/debug noise in test output

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async function mkTempDir() {
  return fsp.mkdtemp(path.join(os.tmpdir(), 'gridsync-selftest-'));
}

async function rmTempDir(dir, attemptsLeft = 5) {
  try {
    await fsp.rm(dir, { recursive: true, force: true });
  } catch (err) {
    if (err.code === 'ENOTEMPTY' && attemptsLeft > 0) {
      // A background write (e.g. a fire-and-forget persistence call) can
      // occasionally still be landing on disk a few ms after stop()
      // resolves. Retrying briefly is standard practice for this class of
      // teardown race and does not mask a production issue -- production
      // code never calls this function.
      await new Promise((resolve) => setTimeout(resolve, 20));
      return rmTempDir(dir, attemptsLeft - 1);
    }
    throw err;
  }
}

async function waitUntil(predicate, timeoutMs = 2000, intervalMs = 10) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    // eslint-disable-next-line no-await-in-loop
    if (await predicate()) return true;
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`waitUntil: condition not met within ${timeoutMs}ms`);
}

function makeTestConfig(dataDir, overrides = {}) {
  const cfg = structuredClone(baseConfig);
  cfg.storage.dataDir = dataDir;
  cfg.storage.driver = 'file-wal';
  cfg.storage.compactionIntervalMs = 999999999; // don't let compaction interfere mid-test
  cfg.commandQueue.baseRetryDelayMs = overrides.baseRetryDelayMs ?? 20;
  cfg.commandQueue.maxRetryDelayMs = overrides.maxRetryDelayMs ?? 100;
  cfg.commandQueue.maxAttempts = overrides.maxAttempts ?? cfg.commandQueue.maxAttempts;
  cfg.circuitBreaker.staleTelemetryMs = overrides.staleTelemetryMs ?? cfg.circuitBreaker.staleTelemetryMs;
  // Orchestrator tests don't need a real network listener, and running one
  // by default risks port collisions with an actual GridSync-OS instance on
  // the same machine. Dedicated API tests opt back in with port 0 (OS-assigned).
  cfg.api.enabled = overrides.apiEnabled ?? false;
  if (overrides.apiPort !== undefined) cfg.api.port = overrides.apiPort;
  if (overrides.apiToken !== undefined) cfg.api.token = overrides.apiToken;
  cfg.auth.jwtSecret = overrides.jwtSecret ?? 'test-only-jwt-secret-not-for-production-use';
  cfg.auth.jwtExpiresInSeconds = overrides.jwtExpiresInSeconds ?? cfg.auth.jwtExpiresInSeconds;
  if (overrides.bootstrapAdminUsername !== undefined) cfg.auth.bootstrapAdminUsername = overrides.bootstrapAdminUsername;
  if (overrides.bootstrapAdminPassword !== undefined) cfg.auth.bootstrapAdminPassword = overrides.bootstrapAdminPassword;
  return cfg;
}

/** Minimal controllable adapter used across ingestion/dispatch/orchestrator tests. */
class FakeAdapter extends AdapterBase {
  constructor(logger) {
    super('FAKE', logger);
    this.sentCommands = [];
    this.sendBehavior = null; // (command) => throws | returns value
  }

  async connect() {
    this.connected = true;
    this.emit('connected');
  }

  async disconnect() {
    this.connected = false;
    this.emit('disconnected', 'manual');
  }

  async sendCommand(command) {
    this.sentCommands.push(command);
    if (this.sendBehavior) return this.sendBehavior(command);
    return { acked: true };
  }

  emitTelemetry(raw, meta) {
    this.emit('data', raw, meta);
  }
}

/** Minimal stand-in for the object mqtt.js's `connect()` returns -- just enough surface for MqttAdapter. */
class FakeMqttClient extends EventEmitter {
  constructor() {
    super();
    this.endCallCount = 0;
    this.subscribeCalls = [];
    this.publishCalls = [];
  }

  subscribe(topic, cb) {
    this.subscribeCalls.push(topic);
    cb(null);
  }

  publish(topic, payload, opts, cb) {
    this.publishCalls.push({ topic, payload });
    cb(null);
  }

  end(force, opts, cb) {
    this.endCallCount += 1;
    const callback = typeof opts === 'function' ? opts : cb;
    if (callback) process.nextTick(() => callback());
  }
}

/** Records log calls instead of writing them, so tests can assert on log volume/level without parsing stdout. */
class RecordingLogger {
  constructor() {
    this.records = [];
  }

  child() {
    return this; // all "child" scopes share the same record array for simplicity
  }

  debug(msg, meta) { this.records.push({ level: 'debug', msg, meta }); }
  info(msg, meta) { this.records.push({ level: 'info', msg, meta }); }
  warn(msg, meta) { this.records.push({ level: 'warn', msg, meta }); }
  error(msg, meta) { this.records.push({ level: 'error', msg, meta }); }

  countByLevel(level) {
    return this.records.filter((r) => r.level === level).length;
  }
}

// ---------------------------------------------------------------------------
// RingBuffer
// ---------------------------------------------------------------------------

test('RingBuffer: FIFO order preserved and capacity enforced', () => {
  const rb = new RingBuffer(3);
  assert.equal(rb.push('a'), false);
  assert.equal(rb.push('b'), false);
  assert.equal(rb.push('c'), false);
  assert.equal(rb.isFull, true);
  // Buffer full -- pushing evicts oldest ('a') and reports wasFull=true.
  assert.equal(rb.push('d'), true);
  assert.deepEqual(rb.drain(10), ['b', 'c', 'd']);
  assert.equal(rb.isEmpty, true);
});

test('RingBuffer: rejects invalid capacity', () => {
  assert.throws(() => new RingBuffer(0), RangeError);
  assert.throws(() => new RingBuffer(-5), RangeError);
});

// ---------------------------------------------------------------------------
// Normalizer -- protocol fragmentation handling
// ---------------------------------------------------------------------------

test('Normalizer: converts a valid MQTT-sourced payload to canonical schema', () => {
  const point = Normalizer.normalize(
    { deviceId: 'inv-01', deviceType: 'INVERTER', voltage: 231.5, frequency: 50.01, soc: 0.62, timestamp: 1000 },
    { protocol: 'MQTT' },
  );
  assert.equal(point.deviceId, 'inv-01');
  assert.equal(point.protocol, 'MQTT');
  assert.equal(point.metrics.voltage, 231.5);
});

test('Normalizer: rejects payload with missing deviceId', () => {
  assert.throws(
    () => Normalizer.normalize({ deviceType: 'INVERTER', voltage: 230 }, { protocol: 'MODBUS' }),
    ValidationError,
  );
});

test('Normalizer: rejects unknown deviceType', () => {
  assert.throws(
    () => Normalizer.normalize({ deviceId: 'x', deviceType: 'TOASTER', voltage: 230 }, { protocol: 'MODBUS' }),
    ValidationError,
  );
});

test('Normalizer: rejects non-numeric metric values (NaN/strings) instead of silently coercing', () => {
  assert.throws(
    () => Normalizer.normalize({ deviceId: 'x', deviceType: 'METER', voltage: 'high' }, { protocol: 'DNP3' }),
    ValidationError,
  );
});

test('Normalizer: rejects payload with no recognized metrics at all', () => {
  assert.throws(
    () => Normalizer.normalize({ deviceId: 'x', deviceType: 'METER' }, { protocol: 'DNP3' }),
    ValidationError,
  );
});

// ---------------------------------------------------------------------------
// ConstraintValidator
// ---------------------------------------------------------------------------

test('ConstraintValidator: flags out-of-range voltage/frequency/soc on telemetry', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const violations = cv.checkTelemetry({
    deviceId: 'inv-01',
    metrics: { voltage: 300, frequency: 48, soc: 0.99 },
  });
  const types = violations.map((v) => v.type);
  assert.ok(types.includes('VOLTAGE_OUT_OF_RANGE'));
  assert.ok(types.includes('FREQUENCY_OUT_OF_RANGE'));
  assert.ok(types.includes('SOC_OUT_OF_RANGE'));
});

test('ConstraintValidator: clamps an over-limit CURTAIL command instead of blindly forwarding it', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const shaped = cv.validateCommand({ type: 'CURTAIL', deviceId: 'inv-01', value: 999999 }, null);
  assert.equal(shaped.value, baseConfig.gridConstraints.maxCurtailKw);
});

test('ConstraintValidator: rejects DISCHARGE when SoC is at/below the safety reserve', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  assert.throws(() => {
    cv.validateCommand({ type: 'DISCHARGE', deviceId: 'batt-01', value: 10 }, { metrics: { soc: 0.02 } });
  });
});

test('ConstraintValidator: rejects DISCHARGE with unknown SoC (fail closed, not open)', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  assert.throws(() => {
    cv.validateCommand({ type: 'DISCHARGE', deviceId: 'batt-01', value: 10 }, null);
  });
});

test('ConstraintValidator: allows and clamps a valid DISCHARGE within bounds', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const shaped = cv.validateCommand({ type: 'DISCHARGE', deviceId: 'batt-01', value: 999 }, { metrics: { soc: 0.5 } });
  assert.equal(shaped.value, baseConfig.gridConstraints.maxDischargeKw);
});

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

test('CircuitBreaker: opens for a device that has never reported telemetry', () => {
  const cb = new CircuitBreaker({ staleTelemetryMs: 1000, maxErrorsPerWindow: 50, errorWindowMs: 10000 });
  const status = cb.isOpen('unknown-device');
  assert.equal(status.open, true);
  assert.equal(status.reason, 'NO_TELEMETRY_EVER_RECEIVED');
});

test('CircuitBreaker: opens when telemetry has gone stale beyond threshold', () => {
  const cb = new CircuitBreaker({ staleTelemetryMs: 100, maxErrorsPerWindow: 50, errorWindowMs: 10000 });
  cb.recordTelemetry('inv-01', Date.now() - 500);
  const status = cb.isOpen('inv-01');
  assert.equal(status.open, true);
  assert.equal(status.reason, 'STALE_TELEMETRY');
});

test('CircuitBreaker: stays closed for fresh telemetry', () => {
  const cb = new CircuitBreaker({ staleTelemetryMs: 5000, maxErrorsPerWindow: 50, errorWindowMs: 10000 });
  cb.recordTelemetry('inv-01', Date.now());
  assert.equal(cb.isOpen('inv-01').open, false);
});

test('CircuitBreaker: global breaker trips after error-rate threshold exceeded', () => {
  const cb = new CircuitBreaker({ staleTelemetryMs: 5000, maxErrorsPerWindow: 3, errorWindowMs: 10000 });
  cb.recordTelemetry('inv-01', Date.now());
  for (let i = 0; i < 5; i += 1) cb.recordError();
  assert.equal(cb.isOpen('inv-01').open, true);
  assert.equal(cb.isOpen('inv-01').reason, 'GLOBAL_ERROR_RATE_TRIPPED');
});

// ---------------------------------------------------------------------------
// StateMachine -- pure transition function
// ---------------------------------------------------------------------------

test('StateMachine: normal telemetry keeps device in NORMAL mode with no effects', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const sm = new StateMachine(cv);
  const prev = StateMachine.initialDeviceState('inv-01');
  const point = { deviceId: 'inv-01', metrics: { voltage: 230, frequency: 50, soc: 0.5 } };
  const { newState, effects } = sm.transition(prev, point);
  assert.equal(newState.mode, MODES.NORMAL);
  assert.equal(effects.length, 0);
});

test('StateMachine: overvoltage triggers an auto-curtailment effect within configured limits', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const sm = new StateMachine(cv);
  const prev = StateMachine.initialDeviceState('inv-01');
  const point = { deviceId: 'inv-01', metrics: { voltage: 270, frequency: 50, soc: 0.5 } };
  const { newState, effects } = sm.transition(prev, point);
  assert.equal(newState.mode, MODES.CURTAILED);
  assert.equal(effects.length, 1);
  assert.equal(effects[0].type, 'CURTAIL');
  assert.ok(effects[0].value <= baseConfig.gridConstraints.maxCurtailKw);
});

test('StateMachine: recovery from CURTAILED emits a STANDBY release effect', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const sm = new StateMachine(cv);
  let state = StateMachine.initialDeviceState('inv-01');
  const overvoltage = { deviceId: 'inv-01', metrics: { voltage: 270, frequency: 50, soc: 0.5 } };
  ({ newState: state } = sm.transition(state, overvoltage));
  assert.equal(state.mode, MODES.CURTAILED);

  const recovered = { deviceId: 'inv-01', metrics: { voltage: 230, frequency: 50, soc: 0.5 } };
  const { newState, effects } = sm.transition(state, recovered);
  assert.equal(newState.mode, MODES.NORMAL);
  assert.equal(effects.length, 1);
  assert.equal(effects[0].type, 'STANDBY');
});

test('StateMachine: transition is deterministic (same inputs -> same outputs)', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const sm = new StateMachine(cv);
  const prev = StateMachine.initialDeviceState('inv-01');
  const point = { deviceId: 'inv-01', metrics: { voltage: 260, frequency: 50, soc: 0.5 } };
  const run1 = sm.transition(prev, point);
  const run2 = sm.transition(prev, point);
  assert.deepEqual(run1.newState, run2.newState);
  assert.deepEqual(run1.effects, run2.effects);
});

// ---------------------------------------------------------------------------
// ConstraintValidator: RESET command
// ---------------------------------------------------------------------------

test('ConstraintValidator: RESET is always allowed, even with unknown device state', () => {
  const cv = new ConstraintValidator(baseConfig.gridConstraints);
  const shaped = cv.validateCommand({ type: 'RESET', deviceId: 'inv-01', value: 999 }, null);
  assert.equal(shaped.value, 0);
});

// ---------------------------------------------------------------------------
// AlarmEngine: pure trigger/clear decision logic
// ---------------------------------------------------------------------------

function makeAlarmEngine(overrides = {}) {
  return new AlarmEngine({
    gridConstraints: baseConfig.gridConstraints,
    commTimeoutMs: overrides.commTimeoutMs ?? 5000,
    staleTelemetryMs: overrides.staleTelemetryMs ?? 10000,
    logger: quietLogger,
  });
}

test('AlarmEngine: triggers OVER_VOLTAGE and clears it when telemetry returns to range', () => {
  const engine = makeAlarmEngine();
  const over = engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 300 } });
  assert.equal(over.length, 1);
  assert.equal(over[0].type, 'OVER_VOLTAGE');
  assert.equal(over[0].event, 'TRIGGERED');
  assert.equal(over[0].severity, 'CRITICAL');
  assert.equal(engine.listActive().length, 1);

  const normal = engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 230 } });
  assert.equal(normal.length, 1);
  assert.equal(normal[0].event, 'CLEARED');
  assert.equal(engine.listActive().length, 0);
});

test('AlarmEngine: triggers UNDER_VOLTAGE, HIGH_FREQUENCY, and LOW_SOC independently', () => {
  const engine = makeAlarmEngine();
  const events = engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 100, frequency: 52, soc: 0.01 } });
  const types = events.filter((e) => e.event === 'TRIGGERED').map((e) => e.type);
  assert.ok(types.includes('UNDER_VOLTAGE'));
  assert.ok(types.includes('HIGH_FREQUENCY'));
  assert.ok(types.includes('LOW_SOC'));
});

test('AlarmEngine: does not trigger a duplicate alarm while a condition persists', () => {
  const engine = makeAlarmEngine();
  const first = engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 300 } });
  const second = engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 305 } });
  assert.equal(first.filter((e) => e.type === 'OVER_VOLTAGE').length, 1);
  assert.equal(second.filter((e) => e.type === 'OVER_VOLTAGE').length, 0, 'no duplicate TRIGGERED event while still active');
  assert.equal(engine.listActive().length, 1);
});

test('AlarmEngine: staleness sweep triggers COMM_TIMEOUT before DEVICE_OFFLINE (softer threshold first)', () => {
  const engine = makeAlarmEngine({ commTimeoutMs: 5000, staleTelemetryMs: 10000 });
  const now = 1_000_000;
  const lastSeen = now - 7000; // past commTimeoutMs, not yet past staleTelemetryMs

  const events = engine.evaluateStaleness('inv-01', lastSeen, now);
  const types = events.filter((e) => e.event === 'TRIGGERED').map((e) => e.type);
  assert.ok(types.includes('COMM_TIMEOUT'));
  assert.ok(!types.includes('DEVICE_OFFLINE'));

  const laterEvents = engine.evaluateStaleness('inv-01', lastSeen, now + 5000); // now past both thresholds
  const laterTypes = laterEvents.filter((e) => e.event === 'TRIGGERED').map((e) => e.type);
  assert.ok(laterTypes.includes('DEVICE_OFFLINE'));
});

test('AlarmEngine: acknowledge() marks an active alarm acknowledged; returns null for a non-active id', () => {
  const engine = makeAlarmEngine();
  const [triggered] = engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 300 } });
  const ack = engine.acknowledge(triggered.alarmId, 'operator1');
  assert.equal(ack.acknowledged, true);
  assert.equal(ack.acknowledgedBy, 'operator1');
  assert.equal(ack.event, 'ACKNOWLEDGED');

  assert.equal(engine.acknowledge('nonexistent-id', 'operator1'), null);
});

test('AlarmEngine: acknowledgeAllForDevice() only affects that device\'s alarms', () => {
  const engine = makeAlarmEngine();
  engine.evaluateTelemetry({ deviceId: 'inv-01', metrics: { voltage: 300 } });
  engine.evaluateTelemetry({ deviceId: 'inv-02', metrics: { voltage: 300 } });

  const acked = engine.acknowledgeAllForDevice('inv-01', 'operator1');
  assert.equal(acked.length, 1);
  assert.equal(acked[0].deviceId, 'inv-01');

  const active = engine.listActive();
  const inv02Alarm = active.find((a) => a.deviceId === 'inv-02');
  assert.equal(inv02Alarm.acknowledged, false, 'acknowledging one device must not affect another');
});

test('CommandQueue: a command that was mid-dispatch when the process "crashed" is recovered on restart', async () => {
  const dataDir = await mkTempDir();
  try {
    const storage1 = new FileWalStorage({
      dataDir,
      compactionIntervalMs: 999999999,
      maxWalLinesBeforeCompaction: 999999999,
      logger: quietLogger,
    });
    await storage1.init();
    const queue1 = new CommandQueue({ storage: storage1, logger: quietLogger });

    const record = await queue1.enqueue({ type: 'CURTAIL', deviceId: 'inv-crash', value: 40 });
    await queue1.markDispatching(record.commandId);
    // Simulate a hard crash right here: never call markAcked/markFailedAttempt,
    // just tear down storage as if the process died mid-flight.
    await storage1.close();

    // "Restart": brand-new storage + queue instances pointed at the same data dir.
    const storage2 = new FileWalStorage({
      dataDir,
      compactionIntervalMs: 999999999,
      maxWalLinesBeforeCompaction: 999999999,
      logger: quietLogger,
    });
    await storage2.init();
    const queue2 = new CommandQueue({ storage: storage2, logger: quietLogger });
    const recoveredCount = await queue2.recover();

    assert.equal(recoveredCount, 1, 'exactly one in-flight command should be recovered');
    const next = queue2.takeNextPending();
    assert.ok(next, 'recovered command must be available for redispatch');
    assert.equal(next.commandId, record.commandId);
    assert.equal(next.status, 'PENDING', 'recovered command must re-enter as PENDING regardless of prior DISPATCHING status');

    await storage2.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('CommandQueue: ACKED commands are NOT recovered after restart (already delivered)', async () => {
  const dataDir = await mkTempDir();
  try {
    const storage1 = new FileWalStorage({
      dataDir,
      compactionIntervalMs: 999999999,
      maxWalLinesBeforeCompaction: 999999999,
      logger: quietLogger,
    });
    await storage1.init();
    const queue1 = new CommandQueue({ storage: storage1, logger: quietLogger });
    const record = await queue1.enqueue({ type: 'STANDBY', deviceId: 'inv-ok', value: 0 });
    await queue1.markDispatching(record.commandId);
    await queue1.markAcked(record.commandId);
    await storage1.close();

    const storage2 = new FileWalStorage({
      dataDir,
      compactionIntervalMs: 999999999,
      maxWalLinesBeforeCompaction: 999999999,
      logger: quietLogger,
    });
    await storage2.init();
    const queue2 = new CommandQueue({ storage: storage2, logger: quietLogger });
    const recoveredCount = await queue2.recover();
    assert.equal(recoveredCount, 0);
    await storage2.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('FileWalStorage: alarm events persist and queryAlarmHistory returns latest state per alarm, newest first', async () => {
  const dataDir = await mkTempDir();
  try {
    const storage = new FileWalStorage({
      dataDir,
      compactionIntervalMs: 999999999,
      maxWalLinesBeforeCompaction: 999999999,
      logger: quietLogger,
    });
    await storage.init();

    await storage.appendAlarmEvent({ alarmId: 'a1', deviceId: 'inv-01', event: 'TRIGGERED', status: 'ACTIVE', type: 'OVER_VOLTAGE', ts: 1000 });
    await storage.appendAlarmEvent({ alarmId: 'a2', deviceId: 'inv-02', event: 'TRIGGERED', status: 'ACTIVE', type: 'LOW_SOC', ts: 2000 });
    await storage.appendAlarmEvent({ alarmId: 'a1', deviceId: 'inv-01', event: 'CLEARED', status: 'CLEARED', type: 'OVER_VOLTAGE', ts: 3000 });

    const history = await storage.queryAlarmHistory(50);
    assert.equal(history.length, 2, 'one record per alarmId, latest state wins');
    assert.equal(history[0].alarmId, 'a1', 'newest-first ordering (a1 was updated last, at ts 3000)');
    assert.equal(history[0].status, 'CLEARED');
    assert.equal(history[1].status, 'ACTIVE');

    const filtered = await storage.queryAlarmHistory(50, { deviceId: 'inv-02' });
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].alarmId, 'a2');

    await storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('FileWalStorage: queryCommandHistory respects deviceId and status filters', async () => {
  const dataDir = await mkTempDir();
  try {
    const storage = new FileWalStorage({
      dataDir,
      compactionIntervalMs: 999999999,
      maxWalLinesBeforeCompaction: 999999999,
      logger: quietLogger,
    });
    await storage.init();

    await storage.appendCommandEvent({ commandId: 'c1', deviceId: 'inv-01', event: 'CREATED', status: 'ACKED', ts: 1000 });
    await storage.appendCommandEvent({ commandId: 'c2', deviceId: 'inv-02', event: 'CREATED', status: 'FAILED', ts: 2000 });

    const byDevice = await storage.queryCommandHistory(50, { deviceId: 'inv-01' });
    assert.equal(byDevice.length, 1);
    assert.equal(byDevice[0].commandId, 'c1');

    const byStatus = await storage.queryCommandHistory(50, { status: 'FAILED' });
    assert.equal(byStatus.length, 1);
    assert.equal(byStatus[0].commandId, 'c2');

    await storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('SqliteStorage: full interface (telemetry, commands, alarms, filters) -- previously had zero coverage', { skip: !SqliteStorage.isSupported() }, async () => {
  const dataDir = await mkTempDir();
  try {
    const storage = new SqliteStorage({ dataDir, logger: quietLogger });
    await storage.init();

    await storage.appendTelemetry({ deviceId: 'inv-01', protocol: 'MQTT', deviceType: 'INVERTER', timestamp: 1000, metrics: { voltage: 230 } });
    await storage.appendTelemetry({ deviceId: 'inv-01', protocol: 'MQTT', deviceType: 'INVERTER', timestamp: 2000, metrics: { voltage: 231 } });
    const telemetry = await storage.queryTelemetry('inv-01', 10);
    assert.equal(telemetry.length, 2);
    assert.equal(telemetry[0].timestamp, 2000, 'newest first');

    await storage.appendCommandEvent({ commandId: 'c1', deviceId: 'inv-01', event: 'CREATED', status: 'ACKED', attempts: 0, ts: 1000 });
    await storage.appendCommandEvent({ commandId: 'c2', deviceId: 'inv-02', event: 'CREATED', status: 'FAILED', attempts: 1, ts: 2000 });
    assert.equal((await storage.queryCommandHistory(10)).length, 2);
    assert.equal((await storage.queryCommandHistory(10, { deviceId: 'inv-01' })).length, 1);
    assert.equal((await storage.queryCommandHistory(10, { status: 'FAILED' })).length, 1);
    assert.equal((await storage.loadPendingCommands()).length, 0, 'both commands are terminal (ACKED/FAILED)');

    await storage.appendAlarmEvent({ alarmId: 'a1', deviceId: 'inv-01', event: 'TRIGGERED', status: 'ACTIVE', type: 'OVER_VOLTAGE', ts: 1000 });
    await storage.appendAlarmEvent({ alarmId: 'a2', deviceId: 'inv-02', event: 'TRIGGERED', status: 'ACTIVE', type: 'LOW_SOC', ts: 2000 });
    await storage.appendAlarmEvent({ alarmId: 'a1', deviceId: 'inv-01', event: 'CLEARED', status: 'CLEARED', type: 'OVER_VOLTAGE', ts: 3000 });
    const alarmHistory = await storage.queryAlarmHistory(10);
    assert.equal(alarmHistory.length, 2, 'latest state per alarmId');
    assert.equal(alarmHistory.find((a) => a.alarmId === 'a1').status, 'CLEARED');
    assert.equal((await storage.queryAlarmHistory(10, { deviceId: 'inv-02' })).length, 1);
    assert.equal((await storage.queryAlarmHistory(10, { status: 'CLEARED' })).length, 1);

    await storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

// ---------------------------------------------------------------------------
// CommandDispatcher: circuit breaker, constraint rejection, and network-drop retry
// ---------------------------------------------------------------------------

async function buildDispatcherHarness(dataDir, overrides = {}) {
  const cfg = makeTestConfig(dataDir, overrides);
  const storage = new FileWalStorage({ ...cfg.storage, logger: quietLogger });
  await storage.init();
  const queue = new CommandQueue({ storage, logger: quietLogger });
  const circuitBreaker = new CircuitBreaker({ ...cfg.circuitBreaker, logger: quietLogger });
  const constraintValidator = new ConstraintValidator(cfg.gridConstraints);
  const deviceRegistry = new DeviceRegistry({ dataDir, logger: quietLogger });
  await deviceRegistry.init();
  const adapter = new FakeAdapter(quietLogger);
  const latestStates = new Map();

  const dispatcher = new CommandDispatcher({
    queue,
    circuitBreaker,
    constraintValidator,
    deviceRegistry,
    resolveAdapter: () => adapter,
    getLatestState: (deviceId) => latestStates.get(deviceId) || null,
    config: cfg,
    logger: quietLogger,
  });
  dispatcher.start();

  return { cfg, storage, queue, circuitBreaker, constraintValidator, deviceRegistry, adapter, latestStates, dispatcher };
}

test('CommandDispatcher: blocks commands to a device with stale/no telemetry via the circuit breaker', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildDispatcherHarness(dataDir, { maxAttempts: 2, baseRetryDelayMs: 15, maxRetryDelayMs: 30 });
    // Deliberately never call recordTelemetry -- breaker should be open (NO_TELEMETRY_EVER_RECEIVED).
    await h.queue.enqueue({ type: 'CURTAIL', deviceId: 'inv-nodata', value: 20 });

    await waitUntil(() => h.queue.size() === 0, 1000);
    assert.equal(h.adapter.sentCommands.length, 0, 'a breaker-blocked command must never reach the adapter');

    h.dispatcher.stop();
    await h.storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('CommandDispatcher: DISABLED device blocks dispatch (retryable); re-enabling lets a pending retry through', async () => {
  const dataDir = await mkTempDir();
  try {
    // maxAttempts is deliberately generous: with a 15-30ms backoff, a small
    // maxAttempts would exhaust the retry budget (permanent FAILED) in well
    // under 100ms -- faster than this test could re-enable the device and
    // still observe a pending retry succeed. This isn't a hypothetical --
    // an earlier version of this test used maxAttempts:5 and failed for
    // exactly that reason (confirmed via a standalone repro).
    const h = await buildDispatcherHarness(dataDir, { maxAttempts: 200, baseRetryDelayMs: 15, maxRetryDelayMs: 30 });
    h.circuitBreaker.recordTelemetry('inv-disabled', Date.now());
    await h.deviceRegistry.register({ deviceId: 'inv-disabled' });
    await h.deviceRegistry.setStatus('inv-disabled', 'DISABLED');

    await h.queue.enqueue({ type: 'CURTAIL', deviceId: 'inv-disabled', value: 10 });
    await new Promise((resolve) => setTimeout(resolve, 60)); // let a couple of retry attempts happen
    assert.equal(h.adapter.sentCommands.length, 0, 'a disabled device must never actually receive the command');
    // Note: queue.size() reflects only commands immediately eligible for
    // pickup, not ones currently waiting out a backoff delay -- use
    // listPending() (all non-terminal records) to check it hasn't been
    // permanently FAILED yet.
    assert.ok(h.queue.listPending().some((c) => c.deviceId === 'inv-disabled'), 'command must still be pending, not exhausted, at this point');

    await h.deviceRegistry.setStatus('inv-disabled', 'ENABLED');
    await waitUntil(() => h.adapter.sentCommands.length === 1, 2000);

    h.dispatcher.stop();
    await h.storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('CommandDispatcher: REMOVED device also blocks dispatch', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildDispatcherHarness(dataDir, { maxAttempts: 2, baseRetryDelayMs: 15, maxRetryDelayMs: 30 });
    h.circuitBreaker.recordTelemetry('inv-removed', Date.now());
    await h.deviceRegistry.register({ deviceId: 'inv-removed' });
    await h.deviceRegistry.setStatus('inv-removed', 'REMOVED');

    await h.queue.enqueue({ type: 'STANDBY', deviceId: 'inv-removed', value: 0 });
    await waitUntil(() => h.queue.size() === 0, 1000); // exhausts retries and fails

    assert.equal(h.adapter.sentCommands.length, 0);

    h.dispatcher.stop();
    await h.storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('CommandDispatcher: constraint-violating command fails fast without ever hitting the adapter', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildDispatcherHarness(dataDir);
    h.circuitBreaker.recordTelemetry('batt-low', Date.now());
    h.latestStates.set('batt-low', { metrics: { soc: 0.01 } }); // below reserve
    await h.queue.enqueue({ type: 'DISCHARGE', deviceId: 'batt-low', value: 10 });

    await waitUntil(() => h.queue.size() === 0, 1000);
    assert.equal(h.adapter.sentCommands.length, 0, 'a constraint-rejected command must never be sent to the device');

    h.dispatcher.stop();
    await h.storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('CommandDispatcher: a command clamped to safe bounds still gets delivered', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildDispatcherHarness(dataDir);
    h.circuitBreaker.recordTelemetry('inv-clamp', Date.now());
    await h.queue.enqueue({ type: 'CURTAIL', deviceId: 'inv-clamp', value: 5000 });

    await waitUntil(() => h.adapter.sentCommands.length === 1, 1000);
    assert.equal(h.adapter.sentCommands[0].value, h.cfg.gridConstraints.maxCurtailKw);
    await waitUntil(() => h.queue.size() === 0, 1000);

    h.dispatcher.stop();
    await h.storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('CommandDispatcher: survives a simulated network drop -- retries and eventually delivers (no data loss)', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildDispatcherHarness(dataDir, { baseRetryDelayMs: 15, maxRetryDelayMs: 30, maxAttempts: 5 });
    h.circuitBreaker.recordTelemetry('inv-flaky', Date.now());

    let callCount = 0;
    h.adapter.sendBehavior = () => {
      callCount += 1;
      if (callCount === 1) {
        throw new Error('simulated network drop');
      }
      return { acked: true };
    };

    await h.queue.enqueue({ type: 'CURTAIL', deviceId: 'inv-flaky', value: 10 });

    await waitUntil(() => callCount >= 2, 2000);
    await waitUntil(() => h.queue.size() === 0, 2000);
    assert.equal(h.adapter.sentCommands.length, 2, 'first attempt drops, second attempt succeeds');

    h.dispatcher.stop();
    await h.storage.close();
  } finally {
    await rmTempDir(dataDir);
  }
});

// ---------------------------------------------------------------------------
// IngestionManager: backpressure, malformed-input resilience, high-velocity burst
// ---------------------------------------------------------------------------

test('IngestionManager: overflow drops oldest points and records the drop metric (bounded memory)', async () => {
  const cfg = structuredClone(baseConfig);
  cfg.ingestion.maxBufferSize = 5;
  cfg.ingestion.batchSize = 1000; // irrelevant here -- we push faster than any drain can run

  const adapter = new FakeAdapter(quietLogger);
  const received = [];
  const mgr = new IngestionManager({
    config: cfg,
    logger: quietLogger,
    onPoint: (p) => received.push(p),
    onInvalid: () => {},
  });
  mgr.registerAdapter('fake', adapter);

  // All of these fire synchronously in the same tick, before the
  // setImmediate-scheduled drain loop gets a chance to run -- so the
  // 5-slot buffer must overflow deterministically.
  for (let i = 0; i < 20; i += 1) {
    adapter.emitTelemetry(
      { deviceId: 'meter-x', deviceType: 'METER', voltage: 230, timestamp: Date.now() + i },
      { protocol: 'MQTT', deviceId: 'meter-x' },
    );
  }

  assert.equal(mgr.metrics.received, 20);
  assert.ok(mgr.metrics.dropped >= 15, `expected at least 15 drops with a 5-slot buffer, got ${mgr.metrics.dropped}`);

  await waitUntil(() => mgr.metrics.normalized + mgr.metrics.invalid >= 5, 1000);
});

test('IngestionManager: malformed input is discarded without crashing the pipeline, valid points still flow', async () => {
  const cfg = structuredClone(baseConfig);
  const adapter = new FakeAdapter(quietLogger);
  const received = [];
  let invalidCount = 0;
  const mgr = new IngestionManager({
    config: cfg,
    logger: quietLogger,
    onPoint: (p) => received.push(p),
    onInvalid: () => {
      invalidCount += 1;
    },
  });
  mgr.registerAdapter('fake', adapter);

  const badCases = [
    { raw: null, meta: { protocol: 'MQTT' } },
    { raw: 42, meta: { protocol: 'MQTT' } },
    { raw: { deviceType: 'INVERTER', voltage: 230 }, meta: { protocol: 'MQTT' } }, // no deviceId anywhere (payload or meta)
    { raw: { deviceId: 'x', deviceType: 'SPACESHIP', voltage: 230 }, meta: { protocol: 'MQTT', deviceId: 'x' } },
    { raw: { deviceId: 'x', deviceType: 'METER', voltage: 'not-a-number' }, meta: { protocol: 'MQTT', deviceId: 'x' } },
    { raw: { deviceId: 'x', deviceType: 'METER' }, meta: { protocol: 'MQTT', deviceId: 'x' } }, // no recognized metrics
  ];
  for (const { raw, meta } of badCases) {
    adapter.emitTelemetry(raw, meta);
  }
  // A valid point sent right after the bad ones must still be processed --
  // proves one bad point doesn't wedge the pipeline for subsequent ones.
  adapter.emitTelemetry(
    { deviceId: 'meter-good', deviceType: 'METER', voltage: 230, timestamp: Date.now() },
    { protocol: 'MQTT', deviceId: 'meter-good' },
  );

  await waitUntil(() => received.length === 1 && invalidCount === badCases.length, 1000);
  assert.equal(received[0].deviceId, 'meter-good');
});

test('IngestionManager: processes a 5,000-point high-velocity burst without throwing or hanging', async () => {
  const cfg = structuredClone(baseConfig);
  const adapter = new FakeAdapter(quietLogger);
  let normalizedCount = 0;
  const mgr = new IngestionManager({
    config: cfg,
    logger: quietLogger,
    onPoint: () => {
      normalizedCount += 1;
    },
    onInvalid: () => {},
  });
  mgr.registerAdapter('fake', adapter);

  const N = 5000;
  for (let i = 0; i < N; i += 1) {
    adapter.emitTelemetry(
      { deviceId: `meter-${i % 20}`, deviceType: 'METER', voltage: 225 + (i % 10), timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: `meter-${i % 20}` },
    );
  }

  await waitUntil(() => normalizedCount === N, 5000);
  assert.equal(mgr.metrics.received, N);
  assert.equal(mgr.metrics.normalized, N);
  assert.equal(mgr.metrics.invalid, 0);
});

// ---------------------------------------------------------------------------
// MqttAdapter: reconnect-failure log throttling + maxReconnectAttempts give-up
// ---------------------------------------------------------------------------

test('MqttAdapter: throttles reconnect-failure logs (1st at error, then every 10th at warn)', async () => {
  const fakeClient = new FakeMqttClient();
  const fakeMqttLib = { connect: () => fakeClient };
  const recLogger = new RecordingLogger();

  const adapter = new MqttAdapter({
    url: 'mqtt://fake-broker',
    topicPrefix: 'x/#',
    reconnectPeriodMs: 10,
    connectTimeoutMs: 50,
    logger: recLogger,
    mqttLib: fakeMqttLib,
  });

  const connectPromise = adapter.connect().catch(() => {}); // first failure rejects -- expected, swallow it here

  for (let i = 0; i < 25; i += 1) {
    fakeClient.emit('error', new Error('ECONNREFUSED'));
  }
  await connectPromise;

  assert.equal(adapter._consecutiveErrorCount, 25);
  // Only attempt #1 logs at error level for this failure sequence.
  const errorLogs = recLogger.records.filter((r) => r.level === 'error' && r.msg === 'MQTT connection failed');
  assert.equal(errorLogs.length, 1);
  // Attempts #10 and #20 log a concise warn; #25 does not (not a multiple of 10).
  const warnLogs = recLogger.records.filter((r) => r.level === 'warn' && r.msg.includes('still disconnected'));
  assert.equal(warnLogs.length, 2);
  assert.match(warnLogs[0].msg, /after 10 attempts/);
  assert.match(warnLogs[1].msg, /after 20 attempts/);
});

test('MqttAdapter: resets the error counter after a successful connection', async () => {
  const fakeClient = new FakeMqttClient();
  const fakeMqttLib = { connect: () => fakeClient };
  const recLogger = new RecordingLogger();

  const adapter = new MqttAdapter({
    url: 'mqtt://fake-broker',
    topicPrefix: 'x/#',
    reconnectPeriodMs: 10,
    connectTimeoutMs: 50,
    logger: recLogger,
    mqttLib: fakeMqttLib,
  });

  const connectPromise = adapter.connect().catch(() => {}); // 1st attempt fails below and settles (rejects) the promise -- expected and swallowed here; we only care about post-recovery *state*, not this promise resolving
  fakeClient.emit('error', new Error('ECONNREFUSED'));
  fakeClient.emit('error', new Error('ECONNREFUSED'));
  assert.equal(adapter._consecutiveErrorCount, 2);

  fakeClient.emit('connect'); // simulate the broker becoming reachable on a later reconnect
  await connectPromise;

  assert.equal(adapter._consecutiveErrorCount, 0);
  assert.equal(adapter.connected, true);
});

test('MqttAdapter: gives up after maxReconnectAttempts and disconnect() afterward is a safe no-op', async () => {
  const fakeClient = new FakeMqttClient();
  const fakeMqttLib = { connect: () => fakeClient };
  const recLogger = new RecordingLogger();

  const adapter = new MqttAdapter({
    url: 'mqtt://fake-broker',
    topicPrefix: 'x/#',
    reconnectPeriodMs: 10,
    connectTimeoutMs: 50,
    maxReconnectAttempts: 5,
    logger: recLogger,
    mqttLib: fakeMqttLib,
  });

  const connectPromise = adapter.connect().catch(() => {});
  for (let i = 0; i < 5; i += 1) {
    fakeClient.emit('error', new Error('ECONNREFUSED'));
  }
  await connectPromise;

  assert.equal(adapter._gaveUp, true);
  assert.equal(adapter.client, null, 'client must be nulled after give-up to prevent a later double .end() call');
  assert.equal(fakeClient.endCallCount, 1);

  // Regression check for the bug found during review: calling disconnect()
  // after give-up must not attempt a second .end() on the same client.
  await assert.doesNotReject(() => adapter.disconnect());
  assert.equal(fakeClient.endCallCount, 1, 'disconnect() after give-up must not call .end() again');
});

// ---------------------------------------------------------------------------
// PasswordHasher: scrypt-based hashing, constant-time verification
// ---------------------------------------------------------------------------

test('PasswordHasher: hash/verify roundtrip succeeds; wrong password fails', async () => {
  const stored = await hashPassword('correct-horse-battery-staple');
  assert.equal(await verifyPassword('correct-horse-battery-staple', stored), true);
  assert.equal(await verifyPassword('wrong-password', stored), false);
});

test('PasswordHasher: same password produces different hashes (unique salt per call)', async () => {
  const a = await hashPassword('same-password-123');
  const b = await hashPassword('same-password-123');
  assert.notEqual(a, b);
  assert.equal(await verifyPassword('same-password-123', a), true);
  assert.equal(await verifyPassword('same-password-123', b), true);
});

test('PasswordHasher: verifyPassword never throws on malformed stored values', async () => {
  assert.equal(await verifyPassword('anything', 'not-a-valid-stored-hash'), false);
  assert.equal(await verifyPassword('anything', ''), false);
  assert.equal(await verifyPassword('anything', null), false);
});

// ---------------------------------------------------------------------------
// Jwt: minimal HS256 sign/verify
// ---------------------------------------------------------------------------

test('Jwt: sign/verify roundtrip preserves claims', () => {
  const token = Jwt.sign({ sub: 'user-1', role: 'ADMIN' }, 'a-secret-at-least-16-chars');
  const payload = Jwt.verify(token, 'a-secret-at-least-16-chars');
  assert.equal(payload.sub, 'user-1');
  assert.equal(payload.role, 'ADMIN');
  assert.ok(payload.iat);
});

test('Jwt: rejects a token signed with a different secret', () => {
  const token = Jwt.sign({ sub: 'user-1' }, 'secret-one-at-least-16c');
  assert.throws(() => Jwt.verify(token, 'secret-two-at-least-16c'));
});

test('Jwt: rejects a tampered payload (signature no longer matches)', () => {
  const token = Jwt.sign({ sub: 'user-1', role: 'VIEWER' }, 'a-secret-at-least-16-chars');
  const [header, payload, sig] = token.split('.');
  const tamperedPayload = Buffer.from(JSON.stringify({ sub: 'user-1', role: 'ADMIN' })).toString('base64url');
  assert.throws(() => Jwt.verify(`${header}.${tamperedPayload}.${sig}`, 'a-secret-at-least-16-chars'));
});

test('Jwt: rejects an expired token', () => {
  const token = Jwt.sign({ sub: 'user-1' }, 'a-secret-at-least-16-chars', { expiresInSeconds: -1 });
  assert.throws(() => Jwt.verify(token, 'a-secret-at-least-16-chars'), /expired/i);
});

test('Jwt: rejects a malformed token', () => {
  assert.throws(() => Jwt.verify('not-a-jwt', 'a-secret-at-least-16-chars'));
  assert.throws(() => Jwt.verify('a.b', 'a-secret-at-least-16-chars'));
});

// ---------------------------------------------------------------------------
// UserStore: JSON-file backed accounts
// ---------------------------------------------------------------------------

test('UserStore: create + verifyCredentials roundtrip; sanitized (no password hash exposed)', async () => {
  const dataDir = await mkTempDir();
  try {
    const store = new UserStore({ dataDir, logger: quietLogger });
    await store.init();
    const created = await store.createUser({ username: 'alice', password: 'alice-password-1', role: 'OPERATOR' });
    assert.equal(created.username, 'alice');
    assert.ok(!('passwordHash' in created));

    const verified = await store.verifyCredentials('alice', 'alice-password-1');
    assert.ok(verified);
    assert.equal(verified.role, 'OPERATOR');

    const failed = await store.verifyCredentials('alice', 'wrong-password');
    assert.equal(failed, null);
  } finally {
    await rmTempDir(dataDir);
  }
});

test('UserStore: rejects a duplicate username', async () => {
  const dataDir = await mkTempDir();
  try {
    const store = new UserStore({ dataDir, logger: quietLogger });
    await store.init();
    await store.createUser({ username: 'bob', password: 'bob-password-123', role: 'VIEWER' });
    await assert.rejects(() => store.createUser({ username: 'bob', password: 'another-pass-1', role: 'VIEWER' }));
  } finally {
    await rmTempDir(dataDir);
  }
});

test('UserStore: disabled accounts cannot authenticate', async () => {
  const dataDir = await mkTempDir();
  try {
    const store = new UserStore({ dataDir, logger: quietLogger });
    await store.init();
    const user = await store.createUser({ username: 'carol', password: 'carol-password-1', role: 'ADMIN' });
    assert.ok(await store.verifyCredentials('carol', 'carol-password-1'));

    await store.setDisabled(user.id, true);
    assert.equal(await store.verifyCredentials('carol', 'carol-password-1'), null);
  } finally {
    await rmTempDir(dataDir);
  }
});

test('UserStore: rejects passwords shorter than 8 characters', async () => {
  const dataDir = await mkTempDir();
  try {
    const store = new UserStore({ dataDir, logger: quietLogger });
    await store.init();
    await assert.rejects(() => store.createUser({ username: 'shortpw', password: 'short', role: 'VIEWER' }));
  } finally {
    await rmTempDir(dataDir);
  }
});

// ---------------------------------------------------------------------------
// DeviceRegistry: metadata/control overlay, in-memory-cache read path
// ---------------------------------------------------------------------------

test('DeviceRegistry: ensureRegistered creates on first call, is idempotent afterward', async () => {
  const dataDir = await mkTempDir();
  try {
    const registry = new DeviceRegistry({ dataDir, logger: quietLogger });
    await registry.init();
    const first = await registry.ensureRegistered('inv-01', { name: 'inv-01' });
    assert.equal(first.status, 'ENABLED');
    assert.equal(first.autoRegistered, true);

    const second = await registry.ensureRegistered('inv-01', { name: 'should-be-ignored' });
    assert.equal(second.name, 'inv-01', 'second call must not overwrite the existing record');
    assert.equal(registry.list().length, 1);
  } finally {
    await rmTempDir(dataDir);
  }
});

test('DeviceRegistry: register() rejects a duplicate deviceId; update()/setStatus() work and validate existence', async () => {
  const dataDir = await mkTempDir();
  try {
    const registry = new DeviceRegistry({ dataDir, logger: quietLogger });
    await registry.init();
    await registry.register({ deviceId: 'inv-01', name: 'Inverter One', location: 'Roof A' });
    await assert.rejects(() => registry.register({ deviceId: 'inv-01', name: 'dup' }));

    const updated = await registry.update('inv-01', { firmwareVersion: 'v1.2.3' });
    assert.equal(updated.firmwareVersion, 'v1.2.3');
    assert.equal(updated.name, 'Inverter One', 'unspecified fields are left unchanged');
    await assert.rejects(() => registry.update('unknown-device', { name: 'x' }));

    const disabled = await registry.setStatus('inv-01', 'DISABLED');
    assert.equal(disabled.status, 'DISABLED');
    await assert.rejects(() => registry.setStatus('unknown-device', 'ENABLED'));
  } finally {
    await rmTempDir(dataDir);
  }
});

test('DeviceRegistry: remove is a soft-delete (REMOVED status); blocks telemetry and edits, but list() excludes it by default', async () => {
  const dataDir = await mkTempDir();
  try {
    const registry = new DeviceRegistry({ dataDir, logger: quietLogger });
    await registry.init();
    await registry.register({ deviceId: 'inv-01' });
    assert.equal(registry.isBlocked('inv-01'), false);

    await registry.setStatus('inv-01', 'REMOVED');
    assert.equal(registry.isBlocked('inv-01'), true);
    assert.equal(registry.canReceiveCommands('inv-01'), false);
    await assert.rejects(() => registry.update('inv-01', { name: 'x' }), /removed/i);

    assert.equal(registry.list().length, 0, 'REMOVED devices excluded by default');
    assert.equal(registry.list({ includeRemoved: true }).length, 1);

    // Re-enabling restores it.
    await registry.setStatus('inv-01', 'ENABLED');
    assert.equal(registry.isBlocked('inv-01'), false);
    assert.equal(registry.list().length, 1);
  } finally {
    await rmTempDir(dataDir);
  }
});

test('DeviceRegistry: canReceiveCommands is true for an unregistered deviceId (registry never blocks by omission)', () => {
  const registry = new DeviceRegistry({ dataDir: '/tmp/unused-for-this-test', logger: quietLogger });
  assert.equal(registry.canReceiveCommands('never-seen-device'), true);
  assert.equal(registry.isBlocked('never-seen-device'), false);
});

test('DeviceRegistry: state persists and reloads correctly across a simulated restart', async () => {
  const dataDir = await mkTempDir();
  try {
    const registry1 = new DeviceRegistry({ dataDir, logger: quietLogger });
    await registry1.init();
    await registry1.register({ deviceId: 'inv-01', name: 'Inverter One', firmwareVersion: 'v1.0.0' });
    await registry1.setStatus('inv-01', 'DISABLED');

    const registry2 = new DeviceRegistry({ dataDir, logger: quietLogger });
    await registry2.init();
    const reloaded = registry2.get('inv-01');
    assert.equal(reloaded.name, 'Inverter One');
    assert.equal(reloaded.firmwareVersion, 'v1.0.0');
    assert.equal(reloaded.status, 'DISABLED');
  } finally {
    await rmTempDir(dataDir);
  }
});

test('Router: matches a static route and rejects wrong method', () => {
  const r = new Router();
  r.get('/health', () => 'ok');
  const match = r.match('GET', '/health');
  assert.ok(match);
  assert.equal(match.handler(), 'ok');
  assert.equal(r.match('POST', '/health'), null);
});

test('Router: extracts :param segments and URL-decodes them', () => {
  const r = new Router();
  r.get('/api/devices/:deviceId/telemetry', () => {});
  const match = r.match('GET', '/api/devices/inv%2001/telemetry');
  assert.ok(match);
  assert.equal(match.params.deviceId, 'inv 01');
});

test('Router: returns null for a path that matches no route', () => {
  const r = new Router();
  r.get('/health', () => {});
  assert.equal(r.match('GET', '/nope'), null);
});

// ---------------------------------------------------------------------------
// ApiServer: full end-to-end HTTP tests against a real server on an
// OS-assigned ephemeral port (port 0) -- zero collision risk with any other
// GridSync-OS instance or other tests running concurrently.
// ---------------------------------------------------------------------------

async function buildApiHarness(dataDir, { legacyToken } = {}) {
  const cfg = makeTestConfig(dataDir, { apiEnabled: true, apiPort: 0, apiToken: legacyToken });
  const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
  const fakeAdapter = new FakeAdapter(quietLogger);
  orchestrator.registerAdapter('fake', fakeAdapter);
  await orchestrator.start();
  const port = orchestrator.apiServer.server.address().port;
  const base = `http://127.0.0.1:${port}`;

  // Seed one account per role -- covers the common case for most tests;
  // individual tests can create additional users as needed.
  await orchestrator.userStore.createUser({ username: 'admin1', password: 'admin-password-123', role: 'ADMIN' });
  await orchestrator.userStore.createUser({ username: 'operator1', password: 'operator-password-123', role: 'OPERATOR' });
  await orchestrator.userStore.createUser({ username: 'viewer1', password: 'viewer-password-123', role: 'VIEWER' });

  async function loginAs(username, password) {
    const res = await fetch(`${base}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    if (!res.ok) throw new Error(`login failed for "${username}": HTTP ${res.status}`);
    return (await res.json()).token;
  }

  function authHeader(token) {
    return { Authorization: `Bearer ${token}` };
  }

  const adminToken = await loginAs('admin1', 'admin-password-123');
  const operatorToken = await loginAs('operator1', 'operator-password-123');
  const viewerToken = await loginAs('viewer1', 'viewer-password-123');

  return { orchestrator, fakeAdapter, base, loginAs, authHeader, adminToken, operatorToken, viewerToken };
}

test('ApiServer: GET /health responds ok', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const res = await fetch(`${h.base}/health`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.status, 'ok');
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: /api/devices reflects real telemetry flowing through the orchestrator', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const auth = h.authHeader(h.viewerToken);

    let res = await fetch(`${h.base}/api/devices`, { headers: auth });
    assert.equal((await res.json()).devices.length, 0);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-inv-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-inv-01' },
    );
    await waitUntil(async () => {
      res = await fetch(`${h.base}/api/devices`, { headers: auth });
      const body = await res.json();
      return body.devices.length === 1;
    }, 2000);

    const body = await (await fetch(`${h.base}/api/devices`, { headers: auth })).json();
    assert.equal(body.devices[0].deviceId, 'api-inv-01');
    assert.equal(body.devices[0].mode, 'NORMAL');

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: GET /api/devices/:deviceId returns 404 for an unknown device, 200 for a known one', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const auth = h.authHeader(h.viewerToken);

    const notFound = await fetch(`${h.base}/api/devices/does-not-exist`, { headers: auth });
    assert.equal(notFound.status, 404);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-inv-02', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-inv-02' },
    );
    await waitUntil(async () => (await fetch(`${h.base}/api/devices/api-inv-02`, { headers: auth })).status === 200, 2000);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: telemetry history endpoint returns ingested points and respects limit', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const auth = h.authHeader(h.viewerToken);
    for (let i = 0; i < 5; i += 1) {
      h.fakeAdapter.emitTelemetry(
        { deviceId: 'api-hist-01', deviceType: 'METER', voltage: 225 + i, timestamp: Date.now() + i },
        { protocol: 'MQTT', deviceId: 'api-hist-01' },
      );
    }
    await waitUntil(async () => {
      const res = await fetch(`${h.base}/api/devices/api-hist-01/telemetry`, { headers: auth });
      const body = await res.json();
      return body.count === 5;
    }, 2000);

    const limited = await (await fetch(`${h.base}/api/devices/api-hist-01/telemetry?limit=2`, { headers: auth })).json();
    assert.equal(limited.count, 2);
    // Newest-first ordering.
    assert.ok(limited.points[0].metrics.voltage > limited.points[1].metrics.voltage);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: every /api/* endpoint requires authentication (401 with no credentials)', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const endpoints = ['/api/snapshot', '/api/devices', '/api/commands/pending', '/api/commands/history'];
    for (const ep of endpoints) {
      // eslint-disable-next-line no-await-in-loop
      const res = await fetch(`${h.base}${ep}`);
      assert.equal(res.status, 401, `expected 401 for ${ep} with no auth, got ${res.status}`);
    }
    const postRes = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'STANDBY', deviceId: 'x', value: 0 }),
    });
    assert.equal(postRes.status, 401);
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: legacy GS_API_TOKEN still works as ADMIN-equivalent (backward compatibility)', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir, { legacyToken: 'legacy-secret-123' });
    const res = await fetch(`${h.base}/api/devices`, { headers: { Authorization: 'Bearer legacy-secret-123' } });
    assert.equal(res.status, 200);
    // Admin-only endpoint should also work via the legacy token.
    const usersRes = await fetch(`${h.base}/api/auth/users`, { headers: { Authorization: 'Bearer legacy-secret-123' } });
    assert.equal(usersRes.status, 200);
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: login succeeds with correct credentials, fails with wrong password', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);

    const ok = await fetch(`${h.base}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'operator1', password: 'operator-password-123' }),
    });
    assert.equal(ok.status, 200);
    const okBody = await ok.json();
    assert.ok(okBody.token);
    assert.equal(okBody.user.role, 'OPERATOR');

    const wrong = await fetch(`${h.base}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'operator1', password: 'wrong-password' }),
    });
    assert.equal(wrong.status, 401);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: RBAC -- VIEWER is blocked (403) from issuing commands, OPERATOR is allowed (202) and it actually dispatches', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-cmd-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-cmd-01' },
    );
    await waitUntil(() => h.orchestrator.getDeviceDetail('api-cmd-01') !== null, 2000);

    const asViewer = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...h.authHeader(h.viewerToken) },
      body: JSON.stringify({ type: 'CURTAIL', deviceId: 'api-cmd-01', value: 10 }),
    });
    assert.equal(asViewer.status, 403, 'VIEWER must not be able to issue commands');
    assert.equal(h.fakeAdapter.sentCommands.length, 0);

    const asOperator = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...h.authHeader(h.operatorToken) },
      body: JSON.stringify({ type: 'CURTAIL', deviceId: 'api-cmd-01', value: 10 }),
    });
    assert.equal(asOperator.status, 202);
    const body = await asOperator.json();
    assert.ok(body.commandId);

    await waitUntil(() => h.fakeAdapter.sentCommands.some((c) => c.deviceId === 'api-cmd-01'), 2000);

    // Audit trail: the command record should show who issued it.
    const history = await (await fetch(`${h.base}/api/commands/history`, { headers: h.authHeader(h.viewerToken) })).json();
    const issued = history.commands.find((c) => c.commandId === body.commandId);
    assert.equal(issued.issuedBy, 'operator1');

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: RBAC -- only ADMIN can create users; OPERATOR/VIEWER get 403', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const newUserBody = JSON.stringify({ username: 'newbie', password: 'newbie-password-1', role: 'VIEWER' });

    const asOperator = await fetch(`${h.base}/api/auth/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...h.authHeader(h.operatorToken) },
      body: newUserBody,
    });
    assert.equal(asOperator.status, 403);

    const asAdmin = await fetch(`${h.base}/api/auth/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...h.authHeader(h.adminToken) },
      body: newUserBody,
    });
    assert.equal(asAdmin.status, 201);

    const list = await (await fetch(`${h.base}/api/auth/users`, { headers: h.authHeader(h.adminToken) })).json();
    assert.ok(list.users.some((u) => u.username === 'newbie'));
    // Sanitized: password hash must never be exposed via the API.
    assert.ok(list.users.every((u) => !('passwordHash' in u)));

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: logout revokes the token -- subsequent requests with it get 401', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const auth = h.authHeader(h.viewerToken);

    const before = await fetch(`${h.base}/api/snapshot`, { headers: auth });
    assert.equal(before.status, 200);

    const logout = await fetch(`${h.base}/api/auth/logout`, { method: 'POST', headers: auth });
    assert.equal(logout.status, 200);

    const after = await fetch(`${h.base}/api/snapshot`, { headers: auth });
    assert.equal(after.status, 401);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: expired JWT is rejected', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir, { apiEnabled: true, apiPort: 0, jwtExpiresInSeconds: -1 }); // already expired
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    await orchestrator.start();
    await orchestrator.userStore.createUser({ username: 'shortlived', password: 'password-12345', role: 'VIEWER' });
    const port = orchestrator.apiServer.server.address().port;
    const base = `http://127.0.0.1:${port}`;

    const loginRes = await fetch(`${base}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'shortlived', password: 'password-12345' }),
    });
    const { token } = await loginRes.json();

    const res = await fetch(`${base}/api/snapshot`, { headers: { Authorization: `Bearer ${token}` } });
    assert.equal(res.status, 401);

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: bootstrap admin is auto-created when configured and the user store is empty', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir, {
      apiEnabled: true,
      apiPort: 0,
      bootstrapAdminUsername: 'bootadmin',
      bootstrapAdminPassword: 'bootstrap-password-1',
    });
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    await orchestrator.start();

    assert.equal(await orchestrator.userStore.count(), 1);
    const user = await orchestrator.userStore.findByUsername('bootadmin');
    assert.equal(user.role, 'ADMIN');

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: malformed JSON body returns 400, oversized body returns 413', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir, { legacyToken: 'tok' });
    const auth = { Authorization: 'Bearer tok' };

    const malformed = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...auth },
      body: '{not valid json',
    });
    assert.equal(malformed.status, 400);

    const maxBodyBytes = h.orchestrator.config.api.maxBodyBytes;
    const oversized = 'x'.repeat(maxBodyBytes + 1000);
    const tooLarge = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...auth },
      body: oversized,
    });
    assert.equal(tooLarge.status, 413);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: unknown route returns 404 regardless of auth', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const res = await fetch(`${h.base}/nope/not/a/route`);
    assert.equal(res.status, 404);
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

// ---------------------------------------------------------------------------
// v0.4.0: Alarm Engine integration (orchestrator + API), RESET command,
// staleness sweep, and command/alarm history filtering
// ---------------------------------------------------------------------------

test('Orchestrator: overvoltage telemetry produces an active alarm; recovery clears it', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir);
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();

    fakeAdapter.emitTelemetry(
      { deviceId: 'alarm-inv-01', deviceType: 'INVERTER', voltage: 280, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'alarm-inv-01' },
    );
    await waitUntil(() => orchestrator.listActiveAlarms().some((a) => a.type === 'OVER_VOLTAGE'), 2000);

    fakeAdapter.emitTelemetry(
      { deviceId: 'alarm-inv-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'alarm-inv-01' },
    );
    await waitUntil(() => !orchestrator.listActiveAlarms().some((a) => a.type === 'OVER_VOLTAGE'), 2000);

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('Orchestrator: staleness sweep triggers DEVICE_OFFLINE for a device that has gone silent', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir, { staleTelemetryMs: 100 });
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    await orchestrator.start();

    orchestrator.circuitBreaker.recordTelemetry('ghost-device', Date.now() - 5000); // long past staleTelemetryMs
    await orchestrator._sweepStaleness();

    const active = orchestrator.listActiveAlarms();
    assert.ok(active.some((a) => a.deviceId === 'ghost-device' && a.type === 'DEVICE_OFFLINE'));

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('Orchestrator: issuing RESET acknowledges all active alarms for that device', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir);
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();

    fakeAdapter.emitTelemetry(
      { deviceId: 'reset-inv-01', deviceType: 'INVERTER', voltage: 280, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'reset-inv-01' },
    );
    await waitUntil(() => orchestrator.listActiveAlarms().some((a) => a.deviceId === 'reset-inv-01'), 2000);

    await orchestrator.issueManualCommand({ type: 'RESET', deviceId: 'reset-inv-01', issuedBy: 'operator1' });

    const alarm = orchestrator.listActiveAlarms().find((a) => a.deviceId === 'reset-inv-01');
    assert.ok(alarm, 'alarm should still be active -- RESET acknowledges, does not force-clear an unresolved condition');
    assert.equal(alarm.acknowledged, true);
    assert.equal(alarm.acknowledgedBy, 'operator1');

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: GET /api/alarms/active reflects real triggered alarms', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const auth = h.authHeader(h.viewerToken);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-alarm-01', deviceType: 'INVERTER', voltage: 280, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-alarm-01' },
    );
    await waitUntil(async () => {
      const res = await fetch(`${h.base}/api/alarms/active`, { headers: auth });
      const body = await res.json();
      return body.alarms.some((a) => a.deviceId === 'api-alarm-01');
    }, 2000);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: alarm acknowledge is RBAC-gated (403 VIEWER, 200 OPERATOR) and reflected afterward', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-ack-01', deviceType: 'INVERTER', voltage: 280, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-ack-01' },
    );
    let alarmId;
    await waitUntil(async () => {
      const res = await fetch(`${h.base}/api/alarms/active`, { headers: h.authHeader(h.viewerToken) });
      const body = await res.json();
      const found = body.alarms.find((a) => a.deviceId === 'api-ack-01');
      if (found) alarmId = found.alarmId;
      return !!found;
    }, 2000);

    const asViewer = await fetch(`${h.base}/api/alarms/${alarmId}/acknowledge`, { method: 'POST', headers: h.authHeader(h.viewerToken) });
    assert.equal(asViewer.status, 403);

    const asOperator = await fetch(`${h.base}/api/alarms/${alarmId}/acknowledge`, { method: 'POST', headers: h.authHeader(h.operatorToken) });
    assert.equal(asOperator.status, 200);

    const active = await (await fetch(`${h.base}/api/alarms/active`, { headers: h.authHeader(h.viewerToken) })).json();
    const alarm = active.alarms.find((a) => a.alarmId === alarmId);
    assert.equal(alarm.acknowledged, true);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: acknowledging an unknown/non-active alarm id returns 404', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const res = await fetch(`${h.base}/api/alarms/does-not-exist/acknowledge`, {
      method: 'POST',
      headers: h.authHeader(h.operatorToken),
    });
    assert.equal(res.status, 404);
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: GET /api/commands/history supports deviceId and status filters', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const auth = h.authHeader(h.viewerToken);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'filter-dev-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'filter-dev-01' },
    );
    await waitUntil(() => h.orchestrator.getDeviceDetail('filter-dev-01') !== null, 2000);
    await h.orchestrator.issueManualCommand({ type: 'STANDBY', deviceId: 'filter-dev-01', issuedBy: 'operator1' });
    await waitUntil(() => h.fakeAdapter.sentCommands.some((c) => c.deviceId === 'filter-dev-01'), 2000);

    const filtered = await (await fetch(`${h.base}/api/commands/history?deviceId=filter-dev-01`, { headers: auth })).json();
    assert.ok(filtered.commands.length >= 1);
    assert.ok(filtered.commands.every((c) => c.deviceId === 'filter-dev-01'));

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

// ---------------------------------------------------------------------------
// v0.5.0: Device Management -- orchestrator integration + API RBAC/CRUD
// ---------------------------------------------------------------------------

test('Orchestrator: telemetry auto-registers a new device in the registry', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir);
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();

    fakeAdapter.emitTelemetry(
      { deviceId: 'auto-reg-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'auto-reg-01' },
    );
    await waitUntil(() => orchestrator.deviceRegistry.get('auto-reg-01') !== null, 2000);
    assert.equal(orchestrator.deviceRegistry.get('auto-reg-01').status, 'ENABLED');

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('Orchestrator: telemetry from a REMOVED device is dropped, not reprocessed', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir);
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();
    await orchestrator.deviceRegistry.register({ deviceId: 'removed-dev-01' });
    await orchestrator.deviceRegistry.setStatus('removed-dev-01', 'REMOVED');

    const before = orchestrator.getSnapshot().blockedTelemetry;
    fakeAdapter.emitTelemetry(
      { deviceId: 'removed-dev-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'removed-dev-01' },
    );
    await waitUntil(() => orchestrator.getSnapshot().blockedTelemetry > before, 2000);

    assert.equal(orchestrator.getDeviceDetail('removed-dev-01'), null, 'telemetry must never have been processed into device state');

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('Orchestrator: removeDevice() clears live state immediately (disappears from listDevices/getDeviceDetail)', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir);
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();

    fakeAdapter.emitTelemetry(
      { deviceId: 'to-remove-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'to-remove-01' },
    );
    await waitUntil(() => orchestrator.getDeviceDetail('to-remove-01') !== null, 2000);

    await orchestrator.removeDevice('to-remove-01');
    assert.equal(orchestrator.getDeviceDetail('to-remove-01'), null);
    assert.ok(!orchestrator.listDevices().some((d) => d.deviceId === 'to-remove-01'));
    // But it's still visible in the audit trail if explicitly requested.
    assert.ok(orchestrator.listRegisteredDevices({ includeRemoved: true }).some((d) => d.deviceId === 'to-remove-01'));

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('Orchestrator: disabling a device blocks auto-generated FSM commands too (not just manual ones)', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir);
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();

    fakeAdapter.emitTelemetry(
      { deviceId: 'disabled-fsm-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'disabled-fsm-01' },
    );
    await waitUntil(() => orchestrator.getDeviceDetail('disabled-fsm-01') !== null, 2000);
    await orchestrator.setDeviceStatus('disabled-fsm-01', 'DISABLED');

    // Overvoltage would normally trigger an auto-CURTAIL command via the state machine.
    fakeAdapter.emitTelemetry(
      { deviceId: 'disabled-fsm-01', deviceType: 'INVERTER', voltage: 270, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'disabled-fsm-01' },
    );
    await waitUntil(() => orchestrator.commandQueue.size() === 0, 2000); // the attempt fails/retries out, queue drains

    assert.equal(fakeAdapter.sentCommands.length, 0, 'a disabled device must not receive even an auto-generated safety command');

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: GET /api/devices/registry is not shadowed by the /api/devices/:deviceId route', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const res = await fetch(`${h.base}/api/devices/registry`, { headers: h.authHeader(h.viewerToken) });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.ok(Array.isArray(body.devices), '"registry" must not be captured as a literal deviceId by the :deviceId route');
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: device registry CRUD is ADMIN-gated end-to-end (register, update, enable/disable, remove)', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const adminAuth = { 'Content-Type': 'application/json', ...h.authHeader(h.adminToken) };
    const operatorAuth = { 'Content-Type': 'application/json', ...h.authHeader(h.operatorToken) };

    // OPERATOR cannot register a device.
    const asOperator = await fetch(`${h.base}/api/devices`, { method: 'POST', headers: operatorAuth, body: JSON.stringify({ deviceId: 'crud-dev-01' }) });
    assert.equal(asOperator.status, 403);

    // ADMIN registers it.
    const registerRes = await fetch(`${h.base}/api/devices`, {
      method: 'POST',
      headers: adminAuth,
      body: JSON.stringify({ deviceId: 'crud-dev-01', name: 'Test Device', location: 'Lab' }),
    });
    assert.equal(registerRes.status, 201);

    // Duplicate registration fails.
    const dupRes = await fetch(`${h.base}/api/devices`, { method: 'POST', headers: adminAuth, body: JSON.stringify({ deviceId: 'crud-dev-01' }) });
    assert.equal(dupRes.status, 400);

    // ADMIN updates metadata.
    const patchRes = await fetch(`${h.base}/api/devices/crud-dev-01`, {
      method: 'PATCH',
      headers: adminAuth,
      body: JSON.stringify({ firmwareVersion: 'v3.0.0' }),
    });
    assert.equal(patchRes.status, 200);
    const patched = await patchRes.json();
    assert.equal(patched.device.firmwareVersion, 'v3.0.0');

    // OPERATOR cannot disable.
    const disableAsOperator = await fetch(`${h.base}/api/devices/crud-dev-01/disable`, { method: 'POST', headers: operatorAuth });
    assert.equal(disableAsOperator.status, 403);

    // ADMIN disables, then re-enables.
    const disableRes = await fetch(`${h.base}/api/devices/crud-dev-01/disable`, { method: 'POST', headers: adminAuth });
    assert.equal(disableRes.status, 200);
    assert.equal((await disableRes.json()).device.status, 'DISABLED');

    const enableRes = await fetch(`${h.base}/api/devices/crud-dev-01/enable`, { method: 'POST', headers: adminAuth });
    assert.equal((await enableRes.json()).device.status, 'ENABLED');

    // ADMIN removes it; it drops out of the default registry listing.
    const removeRes = await fetch(`${h.base}/api/devices/crud-dev-01`, { method: 'DELETE', headers: adminAuth });
    assert.equal(removeRes.status, 200);

    const registryList = await (await fetch(`${h.base}/api/devices/registry`, { headers: h.authHeader(h.viewerToken) })).json();
    assert.ok(!registryList.devices.some((d) => d.deviceId === 'crud-dev-01'));

    const withRemoved = await (await fetch(`${h.base}/api/devices/registry?includeRemoved=true`, { headers: h.authHeader(h.viewerToken) })).json();
    assert.ok(withRemoved.devices.some((d) => d.deviceId === 'crud-dev-01' && d.status === 'REMOVED'));

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: PATCH/enable/disable/DELETE on an unregistered deviceId return 404', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    const adminAuth = { 'Content-Type': 'application/json', ...h.authHeader(h.adminToken) };

    const patchRes = await fetch(`${h.base}/api/devices/nope`, { method: 'PATCH', headers: adminAuth, body: JSON.stringify({ name: 'x' }) });
    assert.equal(patchRes.status, 404);

    const enableRes = await fetch(`${h.base}/api/devices/nope/enable`, { method: 'POST', headers: adminAuth });
    assert.equal(enableRes.status, 404);

    const removeRes = await fetch(`${h.base}/api/devices/nope`, { method: 'DELETE', headers: adminAuth });
    assert.equal(removeRes.status, 404);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});


// Full orchestrator: simulated device inputs -> commands stay within safe params
// ---------------------------------------------------------------------------

test('Orchestrator (end-to-end): overvoltage telemetry auto-curtails within safe bounds, recovery releases it', async () => {
  const dataDir = await mkTempDir();
  try {
    const cfg = makeTestConfig(dataDir, { baseRetryDelayMs: 15, maxRetryDelayMs: 30 });
    const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
    const fakeAdapter = new FakeAdapter(quietLogger);
    orchestrator.registerAdapter('fake', fakeAdapter);
    await orchestrator.start();

    fakeAdapter.emitTelemetry(
      { deviceId: 'inv-e2e', deviceType: 'INVERTER', voltage: 265, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'inv-e2e' },
    );

    await waitUntil(() => fakeAdapter.sentCommands.some((c) => c.type === 'CURTAIL'), 2000);
    const curtailCmd = fakeAdapter.sentCommands.find((c) => c.type === 'CURTAIL');
    assert.ok(curtailCmd.value >= 0 && curtailCmd.value <= cfg.gridConstraints.maxCurtailKw, 'curtail command must stay within safe operating bounds');

    const snapAfterCurtail = orchestrator.getSnapshot();
    assert.equal(snapAfterCurtail.devices.find((d) => d.deviceId === 'inv-e2e').mode, 'CURTAILED');

    fakeAdapter.emitTelemetry(
      { deviceId: 'inv-e2e', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'inv-e2e' },
    );

    await waitUntil(() => fakeAdapter.sentCommands.some((c) => c.type === 'STANDBY'), 2000);
    const snapAfterRelease = orchestrator.getSnapshot();
    assert.equal(snapAfterRelease.devices.find((d) => d.deviceId === 'inv-e2e').mode, 'NORMAL');

    await orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});
