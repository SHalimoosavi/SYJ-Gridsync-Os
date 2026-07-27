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
const { CommandQueue } = require('../src/commands/CommandQueue');
const { CommandDispatcher } = require('../src/commands/CommandDispatcher');
const { FileWalStorage } = require('../src/storage/FileWalStorage');
const { AdapterBase } = require('../src/adapters/AdapterBase');
const { MqttAdapter } = require('../src/adapters/MqttAdapter');
const { GridSyncOrchestrator } = require('../src/orchestrator/GridSyncOrchestrator');
const { ValidationError } = require('../src/utils/errors');
const { Router } = require('../src/api/Router');

const quietLogger = new Logger('selftest', 'error'); // suppress info/debug noise in test output

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async function mkTempDir() {
  return fsp.mkdtemp(path.join(os.tmpdir(), 'gridsync-selftest-'));
}

async function rmTempDir(dir) {
  await fsp.rm(dir, { recursive: true, force: true });
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
// CommandQueue durability + crash recovery (THE data-integrity requirement)
// ---------------------------------------------------------------------------

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
  const adapter = new FakeAdapter(quietLogger);
  const latestStates = new Map();

  const dispatcher = new CommandDispatcher({
    queue,
    circuitBreaker,
    constraintValidator,
    resolveAdapter: () => adapter,
    getLatestState: (deviceId) => latestStates.get(deviceId) || null,
    config: cfg,
    logger: quietLogger,
  });
  dispatcher.start();

  return { cfg, storage, queue, circuitBreaker, constraintValidator, adapter, latestStates, dispatcher };
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
// Router: path matching + :param extraction, tested independent of HTTP
// ---------------------------------------------------------------------------

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

async function buildApiHarness(dataDir, { token } = {}) {
  const cfg = makeTestConfig(dataDir, { apiEnabled: true, apiPort: 0, apiToken: token });
  const orchestrator = new GridSyncOrchestrator({ config: cfg, logger: quietLogger });
  const fakeAdapter = new FakeAdapter(quietLogger);
  orchestrator.registerAdapter('fake', fakeAdapter);
  await orchestrator.start();
  const port = orchestrator.apiServer.server.address().port;
  const base = `http://127.0.0.1:${port}`;
  return { orchestrator, fakeAdapter, base };
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

    let res = await fetch(`${h.base}/api/devices`);
    assert.equal((await res.json()).devices.length, 0);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-inv-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-inv-01' },
    );
    await waitUntil(async () => {
      res = await fetch(`${h.base}/api/devices`);
      const body = await res.json();
      return body.devices.length === 1;
    }, 2000);

    const body = await (await fetch(`${h.base}/api/devices`)).json();
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

    const notFound = await fetch(`${h.base}/api/devices/does-not-exist`);
    assert.equal(notFound.status, 404);

    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-inv-02', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-inv-02' },
    );
    await waitUntil(async () => (await fetch(`${h.base}/api/devices/api-inv-02`)).status === 200, 2000);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: telemetry history endpoint returns ingested points and respects limit', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir);
    for (let i = 0; i < 5; i += 1) {
      h.fakeAdapter.emitTelemetry(
        { deviceId: 'api-hist-01', deviceType: 'METER', voltage: 225 + i, timestamp: Date.now() + i },
        { protocol: 'MQTT', deviceId: 'api-hist-01' },
      );
    }
    await waitUntil(async () => {
      const res = await fetch(`${h.base}/api/devices/api-hist-01/telemetry`);
      const body = await res.json();
      return body.count === 5;
    }, 2000);

    const limited = await (await fetch(`${h.base}/api/devices/api-hist-01/telemetry?limit=2`)).json();
    assert.equal(limited.count, 2);
    // Newest-first ordering.
    assert.ok(limited.points[0].metrics.voltage > limited.points[1].metrics.voltage);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: POST /api/commands fails closed (503) when no token is configured', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir); // no token passed
    const res = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'STANDBY', deviceId: 'x', value: 0 }),
    });
    assert.equal(res.status, 503);
    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: POST /api/commands rejects a wrong/missing token (401), accepts the correct one (202) and actually enqueues it', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir, { token: 'secret-token-123' });

    // A manual command can only be routed once the target device has been
    // seen by an adapter at least once -- this is correct system behavior
    // (we can't route to a device with no known transport), so establish
    // that via telemetry first, same as a real operator workflow would.
    h.fakeAdapter.emitTelemetry(
      { deviceId: 'api-cmd-01', deviceType: 'INVERTER', voltage: 230, frequency: 50, soc: 0.5, timestamp: Date.now() },
      { protocol: 'MQTT', deviceId: 'api-cmd-01' },
    );
    await waitUntil(() => h.orchestrator.getDeviceDetail('api-cmd-01') !== null, 2000);

    const wrongToken = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer nope' },
      body: JSON.stringify({ type: 'CURTAIL', deviceId: 'api-cmd-01', value: 10 }),
    });
    assert.equal(wrongToken.status, 401);

    const ok = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer secret-token-123' },
      body: JSON.stringify({ type: 'CURTAIL', deviceId: 'api-cmd-01', value: 10 }),
    });
    assert.equal(ok.status, 202);
    const okBody = await ok.json();
    assert.ok(okBody.commandId);

    await waitUntil(() => h.fakeAdapter.sentCommands.some((c) => c.deviceId === 'api-cmd-01'), 2000);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: malformed JSON body returns 400, oversized body returns 413', async () => {
  const dataDir = await mkTempDir();
  try {
    const h = await buildApiHarness(dataDir, { token: 'tok' });

    const malformed = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer tok' },
      body: '{not valid json',
    });
    assert.equal(malformed.status, 400);

    const maxBodyBytes = h.orchestrator.config.api.maxBodyBytes;
    const oversized = 'x'.repeat(maxBodyBytes + 1000);
    const tooLarge = await fetch(`${h.base}/api/commands`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer tok' },
      body: oversized,
    });
    assert.equal(tooLarge.status, 413);

    await h.orchestrator.stop();
  } finally {
    await rmTempDir(dataDir);
  }
});

test('ApiServer: unknown route returns 404', async () => {
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
