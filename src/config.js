'use strict';

function num(envKey, fallback) {
  const v = process.env[envKey];
  if (v === undefined) return fallback;
  const parsed = Number(v);
  return Number.isFinite(parsed) ? parsed : fallback;
}

module.exports = {
  // -- Ingestion / event-loop protection --
  ingestion: {
    // Max points buffered per adapter before backpressure kicks in.
    maxBufferSize: num('GS_MAX_BUFFER', 10000),
    // Points drained per tick before yielding via setImmediate.
    batchSize: num('GS_BATCH_SIZE', 200),
    // If the buffer is full and the adapter cannot be paused, oldest points are dropped.
    backpressurePolicy: process.env.GS_BACKPRESSURE || 'pause-or-drop-oldest',
  },

  // -- Circuit breaker (data-integrity safety net) --
  circuitBreaker: {
    // If a device hasn't reported telemetry within this window, its breaker opens
    // and all outbound commands to it are blocked.
    staleTelemetryMs: num('GS_STALE_MS', 15000),
    // Global breaker: if ingestion error rate (errors per rolling window) exceeds this, trip.
    maxErrorsPerWindow: num('GS_MAX_ERR_WINDOW', 50),
    errorWindowMs: num('GS_ERR_WINDOW_MS', 10000),
  },

  // -- Grid safety constraints (example LV distribution values; tune per jurisdiction) --
  gridConstraints: {
    voltage: { min: 207, max: 253 }, // volts, 230V nominal +/-10%
    frequency: { min: 49.5, max: 50.5 }, // Hz
    soc: { min: 0.05, max: 0.95 }, // battery state of charge, fraction
    maxDischargeKw: 50,
    maxChargeKw: 50,
    maxCurtailKw: 100,
  },

  // -- Command durability / retry --
  commandQueue: {
    maxAttempts: num('GS_CMD_MAX_ATTEMPTS', 5),
    baseRetryDelayMs: num('GS_CMD_RETRY_BASE_MS', 500),
    maxRetryDelayMs: num('GS_CMD_RETRY_MAX_MS', 15000),
  },

  // -- Storage --
  storage: {
    driver: process.env.GS_STORAGE_DRIVER || 'file-wal', // 'file-wal' | 'sqlite'
    dataDir: process.env.GS_DATA_DIR || require('path').join(__dirname, '..', 'data'),
    compactionIntervalMs: num('GS_COMPACTION_INTERVAL_MS', 60000),
    maxWalLinesBeforeCompaction: num('GS_WAL_MAX_LINES', 5000),
  },

  // -- MQTT broker (default ingestion transport) --
  mqtt: {
    url: process.env.GS_MQTT_URL || 'mqtt://localhost:1883',
    topicPrefix: process.env.GS_MQTT_TOPIC_PREFIX || 'gridsync/telemetry/#',
    reconnectPeriodMs: num('GS_MQTT_RECONNECT_MS', 2000),
    connectTimeoutMs: num('GS_MQTT_CONNECT_TIMEOUT_MS', 10000),
    // 0 = retry forever (original behavior). Set to cap reconnect attempts
    // before the adapter gives up and stops trying entirely.
    maxReconnectAttempts: num('GS_MQTT_MAX_RECONNECT_ATTEMPTS', 0),
  },
};
