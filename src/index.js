'use strict';

const { Logger } = require('./utils/logger');
const config = require('./config');
const { GridSyncOrchestrator } = require('./orchestrator/GridSyncOrchestrator');
const { MqttAdapter } = require('./adapters/MqttAdapter');
const { ModbusAdapter } = require('./adapters/ModbusAdapter');
const { Dnp3Adapter } = require('./adapters/Dnp3Adapter');

const logger = new Logger('gridsync-os');

/**
 * Last-resort process-level safety nets. Every async boundary in this
 * codebase is already wrapped in try/catch, so reaching these handlers
 * indicates a bug slipped through -- we log it with full context and fail
 * toward a controlled shutdown rather than letting Node crash silently or
 * continue in a possibly-corrupted state.
 */
process.on('unhandledRejection', (reason) => {
  logger.error('UNHANDLED PROMISE REJECTION -- this indicates a missing .catch() somewhere', {
    reason: reason instanceof Error ? { message: reason.message, stack: reason.stack } : reason,
  });
});

process.on('uncaughtException', (err) => {
  logger.error('UNCAUGHT EXCEPTION -- attempting graceful shutdown', { err });
  // Do not immediately process.exit() -- give in-flight command WAL writes
  // a chance to flush. shutdown() itself has no unbounded awaits.
  shutdown(1);
});

async function main() {
  const orchestrator = new GridSyncOrchestrator({ config, logger });

  orchestrator.registerAdapter('mqtt-main', new MqttAdapter({ ...config.mqtt, logger }));
  orchestrator.registerAdapter('modbus-fleet', new ModbusAdapter({
    deviceIds: ['inverter-01', 'inverter-02'],
    pollIntervalMs: 1000,
    logger,
    simulate: true,
  }));
  orchestrator.registerAdapter('dnp3-meters', new Dnp3Adapter({
    deviceIds: ['meter-01'],
    pollIntervalMs: 2000,
    logger,
    simulate: true,
  }));

  await orchestrator.start();

  if (orchestrator.apiServer && orchestrator.apiServer.server) {
    const { host, port } = config.api;
    logger.info(`Dashboard available at http://${host}:${port}/`);
  }

  const heartbeat = setInterval(() => {
    logger.info('heartbeat', orchestrator.getSnapshot());
  }, 15000);
  heartbeat.unref?.();

  global.__gridsyncOrchestrator = orchestrator; // for shutdown() below
  global.__gridsyncHeartbeat = heartbeat;
}

async function shutdown(exitCode = 0) {
  try {
    if (global.__gridsyncHeartbeat) clearInterval(global.__gridsyncHeartbeat);
    if (global.__gridsyncOrchestrator) await global.__gridsyncOrchestrator.stop();
  } catch (err) {
    logger.error('error during shutdown', { err });
  } finally {
    process.exit(exitCode);
  }
}

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down gracefully');
  shutdown(0);
});
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  shutdown(0);
});

main().catch((err) => {
  logger.error('fatal error during startup', { err });
  process.exit(1);
});
