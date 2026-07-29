'use strict';

const { clamp, assertPlainObject } = require('../utils/validation');
const { ConstraintViolationError } = require('../utils/errors');

const COMMAND_TYPES = ['CURTAIL', 'DISCHARGE', 'CHARGE', 'STANDBY', 'RESET'];

class ConstraintValidator {
  constructor(gridConstraints) {
    assertPlainObject(gridConstraints, 'gridConstraints');
    this.constraints = gridConstraints;
  }

  /**
   * Non-throwing check: inspects a normalized telemetry point against grid
   * limits and returns a list of violation descriptors (empty if all clear).
   * This feeds the state machine's decision on whether a protective command
   * (e.g. auto-curtailment on overvoltage) should be generated.
   */
  checkTelemetry(point) {
    const violations = [];
    const { voltage, frequency, soc } = point.metrics;
    const c = this.constraints;

    if (voltage !== undefined && (voltage < c.voltage.min || voltage > c.voltage.max)) {
      violations.push({ type: 'VOLTAGE_OUT_OF_RANGE', value: voltage, min: c.voltage.min, max: c.voltage.max });
    }
    if (frequency !== undefined && (frequency < c.frequency.min || frequency > c.frequency.max)) {
      violations.push({ type: 'FREQUENCY_OUT_OF_RANGE', value: frequency, min: c.frequency.min, max: c.frequency.max });
    }
    if (soc !== undefined && (soc < c.soc.min || soc > c.soc.max)) {
      violations.push({ type: 'SOC_OUT_OF_RANGE', value: soc, min: c.soc.min, max: c.soc.max });
    }
    return violations;
  }

  /**
   * Validates and safely shapes an outbound command *before* it is allowed
   * into the durable command queue. Throws ConstraintViolationError for
   * conditions that make the command fundamentally unsafe to issue at all
   * (e.g. discharging a battery that's already near-empty); clamps
   * otherwise-valid values into safe bounds rather than rejecting them
   * outright, since a slightly-over-limit request usually reflects a
   * reasonable intent ("discharge as much as possible") rather than an error.
   *
   * @param {object} command - {type, deviceId, value}
   * @param {object|null} latestState - most recent normalized telemetry point for deviceId, or null
   * @returns {object} the command with `.value` clamped to safe bounds
   */
  validateCommand(command, latestState) {
    assertPlainObject(command, 'command');
    const { type } = command;
    if (!COMMAND_TYPES.includes(type)) {
      throw new ConstraintViolationError(`Unknown command type "${type}"`, { command });
    }
    const c = this.constraints;
    const shaped = { ...command };

    switch (type) {
      case 'CURTAIL': {
        shaped.value = clamp(Number(command.value) || 0, 0, c.maxCurtailKw);
        break;
      }
      case 'DISCHARGE': {
        const soc = latestState?.metrics?.soc;
        if (soc === undefined) {
          throw new ConstraintViolationError('Cannot authorize DISCHARGE: no known state-of-charge for device', {
            deviceId: command.deviceId,
          });
        }
        if (soc <= c.soc.min) {
          throw new ConstraintViolationError('Cannot authorize DISCHARGE: state-of-charge at or below minimum reserve', {
            deviceId: command.deviceId,
            soc,
            min: c.soc.min,
          });
        }
        shaped.value = clamp(Number(command.value) || 0, 0, c.maxDischargeKw);
        break;
      }
      case 'CHARGE': {
        const soc = latestState?.metrics?.soc;
        if (soc === undefined) {
          throw new ConstraintViolationError('Cannot authorize CHARGE: no known state-of-charge for device', {
            deviceId: command.deviceId,
          });
        }
        if (soc >= c.soc.max) {
          throw new ConstraintViolationError('Cannot authorize CHARGE: state-of-charge at or above maximum', {
            deviceId: command.deviceId,
            soc,
            max: c.soc.max,
          });
        }
        shaped.value = clamp(Number(command.value) || 0, 0, c.maxChargeKw);
        break;
      }
      case 'STANDBY': {
        shaped.value = 0;
        break;
      }
      case 'RESET': {
        // Always allowed regardless of telemetry state -- an operator must
        // be able to attempt a reset even on a device currently in ALERT.
        // Side effect (acknowledging active alarms) is handled by the
        // orchestrator, not here -- this validator only shapes the command.
        shaped.value = 0;
        break;
      }
      default:
        // Unreachable due to the COMMAND_TYPES guard above, kept for defense-in-depth.
        throw new ConstraintViolationError(`Unhandled command type "${type}"`, { command });
    }

    return shaped;
  }
}

module.exports = { ConstraintValidator, COMMAND_TYPES };
