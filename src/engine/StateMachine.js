'use strict';

const { assertNonEmptyString } = require('../utils/validation');

const MODES = Object.freeze({
  NORMAL: 'NORMAL',
  CURTAILED: 'CURTAILED',
  ALERT: 'ALERT',
});

/**
 * StateMachine is a deterministic, side-effect-free transition function:
 * given the same (deviceState, telemetryPoint) pair, it always produces the
 * same (newState, effects) pair. All I/O (dispatching commands, persisting
 * state) happens *outside* this class, driven by the effects it returns.
 * This is what makes it exhaustively unit-testable and what the "Self-Test"
 * suite exercises directly, without mocking any transport.
 */
class StateMachine {
  constructor(constraintValidator) {
    this.validator = constraintValidator;
  }

  static initialDeviceState(deviceId) {
    assertNonEmptyString(deviceId, 'deviceId');
    return {
      deviceId,
      mode: MODES.NORMAL,
      lastPoint: null,
      consecutiveViolations: 0,
      violations: [],
    };
  }

  /**
   * @param {object} prevState - previous device state (see initialDeviceState shape)
   * @param {object} point - canonical telemetry point (see Normalizer)
   * @returns {{newState: object, effects: object[]}} effects are proposed
   *   commands, NOT yet validated for value bounds or dispatched -- that
   *   happens in ConstraintValidator.validateCommand + CommandDispatcher.
   */
  transition(prevState, point) {
    if (!prevState || prevState.deviceId !== point.deviceId) {
      throw new Error('StateMachine.transition: prevState.deviceId must match point.deviceId');
    }

    const violations = this.validator.checkTelemetry(point);
    const effects = [];

    if (violations.length > 0) {
      const consecutiveViolations = prevState.consecutiveViolations + 1;
      let mode = MODES.ALERT;

      const overvoltage = violations.find((v) => v.type === 'VOLTAGE_OUT_OF_RANGE' && v.value > v.max);
      if (overvoltage && prevState.mode !== MODES.CURTAILED) {
        mode = MODES.CURTAILED;
        effects.push({
          type: 'CURTAIL',
          deviceId: point.deviceId,
          value: this.validator.constraints.maxCurtailKw,
          reason: 'AUTO_OVERVOLTAGE_PROTECTION',
        });
      } else if (prevState.mode === MODES.CURTAILED) {
        // Stay curtailed while any violation persists.
        mode = MODES.CURTAILED;
      }

      return {
        newState: { ...prevState, mode, lastPoint: point, consecutiveViolations, violations },
        effects,
      };
    }

    // No violations this cycle.
    const effectsClear = [];
    if (prevState.mode === MODES.CURTAILED) {
      effectsClear.push({
        type: 'STANDBY',
        deviceId: point.deviceId,
        value: 0,
        reason: 'RELEASE_CURTAILMENT_TELEMETRY_NORMALIZED',
      });
    }

    return {
      newState: { ...prevState, mode: MODES.NORMAL, lastPoint: point, consecutiveViolations: 0, violations: [] },
      effects: effectsClear,
    };
  }
}

module.exports = { StateMachine, MODES };
