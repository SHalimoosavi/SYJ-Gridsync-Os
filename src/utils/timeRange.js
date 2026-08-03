'use strict';

/**
 * Named range presets, in milliseconds. `month` is approximated as 30 days
 * -- there's no calendar-aware "same day last month" logic here, just a
 * fixed rolling window. Good enough for "recent trend" reporting; not a
 * substitute for real calendar-bucketed rollups.
 */
const RANGE_PRESETS = {
  hour: 60 * 60 * 1000,
  day: 24 * 60 * 60 * 1000,
  week: 7 * 24 * 60 * 60 * 1000,
  month: 30 * 24 * 60 * 60 * 1000,
};

const RANGE_NAMES = Object.keys(RANGE_PRESETS);

/**
 * @param {string} rangeName - one of RANGE_NAMES
 * @param {number} now - ms epoch, injectable for testing
 * @returns {{startTime: number, endTime: number}|null} null if rangeName is unrecognized
 */
function resolveRange(rangeName, now = Date.now()) {
  const durationMs = RANGE_PRESETS[rangeName];
  if (!durationMs) return null;
  return { startTime: now - durationMs, endTime: now };
}

/**
 * Reads `range`, `startTime`, `endTime` off a URLSearchParams-like object
 * (anything with a `.get(key)` method) and resolves them into a single
 * {startTime, endTime} filter object. Explicit startTime/endTime values
 * always take precedence over a named range for that specific field, so
 * `?range=day&endTime=...` (a day ending at a specific point) works too.
 * Invalid/unrecognized values are silently ignored rather than throwing --
 * this is a best-effort convenience parse, not strict input validation.
 * @returns {{startTime?: number, endTime?: number}}
 */
function resolveTimeFilter(query, now = Date.now()) {
  const filters = {};

  const rangeName = query.get('range');
  if (rangeName) {
    const resolved = resolveRange(rangeName, now);
    if (resolved) {
      filters.startTime = resolved.startTime;
      filters.endTime = resolved.endTime;
    }
  }

  const startTimeRaw = query.get('startTime');
  if (startTimeRaw !== null) {
    const n = Number(startTimeRaw);
    if (Number.isFinite(n)) filters.startTime = n;
  }

  const endTimeRaw = query.get('endTime');
  if (endTimeRaw !== null) {
    const n = Number(endTimeRaw);
    if (Number.isFinite(n)) filters.endTime = n;
  }

  return filters;
}

module.exports = { RANGE_PRESETS, RANGE_NAMES, resolveRange, resolveTimeFilter };
