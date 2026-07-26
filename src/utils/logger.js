'use strict';

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

/**
 * Minimal structured logger. Deliberately dependency-free so it never
 * becomes a Termux install headache. Swap for pino/winston later if desired
 * -- every call site uses this same 4-method interface.
 */
class Logger {
  constructor(scope = 'gridsync', level = process.env.LOG_LEVEL || 'info') {
    this.scope = scope;
    this.levelValue = LEVELS[level] ?? LEVELS.info;
  }

  _write(level, msg, meta) {
    if (LEVELS[level] < this.levelValue) return;
    const line = {
      ts: new Date().toISOString(),
      level,
      scope: this.scope,
      msg,
    };
    if (meta && Object.keys(meta).length) line.meta = meta;
    const serialized = JSON.stringify(line);
    if (level === 'error' || level === 'warn') {
      process.stderr.write(serialized + '\n');
    } else {
      process.stdout.write(serialized + '\n');
    }
  }

  child(subScope) {
    return new Logger(`${this.scope}:${subScope}`, this._levelName());
  }

  _levelName() {
    return Object.keys(LEVELS).find((k) => LEVELS[k] === this.levelValue) || 'info';
  }

  debug(msg, meta) { this._write('debug', msg, meta); }
  info(msg, meta) { this._write('info', msg, meta); }
  warn(msg, meta) { this._write('warn', msg, meta); }
  error(msg, meta) {
    // Never let a logging call itself throw on a weird Error object.
    let safeMeta = meta;
    if (meta && meta.err instanceof Error) {
      safeMeta = { ...meta, err: { message: meta.err.message, code: meta.err.code, stack: meta.err.stack } };
    }
    this._write('error', msg, safeMeta);
  }
}

module.exports = { Logger, LEVELS };
