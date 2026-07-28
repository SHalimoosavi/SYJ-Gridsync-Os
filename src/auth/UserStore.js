'use strict';

const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { hashPassword, verifyPassword } = require('./PasswordHasher');
const { assertNonEmptyString, assertOneOf } = require('../utils/validation');
const { StorageError, ValidationError } = require('../utils/errors');

const ROLES = ['ADMIN', 'OPERATOR', 'VIEWER'];
const ROLE_RANK = { VIEWER: 1, OPERATOR: 2, ADMIN: 3 };

/**
 * UserStore: JSON-file backed account store. Whole-file read/write with a
 * serialized write chain (same pattern as FileWalStorage) -- user counts
 * are small (tens, not thousands) so this is simpler than a WAL and still
 * safe under concurrent writes.
 */
class UserStore {
  constructor({ dataDir, logger }) {
    this.filePath = path.join(dataDir, 'users.json');
    this.logger = logger ? logger.child('user-store') : null;
    this._writeChain = Promise.resolve();
  }

  async init() {
    try {
      await fsp.access(this.filePath);
    } catch {
      await fsp.writeFile(this.filePath, '[]');
    }
  }

  async _readAll() {
    try {
      const content = await fsp.readFile(this.filePath, 'utf8');
      return JSON.parse(content || '[]');
    } catch (err) {
      if (err.code === 'ENOENT') return [];
      throw new StorageError('Failed to read user store', { cause: err.message });
    }
  }

  _enqueueWrite(task) {
    const prevChain = this._writeChain;
    const runPromise = prevChain.then(() => task());
    this._writeChain = runPromise.then(() => undefined, () => undefined);
    return runPromise.catch((err) => {
      throw err instanceof StorageError ? err : new StorageError('User store write failed', { cause: err.message });
    });
  }

  async count() {
    return (await this._readAll()).length;
  }

  async createUser({ username, password, role }) {
    assertNonEmptyString(username, 'username');
    assertNonEmptyString(password, 'password');
    assertOneOf(role, ROLES, 'role');
    if (password.length < 8) {
      throw new ValidationError('Password must be at least 8 characters', { field: 'password' });
    }

    return this._enqueueWrite(async () => {
      const users = await this._readAll();
      const normalizedUsername = username.trim().toLowerCase();
      if (users.some((u) => u.username === normalizedUsername)) {
        throw new ValidationError(`Username "${username}" is already taken`, { username });
      }
      const user = {
        id: crypto.randomUUID(),
        username: normalizedUsername,
        passwordHash: await hashPassword(password),
        role,
        disabled: false,
        createdAt: Date.now(),
      };
      users.push(user);
      await fsp.writeFile(this.filePath, JSON.stringify(users, null, 2));
      return sanitize(user);
    });
  }

  async findByUsername(username) {
    if (typeof username !== 'string') return null;
    const users = await this._readAll();
    return users.find((u) => u.username === username.trim().toLowerCase()) || null;
  }

  async findById(id) {
    const users = await this._readAll();
    return users.find((u) => u.id === id) || null;
  }

  /** @returns {Promise<object|null>} the sanitized user (no password hash) if credentials are valid and account is enabled, else null. */
  async verifyCredentials(username, password) {
    const user = await this.findByUsername(username);
    if (!user || user.disabled) return null;
    const valid = await verifyPassword(password, user.passwordHash);
    return valid ? sanitize(user) : null;
  }

  async listUsers() {
    const users = await this._readAll();
    return users.map(sanitize);
  }

  async setDisabled(userId, disabled) {
    return this._enqueueWrite(async () => {
      const users = await this._readAll();
      const user = users.find((u) => u.id === userId);
      if (!user) throw new ValidationError(`Unknown user id: ${userId}`, { userId });
      user.disabled = !!disabled;
      await fsp.writeFile(this.filePath, JSON.stringify(users, null, 2));
      return sanitize(user);
    });
  }
}

function sanitize(user) {
  const { passwordHash, ...rest } = user;
  return rest;
}

module.exports = { UserStore, ROLES, ROLE_RANK };
