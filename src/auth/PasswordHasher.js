'use strict';

const crypto = require('node:crypto');
const { promisify } = require('node:util');
const { assertNonEmptyString } = require('../utils/validation');

const scrypt = promisify(crypto.scrypt);
const KEY_LENGTH = 64;
const SALT_BYTES = 16;

/**
 * Password hashing via Node's built-in `crypto.scrypt` -- no bcrypt/argon2
 * dependency needed. Stored format is `salt:derivedKeyHex`, both hex-encoded.
 */
async function hashPassword(password) {
  assertNonEmptyString(password, 'password');
  const salt = crypto.randomBytes(SALT_BYTES).toString('hex');
  const derivedKey = await scrypt(password, salt, KEY_LENGTH);
  return `${salt}:${derivedKey.toString('hex')}`;
}

/**
 * @returns {Promise<boolean>} true if `password` matches the stored hash.
 * Uses a constant-time comparison to avoid timing side-channels. Never
 * throws on malformed stored values -- treats them as a non-match.
 */
async function verifyPassword(password, stored) {
  if (typeof password !== 'string' || typeof stored !== 'string') return false;
  const sepIndex = stored.indexOf(':');
  if (sepIndex === -1) return false;
  const salt = stored.slice(0, sepIndex);
  const hashHex = stored.slice(sepIndex + 1);
  if (!salt || !hashHex) return false;

  let derivedKey;
  try {
    derivedKey = await scrypt(password, salt, KEY_LENGTH);
  } catch {
    return false;
  }
  const storedBuf = Buffer.from(hashHex, 'hex');
  if (storedBuf.length !== derivedKey.length) return false;
  return crypto.timingSafeEqual(storedBuf, derivedKey);
}

module.exports = { hashPassword, verifyPassword };
