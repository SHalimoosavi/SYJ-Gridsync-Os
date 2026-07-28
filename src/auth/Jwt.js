'use strict';

const crypto = require('node:crypto');

function base64url(input) {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function base64urlDecode(str) {
  let padded = str.replace(/-/g, '+').replace(/_/g, '/');
  while (padded.length % 4) padded += '=';
  return Buffer.from(padded, 'base64');
}

/**
 * Minimal HS256 JWT implementation -- no jsonwebtoken/jose dependency.
 * Deliberately supports only what this project needs (HS256, exp claim);
 * not a general-purpose JWT library.
 */
class JwtError extends Error {
  constructor(message) {
    super(message);
    this.name = 'JwtError';
  }
}

function sign(payload, secret, { expiresInSeconds } = {}) {
  if (typeof secret !== 'string' || secret.length < 16) {
    throw new JwtError('JWT secret must be a string of at least 16 characters');
  }
  const header = { alg: 'HS256', typ: 'JWT' };
  const nowSeconds = Math.floor(Date.now() / 1000);
  const fullPayload = { ...payload, iat: nowSeconds };
  if (expiresInSeconds) fullPayload.exp = nowSeconds + expiresInSeconds;

  const headerPart = base64url(JSON.stringify(header));
  const payloadPart = base64url(JSON.stringify(fullPayload));
  const signature = crypto.createHmac('sha256', secret).update(`${headerPart}.${payloadPart}`).digest();
  const signaturePart = base64url(signature);

  return `${headerPart}.${payloadPart}.${signaturePart}`;
}

/**
 * @returns {object} the decoded payload if the token is valid and unexpired.
 * @throws {JwtError} on any malformed token, bad signature, or expiry.
 */
function verify(token, secret) {
  if (typeof token !== 'string') throw new JwtError('Token must be a string');
  const parts = token.split('.');
  if (parts.length !== 3) throw new JwtError('Malformed token');
  const [headerPart, payloadPart, signaturePart] = parts;

  const expectedSignature = base64url(
    crypto.createHmac('sha256', secret).update(`${headerPart}.${payloadPart}`).digest(),
  );

  const provided = Buffer.from(signaturePart);
  const expected = Buffer.from(expectedSignature);
  if (provided.length !== expected.length || !crypto.timingSafeEqual(provided, expected)) {
    throw new JwtError('Invalid signature');
  }

  let payload;
  try {
    payload = JSON.parse(base64urlDecode(payloadPart).toString('utf8'));
  } catch {
    throw new JwtError('Malformed token payload');
  }

  if (payload.exp && Math.floor(Date.now() / 1000) >= payload.exp) {
    throw new JwtError('Token expired');
  }

  return payload;
}

module.exports = { sign, verify, JwtError };
