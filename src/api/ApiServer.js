'use strict';

const http = require('node:http');
const crypto = require('node:crypto');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { Router } = require('./Router');
const Jwt = require('../auth/Jwt');
const { ROLE_RANK, ROLES } = require('../auth/UserStore');
const { ValidationError } = require('../utils/errors');
const { assertNonEmptyString, assertOneOf } = require('../utils/validation');

const DASHBOARD_HTML_PATH = path.join(__dirname, 'dashboard.html');
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 500;
const API_TOKEN_MIN_SECONDS = 60 * 60 * 24 * 365 * 5; // "long-lived": 5 years, for named API tokens

function clampLimit(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_LIMIT;
  return Math.min(Math.floor(n), MAX_LIMIT);
}

/**
 * ApiServer: monitoring endpoints plus command issuance and account
 * management, all protected by JWT + role-based access control. Built on
 * Node's built-in `http` module and `crypto` -- zero additional
 * dependencies, consistent with the rest of the project.
 *
 * Security posture, deliberately conservative for a system that can issue
 * grid-control commands:
 *  - Binds to 127.0.0.1 by default (see config.js).
 *  - Every /api/* route (except login) requires authentication. Three
 *    roles -- VIEWER (read-only), OPERATOR (+ command issuance), ADMIN
 *    (+ user/token management) -- each route declares its minimum role.
 *  - The legacy `GS_API_TOKEN` bearer token, if configured, is always
 *    treated as ADMIN-equivalent for backward compatibility with
 *    automation that predates the account system.
 *  - Request bodies are size-capped to prevent a single client from
 *    exhausting memory.
 *  - Every route handler is wrapped so a thrown/rejected error can never
 *    crash the process or leave a request hanging without a response.
 */
class ApiServer {
  constructor({ orchestrator, userStore, config, logger }) {
    this.orchestrator = orchestrator;
    this.userStore = userStore;
    this.config = config.api;
    this.authConfig = config.auth;
    this.logger = logger.child('api');
    this.server = null;
    this.router = new Router();
    this._dashboardHtml = null; // lazy-loaded and cached on first request
    this._revokedJti = new Set(); // logged-out / revoked token ids, cleared on process restart

    if (this.authConfig.jwtSecret) {
      this._jwtSecret = this.authConfig.jwtSecret;
    } else {
      this._jwtSecret = crypto.randomBytes(32).toString('hex');
      this.logger.warn('GS_JWT_SECRET not set -- generated a random secret for this process. Sessions will not survive a restart.');
    }

    this._registerRoutes();
  }

  _registerRoutes() {
    const r = this.router;

    // -- Public routes: no authentication required --
    r.get('/health', (req, res) => {
      this._json(res, 200, { status: 'ok', uptimeSeconds: Math.round(process.uptime()), timestamp: Date.now() });
    });
    r.get('/', (req, res) => this._serveDashboard(res));
    r.get('/dashboard', (req, res) => this._serveDashboard(res));

    r.post('/api/auth/login', async (req, res) => {
      let body;
      try {
        body = await this._readJsonBody(req);
      } catch (err) {
        this._json(res, err.statusCode || 400, { error: err.message });
        return;
      }
      try {
        assertNonEmptyString(body.username, 'username');
        assertNonEmptyString(body.password, 'password');
      } catch (err) {
        this._json(res, 400, { error: err.message });
        return;
      }
      const user = await this.userStore.verifyCredentials(body.username, body.password);
      if (!user) {
        this._json(res, 401, { error: 'Invalid username or password' });
        return;
      }
      const jti = crypto.randomUUID();
      const token = Jwt.sign(
        { sub: user.id, username: user.username, role: user.role, jti, type: 'session' },
        this._jwtSecret,
        { expiresInSeconds: this.authConfig.jwtExpiresInSeconds },
      );
      this._json(res, 200, {
        token,
        expiresInSeconds: this.authConfig.jwtExpiresInSeconds,
        user: { username: user.username, role: user.role },
      });
    });

    // -- Everything below requires authentication --

    r.post('/api/auth/logout', this._guard('VIEWER', (req, res) => {
      if (req.authUser.jti) this._revokedJti.add(req.authUser.jti);
      this._json(res, 200, { success: true });
    }));

    r.get('/api/auth/me', this._guard('VIEWER', (req, res) => {
      this._json(res, 200, { username: req.authUser.username, role: req.authUser.role });
    }));

    r.get('/api/snapshot', this._guard('VIEWER', (req, res) => {
      this._json(res, 200, this.orchestrator.getSnapshot());
    }));

    r.get('/api/devices', this._guard('VIEWER', (req, res) => {
      this._json(res, 200, { devices: this.orchestrator.listDevices() });
    }));

    r.get('/api/devices/:deviceId', this._guard('VIEWER', (req, res, params) => {
      const detail = this.orchestrator.getDeviceDetail(params.deviceId);
      if (!detail) {
        this._json(res, 404, { error: `Unknown device: ${params.deviceId}` });
        return;
      }
      this._json(res, 200, detail);
    }));

    r.get('/api/devices/:deviceId/telemetry', this._guard('VIEWER', async (req, res, params, query) => {
      const limit = clampLimit(query.get('limit'));
      try {
        const points = await this.orchestrator.storage.queryTelemetry(params.deviceId, limit);
        this._json(res, 200, { deviceId: params.deviceId, count: points.length, points });
      } catch (err) {
        this.logger.error('telemetry query failed', { deviceId: params.deviceId, err });
        this._json(res, 500, { error: 'Failed to query telemetry' });
      }
    }));

    r.get('/api/commands/pending', this._guard('VIEWER', (req, res) => {
      this._json(res, 200, { commands: this.orchestrator.commandQueue.listPending() });
    }));

    r.get('/api/commands/history', this._guard('VIEWER', async (req, res, params, query) => {
      const limit = clampLimit(query.get('limit'));
      const filters = {};
      if (query.get('deviceId')) filters.deviceId = query.get('deviceId');
      if (query.get('status')) filters.status = query.get('status');
      try {
        const commands = await this.orchestrator.storage.queryCommandHistory(limit, filters);
        this._json(res, 200, { count: commands.length, commands });
      } catch (err) {
        this.logger.error('command history query failed', { err });
        this._json(res, 500, { error: 'Failed to query command history' });
      }
    }));

    r.get('/api/alarms/active', this._guard('VIEWER', (req, res) => {
      this._json(res, 200, { alarms: this.orchestrator.listActiveAlarms() });
    }));

    r.get('/api/alarms/history', this._guard('VIEWER', async (req, res, params, query) => {
      const limit = clampLimit(query.get('limit'));
      const filters = {};
      if (query.get('deviceId')) filters.deviceId = query.get('deviceId');
      if (query.get('status')) filters.status = query.get('status');
      try {
        const alarms = await this.orchestrator.storage.queryAlarmHistory(limit, filters);
        this._json(res, 200, { count: alarms.length, alarms });
      } catch (err) {
        this.logger.error('alarm history query failed', { err });
        this._json(res, 500, { error: 'Failed to query alarm history' });
      }
    }));

    r.post('/api/alarms/:alarmId/acknowledge', this._guard('OPERATOR', async (req, res, params) => {
      try {
        const event = await this.orchestrator.acknowledgeAlarm(params.alarmId, req.authUser.username);
        if (!event) {
          this._json(res, 404, { error: `No active alarm with id ${params.alarmId}` });
          return;
        }
        this._json(res, 200, { alarm: event });
      } catch (err) {
        this.logger.error('alarm acknowledge failed', { alarmId: params.alarmId, err });
        this._json(res, 500, { error: 'Failed to acknowledge alarm' });
      }
    }));

    r.post('/api/commands', this._guard('OPERATOR', async (req, res) => {
      let body;
      try {
        body = await this._readJsonBody(req);
      } catch (err) {
        this._json(res, err.statusCode || 400, { error: err.message });
        return;
      }
      try {
        assertNonEmptyString(body.type, 'type');
        assertNonEmptyString(body.deviceId, 'deviceId');
        const record = await this.orchestrator.issueManualCommand({
          type: body.type,
          deviceId: body.deviceId,
          value: body.value,
          reason: body.reason,
          issuedBy: req.authUser.username,
        });
        this._json(res, 202, { commandId: record.commandId, status: record.status });
      } catch (err) {
        if (err instanceof ValidationError) {
          this._json(res, 400, { error: err.message });
        } else {
          this.logger.error('manual command issuance failed', { err });
          this._json(res, 500, { error: 'Failed to enqueue command' });
        }
      }
    }));

    // -- Admin-only: user & API token management --

    r.get('/api/auth/users', this._guard('ADMIN', async (req, res) => {
      this._json(res, 200, { users: await this.userStore.listUsers() });
    }));

    r.post('/api/auth/users', this._guard('ADMIN', async (req, res) => {
      let body;
      try {
        body = await this._readJsonBody(req);
      } catch (err) {
        this._json(res, err.statusCode || 400, { error: err.message });
        return;
      }
      try {
        assertNonEmptyString(body.username, 'username');
        assertNonEmptyString(body.password, 'password');
        assertOneOf(body.role, ROLES, 'role');
        const user = await this.userStore.createUser({ username: body.username, password: body.password, role: body.role });
        this._json(res, 201, { user });
      } catch (err) {
        if (err instanceof ValidationError) {
          this._json(res, 400, { error: err.message });
        } else {
          this.logger.error('user creation failed', { err });
          this._json(res, 500, { error: 'Failed to create user' });
        }
      }
    }));

    r.post('/api/auth/users/:userId/disable', this._guard('ADMIN', async (req, res, params) => {
      try {
        const user = await this.userStore.setDisabled(params.userId, true);
        this._json(res, 200, { user });
      } catch (err) {
        if (err instanceof ValidationError) {
          this._json(res, 404, { error: err.message });
        } else {
          this.logger.error('user disable failed', { err });
          this._json(res, 500, { error: 'Failed to disable user' });
        }
      }
    }));

    r.post('/api/auth/tokens', this._guard('ADMIN', async (req, res) => {
      let body;
      try {
        body = await this._readJsonBody(req);
      } catch (err) {
        this._json(res, err.statusCode || 400, { error: err.message });
        return;
      }
      try {
        assertNonEmptyString(body.name, 'name');
        assertOneOf(body.role, ROLES, 'role');
      } catch (err) {
        this._json(res, 400, { error: err.message });
        return;
      }
      const jti = crypto.randomUUID();
      const token = Jwt.sign(
        { sub: `api-token:${body.name}`, username: `api-token:${body.name}`, role: body.role, jti, type: 'api_token' },
        this._jwtSecret,
        { expiresInSeconds: API_TOKEN_MIN_SECONDS },
      );
      this._json(res, 201, { token, jti, name: body.name, role: body.role });
    }));

    r.post('/api/auth/tokens/:jti/revoke', this._guard('ADMIN', (req, res, params) => {
      this._revokedJti.add(params.jti);
      this._json(res, 200, { revoked: params.jti });
    }));
  }

  /**
   * Wraps a route handler so it only runs if the request carries a valid,
   * non-revoked, sufficiently-privileged credential. On success, attaches
   * `req.authUser = {username, role, jti?}` before calling through.
   */
  _guard(minRole, handler) {
    return async (req, res, params, query) => {
      const authUser = this._authenticate(req);
      if (!authUser) {
        this._json(res, 401, { error: 'Unauthorized' });
        return;
      }
      if (ROLE_RANK[authUser.role] < ROLE_RANK[minRole]) {
        this._json(res, 403, { error: `Forbidden: requires role ${minRole} or higher` });
        return;
      }
      req.authUser = authUser;
      return handler(req, res, params, query);
    };
  }

  /** @returns {{username: string, role: string, jti?: string}|null} */
  _authenticate(req) {
    const header = req.headers.authorization || '';
    if (!header.startsWith('Bearer ')) return null;
    const token = header.slice('Bearer '.length);

    // Legacy shared token, always ADMIN-equivalent -- preserved for
    // backward compatibility with automation set up before the account system.
    if (this.config.token && token === this.config.token) {
      return { username: 'legacy-token', role: 'ADMIN' };
    }

    try {
      const payload = Jwt.verify(token, this._jwtSecret);
      if (payload.jti && this._revokedJti.has(payload.jti)) return null;
      if (!payload.role || !ROLE_RANK[payload.role]) return null;
      return { username: payload.username, role: payload.role, jti: payload.jti };
    } catch {
      return null;
    }
  }

  /** Reads and JSON-parses a request body, enforcing the configured size cap. */
  _readJsonBody(req) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let totalBytes = 0;
      const chunks = [];
      const maxBytes = this.config.maxBodyBytes;

      req.on('data', (chunk) => {
        if (settled) return; // already settled (e.g. oversized) -- discard further chunks, don't re-process
        totalBytes += chunk.length;
        if (totalBytes > maxBytes) {
          settled = true;
          // Deliberately do NOT destroy the socket here: doing so would
          // prevent the 413 response below from ever reaching the client
          // (this was a real bug found during testing -- the client's
          // fetch() call would hang waiting for a response that could
          // never be sent). The stream stays open and continues draining
          // in the background; we simply stop buffering it (bounded memory
          // is what actually matters here).
          const err = new Error('Request body too large');
          err.statusCode = 413;
          reject(err);
          return;
        }
        chunks.push(chunk);
      });

      req.on('end', () => {
        if (settled) return;
        settled = true;
        const raw = Buffer.concat(chunks).toString('utf8');
        if (!raw.trim()) {
          resolve({});
          return;
        }
        try {
          resolve(JSON.parse(raw));
        } catch {
          const err = new Error('Malformed JSON body');
          err.statusCode = 400;
          reject(err);
        }
      });

      req.on('error', (err) => {
        if (settled) return;
        settled = true;
        reject(err);
      });
    });
  }

  async _serveDashboard(res) {
    try {
      if (!this._dashboardHtml) {
        this._dashboardHtml = await fsp.readFile(DASHBOARD_HTML_PATH, 'utf8');
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(this._dashboardHtml);
    } catch (err) {
      this.logger.error('failed to serve dashboard', { err });
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Dashboard unavailable');
    }
  }

  _json(res, status, obj) {
    if (res.headersSent) return; // defensive: never attempt a second response
    const body = JSON.stringify(obj);
    res.writeHead(status, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    });
    res.end(body);
  }

  _handleRequest(req, res) {
    let pathname;
    try {
      pathname = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    } catch {
      this._json(res, 400, { error: 'Invalid request URL' });
      return;
    }

    const match = this.router.match(req.method, pathname.pathname);
    if (!match) {
      this._json(res, 404, { error: 'Not Found' });
      return;
    }

    // Every handler is awaited via this Promise chain so a thrown error or
    // rejected promise inside a route handler can never crash the process
    // or leave the client hanging without a response.
    Promise.resolve(match.handler(req, res, match.params, pathname.searchParams)).catch((err) => {
      this.logger.error('unhandled error in route handler', { path: pathname.pathname, err });
      this._json(res, 500, { error: 'Internal Server Error' });
    });
  }

  async start() {
    if (this.server) return;
    return new Promise((resolve, reject) => {
      const server = http.createServer((req, res) => this._handleRequest(req, res));
      let settled = false;
      server.on('error', (err) => {
        this.logger.error('API server error', { err });
        if (!settled) {
          settled = true;
          reject(err);
        }
      });
      server.listen(this.config.port, this.config.host, () => {
        this.server = server;
        const actualPort = server.address().port;
        this.logger.info('API server listening', { host: this.config.host, port: actualPort });
        if (!settled) {
          settled = true;
          resolve();
        }
      });
    });
  }

  async stop() {
    if (!this.server) return;
    const server = this.server;
    this.server = null;
    await new Promise((resolve) => {
      server.close(() => resolve());
      // server.close() alone only stops accepting new connections and waits
      // for existing ones to end naturally -- an idle keep-alive connection
      // (default 5s timeout) would otherwise stall shutdown for up to that
      // long. Force-closing all connections makes close() resolve promptly.
      server.closeAllConnections();
    });
  }
}

module.exports = { ApiServer };
