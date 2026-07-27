'use strict';

const http = require('node:http');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { Router } = require('./Router');
const { ValidationError } = require('../utils/errors');
const { assertNonEmptyString } = require('../utils/validation');

const DASHBOARD_HTML_PATH = path.join(__dirname, 'dashboard.html');
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 500;

function clampLimit(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_LIMIT;
  return Math.min(Math.floor(n), MAX_LIMIT);
}

/**
 * ApiServer: read-only monitoring endpoints plus one mutating endpoint
 * (manual command issuance). Built on Node's built-in `http` module --
 * zero additional dependencies, consistent with the rest of the project.
 *
 * Security posture, deliberately conservative for a system that can issue
 * grid-control commands:
 *  - Binds to 127.0.0.1 by default (see config.js).
 *  - POST /api/commands fails CLOSED (503) if no token is configured,
 *    rather than silently accepting unauthenticated commands.
 *  - Request bodies are size-capped to prevent a single client from
 *    exhausting memory.
 *  - Every route handler is wrapped so a thrown/rejected error can never
 *    crash the process or leave a request hanging without a response.
 */
class ApiServer {
  constructor({ orchestrator, config, logger }) {
    this.orchestrator = orchestrator;
    this.config = config.api;
    this.logger = logger.child('api');
    this.server = null;
    this.router = new Router();
    this._dashboardHtml = null; // lazy-loaded and cached on first request
    this._registerRoutes();
  }

  _registerRoutes() {
    const r = this.router;

    r.get('/health', (req, res) => {
      this._json(res, 200, { status: 'ok', uptimeSeconds: Math.round(process.uptime()), timestamp: Date.now() });
    });

    r.get('/', (req, res) => this._serveDashboard(res));
    r.get('/dashboard', (req, res) => this._serveDashboard(res));

    r.get('/api/snapshot', (req, res) => {
      this._json(res, 200, this.orchestrator.getSnapshot());
    });

    r.get('/api/devices', (req, res) => {
      this._json(res, 200, { devices: this.orchestrator.listDevices() });
    });

    r.get('/api/devices/:deviceId', (req, res, params) => {
      const detail = this.orchestrator.getDeviceDetail(params.deviceId);
      if (!detail) {
        this._json(res, 404, { error: `Unknown device: ${params.deviceId}` });
        return;
      }
      this._json(res, 200, detail);
    });

    r.get('/api/devices/:deviceId/telemetry', async (req, res, params, query) => {
      const limit = clampLimit(query.get('limit'));
      try {
        const points = await this.orchestrator.storage.queryTelemetry(params.deviceId, limit);
        this._json(res, 200, { deviceId: params.deviceId, count: points.length, points });
      } catch (err) {
        this.logger.error('telemetry query failed', { deviceId: params.deviceId, err });
        this._json(res, 500, { error: 'Failed to query telemetry' });
      }
    });

    r.get('/api/commands/pending', (req, res) => {
      this._json(res, 200, { commands: this.orchestrator.commandQueue.listPending() });
    });

    r.get('/api/commands/history', async (req, res, params, query) => {
      const limit = clampLimit(query.get('limit'));
      try {
        const commands = await this.orchestrator.storage.queryCommandHistory(limit);
        this._json(res, 200, { count: commands.length, commands });
      } catch (err) {
        this.logger.error('command history query failed', { err });
        this._json(res, 500, { error: 'Failed to query command history' });
      }
    });

    r.post('/api/commands', async (req, res) => {
      if (!this._checkAuth(req, res)) return; // response already written on failure

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
    });
  }

  /** @returns {boolean} true if authorized; writes the failure response itself and returns false otherwise. */
  _checkAuth(req, res) {
    const token = this.config.token;
    if (!token) {
      this._json(res, 503, {
        error: 'API token not configured; set GS_API_TOKEN to enable command issuance via the API',
      });
      return false;
    }
    const header = req.headers.authorization || '';
    if (header !== `Bearer ${token}`) {
      this._json(res, 401, { error: 'Unauthorized' });
      return false;
    }
    return true;
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
