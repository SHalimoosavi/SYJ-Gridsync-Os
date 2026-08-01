'use strict';

/**
 * Minimal path router: no dependencies, supports `:param` segments.
 * Kept separate from ApiServer so its matching logic is unit-testable
 * without spinning up a real HTTP server.
 */
class Router {
  constructor() {
    /** @type {{method: string, regex: RegExp, paramNames: string[], handler: Function}[]} */
    this._routes = [];
  }

  add(method, pattern, handler) {
    const paramNames = [];
    const regexStr = pattern
      .split('/')
      .map((segment) => {
        if (segment.startsWith(':')) {
          paramNames.push(segment.slice(1));
          return '([^/]+)';
        }
        // Escape regex special characters in static path segments.
        return segment.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      })
      .join('/');
    const regex = new RegExp(`^${regexStr}$`);
    this._routes.push({ method: method.toUpperCase(), regex, paramNames, handler });
  }

  get(pattern, handler) {
    this.add('GET', pattern, handler);
  }

  post(pattern, handler) {
    this.add('POST', pattern, handler);
  }

  patch(pattern, handler) {
    this.add('PATCH', pattern, handler);
  }

  delete(pattern, handler) {
    this.add('DELETE', pattern, handler);
  }

  /**
   * @returns {{handler: Function, params: Object<string,string>}|null}
   */
  match(method, pathname) {
    for (const route of this._routes) {
      if (route.method !== method.toUpperCase()) continue;
      const m = route.regex.exec(pathname);
      if (!m) continue;
      const params = {};
      route.paramNames.forEach((name, i) => {
        params[name] = decodeURIComponent(m[i + 1]);
      });
      return { handler: route.handler, params };
    }
    return null;
  }
}

module.exports = { Router };
