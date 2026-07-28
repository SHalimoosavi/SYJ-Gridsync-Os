#!/usr/bin/env node
'use strict';

/**
 * Direct-to-disk user creation, bypassing the HTTP API entirely. Useful for:
 *  - Initial setup, as an alternative to GS_BOOTSTRAP_ADMIN_USERNAME/PASSWORD
 *  - Recovery if you're locked out (no admin account reachable via the API)
 *
 * Usage:
 *   node scripts/create-user.js --username admin --password "at-least-8-chars" --role ADMIN
 *
 * Roles: ADMIN, OPERATOR, VIEWER
 */

const path = require('node:path');
const { UserStore, ROLES } = require('../src/auth/UserStore');
const config = require('../src/config');

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i].startsWith('--')) {
      const key = argv[i].slice(2);
      const value = argv[i + 1];
      out[key] = value;
      i += 1;
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const { username, password, role } = args;

  if (!username || !password || !role) {
    console.error('Usage: node scripts/create-user.js --username <name> --password <pass> --role <ADMIN|OPERATOR|VIEWER>');
    process.exitCode = 1;
    return;
  }
  if (!ROLES.includes(role)) {
    console.error(`Invalid role "${role}". Must be one of: ${ROLES.join(', ')}`);
    process.exitCode = 1;
    return;
  }

  const dataDir = config.storage.dataDir;
  const userStore = new UserStore({ dataDir, logger: null });
  await userStore.init();

  try {
    const user = await userStore.createUser({ username, password, role });
    console.log(`Created user "${user.username}" with role ${user.role} (id: ${user.id})`);
    console.log(`Data directory: ${path.resolve(dataDir)}`);
  } catch (err) {
    console.error(`Failed to create user: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
