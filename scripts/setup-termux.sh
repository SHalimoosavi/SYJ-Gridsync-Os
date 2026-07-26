#!/data/data/com.termux/files/usr/bin/bash
# GridSync-OS -- Termux environment bootstrap.
# Safe to re-run: every step checks before acting.
set -euo pipefail

echo "==> Updating Termux package index..."
pkg update -y

echo "==> Installing Node.js and git (skipped if already present)..."
pkg install -y nodejs git

echo "==> Node version: $(node -v)"
echo "==> npm version: $(npm -v)"

if [ ! -f package.json ]; then
  echo "ERROR: run this script from inside the gridsync-os project directory." >&2
  exit 1
fi

echo "==> Installing project dependencies (pure JS, no native build step)..."
npm install

echo "==> Running self-test suite..."
npm run selftest

echo ""
echo "==> Done. Next steps:"
echo "    git init && git add -A && git commit -m 'initial commit'"
echo "    npm start"
