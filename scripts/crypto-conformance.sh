#!/usr/bin/env bash
# Web Crypto conformance gate (CF-WEB-CRYPTO-100.md).
# Phase 0: celld fixture smoke + optional workerd differential.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== crypto-conformance: fixture load (node) =="
if command -v node >/dev/null 2>&1; then
  node tests/crypto-conformance/run-smoke.mjs
else
  echo "SKIP: node not on PATH (cannot load fixtures)"
fi

echo
echo "== crypto-conformance: celld isolate harness =="
if [[ -n "${CELLD_CRYPTO_HARNESS:-}" && -x "${CELLD_CRYPTO_HARNESS}" ]]; then
  "${CELLD_CRYPTO_HARNESS}"
elif cargo test -p celld --lib -- --list 2>/dev/null | grep -q crypto; then
  echo "running cargo test -p celld --lib (crypto-related lib tests)"
  cargo test -p celld --lib
else
  echo "NO isolate harness in-tree yet (see tests/crypto-conformance/README.md)."
  echo "Fixtures export run(); wire a celld worker that imports each fixture."
fi

echo
echo "== crypto-conformance: workerd differential =="
if command -v workerd >/dev/null 2>&1; then
  echo "workerd found: $(command -v workerd)"
  echo "TODO: run the same fixtures on workerd and diff JSON with celld."
  echo "Prerequisite: a workerd config that exposes crypto.subtle to the fixtures."
else
  echo "SKIP: workerd not on PATH."
  echo "Install Cloudflare workerd to enable celld ↔ workerd output equality."
  echo "Until then this gate is celld-only (golden / smoke), not a Full matrix."
fi
