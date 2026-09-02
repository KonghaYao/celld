# Web Crypto conformance (celld ↔ workerd)

Run from `celld/`:

```bash
./scripts/crypto-conformance.sh

# Phase 0: load fixtures only (no isolate)
node tests/crypto-conformance/run-smoke.mjs
```

Fixtures export `export async function run()` returning JSON-serializable values.

**workerd prerequisite**: full differential (`celld` output == `workerd` output)
needs `workerd` on `PATH` plus a worker config that imports each fixture. The
script documents this and skips the workerd half when the binary is missing.

There is no in-process JS isolate harness in this tree yet; `cargo test -p celld --lib`
covers host ops (`rsa-oaep-*`, `rsa-pss-*`, Ed25519). Set `CELLD_CRYPTO_HARNESS`
when a runner exists.
