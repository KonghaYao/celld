# Web Crypto conformance (celld ↔ workerd)

Run from `celld/` (requires `workerd` on PATH for full gate):

```bash
# Phase 0: celld-only smoke (no workerd yet)
node tests/crypto-conformance/run-smoke.mjs

# Future: differential
# ./scripts/crypto-conformance.sh
```

Fixtures export `export async function run()` returning JSON-serializable values.
