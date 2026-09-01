#!/usr/bin/env node
/**
 * Smoke-run crypto fixtures inside celld (Phase 0).
 * Full differential: workerd + celld harness (TODO).
 */
import { pathToFileURL } from "node:url";
import { readdir } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturesDir = join(__dirname, "fixtures");

const files = (await readdir(fixturesDir)).filter((f) => f.endsWith(".mjs")).sort();

console.log("crypto-conformance smoke: fixtures=%d (runner needs celld isolate — stub)", files.length);
for (const f of files) {
  const mod = await import(pathToFileURL(join(fixturesDir, f)).href);
  if (typeof mod.run !== "function") {
    console.error("FAIL", f, "missing export run()");
    process.exitCode = 1;
    continue;
  }
  console.log("  loaded", f);
}

console.log("\nNext: wire run-smoke to celld test worker (see docs/plans/CF-WEB-CRYPTO-100.md)");
