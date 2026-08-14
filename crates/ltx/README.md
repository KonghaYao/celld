# celld-ltx

celld's in-process SQLite-to-object-store replication engine. It reads each
cell's SQLite WAL, writes LTX files to a fleet bucket, restores a database from
that bucket, and — as it grows — exposes the replicated position so celld can
gate a write's response until it is durable. This is the foundation of celld's
durability goal: RPO=0 for every cell write, matching Durable Objects.

## Provenance and attribution

Seeded on 2026-08-03 from a read-only snapshot of rustyriver
(https://github.com/mikenomitch/rustyriver), a from-scratch Rust
reimplementation of Litestream v0.5 and the LTX file format. celld owns and
evolves this snapshot as first-class celld source; it does not track an
upstream branch.

Attribution for the vendored and ported work:

- **rustyriver** — Copyright 2026 The rustyriver authors, licensed under the
  Apache License, Version 2.0.
- **Litestream** (https://github.com/benbjohnson/litestream) — Copyright (c)
  Ben Johnson and the Litestream authors, licensed under the Apache License,
  Version 2.0. The original replication behavior comes from tag v0.5.11. The
  current format oracle uses tag v0.5.16 because it includes LTX v0.5.2.
- **LTX file format and reference implementation**
  (https://github.com/superfly/ltx), tag v0.5.2 — Copyright (c) Superfly, Inc.,
  licensed under the Apache License, Version 2.0.
- **pierrec/lz4 block compressor**
  (https://github.com/pierrec/lz4), tag v4.1.23 — Copyright (c) 2015 Pierre
  Curto, licensed under the BSD 3-Clause License. The Rust port preserves the
  exact compressed bytes that the LTX v0.5.2 writer produces.

The port is not complete, deliberately. `leaser.rs` — Litestream's
object-storage lease (`leaser.go`, `heartbeat.go`, `s3/leaser.go`) — was
ported and then removed on 2026-08-06, unused. celld fences cell ownership
with a conditional-write record carrying an epoch, and fences the data path by
stamping that epoch into the LTX prefix; a lease file under the replica prefix
would be a second, competing layer. Upstream's own leaser is unwired for the
same reason. Recover it from git if a future design needs it.

The full Apache License, Version 2.0 text is in [LICENSE](LICENSE). The full
BSD 3-Clause License text is in
[LICENSE.pierrec-lz4](LICENSE.pierrec-lz4).

## Tests

The suite ports the upstream conformance vectors. The codec and compactor
fixtures compare complete output bytes with `superfly/ltx` v0.5.2. The tests
also compact the older LZ4 frame fixtures from Litestream v0.5.11.

Celld and Litestream v0.5.16 can read the new block files and the older frame
files. Litestream v0.5.11 can read only the older frame files. This is a reader
compatibility boundary because LTX keeps file version 3 for both layouts.

The ordinary celld L0 writer still emits the older frame representation. The
v0.5.2 encoder and compactor emit exact block files. Celld contains a
node-wide scheduler that publishes additive L1 files, and the scheduler is on
by default. Every takeover target must have the dual decoder before the first
L1 publication, so a mixed fleet must set `CELLD_LTX_COMPACTION=0` until all
nodes can read block files. The same reader-first requirement applies to a
later L0 writer switch.

`differential_xtool` checks LTX reads and writes against a real Litestream
binary in three directions. It skips when the binary is absent, so the fast CI
does not require the Go toolchain. The release gate builds the pinned binary
and runs the real oracle.
