// This external-oracle test inspects files produced by two independent tools.
#![allow(clippy::disallowed_methods)]

//! differential_xtool — cross-tool differential tests against the real
//! `litestream` binary, pinned by
//! [`PINNED_LITESTREAM_VERSION`].
//!
//! This is the strongest correctness oracle in the project. The expected value
//! comes from the real Litestream binary, not from the implementation under test.
//!
//! * **D1 (write path):** rustyriver replicates a DB into a file replica, then the
//!   **real `litestream restore`** reproduces it → **Oracle A** vs the source.
//!   Proves our *written* LTX format is real-Litestream-readable. If this passes,
//!   our serializer is wire-compatible with upstream.
//! * **D2 (restore path):** the **real `litestream replicate`** writes a replica,
//!   then **rustyriver restores** it → **Oracle A** vs the source. Proves our
//!   *reader* handles real-Litestream output.
//! * **D3 (format cross-check):** both tools restore the **same** replica → the two
//!   output DB files are **byte-identical** (**Oracle B**, after a TRUNCATE
//!   checkpoint). Isolates format fidelity from SQLite-version noise because both
//!   replay identical page images. Run in both directions (over a
//!   rustyriver-written replica AND a real-Litestream-written replica).
//! * **D4 (compaction write path):** celld compacts L0 into an L1 object, then the
//!   **real `litestream restore`** reads that object → **Oracle A** vs the source.
//!   D1 only covers the L0 writer, which still emits the pre-v0.5.2 frame
//!   representation; the compactor emits the v0.5.2 block representation, and D4
//!   is the only case where a real reader sees one.
//!
//! ## Skip policy
//! Every test self-skips (logs + `return`, never a silent pass and never a
//! failure) when the `litestream` binary or `sqlite3` is absent from PATH. The
//! skip is a runtime guard, not a compile-time ignore attribute, and it does not
//! weaken an assertion.
//!
//! A skip is also how this gate goes vacuous, so `CELLD_LTX_LITESTREAM_REQUIRED=1`
//! turns every skip into a failure. CI installs the pinned binary and sets that
//! variable, so a broken install reds the run instead of passing an empty gate.
//!
//! ## Why a file replica (not S3)
//! D1–D3 exercise the *format* and the *restore algorithm*, which are transport
//! agnostic. The file `ReplicaClient` is byte-for-byte the same object layout the
//! S3 client uses (`<root>/ltx/<level>/<min>-<max>.ltx`), and the real binary's
//! `file://` backend reads/writes that identical tree — so a file replica is the
//! cleanest, hermetic way to put both tools on the *same* bytes. The S3
//! transport is separately proven end-to-end by T7 (`integration_s3.rs`).

use celld_ltx::client::file::FileReplicaClient;
use celld_ltx::db::Db;
use celld_ltx::ltx::{HEADER_SIZE, PAGE_HEADER_FLAG_SIZE};
use celld_ltx::replica::{self, Replica};
use celld_ltx::replica_compactor::ReplicaCompactor;
use celld_ltx::{ltx_file_path, TXID};
use rusqlite::Connection;
use std::path::Path;
use std::process::Command;

/// Absolute path to the `db_equal.sh` oracle.
fn db_equal_script() -> String {
    format!("{}/scripts/db_equal.sh", env!("CARGO_MANIFEST_DIR"))
}

/// Runs `db_equal.sh <mode> <a> <b>`; `Ok(())` on exit 0, else the captured
/// stdout+stderr so a failure pinpoints the mismatch. Requires `sqlite3`.
///
/// `mode` is `"A"` (logical equality) or `"B"` (byte-identical main file).
fn db_equal(mode: &str, a: &Path, b: &Path) -> Result<(), String> {
    let out = Command::new("bash")
        .arg(db_equal_script())
        .arg(mode)
        .arg(a)
        .arg(b)
        .output()
        .map_err(|e| format!("spawn db_equal.sh: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "db_equal {mode} failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ))
    }
}

/// The pinned oracle version, and the single place this repository records it.
/// The CI workflow derives the release it downloads from this constant, so the
/// pin cannot drift between the test and the job that feeds it.
///
/// Litestream v0.5.16 uses superfly/ltx v0.5.2. An older v0.5 binary cannot read
/// the size-prefixed block representation that the compactor publishes, so the
/// test must not accept any v0.5 binary as an equivalent oracle.
pub const PINNED_LITESTREAM_VERSION: &str = "0.5.16";

/// The real binary under test: `$LITESTREAM_BIN`, or `litestream` on PATH.
fn litestream_bin() -> String {
    std::env::var("LITESTREAM_BIN").unwrap_or_else(|_| "litestream".into())
}

/// `CELLD_LTX_LITESTREAM_REQUIRED=1` forbids every skip in this file. A job that
/// installs the oracle and then skips the whole gate is green for the wrong
/// reason, so CI sets this and a failed install reds the run. This mirrors
/// `CELLD_LTX_S3_REQUIRED` in `integration_s3.rs`.
fn required() -> bool {
    std::env::var("CELLD_LTX_LITESTREAM_REQUIRED").as_deref() == Ok("1")
}

/// Records a missing prerequisite: a panic in required mode, a logged skip
/// otherwise.
fn unavailable(test: &str, reason: &str) {
    assert!(
        !required(),
        "{test}: {reason} (CELLD_LTX_LITESTREAM_REQUIRED=1 forbids the skip)"
    );
    eprintln!("skipping {test}: {reason}");
}

/// True when the resolved binary exists AND speaks the pinned replica era.
fn litestream_usable(test: &str) -> bool {
    let out = Command::new(litestream_bin()).arg("version").output();
    match out {
        Err(_) => {
            unavailable(
                test,
                &format!(
                    "`{}` is not runnable; set LITESTREAM_BIN or put litestream on PATH",
                    litestream_bin()
                ),
            );
            false
        }
        Ok(o) => {
            let v = String::from_utf8_lossy(&o.stdout).trim().to_string();
            // The release prints the bare version; a source build can prefix a
            // `v`. Both name the same pin, so both are accepted.
            let pinned = PINNED_LITESTREAM_VERSION;
            if v == pinned || v.strip_prefix('v') == Some(pinned) {
                true
            } else {
                unavailable(
                    test,
                    &format!(
                        "litestream {v:?} is not the pinned v{pinned} oracle; \
                         point LITESTREAM_BIN at v{pinned}"
                    ),
                );
                false
            }
        }
    }
}

/// True if `bin` resolves on PATH (exits with any status code when run with a
/// help-ish flag). Mirrors the probe used by the other integration suites.
fn has_bin(bin: &str) -> bool {
    Command::new(bin)
        .arg("--help")
        .output()
        .map(|o| o.status.success() || o.status.code().is_some())
        .unwrap_or(false)
}

/// Returns `true` and logs a skip note if either required tool is missing or
/// the wrong era. The differential gate is meaningless without BOTH the real
/// binary and `sqlite3`.
fn skip_if_tools_missing(test: &str) -> bool {
    if !litestream_usable(test) {
        return true;
    }
    if !has_bin("sqlite3") {
        unavailable(
            test,
            "`sqlite3` is not on PATH (the db_equal oracle needs it)",
        );
        return true;
    }
    false
}

/// A `file://<abs-path>` URL for the real binary's `file:` backend. The path must
/// be absolute (litestream rejects a relative `file://` host segment).
fn file_url(root: &Path) -> String {
    let abs = root
        .canonicalize()
        .unwrap_or_else(|_| root.to_path_buf())
        .to_string_lossy()
        .into_owned();
    format!("file://{abs}")
}

/// Opens an application writer connection in WAL mode — the host's own SQLite
/// handle writing alongside the managed `Db`, exactly as a library embedder does.
fn open_writer(path: &Path) -> Connection {
    let c = Connection::open(path).unwrap();
    c.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
    let mode: String = c
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal", "test DB must be in WAL mode");
    c
}

/// The deterministic multi-transaction workload shared by the directions, so each
/// case exercises a snapshot + several incremental L0 files (mixed
/// insert/update/delete/DDL + a multi-page transaction). `apply` receives each
/// SQL statement-group in order; the caller decides what to do after each
/// (rustyriver capture+upload, or a real `litestream replicate -once`).
fn workload() -> &'static [&'static str] {
    &[
        "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)",
        "INSERT INTO kv (k, v) VALUES ('a','1'),('b','2'),('c','3')",
        "UPDATE kv SET v='updated' WHERE k='a'",
        "INSERT INTO kv (k, v) VALUES ('d','4'),('e','5')",
        "DELETE FROM kv WHERE k='b'",
        // A larger multi-page transaction to exercise multi-frame WAL capture.
        "CREATE TABLE big (id INTEGER PRIMARY KEY, blob TEXT);\
         INSERT INTO big (id, blob) SELECT value, hex(randomblob(200)) \
           FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 \
                 FROM c WHERE value<500) SELECT value FROM c);",
    ]
}

/// Drives rustyriver's write path over `workload()`: opens a managed `Db`, an app
/// writer, applies each statement-group, then captures (`db.sync`) and uploads
/// (`replica.sync`) it. Leaves a replica tree at `replica_root` and the source DB
/// at `src_path`. Returns the final captured TXID.
async fn rustyriver_replicate(src_path: &Path, replica_root: &Path) -> TXID {
    let db = Db::open(src_path).unwrap();
    let writer = open_writer(src_path);
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    let mut replica = Replica::new(db, client);

    for sql in workload() {
        writer.execute_batch(sql).unwrap();
        replica.db_mut().unwrap().sync().unwrap();
        replica.sync().await.unwrap();
    }

    let pos = replica.db_mut().unwrap().pos().unwrap();
    assert!(
        pos.txid.0 >= workload().len() as u64,
        "expected at least {} captured TXIDs, got {}",
        workload().len(),
        pos.txid
    );

    // Clean shutdown so the source WAL is released before the real binary or the
    // oracle reads the source file.
    let db = replica.into_db().unwrap();
    db.close().unwrap();
    drop(writer);
    pos.txid
}

/// Drives the REAL binary's write path over `workload()`: seeds WAL mode, then for
/// each statement-group writes via plain `sqlite3` and runs `litestream replicate
/// -once` to flush exactly one sync (no background timing races — the same
/// deterministic method `capture-golden.sh` uses). Leaves a replica tree at
/// `replica_root` and the source DB at `src_path`.
fn litestream_replicate(src_path: &Path, replica_root: &Path) {
    // Seed WAL mode + the first statement-group on a plain connection, then take
    // the initial snapshot.
    let url = file_url(replica_root);
    {
        let c = open_writer(src_path);
        c.execute_batch(workload()[0]).unwrap();
    }
    run_litestream(&["replicate", "-once", &src_path.to_string_lossy(), &url]);

    for sql in &workload()[1..] {
        {
            let c = open_writer(src_path);
            c.execute_batch(sql).unwrap();
        }
        run_litestream(&["replicate", "-once", &src_path.to_string_lossy(), &url]);
    }
}

/// Runs `litestream <args...>`, asserting success and surfacing stderr on failure.
fn run_litestream(args: &[&str]) {
    let out = Command::new(litestream_bin())
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn litestream {args:?}: {e}"));
    assert!(
        out.status.success(),
        "litestream {args:?} failed (status {:?}):\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Runs the real `litestream restore -o <out> <file-url>` for a replica tree.
fn litestream_restore(replica_root: &Path, out: &Path) {
    let url = file_url(replica_root);
    run_litestream(&["restore", "-o", &out.to_string_lossy(), &url]);
    assert!(out.exists(), "litestream restore did not produce {out:?}");
}

// ───────────────────────────── D1 (write path) ─────────────────────────────

/// **D1 — our write → real `litestream restore` → Oracle A vs source.**
///
/// rustyriver replicates the workload into a file replica; the *real* binary
/// restores that tree; the restored DB must be logically identical to the source.
/// This is the load-bearing proof that rustyriver's LTX serializer is byte-format
/// compatible with upstream Litestream — if the real tool can read what we wrote,
/// our format is correct because the independent binary is the oracle.
#[tokio::test(flavor = "multi_thread")]
async fn d1_our_write_real_restore_oracle_a() {
    if skip_if_tools_missing("d1_our_write_real_restore_oracle_a") {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let restored = dir.path().join("restored-by-litestream.db");

    // 1. rustyriver writes the replica.
    rustyriver_replicate(&src, &replica_root).await;

    // 2. The REAL binary restores our tree.
    litestream_restore(&replica_root, &restored);

    // 3. Oracle A: real-restored == source.
    db_equal("A", &src, &restored)
        .expect("D1: real `litestream restore` of our replica must equal the source (Oracle A)");
}

// ──────────────────────────── D2 (restore path) ────────────────────────────

/// **D2 — real `litestream replicate` → our restore → Oracle A vs source.**
///
/// The real binary replicates the workload; rustyriver restores that tree; the
/// restored DB must be logically identical to the source. Proves our LTX *reader*
/// + restore algorithm correctly consume real-Litestream-produced bytes.
#[tokio::test(flavor = "multi_thread")]
async fn d2_real_write_our_restore_oracle_a() {
    if skip_if_tools_missing("d2_real_write_our_restore_oracle_a") {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let restored = dir.path().join("restored-by-rustyriver.db");

    // 1. The REAL binary writes the replica.
    litestream_replicate(&src, &replica_root);

    // 2. rustyriver restores it (client-only restore; no Db needed).
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    replica::restore(&client, &restored, TXID(0))
        .await
        .expect("D2: rustyriver restore of a real-Litestream replica");

    // 3. Oracle A: our-restored == source.
    db_equal("A", &src, &restored)
        .expect("D2: our restore of a real `litestream` replica must equal the source (Oracle A)");
}

// ─────────────────────────── D3 (format cross-check) ───────────────────────

/// **D3 (over a rustyriver-written replica) — both tools restore the SAME tree →
/// byte-identical (Oracle B).**
///
/// rustyriver writes the replica, then BOTH rustyriver and the real binary restore
/// it; after a TRUNCATE checkpoint the two output main-DB files must be
/// byte-for-byte identical. Because both restorers replay the *same* page images,
/// any byte difference is pure format/restore-algorithm divergence — the most
/// sensitive format-fidelity check (Risk R-1/R-2). This direction additionally
/// proves our *writer* emits page images the real decoder reconstructs bit-exactly.
#[tokio::test(flavor = "multi_thread")]
async fn d3_byte_identical_over_our_replica() {
    if skip_if_tools_missing("d3_byte_identical_over_our_replica") {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let ours = dir.path().join("ours.db");
    let theirs = dir.path().join("theirs.db");

    // rustyriver writes the replica.
    rustyriver_replicate(&src, &replica_root).await;

    // Our restore.
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    replica::restore(&client, &ours, TXID(0))
        .await
        .expect("D3: our restore of our replica");

    // The real binary's restore of the SAME tree.
    litestream_restore(&replica_root, &theirs);

    // Oracle B: byte-identical main DB files after a TRUNCATE checkpoint.
    db_equal("B", &theirs, &ours).expect(
        "D3: our restore and `litestream restore` of our replica must be byte-identical (Oracle B)",
    );
}

/// **D3 (over a real-Litestream-written replica) — both tools restore the SAME
/// tree → byte-identical (Oracle B).**
///
/// The mirror of `d3_byte_identical_over_our_replica`: the real binary writes the
/// replica, then both tools restore it, and the outputs must be byte-identical.
/// Running D3 in both write-directions catches an asymmetry where one tool's
/// *writer* and the other's *reader* happen to agree only on self-produced bytes.
#[tokio::test(flavor = "multi_thread")]
async fn d3_byte_identical_over_real_replica() {
    if skip_if_tools_missing("d3_byte_identical_over_real_replica") {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let ours = dir.path().join("ours.db");
    let theirs = dir.path().join("theirs.db");

    // The real binary writes the replica.
    litestream_replicate(&src, &replica_root);

    // Our restore.
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    replica::restore(&client, &ours, TXID(0))
        .await
        .expect("D3: our restore of a real replica");

    // The real binary's restore of the SAME tree.
    litestream_restore(&replica_root, &theirs);

    // Oracle B: byte-identical main DB files after a TRUNCATE checkpoint.
    db_equal("B", &theirs, &ours).expect(
        "D3: our restore and `litestream restore` of a real replica must be byte-identical (Oracle B)",
    );
}

/// **D2 at a TARGET TXID — real write → our point-in-time restore → Oracle A.**
///
/// Exercises the `-txid`-equivalent restore path against real-Litestream bytes:
/// the real binary replicates the full workload, rustyriver restores up to an
/// intermediate TXID, and the result must equal what the *real* binary restores at
/// that same `-txid`. Both restorers consume the identical real replica, so this
/// confirms our `calc_restore_plan` selects the same chain the real tool does for
/// a point-in-time target. (Uses Oracle A because a partial restore stops at a
/// mid-stream TXID; the comparison is real-vs-ours at the same target.)
#[tokio::test(flavor = "multi_thread")]
async fn d2_real_write_our_restore_at_target_txid() {
    if skip_if_tools_missing("d2_real_write_our_restore_at_target_txid") {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let ours = dir.path().join("ours-at-txid.db");
    let theirs = dir.path().join("theirs-at-txid.db");

    // The real binary writes the full workload.
    litestream_replicate(&src, &replica_root);

    // Pick an intermediate target: the count of statement-groups gives the max
    // TXID; restore to roughly the middle of the chain.
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    let files = {
        use celld_ltx::client::ReplicaClient;
        client.ltx_files(0, TXID(0)).await.unwrap()
    };
    let max_txid = files.iter().map(|f| f.max_txid.0).max().unwrap();
    assert!(
        max_txid >= 4,
        "need a multi-file chain (got max {max_txid})"
    );
    let target = TXID(max_txid - 2); // a mid-stream point-in-time target

    // Our restore up to `target`.
    replica::restore(&client, &ours, target)
        .await
        .expect("D2(txid): our restore to target");

    // The real binary's restore up to the same `-txid`.
    let url = file_url(&replica_root);
    let txid_hex = format!("{:016x}", target.0);
    run_litestream(&[
        "restore",
        "-txid",
        &txid_hex,
        "-o",
        &theirs.to_string_lossy(),
        &url,
    ]);
    assert!(theirs.exists(), "litestream restore -txid produced no file");

    // Oracle A: our point-in-time restore == the real tool's at the same TXID.
    db_equal("A", &theirs, &ours).expect(
        "D2(txid): our restore@target must equal real `litestream restore -txid` at the same TXID (Oracle A)",
    );
}

// ─────────────────────── D4 (compaction write path) ───────────────────────

/// Reads the raw bytes of one published LTX object from a file replica tree.
fn read_replica_object(replica_root: &Path, info: &celld_ltx::FileInfo) -> Vec<u8> {
    let path = ltx_file_path(
        &replica_root.to_string_lossy(),
        info.level as u32,
        info.min_txid,
        info.max_txid,
    );
    std::fs::read(path).expect("read the published LTX object")
}

/// **D4 — our L1 compaction → real `litestream restore` → Oracle A vs source.**
///
/// D1 covers the L0 writer, and that writer still emits the pre-v0.5.2 frame
/// representation. The compactor emits the v0.5.2 block representation instead,
/// and until this case nothing made a real reader look at one: the crate
/// compared its encoder output with bytes from Go, then decoded its own
/// compaction output with its own decoder. "Litestream v0.5.16 reads our block
/// files" was therefore a claim and not a test, and the compaction scheduler is
/// on by default, so the claim is load-bearing.
///
/// The replica deliberately keeps both representations. One L1 object covers a
/// prefix of the L0 objects and the remaining L0 objects cover the tail, so the
/// real binary must interleave a block file with frame files. `CalcRestorePlan`
/// takes the file that extends the longest contiguous TXID range, so the plan
/// prefers the L1 object over the L0 objects it covers.
#[tokio::test(flavor = "multi_thread")]
async fn d4_our_compaction_real_restore_oracle_a() {
    if skip_if_tools_missing("d4_our_compaction_real_restore_oracle_a") {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let restored = dir.path().join("restored-by-litestream.db");

    // 1. rustyriver writes the L0 tree.
    let max_txid = rustyriver_replicate(&src, &replica_root).await;

    // 2. rustyriver compacts a PREFIX of that tree into one L1 object. The
    //    limit leaves a tail at L0 on purpose, so the restore plan has to mix
    //    the two page representations rather than read the L1 object alone.
    const PREFIX_FILES: usize = 4;
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    let output = ReplicaCompactor::new(&client)
        .with_limits(PREFIX_FILES, u64::MAX)
        .compact(1)
        .await
        .expect("compact L0 into L1")
        .expect("the L0 level must hold objects to compact");
    assert_eq!(output.info.level, 1, "the destination level must be L1");
    assert!(
        output.info.max_txid < max_txid,
        "the L0 tail must survive the compaction so the plan mixes representations: \
         L1 covers up to {}, the L0 tree reaches {max_txid}",
        output.info.max_txid
    );

    // 3. The published object must really carry the v0.5.2 block layout.
    //    Without this the case passes even if the compactor silently falls back
    //    to the frame representation, which is the very thing D4 exists to
    //    prove a real reader accepts. The first page header follows the file
    //    header; its flags carry the compressed-size prefix bit.
    let published = read_replica_object(&replica_root, &output.info);
    let first_page_flags =
        u16::from_be_bytes([published[HEADER_SIZE + 4], published[HEADER_SIZE + 5]]);
    assert_eq!(
        first_page_flags & PAGE_HEADER_FLAG_SIZE,
        PAGE_HEADER_FLAG_SIZE,
        "the compactor must publish the v0.5.2 block representation, not frames"
    );

    // 4. The REAL binary restores the mixed tree.
    litestream_restore(&replica_root, &restored);

    // 5. Oracle A: real-restored == source.
    db_equal("A", &src, &restored).expect(
        "D4: real `litestream restore` over our compacted L1 object must equal the source (Oracle A)",
    );
}
