// This file-backend integration test inspects its external filesystem result.
#![allow(clippy::disallowed_methods)]

//! integration_file — the round-trip gate against the file `ReplicaClient`.
//!
//! Proves T10: open a fresh SQLite DB, apply several transactions, capture them
//! into L0 LTX files (the `Db` capture loop), replicate them to a file replica
//! (`Replica::sync` → `FileReplicaClient`), restore to a NEW path
//! (`celld_ltx::replica::restore`), and assert the restored DB equals the
//! source via the logical equality oracle (`scripts/db_equal.sh A` — identical
//! `PRAGMA integrity_check`, schema, and per-table content hash).
//!
//! The source DB is written through an ordinary `rusqlite` connection (the
//! "application"); the managed `Db` holds the read lock and captures the WAL,
//! exactly as a host process would embed the library. A second test restores the
//! committed golden replica (captured from the real `litestream` binary) and
//! checks it against the **real `litestream restore`** output (a differential
//! anchor for the reader, prefiguring G3/D2) — skipped only if the binary or
//! `sqlite3` is absent from PATH.

use celld_ltx::client::file::FileReplicaClient;
use celld_ltx::client::ReplicaClient;
use celld_ltx::db::Db;
use celld_ltx::replica::{self, Replica};
use celld_ltx::TXID;
use rusqlite::Connection;
use std::path::Path;
use std::process::Command;

/// Absolute path to the `db_equal.sh` oracle.
fn db_equal_script() -> String {
    format!("{}/scripts/db_equal.sh", env!("CARGO_MANIFEST_DIR"))
}

/// Runs `db_equal.sh <mode> <a> <b>`; returns Ok(()) on exit 0, else the
/// captured stderr/stdout for the failure message. Requires `sqlite3` on PATH.
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

/// The real binary under test: `$LITESTREAM_BIN`, or `litestream` on PATH.
/// The oracle is pinned to the v0.5 replica format (v0.5.11); a v0.3-era
/// binary knows nothing of LTX and fails as a baffling "no matching
/// backups found", so a wrong-era binary skips loudly instead.
fn litestream_bin() -> String {
    std::env::var("LITESTREAM_BIN").unwrap_or_else(|_| "litestream".into())
}

/// `CELLD_LTX_LITESTREAM_REQUIRED=1` forbids the skip below. CI installs the
/// pinned binary and sets the variable, so a failed install reds the run
/// instead of passing a gate that ran nothing. This mirrors
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
            if v.starts_with("v0.5") || v.starts_with("0.5") {
                true
            } else {
                unavailable(
                    test,
                    &format!(
                        "litestream {v:?} is the wrong era (the replica format is \
                         v0.5); point LITESTREAM_BIN at a v0.5 binary"
                    ),
                );
                false
            }
        }
    }
}

/// True if `bin` resolves on PATH.
fn has_bin(bin: &str) -> bool {
    Command::new(bin)
        .arg("--help")
        .output()
        .map(|o| o.status.success() || o.status.code().is_some())
        .unwrap_or(false)
}

/// Opens an application writer connection in WAL mode (the analog of the host's
/// own SQLite handle writing alongside the managed `Db`).
fn open_writer(path: &Path) -> Connection {
    let c = Connection::open(path).unwrap();
    c.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
    let mode: String = c
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    c
}

/// THE G2 GATE: open → write txns → capture → replicate (file) → restore → equal.
#[tokio::test(flavor = "multi_thread")]
async fn round_trip_file_client_reproduces_source() {
    if !has_bin("sqlite3") {
        unavailable(
            "round_trip_file_client_reproduces_source",
            "`sqlite3` is not on PATH (the db_equal Oracle A needs it)",
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("source.db");
    let replica_root = dir.path().join("replica");
    let restored_path = dir.path().join("restored.db");

    // ── 1. Open the managed Db (takes the read lock) + an app writer conn. ──
    let db = Db::open(&src_path).unwrap();
    let writer = open_writer(&src_path);

    // ── 2. Apply several transactions, capturing each into an L0 LTX file. ──
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    let mut replica = Replica::new(db, client);

    // Helper that runs SQL on the app connection, then drives the capture loop
    // (db.sync) and the replica upload (replica.sync) — the SyncAndWait ordering
    // (db.go:500-512).
    macro_rules! commit_and_replicate {
        ($sql:expr) => {{
            writer.execute_batch($sql).unwrap();
            replica.db_mut().unwrap().sync().unwrap();
            replica.sync().await.unwrap();
        }};
    }

    commit_and_replicate!("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
    commit_and_replicate!("INSERT INTO kv (k, v) VALUES ('a','1'),('b','2'),('c','3')");
    commit_and_replicate!("UPDATE kv SET v='updated' WHERE k='a'");
    commit_and_replicate!("INSERT INTO kv (k, v) VALUES ('d','4'),('e','5')");
    commit_and_replicate!("DELETE FROM kv WHERE k='b'");
    // A larger transaction to exercise multi-page WAL frames.
    commit_and_replicate!(
        "CREATE TABLE big (id INTEGER PRIMARY KEY, blob TEXT);\
         INSERT INTO big (id, blob) SELECT value, hex(randomblob(200)) \
           FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value<500) SELECT value FROM c);"
    );

    // The replicated position now matches the database position.
    let dpos = replica.db_mut().unwrap().pos().unwrap();
    assert_eq!(replica.pos().txid, dpos.txid, "replica caught up to the DB");
    assert!(dpos.txid.0 >= 6, "at least 6 transactions captured");

    // Verify the replica tree actually holds the L0 files.
    let files = replica.client.ltx_files(0, TXID(0)).await.unwrap();
    assert!(
        files.len() as u64 >= dpos.txid.0,
        "every captured TXID is on the replica (got {} files, txid={})",
        files.len(),
        dpos.txid
    );

    // ── 3. Restore from the replica into a brand-new path. ──
    replica.restore(&restored_path).await.expect("restore");
    assert!(restored_path.exists(), "restored DB file created");

    // Drop the managed Db so the source WAL is released before the oracle reads
    // it (the oracle checkpoints both DBs).
    drop(replica);
    drop(writer);

    // ── 4. Oracle A: restored DB is logically identical to the source. ──
    db_equal("A", &src_path, &restored_path).expect("Oracle A: restored == source");
}

/// A second G2-style check at a TARGET TXID: restoring to an intermediate TXID
/// reproduces the database as it was at that transaction (point-in-time).
#[tokio::test(flavor = "multi_thread")]
async fn restore_to_target_txid_reproduces_point_in_time() {
    if !has_bin("sqlite3") {
        unavailable(
            "restore_golden_replica_matches_real_litestream",
            "`sqlite3` is not on PATH (the db_equal oracle needs it)",
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("source.db");
    let snapshot_path = dir.path().join("at_txid.db");
    let restored_path = dir.path().join("restored.db");
    let replica_root = dir.path().join("replica");

    let db = Db::open(&src_path).unwrap();
    let writer = open_writer(&src_path);
    let client = FileReplicaClient::new(replica_root.to_string_lossy().into_owned());
    let mut replica = Replica::new(db, client);

    macro_rules! step {
        ($sql:expr) => {{
            writer.execute_batch($sql).unwrap();
            replica.db_mut().unwrap().sync().unwrap();
            replica.sync().await.unwrap();
        }};
    }

    step!("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    step!("INSERT INTO t (v) VALUES ('one'),('two')");
    // Capture the database state at THIS point (the target TXID).
    let target = replica.db_mut().unwrap().pos().unwrap().txid;
    // Make a verbatim copy of the source DB as-of the target by checkpointing a
    // fresh connection's view. Easiest: VACUUM INTO at this moment.
    writer
        .execute(
            "VACUUM INTO ?1",
            [snapshot_path.to_string_lossy().to_string()],
        )
        .unwrap();

    // Continue mutating after the snapshot point.
    step!("UPDATE t SET v='TWO' WHERE id=2");
    step!("INSERT INTO t (v) VALUES ('three')");

    // Restore at the target TXID into a new path.
    replica::restore(&replica.client, &restored_path, target)
        .await
        .expect("restore at target txid");

    drop(replica);
    drop(writer);

    // The restored-at-target DB equals the VACUUM INTO snapshot taken at that
    // exact point (Oracle A).
    db_equal("A", &snapshot_path, &restored_path)
        .expect("Oracle A: restore@target == point-in-time snapshot");
}

/// Differential anchor for the RESTORE READER (prefigures G3/D2): restoring the
/// committed golden replica (real-`litestream`-produced bytes) must yield a DB
/// logically identical to what the real `litestream restore` produces from the
/// same tree. Skipped if `litestream`/`sqlite3` are not on PATH.
#[tokio::test(flavor = "multi_thread")]
async fn restore_golden_replica_matches_real_litestream() {
    if !has_bin("sqlite3") {
        unavailable(
            "restore_golden_replica_matches_real_litestream",
            "`sqlite3` is not on PATH (the db_equal oracle needs it)",
        );
        return;
    }
    if !litestream_usable("restore_golden_replica_matches_real_litestream") {
        return;
    }

    let golden_root = format!(
        "{}/tests/fixtures/golden/replica",
        env!("CARGO_MANIFEST_DIR")
    );

    let dir = tempfile::tempdir().unwrap();
    let ours = dir.path().join("ours.db");
    let theirs = dir.path().join("theirs.db");

    // Our restore.
    let client = FileReplicaClient::new(golden_root.clone());
    replica::restore(&client, &ours, TXID(0))
        .await
        .expect("our restore of the golden replica");

    // Real litestream's restore of the same tree.
    let status = Command::new(litestream_bin())
        .arg("restore")
        .arg("-o")
        .arg(&theirs)
        .arg(format!("file://{golden_root}"))
        .status()
        .expect("spawn litestream restore");
    assert!(status.success(), "litestream restore failed");

    // Oracle A: both restores are logically identical.
    db_equal("A", &theirs, &ours).expect("Oracle A: our restore == litestream restore");
}
