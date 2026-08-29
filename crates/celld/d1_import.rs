// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Owner-side binary SQLite import for D1.
//!
//! The operator route carries only a filesystem path (`D1-IMPORT-RPC.md`);
//! the owner validates the seed, copies it through SQLite's backup API, resets
//! the local LTX lineage, captures a MinTXID==1 snapshot, uploads it, and
//! reopens the cell connection.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use crate::ltx_repl::LtxRepl;
use crate::storage;

static LTX: OnceLock<Arc<LtxRepl>> = OnceLock::new();

/// Install the node's replication backend for [`run`]. Called once at startup
/// when fleet replication is enabled.
pub fn set_ltx(ltx: Arc<LtxRepl>) {
    let _ = LTX.set(ltx);
}

fn ltx() -> Result<Arc<LtxRepl>, String> {
    LTX.get()
        .cloned()
        .ok_or_else(|| "D1 import requires fleet replication".to_string())
}

const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";
const REQUIRED_PAGE_SIZE: i64 = 4096;

#[derive(Debug, Serialize)]
pub struct D1ImportOk {
    pub bytes: u64,
    pub duration_ms: u64,
    pub snapshot_txid: u64,
}

pub struct D1ImportFailure {
    pub message: String,
}

fn d1_import_error(message: impl Into<String>) -> D1ImportFailure {
    D1ImportFailure {
        message: message.into(),
    }
}

/// Validate a seed file before the owner copies it into the cell database.
pub fn validate_seed_path(data_root: &Path, path: &str) -> Result<(PathBuf, u64), String> {
    let path = Path::new(path);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join(path)
    };
    let metadata = std::fs::metadata(&path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err(format!("{} is not a file", path.display()));
    }
    let size = metadata.len();
    let limit = import_max_bytes();
    if size > limit {
        return Err(format!(
            "sqlite seed {size} bytes exceeds limit of {limit} bytes (CELLD_D1_IMPORT_MAX_MB)"
        ));
    }
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let shm = PathBuf::from(format!("{}-shm", path.display()));
    if wal.exists() {
        return Err(format!(
            "refusing import with WAL sidecar {}; checkpoint the database and remove it first",
            wal.display()
        ));
    }
    if shm.exists() {
        return Err(format!(
            "refusing import with SHM sidecar {}; checkpoint the database and remove it first",
            shm.display()
        ));
    }
    if !sqlite_magic_at(&path)? {
        return Err(format!(
            "{} is not a SQLite database (missing SQLite format 3 header)",
            path.display()
        ));
    }
    validate_seed_page_size(&path)?;
    let absolute = std::fs::canonicalize(&path).map_err(|error| error.to_string())?;
    let canonical_root = std::fs::canonicalize(data_root).map_err(|error| error.to_string())?;
    if absolute.starts_with(&canonical_root) {
        return Err("refusing to import from the cell data directory".to_string());
    }
    Ok((absolute, size))
}

fn import_max_bytes() -> u64 {
    std::env::var("CELLD_D1_IMPORT_MAX_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(512)
        .saturating_mul(1024 * 1024)
}

fn sqlite_magic_at(path: &Path) -> Result<bool, String> {
    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut header = [0u8; 16];
    let read = file.read(&mut header).map_err(|error| error.to_string())?;
    Ok(read >= SQLITE_MAGIC.len() && &header[..SQLITE_MAGIC.len()] == SQLITE_MAGIC)
}

fn validate_seed_page_size(path: &Path) -> Result<(), String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| error.to_string())?;
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if page_size != REQUIRED_PAGE_SIZE {
        return Err(format!(
            "sqlite seed page_size is {page_size}, expected {REQUIRED_PAGE_SIZE}"
        ));
    }
    Ok(())
}

/// State captured on the isolate thread before the async LTX import runs.
pub struct PreparedImport {
    scope: String,
    seed: PathBuf,
    epoch: u64,
    sqlite_vec: bool,
    ltx: Arc<LtxRepl>,
    started: std::time::Instant,
}

/// Reopen metadata delivered back on the isolate thread after the async import.
pub(crate) struct ReopenSpec {
    pub(crate) scope: String,
    pub(crate) db_path: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) sqlite_vec: bool,
}

/// Validate the seed and close isolate storage. Must run during a cell turn.
pub fn prepare(
    scope: &str,
    path: &str,
    sqlite_vec: bool,
) -> Result<(PreparedImport, ReopenSpec), D1ImportFailure> {
    let started = std::time::Instant::now();
    let ltx = ltx().map_err(d1_import_error)?;
    let epoch = storage::activation_epoch(scope)
        .ok_or_else(|| d1_import_error(format!("no active database for {scope}")))?;
    let (seed, _) = validate_seed_path(ltx.data_root(), path).map_err(d1_import_error)?;
    let db_path = ltx.cell_db_path(scope, epoch);
    storage::close(scope);
    Ok((
        PreparedImport {
            scope: scope.to_string(),
            seed,
            epoch,
            sqlite_vec,
            ltx,
            started,
        },
        ReopenSpec {
            scope: scope.to_string(),
            db_path,
            epoch,
            sqlite_vec,
        },
    ))
}

/// Run the LTX import after [`prepare`] closed isolate storage.
pub async fn import_prepared(prepared: PreparedImport) -> Result<D1ImportOk, D1ImportFailure> {
    let PreparedImport {
        scope,
        seed,
        epoch,
        ltx,
        started,
        ..
    } = prepared;
    if ltx
        .child_has_base_json(&scope, epoch)
        .await
        .map_err(|error| d1_import_error(error.to_string()))?
    {
        return Err(d1_import_error(
            "child prefix has base.json; d1 import is only for root versions",
        ));
    }
    let imported = ltx
        .import_sqlite_seed(&scope, epoch, &seed)
        .await
        .map_err(|error| d1_import_error(error.to_string()))?;
    Ok(D1ImportOk {
        bytes: imported.bytes,
        duration_ms: started.elapsed().as_millis() as u64,
        snapshot_txid: imported.snapshot_txid,
    })
}

/// Reopen isolate storage after import completes. Must run during a cell turn.
pub(crate) fn reopen(spec: &ReopenSpec) -> Result<(), D1ImportFailure> {
    storage::open_at_epoch(
        &spec.scope,
        &spec.db_path.to_string_lossy(),
        spec.epoch,
        spec.sqlite_vec,
    )
    .map_err(|error| d1_import_error(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write_seed(path: &Path, page_size: i64, sql: &str) -> rusqlite::Result<()> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "page_size", page_size)?;
        connection.execute_batch("VACUUM")?;
        connection.execute_batch(sql)?;
        Ok(())
    }

    #[test]
    fn d1_import_validate_seed_rejects_wal_sidecar() {
        let root = tempdir().unwrap();
        let seed = root.path().join("seed.db");
        write_seed(
            &seed,
            4096,
            "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
        )
        .unwrap();
        fs::write(format!("{}-wal", seed.display()), b"x").unwrap();
        let error = validate_seed_path(root.path(), seed.to_str().unwrap()).unwrap_err();
        assert!(error.contains("WAL sidecar"), "{error}");
    }

    #[test]
    fn d1_import_validate_seed_rejects_non_4096_page_size() {
        let root = tempdir().unwrap();
        let seed = root.path().join("seed.db");
        write_seed(&seed, 8192, "CREATE TABLE t(x INTEGER);").unwrap();
        let error = validate_seed_path(root.path(), seed.to_str().unwrap()).unwrap_err();
        assert!(error.contains("page_size"), "{error}");
    }

    #[test]
    fn d1_import_validate_seed_rejects_data_directory_path() {
        let root = tempdir().unwrap();
        let seed = root.path().join("seed.db");
        write_seed(&seed, 4096, "CREATE TABLE t(x INTEGER);").unwrap();
        let error = validate_seed_path(root.path(), seed.to_str().unwrap()).unwrap_err();
        assert!(error.contains("cell data directory"), "{error}");
    }

    #[test]
    fn d1_import_validate_seed_accepts_outside_data_root() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let seed = outside.path().join("seed.db");
        write_seed(
            &seed,
            4096,
            "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (7);",
        )
        .unwrap();
        let (absolute, size) =
            validate_seed_path(root.path(), seed.to_str().unwrap()).expect("valid seed");
        assert_eq!(absolute, fs::canonicalize(&seed).unwrap());
        assert!(size > 0);
    }
}
