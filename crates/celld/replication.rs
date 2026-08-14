// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Shared replication primitives for the in-process LTX backend
//! ([`crate::ltx_repl::LtxRepl`]). Each cell db lives at
//! `<watch>/<cell>/ltx/e<epoch>/db.sqlite` and replicates to
//! `cells/<cell>/ltx/e<epoch>/` in the bucket — epoch-in-prefix is the
//! data-path fence: a stale owner writes a dead prefix.
use rusqlite::Connection;
use std::path::PathBuf;
use std::time::Duration;

/// Outcome of a blocking replication wait on one cell db.
pub enum SyncWait {
    /// The latest local commit is in the bucket.
    Durable,
    /// The replicator does not track this cell; the caller decides its
    /// fallback.
    Unsupported,
    /// The wait failed or timed out.
    Failed,
}

pub struct RestoredSnapshot {
    pub epoch: u64,
    path: PathBuf,
    directory: PathBuf,
}

impl RestoredSnapshot {
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Construct a snapshot whose `directory` is removed on drop, handing the
    /// caller an inspection copy with RAII cleanup.
    pub(crate) fn new(epoch: u64, path: PathBuf, directory: PathBuf) -> Self {
        Self {
            epoch,
            path,
            directory,
        }
    }
}

impl Drop for RestoredSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[derive(Clone)]
pub struct StorageCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub struct ActivationOptions<'a> {
    pub cell: &'a str,
    pub epoch: u64,
    /// The epoch-one ownership record was created conditionally by this
    /// activation. No earlier replica can exist for this cell.
    pub fresh: bool,
    /// This activation seized the cell from a DIFFERENT node. When false the
    /// ownership record still named us at `epoch - 1`, so no other process
    /// has written the cell since we evicted it and our preserved local
    /// state is authoritative.
    pub took_over: bool,
    /// Open the exact existing local epoch after a certified node-level
    /// handoff. This path performs no remote discovery or restore.
    pub resume_local: bool,
}

pub struct ActivationResult {
    pub path: PathBuf,
    pub restored: bool,
}

/// Enforce a byte ceiling over the preserved snapshots under `watch`,
/// evicting least-recently-used first. The walk is layout-independent.
/// `.hibernated` is the pre-2026-08-05 name and is still swept, or a node
/// upgrading would keep those files forever without counting them.
pub(crate) fn prune_watch(watch: &std::path::Path, max_bytes: u64) -> (usize, usize, u64) {
    use celld_logic::cache::CacheEntry;
    let mut paths = Vec::new();
    let mut entries = Vec::new();
    let mut stack = vec![watch.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(read) = std::fs::read_dir(&dir) else {
            continue;
        };
        for item in read.flatten() {
            let path = item.path();
            let Ok(meta) = item.metadata() else { continue };
            if meta.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .is_some_and(|ext| ext == "evicted" || ext == "hibernated")
            {
                let last_used_ms = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                entries.push(CacheEntry {
                    last_used_ms,
                    bytes: meta.len(),
                });
                paths.push(path);
            }
        }
    }
    let total: u64 = entries.iter().map(|entry| entry.bytes).sum();
    let evict = celld_logic::cache::plan_eviction(&entries, max_bytes);
    // The kept bytes come out of the pass that already visits every evicted
    // index, rather than from a membership test per entry. `evict` is a `Vec`,
    // so asking it whether it holds each index made the accounting
    // O(entries x evicted) -- 138 ms at 64k snapshots, on the blocking thread,
    // every prune, to produce a number only the log line reads.
    let mut freed = 0_u64;
    for &index in &evict {
        freed = freed.saturating_add(entries[index].bytes);
        let _ = std::fs::remove_file(&paths[index]);
    }
    (
        entries.len() - evict.len(),
        evict.len(),
        total.saturating_sub(freed),
    )
}

/// Copy a live database into a standalone snapshot. SQLite's backup API
/// includes committed WAL state without checkpointing or interfering with the
/// replicator's ownership of the WAL.
pub(crate) fn sqlite_snapshot(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> anyhow::Result<()> {
    {
        let source =
            Connection::open_with_flags(source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut destination = Connection::open(destination)?;
        let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(5), None)?;
    }
    Ok(())
}
