// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![warn(clippy::disallowed_macros)]

//! In-process replication backend built on `celld-ltx`.
//!
//! One shared `object_store` client for the whole node, and a managed
//! `celld_ltx::Db` per resident cell that captures the cell's committed WAL
//! and uploads it on demand. No external process, no directory-watch lag — a
//! just-written cell is registered the instant it activates, so the output
//! gate can prove a fresh cell durable with no cold-start window.
//!
//! The object layout is `cells/<cell>/ltx/e<epoch>/` in the bucket, mirroring
//! the local `<watch>/<cell>/ltx/e<epoch>/db.sqlite` tree. This backend builds
//! its own object-store clients rather than going through `bucket::Bucket`, so
//! it carries the fleet's key prefix itself: without that, two fleets sharing
//! one bucket would replicate over each other.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use anyhow::anyhow;
use celld_ltx::object_store::ObjectStore;
use celld_ltx::replica;
use celld_ltx::replica_compactor::ReplicaCompactor;
use celld_ltx::Db;
use celld_ltx::HostTaskError;
use celld_ltx::LtxHost;
use celld_ltx::ObjectStoreClient;
use celld_ltx::ObjectStoreConfig;
use celld_ltx::Pos;
use celld_ltx::Replica;
use celld_ltx::ReplicaClient;
use celld_ltx::TimestampMetadataKey;
use celld_ltx::TXID;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tracing::info;
use tracing::warn;

use crate::asyncrt;
use crate::replication::sqlite_snapshot;
use crate::replication::ActivationOptions;
use crate::replication::ActivationResult;
use crate::replication::RestoredSnapshot;
use crate::replication::StorageCredentials;
use crate::replication::SyncWait;

/// Max cells uploading concurrently across the node. Caps blocking-pool threads
/// and in-flight object-store requests under high write fan-out.
const SYNC_CONCURRENCY: usize = 64;

/// Max LTX object downloads across every restore on this node. A hot cell can
/// contain thousands of L0 files, so serial reads turn a takeover into minutes
/// of terminal failures. This shared ceiling hides round-trip latency without
/// multiplying the bound by the activation count.
const RESTORE_DOWNLOAD_CONCURRENCY: usize = 64;

/// One attempt consumes at most this many source objects. This bound keeps a
/// first compaction of an old, write-hot cell from reading its complete L0
/// history into memory.
const COMPACTION_MAX_FILES: usize = 256;

/// The current `ReplicaClient` interface buffers objects, so bound the complete
/// input set until the client gains a streaming read and write surface.
const COMPACTION_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;

/// One captured-but-not-yet-uploaded L0 segment, the log tier's
/// replication unit (`crate::node_log`).
pub struct ShipEntry {
    pub cell: String,
    pub epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

/// The fleet shipper the log tier installs: one in-flight batch, all-member
/// fsync confirmation. `false` means the batch is not fleet-durable and the
/// gate must ride the bucket upload instead.
pub trait Shipper: Send + Sync + 'static {
    /// Ship one batch; `covered_seq` is the highest sequence whose frames
    /// are all bucket-covered, which followers may truncate behind.
    /// `Some(last_seq)` means every member confirmed the whole batch.
    fn ship<'a>(
        &'a self,
        batch: &'a [ShipEntry],
        covered_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<u64>> + Send + 'a>>;

    /// Called by the ship loop AFTER the shipped credits are applied: the
    /// capture-to-credit interval must read as in-flight to the
    /// reconfigure and seal barriers as one piece (fidelity audit,
    /// DRIFTED #1). Default no-op for shippers with no such window.
    fn batch_credited(&self) {}
    /// A degraded shipper refuses instantly; the ship loop skips capture.
    fn active(&self) -> bool;
    /// The log epoch this shipper writes. Sequences restart at zero each
    /// epoch, so the ship loop's truncation ledger must reset with it — a
    /// stale covered watermark from the previous epoch would tell fresh
    /// followers to delete entries they just fsync'd.
    fn epoch(&self) -> u64;
}

/// The bundle sink (`crate::node_log`): one PUT per node per flush
/// interval, carrying every dirty cell's captured L0 segments verbatim
/// (`crate::bundle`). `true` means the bundle is durable in the bucket and
/// every included frame may credit its cell's bucket coverage. Inactive
/// (or absent) means the per-cell upload path owns tiering, exactly as
/// before bundles existed.
pub trait BundleSink: Send + Sync + 'static {
    fn put_bundle<'a>(
        &'a self,
        entries: Vec<celld_ltx::bundle::BundleEntry>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>>;
    fn active(&self) -> bool;
    /// The un-drained rows for one cell-epoch, from the leader's own
    /// index of the bundles it wrote. Empty when bundles are off.
    fn rows_for(&self, cell: &str, epoch: u64) -> Vec<celld_ltx::LocatedRow>;
    /// One bundle object's complete bytes, for slicing rows out of.
    fn fetch_bundle<'a>(
        &'a self,
        source: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + 'a>>;
}

/// The compactor-facing fetcher: one cell-epoch's view over whatever sink
/// is currently installed. Reading the slot per call makes activation
/// order irrelevant — a cell activated before the sink existed still sees
/// bundles once it does.
struct SinkFetcher {
    slot: Arc<Mutex<Option<Arc<dyn BundleSink>>>>,
    cell: String,
    epoch: u64,
}

impl celld_ltx::BundleFetcher for SinkFetcher {
    fn rows<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = celld_ltx::Result<Vec<celld_ltx::LocatedRow>>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let sink = self.slot.lock().unwrap().clone();
            Ok(sink.map_or_else(Vec::new, |sink| sink.rows_for(&self.cell, self.epoch)))
        })
    }

    fn fetch<'a>(
        &'a self,
        located: &'a celld_ltx::LocatedRow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = celld_ltx::Result<Vec<u8>>> + Send + 'a>>
    {
        Box::pin(async move {
            let sink = self.slot.lock().unwrap().clone();
            let Some(sink) = sink else {
                return Err(celld_ltx::Error::Other("no bundle sink".into()));
            };
            let bytes = sink
                .fetch_bundle(&located.source)
                .await
                .map_err(|e| celld_ltx::Error::Other(e.to_string().into()))?;
            Ok(celld_ltx::bundle::slice(&bytes, &located.row)?.to_vec())
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    pub min_txids: u64,
    pub concurrency: usize,
}

struct CellCompaction {
    cell: String,
    epoch: u64,
    client: celld_ltx::BundleOverlayClient<ObjectStoreClient>,
    local_path: PathBuf,
    host: LtxHost,
    queue: mpsc::UnboundedSender<CompactionWork>,
    min_txids: u64,
    compacted_txid: AtomicU64,
    queued: AtomicBool,
    cancelled: AtomicBool,
    cancel: Notify,
}

struct CompactionWork {
    cell: Weak<Cell>,
    queued_at_mono_ms: u64,
}

/// One resident cell's replication state: the `celld_ltx::Db` shadowing its WAL
/// (behind a `std::sync::Mutex` because the `rusqlite` handle is `!Sync` and
/// must never cross an `.await`, so every capture+upload runs inside a
/// `spawn_blocking` closure) plus the durability tickets the output gate waits
/// on. `req_seq` counts durability requests; `synced_seq` is the highest ticket
/// a completed background sync captured. A write waits for `synced_seq >= its
/// ticket`, so concurrent writes to one cell ride a single batched upload —
/// and, because a sync credits only tickets whose writes committed before it
/// started (which the sync's `db.sync` captures), never one it did not upload.
struct Cell {
    replica: Mutex<Replica<ObjectStoreClient>>,
    /// The same epoch-prefix client the replica holds, for uploads that run
    /// off the replica mutex.
    client: ObjectStoreClient,
    req_seq: AtomicU64,
    synced_seq: AtomicU64,
    /// Highest ticket whose write is fsync'd on every ensemble member —
    /// the log tier's proof. The gate accepts either proof, so this stays 0
    /// forever when no shipper is installed.
    shipped_seq: AtomicU64,
    /// Highest TXID handed to the shipper; frames at or below it are on the
    /// followers (or in the bucket, which restored them).
    shipped_txid: AtomicU64,
    durable_txid: AtomicU64,
    /// Set while a sync for this cell is in flight, so the loop never runs two
    /// at once for one cell (they would serialize on the mutex and waste work).
    syncing: AtomicBool,
    /// Wall-clock ms of the last completed sync, the pacing anchor: with a
    /// healthy shipper the bucket runs at most one upload per flush interval
    /// behind, which is the tier's stated lag budget.
    last_sync_ms: AtomicU64,
    /// Notified when `synced_seq` advances (or a sync fails), waking waiters.
    ready: Notify,
    compaction: Option<CellCompaction>,
}
type CellHandle = Arc<Cell>;

pub struct LtxRepl {
    /// Local root: cell dbs live at `watch/<cell>/ltx/e<epoch>/db.sqlite`.
    watch: PathBuf,
    /// The object-metadata name for an LTX header timestamp. Azure refuses
    /// a hyphen in a metadata name, so the fleet bucket's dialect picks it.
    timestamp_metadata_key: TimestampMetadataKey,
    bucket: String,
    /// The bucket spec's key prefix: empty, or slash-terminated.
    prefix: String,
    endpoint: Option<String>,
    region: String,
    credentials: Option<StorageCredentials>,
    /// One connection pool for the whole node, shared by every cell client.
    store: Arc<dyn ObjectStore>,
    ltx_host: LtxHost,
    /// An explicit SQLite VFS for both managed connections. Production keeps
    /// this unset; the deterministic World selects its host-callback shim.
    vfs_name: Option<String>,
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    /// Woken when a cell's `committed` advances, so the background loop syncs
    /// without polling; a slow tick backstops any missed notification.
    dirty: Arc<Notify>,
    /// Shared by every activation, so the restore bound is per node, not per
    /// cell. The generic LTX restore keeps its sequential compatibility path.
    restore_slots: Arc<Semaphore>,
    compaction_queue: Option<mpsc::UnboundedSender<CompactionWork>>,
    compaction_min_txids: u64,
    /// Preserved eviction snapshots, tracked in memory so the local-cache
    /// prune answers without walking the data directory. See
    /// [`crate::replication::PreservedCache`].
    preserved: Mutex<crate::replication::PreservedCache>,
    /// Woken when a gate ticket arrives, so the ship loop group-commits
    /// without polling.
    dirty_ship: Arc<Notify>,
    shipper: Arc<Mutex<Option<Arc<dyn Shipper>>>>,
    bundle_sink: Arc<Mutex<Option<Arc<dyn BundleSink>>>>,
}

impl LtxRepl {
    /// Internal test constructor over an injected store, so the shipping
    /// restore and replication path runs against an in-memory bucket.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_store_for_test(watch: &Path, store: Arc<dyn ObjectStore>) -> Self {
        Self::start_with_store(watch, store, None, 0)
    }

    /// Build the production loop topology over an injected object store.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_store(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
    ) -> Self {
        Self::start_with_store_and_optional_vfs(watch, store, compaction, flush_ms, None)
    }

    /// Build the same loop topology and route managed SQLite through `vfs`.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_store_on_vfs(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
        vfs: &str,
    ) -> Self {
        Self::start_with_store_and_optional_vfs(
            watch,
            store,
            compaction,
            flush_ms,
            Some(vfs.to_string()),
        )
    }

    #[cfg(all(test, celld_internal_tests))]
    fn start_with_store_and_optional_vfs(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
        vfs_name: Option<String>,
    ) -> Self {
        Self::assemble(
            watch,
            store,
            TimestampMetadataKey::default(),
            "test".into(),
            String::new(),
            None,
            "auto".into(),
            None,
            compaction,
            flush_ms,
            deterministic_ltx_host(),
            vfs_name,
        )
    }

    /// Internal test constructor with additive L1 compaction enabled.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_compaction_for_test(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        min_txids: u64,
        concurrency: usize,
    ) -> Self {
        Self::assemble(
            watch,
            store,
            TimestampMetadataKey::default(),
            "test".into(),
            String::new(),
            None,
            "auto".into(),
            None,
            Some(CompactionConfig {
                min_txids,
                concurrency,
            }),
            0,
            deterministic_ltx_host(),
            None,
        )
    }

    pub(crate) fn start(
        watch: &Path,
        backend: crate::bucket::StorageBackend,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        let compaction = compaction_config_from_env()?;
        // Everything downstream of the store is backend-agnostic already,
        // so the dialect decides construction and the metadata name, and
        // nothing else.
        let store = match backend {
            crate::bucket::StorageBackend::Gcs => crate::bucket::gcs_replica_store(&bucket)?,
            crate::bucket::StorageBackend::Azure => crate::bucket::azure_replica_store(&bucket)?,
            crate::bucket::StorageBackend::S3 => {
                node_config(&bucket, endpoint.as_deref(), &region, credentials.as_ref())
                    .build_store()
                    .map_err(|error| anyhow!("build shared object store: {error}"))?
            }
        };
        // Azure blob metadata names must be C# identifiers, so the standard
        // Litestream key cannot carry its hyphen there. External Litestream
        // tooling reads that key, therefore an az:// replica gives up
        // Litestream-tool timestamp restore. celld never reads it back.
        let timestamp_metadata_key = match backend {
            crate::bucket::StorageBackend::Azure => TimestampMetadataKey::Underscore,
            _ => TimestampMetadataKey::Litestream,
        };
        // The tiering flush interval: with a healthy shipper, at most one
        // upload per cell per interval; it is simultaneously the bucket lag
        // budget. 0 disables pacing.
        let flush_ms = crate::env_vars::with_default("CELLD_LOG_FLUSH_MS", 2000)? as u64;
        Ok(Self::assemble(
            watch,
            store,
            timestamp_metadata_key,
            bucket,
            prefix,
            endpoint,
            region,
            credentials,
            compaction,
            flush_ms,
            production_ltx_host(),
            None,
        ))
    }

    /// The one constructor body: every field and every background loop,
    /// shared by production `start` and the test constructor so the two
    /// can never drift.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        timestamp_metadata_key: TimestampMetadataKey,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
        ltx_host: LtxHost,
        vfs_name: Option<String>,
    ) -> Self {
        let cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        let dirty_ship = Arc::new(Notify::new());
        let shipper: Arc<Mutex<Option<Arc<dyn Shipper>>>> = Arc::default();
        let bundle_sink: Arc<Mutex<Option<Arc<dyn BundleSink>>>> = Arc::default();
        let preserved = Mutex::new(crate::replication::PreservedCache::new(
            ltx_host.filesystem(),
        ));
        // Bound how many cells upload at once so one slow cell cannot stall the
        // others and a thousand hot cells cannot open a thousand uploads.
        asyncrt::spawn(sync_loop(
            cells.clone(),
            dirty.clone(),
            Arc::new(Semaphore::new(SYNC_CONCURRENCY)),
            shipper.clone(),
            bundle_sink.clone(),
            flush_ms,
        ))
        .detach();
        asyncrt::spawn(ship_loop(
            cells.clone(),
            dirty_ship.clone(),
            shipper.clone(),
        ))
        .detach();
        asyncrt::spawn(bundle_loop(cells.clone(), bundle_sink.clone(), flush_ms)).detach();
        Self {
            watch: watch.to_path_buf(),
            timestamp_metadata_key,
            bucket,
            prefix,
            endpoint,
            region,
            credentials,
            store,
            ltx_host,
            vfs_name,
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue: compaction.map(start_compaction_loop),
            compaction_min_txids: compaction.map_or(0, |config| config.min_txids),
            preserved,
            dirty_ship,
            shipper,
            bundle_sink,
        }
    }

    /// Install the fleet shipper. Fleet-durable proofs begin only after the
    /// log record is open in the bucket — the caller guarantees the order.
    pub fn set_shipper(&self, shipper: Arc<dyn Shipper>) {
        *self.shipper.lock().unwrap() = Some(shipper);
        self.dirty_ship.notify_one();
    }

    /// Install the bundle sink. Bundled tiering engages only while both
    /// the shipper and the sink report active; every other state keeps the
    /// per-cell upload path.
    pub fn set_bundle_sink(&self, sink: Arc<dyn BundleSink>) {
        *self.bundle_sink.lock().unwrap() = Some(sink);
    }

    /// The ensemble-change barrier: every frame ever handed to a shipper is
    /// durable in the bucket. Old followers' fragments are abandonable
    /// garbage exactly when this holds.
    pub fn all_shipped_tiered(&self) -> bool {
        self.cells.lock().unwrap().values().all(|cell| {
            cell.durable_txid.load(Ordering::SeqCst) >= cell.shipped_txid.load(Ordering::SeqCst)
        })
    }

    /// Recovery's primitive: PUT one gathered L0 segment to the exact key
    /// the dead leader's own upload would have used. Idempotent by key.
    /// The highest TXID the cell's per-cell prefix already covers, over
    /// every level. Recovery uses it to skip re-uploading rows the drain
    /// points (compaction, eviction sync) have already folded in — one
    /// LIST per cell instead of one PUT per historical row.
    pub async fn covered_txid(&self, cell: &str, epoch: u64) -> u64 {
        let client = self.client_for(cell, epoch);
        let mut covered = 0_u64;
        for level in 0..=1 {
            if let Ok(files) = client.ltx_files(level, TXID(0)).await {
                for file in files {
                    covered = covered.max(file.max_txid.0);
                }
            }
        }
        covered
    }

    pub async fn upload_raw_l0(
        &self,
        cell: &str,
        epoch: u64,
        min_txid: u64,
        max_txid: u64,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        self.client_for(cell, epoch)
            .write_ltx_file(0, TXID(min_txid), TXID(max_txid), bytes)
            .await
            .map_err(|error| {
                anyhow!("upload recovered l0 {cell} e{epoch} t{min_txid}-{max_txid}: {error}")
            })?;
        Ok(())
    }

    /// Merge contiguous single-transaction LTX segments into one segment
    /// covering the whole range. Recovery uses it to upload each cell's
    /// gathered tail as ONE object instead of one per row: the lab's chaos
    /// soak measured per-kill outages growing 133 s -> 353 s because every
    /// crash added hundreds of single-row L0 objects for the object
    /// store's same-key throttling to fight and the next restore plan to
    /// read. Returns None when the rows are not a contiguous ascending
    /// chain — the caller falls back to per-row uploads, never guessing.
    pub fn merge_l0_rows(rows: &[(u64, Vec<u8>)]) -> Option<Vec<u8>> {
        if rows.len() < 2 {
            return None;
        }
        if rows.windows(2).any(|pair| pair[1].0 != pair[0].0 + 1) {
            return None;
        }
        let readers: Vec<std::io::Cursor<&[u8]>> = rows
            .iter()
            .map(|(_, bytes)| std::io::Cursor::new(bytes.as_slice()))
            .collect();
        let mut compactor = celld_ltx::compactor::Compactor::new(Vec::new(), readers);
        // The gathered frames are no-checksum WAL-segment files, so the
        // merged header must be too — a checksummed output header fails
        // encode validation against checksum-less inputs (the same flag
        // ReplicaCompactor sets for the L0->L1 fold).
        compactor.header_flags = celld_ltx::ltx::HEADER_FLAG_NO_CHECKSUM;
        match compactor.compact() {
            Ok(()) => Some(compactor.into_writer()),
            Err(error) => {
                // The fallback is safe (per-row uploads) but must never be
                // silent again: a swallowed error here hid a dead merge
                // through a full fleet round.
                warn!(%error, rows = rows.len(), "recovery tail merge failed; per-row fallback");
                None
            }
        }
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    /// A per-cell client over the shared store, keyed to the cell's epoch
    /// prefix. `cells/<cell>/ltx/e<epoch>` matches [`Self::db_path`]'s remote
    /// twin so the same coordinates address local and replica state.
    fn client_for(&self, cell: &str, epoch: u64) -> ObjectStoreClient {
        let mut config = node_config(
            &self.bucket,
            self.endpoint.as_deref(),
            &self.region,
            self.credentials.as_ref(),
        );
        config.path = format!("{}cells/{cell}/ltx/e{epoch}", self.prefix);
        config.timestamp_metadata_key = self.timestamp_metadata_key;
        ObjectStoreClient::with_store(config, self.store.clone())
    }

    /// Highest epoch under `cells/<cell>/ltx/` that holds any LTX — the newest
    /// durable copy to restore on takeover.
    async fn highest_nonempty_epoch(&self, cell: &str) -> anyhow::Result<Option<u64>> {
        use celld_ltx::object_store::path::Path as ObjPath;
        let base = ObjPath::from(format!("{}cells/{cell}/ltx", self.prefix));
        let listing = self.store.list_with_delimiter(Some(&base)).await?;
        let mut best: Option<u64> = None;
        #[cfg(all(test, celld_internal_tests))]
        let restore_superseded =
            asyncrt::sabotage_active(crate::host_services::EngineSabotage::RestoreSupersededEpoch);
        #[cfg(not(all(test, celld_internal_tests)))]
        let restore_superseded = false;
        for prefix in listing.common_prefixes {
            if let Some(epoch) = prefix
                .filename()
                .and_then(|name| name.strip_prefix('e'))
                .and_then(|value| value.parse::<u64>().ok())
            {
                best = Some(best.map_or(epoch, |current| {
                    if restore_superseded {
                        current.min(epoch)
                    } else {
                        current.max(epoch)
                    }
                }));
            }
        }
        Ok(best)
    }

    /// Does the bucket hold any LTX for this cell at this epoch? The fail-closed
    /// eviction gate: never delete the last local copy of state the bucket
    /// cannot restore.
    pub async fn epoch_replicated(&self, cell: &str, epoch: u64) -> bool {
        let client = self.client_for(cell, epoch);
        matches!(
            replica::calc_restore_plan(&client, TXID(0)).await,
            Ok(plan) if !plan.is_empty()
        )
    }

    pub async fn activate(
        &self,
        options: ActivationOptions<'_>,
    ) -> anyhow::Result<ActivationResult> {
        let ActivationOptions {
            cell,
            epoch,
            fresh,
            took_over,
            resume_local,
            // Consumed by the interlock before the fold; the field stays
            // on the options because the core's claim still names it.
            prior: _,
        } = options;
        let dst = self.db_path(cell, epoch);
        self.ltx_host.create_dir_all(dst.parent().unwrap())?;

        // The takeover interlock moved OUT of this path (lease-fold): the
        // decision core gates foreign takeovers on the folded lease state
        // it already reads (Effect::RecoverNodeLog), and the boot order
        // recovers this node's own predecessor before the lease installs,
        // so by the time any activation reaches here the bucket is
        // provably complete for both the takeover and the named-me path.

        // Reuse a preserved local eviction snapshot only as the previous
        // epoch's baseline. Eviction removes the LTX metadata, so reopening
        // that SQLite image starts a new writer generation at TXID 1. Pairing
        // it with the same remote epoch would mix that new lineage with the
        // old tail (#158). A clean process reload is the separate
        // `resume_local` path: it retains both the live database and its LTX
        // metadata, so it can safely continue the existing epoch.
        // `.evicted` is the current name; `.hibernated` is what releases
        // before 2026-08-05 wrote. Accept both, so an upgrade reuses the
        // snapshots already on disk instead of restoring every cell from the
        // bucket. Writes always use the new name, so the old one dies out.
        let legacy = |path: &PathBuf| path.with_extension("hibernated");
        #[cfg(all(test, celld_internal_tests))]
        let took_over_for_reuse = took_over
            && !asyncrt::sabotage_active(crate::host_services::EngineSabotage::IgnoreTookOver);
        #[cfg(not(all(test, celld_internal_tests)))]
        let took_over_for_reuse = took_over;
        let previous = celld_logic::restore::previous_epoch_reusable(epoch, took_over_for_reuse)
            .then(|| self.db_path(cell, epoch - 1).with_extension("evicted"));
        let is_file = |path: &PathBuf| {
            self.ltx_host
                .metadata(path)
                .is_ok_and(|metadata| metadata.is_file)
        };
        let first_present = |path: PathBuf| {
            if is_file(&path) {
                Some(path)
            } else {
                Some(legacy(&path)).filter(is_file)
            }
        };
        let local_snapshot = (!fresh && !resume_local)
            .then(|| previous.and_then(first_present))
            .flatten();

        let mut restored = resume_local;
        if resume_local {
            anyhow::ensure!(
                is_file(&dst),
                "clean reload database is missing: {}",
                dst.display()
            );
            info!(cell, epoch, "resumed clean local replica");
        } else if let Some(snapshot) = local_snapshot {
            self.ltx_host.rename(&snapshot, &dst)?;
            self.preserved
                .lock()
                .expect("preserved cache poisoned")
                .forget(&snapshot);
            info!(cell, epoch, "reused local eviction snapshot");
            restored = true;
        } else if !fresh {
            // Restore the newest durable epoch's full contiguous chain. The
            // epoch seal that once capped this read is deleted: the
            // cut it fixed defended only never-acked resurrection — not a
            // promise anyone holds — and under the log tier late arrival of
            // ACKED rows into a per-cell prefix is normal (recovery gathers,
            // the drain folds, the healing pass repairs), so a permanent cap
            // turned ordering slips into permanent loss. Without it the
            // healed rows are simply picked up here.
            if let Some(from) = self.highest_nonempty_epoch(cell).await? {
                anyhow::ensure!(
                    from < epoch,
                    "refusing to restore {cell} from used epoch {from} into writer epoch {epoch}"
                );
                let client = self.client_for(cell, from);
                let _ = self.ltx_host.remove_file(&dst);
                let stats = replica::restore_with_host_and_download_slots(
                    &client,
                    &dst,
                    TXID(0),
                    self.ltx_host.clone(),
                    self.restore_slots.clone(),
                )
                .await
                .map_err(|error| anyhow!("restore {cell} e{from}: {error}"))?;
                let levels = stats
                    .by_level
                    .iter()
                    .map(|(level, count)| format!("L{level}:{count}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                info!(
                    event = "restore_plan",
                    cell,
                    epoch = from,
                    objects = stats.objects,
                    bytes = stats.bytes,
                    %levels,
                    "computed restore plan"
                );
                info!(cell, from, to = epoch, "restored remote replica");
                restored = true;
            }
        }

        // Open the managed Db (creates a fresh WAL db when nothing was restored)
        // and pair it with this epoch's client. Registration is immediate: the
        // cell can be proved durable on its very first write. The just-opened
        // db's position is the replica's seed -- 0 for a fresh cell, the
        // restored max otherwise, and equal to the remote under epoch fencing --
        // so the first sync skips the `calc_pos` listing that otherwise storms a
        // rate-limiting store. On the rare decode error we leave it unseeded and
        // fall back to that listing.
        let dst_ = dst.clone();
        let ltx_host = self.ltx_host.clone();
        let vfs_name = self.vfs_name.clone();
        let (db, seed) = asyncrt::blocking(move || {
            #[cfg(all(test, celld_internal_tests))]
            let mut db =
                crate::fault::with_connection_role("celld_ltx_db", || match vfs_name.as_deref() {
                    Some(vfs_name) => Db::open_with_host_and_vfs(&dst_, ltx_host, vfs_name),
                    None => Db::open_with_host(&dst_, ltx_host),
                })?;
            #[cfg(not(all(test, celld_internal_tests)))]
            let mut db = {
                debug_assert!(vfs_name.is_none());
                Db::open_with_host(&dst_, ltx_host)?
            };
            let seed = db.pos().ok();
            anyhow::Ok((db, seed))
        })
        .await?
        .map_err(|error| anyhow!("open managed db {}: {error}", dst.display()))?;
        let mut replica = Replica::new(db, self.client_for(cell, epoch));
        if let Some(pos) = seed {
            replica.seed_pos(pos);
        }
        let handle = Arc::new(Cell {
            replica: Mutex::new(replica),
            client: self.client_for(cell, epoch),
            req_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            shipped_seq: AtomicU64::new(0),
            // Frames at or below the seed came from the bucket (or a proven
            // snapshot); the followers only ever need what follows.
            shipped_txid: AtomicU64::new(seed.map_or(0, |pos| pos.txid.0)),
            last_sync_ms: AtomicU64::new(asyncrt::wall_ms().max(0) as u64),
            durable_txid: AtomicU64::new(seed.map_or(0, |pos| pos.txid.0)),
            syncing: AtomicBool::new(false),
            ready: Notify::new(),
            compaction: self.compaction_queue.as_ref().map(|queue| CellCompaction {
                cell: cell.to_string(),
                epoch,
                // The overlay lets compaction read bundle-resident frames
                // beside the per-cell objects; its output stays pure
                // per-cell L1s, which is the continuous drain.
                client: celld_ltx::BundleOverlayClient::new(
                    self.client_for(cell, epoch),
                    Some(Arc::new(SinkFetcher {
                        slot: self.bundle_sink.clone(),
                        cell: cell.to_string(),
                        epoch,
                    })),
                ),
                local_path: Db::meta_path_for_path(&dst),
                host: self.ltx_host.clone(),
                queue: queue.clone(),
                min_txids: self.compaction_min_txids,
                compacted_txid: AtomicU64::new(0),
                queued: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                cancel: Notify::new(),
            }),
        });
        self.cells
            .lock()
            .unwrap()
            .insert((cell.to_string(), epoch), handle.clone());
        if let Some(pos) = seed {
            maybe_queue_compaction(&handle, pos.txid.0);
        }

        Ok(ActivationResult {
            path: dst,
            restored,
        })
    }

    /// The output gate's primitive: take a durability ticket and return once a
    /// background sync that captured this write has completed, coalescing
    /// concurrent writes to one cell into a single upload. The write committed
    /// before this call, so any sync starting after our ticket captures it —
    /// we wait for `synced_seq >= my ticket`, not for a position, sidestepping
    /// the total_changes↔LTX-txid mismatch that a position compare would hit.
    /// Returns `position` (which the completed sync provably covered) for the
    /// core's coverage check.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            anyhow::bail!("ltx cell not resident: {cell} epoch {epoch}");
        };
        let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
        #[cfg(all(test, celld_internal_tests))]
        if asyncrt::sabotage_active(crate::host_services::EngineSabotage::CoverTicketEarly) {
            return Ok((position, celld_logic::ProofSource::Bucket));
        }
        self.dirty.notify_one();
        self.dirty_ship.notify_one();
        let started = asyncrt::mono_ms();
        let deadline = started.saturating_add(Duration::from_secs(10).as_millis() as u64);
        loop {
            // Register the waiter before checking, so a sync that completes
            // between the check and the await is not missed. Either proof
            // releases the gate: the bucket upload, or every ensemble
            // member's fsync — whichever lands first.
            let ready = handle.ready.notified();
            // Prefer the fleet proof when both hold: it is the arbitrated
            // one, and it spares the caller C1's ownership read.
            let shipped = handle.shipped_seq.load(Ordering::SeqCst) >= ticket;
            if handle.synced_seq.load(Ordering::SeqCst) >= ticket || shipped {
                let source = if shipped {
                    celld_logic::ProofSource::Fleet
                } else {
                    celld_logic::ProofSource::Bucket
                };
                tracing::debug!(
                    target: "timing",
                    event = "durable_wait",
                    cell,
                    wait_us = asyncrt::mono_ms().saturating_sub(started).saturating_mul(1_000),
                    proof = if shipped { "fleet" } else { "bucket" },
                    "durability proof reached"
                );
                return Ok((position, source));
            }
            if asyncrt::timeout_at(deadline, ready).await.is_err() {
                anyhow::bail!("ltx durability timed out for {cell} epoch {epoch}");
            }
        }
    }

    /// A direct, synchronous durability pass for the rare eviction
    /// gates (not the hot write path). Also advances the cell's durable position
    /// so any output-gate waiters ride it.
    pub async fn sync_wait(&self, cell: &str, epoch: u64, _timeout: Duration) -> SyncWait {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            return SyncWait::Unsupported;
        };
        match sync_cell(handle).await {
            Some(true) => SyncWait::Durable,
            Some(false) => SyncWait::Failed,
            None => SyncWait::Unsupported,
        }
    }

    /// Drop this cell's replication handle, leaving every file in place.
    ///
    /// The handle owns the replica, and the replica holds two open SQLite
    /// connections -- one of them inside a long-running read transaction that
    /// pins pages -- so a stop that does not release it keeps that memory for
    /// the life of the process. Every stop has to reach this, because a stop
    /// is where the activation ends.
    ///
    /// No durability pass here, deliberately. This runs on the stops that are
    /// not an orderly handoff, and a fenced node has lost the authority that
    /// would make writing more of this cell's history safe. `evict` below
    /// still syncs, and still refuses to drop a handle whose final pass
    /// failed, because that is the path where the node keeps the cell and is
    /// giving it up on purpose.
    pub fn release(&self, cell: &str, epoch: u64) {
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
    }

    pub async fn evict(&self, cell: &str, epoch: u64, preserve_local: bool) {
        // A final durability pass so no acknowledged write is stranded —
        // and a FAILED pass refuses the eviction outright (fidelity
        // audit, DRIFTED #3): removing the handle on failure hid a cell
        // with shipped-but-unuploaded frames from all_shipped_tiered and
        // the drain, and let a preserved snapshot later re-seed
        // durable_txid past the truth. On refusal the handle stays
        // registered (the barriers keep counting it, the sync loop keeps
        // retrying) and the files stay put for a retried eviction or a
        // local reactivation.
        if matches!(
            self.sync_wait(cell, epoch, Duration::from_secs(10)).await,
            SyncWait::Failed
        ) {
            warn!(
                cell,
                epoch, "eviction refused: the final durability pass failed; the cell stays managed"
            );
            return;
        }
        self.remove_local(cell, epoch, preserve_local);
    }

    /// Discard a reset runtime without another durability attempt.
    ///
    /// The proof that triggered Reset already failed. Retrying it here can keep
    /// the unproved database resident and contradicts Reset's keep-nothing
    /// contract, so this path removes the handle and every live local file.
    pub(crate) fn discard(&self, cell: &str, epoch: u64) {
        self.remove_local(cell, epoch, false);
    }

    fn remove_local(&self, cell: &str, epoch: u64, preserve_local: bool) {
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
        let db = self.db_path(cell, epoch);
        if preserve_local {
            let preserved = db.with_extension("evicted");
            if let Err(error) = self.ltx_host.rename(&db, &preserved) {
                warn!(cell, epoch, %error, "preserve local snapshot failed");
            } else {
                if let Err(error) = self
                    .preserved
                    .lock()
                    .expect("preserved cache poisoned")
                    .insert(preserved)
                {
                    warn!(cell, epoch, %error, "index preserved local snapshot failed");
                }
            }
        }
        // Clear the WAL/meta siblings and the live db regardless: a reactivation
        // restores or reuses the `.hibernated` copy.
        for suffix in ["-wal", "-shm"] {
            let mut sibling = db.clone().into_os_string();
            sibling.push(suffix);
            let _ = self.ltx_host.remove_file(&PathBuf::from(sibling));
        }
        let _ = self.ltx_host.remove_dir_all(&Db::meta_path_for_path(&db));
        if !preserve_local {
            let _ = self.ltx_host.remove_file(&db);
        }
    }

    /// Copy the live epoch into a private read-only snapshot for inspection.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<RestoredSnapshot>> {
        let source = self.db_path(cell, epoch);
        if !self
            .ltx_host
            .metadata(&source)
            .is_ok_and(|metadata| metadata.is_file)
        {
            return Ok(None);
        }
        let directory = self.watch.join(format!(".inspect-{cell}-e{epoch}"));
        let _ = self.ltx_host.remove_dir_all(&directory);
        self.ltx_host.create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        sqlite_snapshot(&source, &path, self.vfs_name.as_deref())?;
        Ok(Some(RestoredSnapshot::new(
            epoch,
            path,
            directory,
            self.ltx_host.filesystem(),
        )))
    }

    /// Restore the newest durable replica into a private snapshot without
    /// claiming or activating the cell.
    pub async fn restore_snapshot(&self, cell: &str) -> anyhow::Result<Option<RestoredSnapshot>> {
        let Some(epoch) = self.highest_nonempty_epoch(cell).await? else {
            return Ok(None);
        };
        let directory = self.watch.join(format!(".restore-{cell}"));
        let _ = self.ltx_host.remove_dir_all(&directory);
        self.ltx_host.create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        replica::restore_with_host_and_download_slots(
            &self.client_for(cell, epoch),
            &path,
            TXID(0),
            self.ltx_host.clone(),
            self.restore_slots.clone(),
        )
        .await
        .map_err(|error| anyhow!("restore snapshot {cell} e{epoch}: {error}"))?;
        Ok(Some(RestoredSnapshot::new(
            epoch,
            path,
            directory,
            self.ltx_host.filesystem(),
        )))
    }

    pub fn prune_local_cache(&self, max_bytes: u64) -> std::io::Result<(usize, usize, u64)> {
        self.preserved
            .lock()
            .expect("preserved cache poisoned")
            .prune(&self.watch, max_bytes)
    }

    /// Close the replicator handle while retaining the live database and WAL
    /// exactly where the local path encodes them.
    pub fn close_for_reload(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
        let path = self.db_path(cell, epoch);
        anyhow::ensure!(
            self.ltx_host
                .metadata(&path)
                .is_ok_and(|metadata| metadata.is_file),
            "resident database is missing: {}",
            path.display()
        );
        Ok(())
    }

    /// Enumerate live-named databases. Cached `.evicted` files are separate
    /// and remain under the ordinary cache byte limit.
    pub fn local_cells(&self) -> Vec<celld_logic::LocalCell> {
        let mut cells = Vec::new();
        let filesystem = self.ltx_host.filesystem();
        let Ok(cell_dirs) = filesystem.read_dir(&self.watch) else {
            return cells;
        };
        for cell_dir in cell_dirs {
            if !cell_dir.is_dir {
                continue;
            }
            let Some(cell) = cell_dir.file_name.to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(epochs) = filesystem.read_dir(&cell_dir.path.join("ltx")) else {
                continue;
            };
            for epoch_dir in epochs {
                let Some(epoch) = epoch_dir
                    .file_name
                    .to_str()
                    .and_then(|name| name.strip_prefix('e'))
                    .and_then(|epoch| epoch.parse::<u64>().ok())
                else {
                    continue;
                };
                if filesystem
                    .metadata(&epoch_dir.path.join("db.sqlite"))
                    .is_ok_and(|metadata| metadata.is_file)
                {
                    cells.push(celld_logic::LocalCell {
                        id: cell.clone(),
                        epoch,
                    });
                }
            }
        }
        cells.sort();
        cells.dedup();
        cells
    }

    /// Delete stale live-named epochs after the runtime has identified and
    /// closed its exact resident set. Remote replicas remain authoritative.
    pub fn prune_stale_live(
        &self,
        keep: &std::collections::BTreeSet<(String, u64)>,
    ) -> anyhow::Result<usize> {
        let stale: Vec<_> = self
            .local_cells()
            .into_iter()
            .filter(|cell| !keep.contains(&(cell.id.clone(), cell.epoch)))
            .collect();
        for cell in &stale {
            let db = self.db_path(&cell.id, cell.epoch);
            if let Some(parent) = db.parent() {
                self.ltx_host.remove_dir_all(parent)?;
                let mut preserved = self.preserved.lock().expect("preserved cache poisoned");
                preserved.forget(&db.with_extension("evicted"));
                preserved.forget(&db.with_extension("hibernated"));
            }
        }
        let remaining: std::collections::BTreeSet<_> = self
            .local_cells()
            .into_iter()
            .map(|cell| (cell.id, cell.epoch))
            .collect();
        anyhow::ensure!(
            &remaining == keep,
            "clean reload inventory mismatch after pruning: expected {}, found {}",
            keep.len(),
            remaining.len()
        );
        Ok(stale.len())
    }

    /// There is no external process, so the in-process replicator is healthy while celld runs.
    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        Ok(None)
    }
}

/// One capture+upload for a cell: advance its durable position on success and
/// wake its waiters. Everything committed before the capture is durable once
/// uploaded, so the target is read before `db.sync`.
///
/// The capture runs under the replica mutex on a blocking thread; the upload
/// runs OFF the mutex. A slow bucket PUT held the lock for its whole round
/// trip, and the log tier's ship capture queued behind it — the lab measured
/// 4-6 s ack spikes at every flush collision. Uploads are idempotent
/// overwrites keyed by TXID, and `syncing` already guarantees one pass per
/// cell, so staging then uploading lock-free is the same protocol
/// `Replica::sync` runs, minus the contention.
///
/// `Some(true)` means success, `Some(false)` means failure, and `None` means
/// that the replica lost its database.
async fn sync_cell(handle: CellHandle) -> Option<bool> {
    // Tickets taken before the capture: their writes committed before
    // `db.sync` runs, so it captures them. Read before the capture so a
    // ticket taken during the sync is credited by the next one, not this.
    type Staged = (u64, Vec<(u64, Vec<u8>)>);
    let handle_ = handle.clone();
    let staged: Option<Result<Staged, ()>> = asyncrt::blocking(move || {
        let captured = handle_.req_seq.load(Ordering::SeqCst);
        let mut replica = handle_.replica.lock().unwrap();
        let from = replica.pos().txid.0 + 1;
        let db = replica.db_mut()?;
        if let Err(error) = db.sync() {
            warn!(%error, "ltx wal capture failed");
            return Some(Err(()));
        }
        let dpos = match db.pos() {
            Ok(pos) => pos,
            Err(error) => {
                warn!(%error, "ltx position read failed");
                return Some(Err(()));
            }
        };
        let mut files = Vec::new();
        for txid in from..=dpos.txid.0 {
            match db.read_ltx_file(0, TXID(txid), TXID(txid)) {
                Ok(bytes) => files.push((txid, bytes)),
                Err(error) => {
                    warn!(%error, txid, "read staged l0 failed");
                    return Some(Err(()));
                }
            }
        }
        Some(Ok((captured, files)))
    })
    .await
    .unwrap_or(Some(Err(())));
    let (captured, files) = match staged {
        None => return None,
        Some(Err(())) => {
            handle.ready.notify_waiters();
            return Some(false);
        }
        Some(Ok(staged)) => staged,
    };
    let last = files.last().map(|(txid, _)| *txid);
    for (txid, bytes) in &files {
        if let Err(error) = handle
            .client
            .write_ltx_file(0, TXID(*txid), TXID(*txid), bytes)
            .await
        {
            warn!(%error, txid, "ltx upload failed");
            handle
                .last_sync_ms
                .store(asyncrt::wall_ms().max(0) as u64, Ordering::SeqCst);
            handle.ready.notify_waiters();
            return Some(false);
        }
    }
    if let Some(last) = last {
        // Advance the replica's uploaded watermark; `syncing` serializes
        // passes, so nothing else moved it meanwhile.
        handle
            .replica
            .lock()
            .unwrap()
            .seed_pos(Pos::new(TXID(last), 0));
        handle.durable_txid.fetch_max(last, Ordering::SeqCst);
    }
    handle.synced_seq.fetch_max(captured, Ordering::SeqCst);
    maybe_queue_compaction(&handle, handle.durable_txid.load(Ordering::SeqCst));
    handle
        .last_sync_ms
        .store(asyncrt::wall_ms().max(0) as u64, Ordering::SeqCst);
    handle.ready.notify_waiters();
    Some(true)
}

fn maybe_queue_compaction(handle: &CellHandle, durable_txid: u64) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    if compaction.cancelled.load(Ordering::SeqCst)
        || durable_txid.saturating_sub(compaction.compacted_txid.load(Ordering::SeqCst))
            < compaction.min_txids
        || compaction
            .queued
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        return;
    }
    if compaction
        .queue
        .send(CompactionWork {
            cell: Arc::downgrade(handle),
            queued_at_mono_ms: asyncrt::mono_ms(),
        })
        .is_err()
    {
        compaction.queued.store(false, Ordering::SeqCst);
    }
}

fn cancel_compaction(handle: &CellHandle) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    compaction.cancelled.store(true, Ordering::SeqCst);
    compaction.cancel.notify_waiters();
}

fn start_compaction_loop(config: CompactionConfig) -> mpsc::UnboundedSender<CompactionWork> {
    let (queue, mut work) = mpsc::unbounded_channel::<CompactionWork>();
    let slots = Arc::new(Semaphore::new(config.concurrency));
    asyncrt::spawn(async move {
        while let Some(work) = work.recv().await {
            let Ok(permit) = slots.clone().acquire_owned().await else {
                break;
            };
            let Some(cell) = work.cell.upgrade() else {
                continue;
            };
            asyncrt::spawn(async move {
                let _permit = permit;
                compact_cell(cell, work.queued_at_mono_ms).await;
            })
            .detach();
        }
    })
    .detach();
    queue
}

async fn compact_cell(handle: CellHandle, queued_at_mono_ms: u64) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    let cancelled = compaction.cancel.notified();
    tokio::pin!(cancelled);
    if compaction.cancelled.load(Ordering::SeqCst) {
        return;
    }

    let queue_ms = asyncrt::mono_ms().saturating_sub(queued_at_mono_ms);
    let started = asyncrt::mono_ms();
    let compactor = ReplicaCompactor::new(&compaction.client)
        .with_host(compaction.host.clone())
        .with_verification(true)
        .with_local_path(&compaction.local_path)
        .with_limits(COMPACTION_MAX_FILES, COMPACTION_MAX_INPUT_BYTES);
    let worker = compactor.compact(1);
    tokio::pin!(worker);
    let result = asyncrt::select! {
        _ = &mut cancelled => None,
        result = &mut worker => Some(result),
    };

    let mut completed = false;
    match result {
        Some(Ok(Some(output))) => {
            let info = output.info;
            compaction
                .compacted_txid
                .store(info.max_txid.0, Ordering::SeqCst);
            completed = true;
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = info.level,
                min_txid = info.min_txid.0,
                max_txid = info.max_txid.0,
                input_objects = output.input_files,
                input_bytes = output.input_bytes,
                local_input_objects = output.local_input_files,
                remote_input_objects = output.input_files - output.local_input_files,
                output_bytes = info.size,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "ok",
                "compacted an additive LTX level"
            );
        }
        Some(Ok(None)) => {
            compaction
                .compacted_txid
                .store(handle.durable_txid.load(Ordering::SeqCst), Ordering::SeqCst);
            completed = true;
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "no_work",
                "the additive LTX level is current"
            );
        }
        Some(Err(error)) => {
            warn!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "error",
                %error,
                "additive LTX compaction failed"
            );
        }
        None => {
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "cancelled",
                "cancelled an additive LTX compaction"
            );
        }
    }
    compaction.queued.store(false, Ordering::SeqCst);

    if completed && !compaction.cancelled.load(Ordering::SeqCst) {
        // Pace consecutive rounds for one cell: a restart with a large tail
        // otherwise drains back-to-back for minutes. The pause matches the
        // round it follows (capped), so a cell compacts at half duty cycle
        // while the worker slot frees for other cells immediately — this
        // task detaches and does not hold the concurrency permit.
        let pause = Duration::from_millis(asyncrt::mono_ms().saturating_sub(started))
            .min(Duration::from_secs(2));
        let handle_ = handle.clone();
        asyncrt::spawn(async move {
            asyncrt::sleep(pause).await;
            let durable_txid = handle_.durable_txid.load(Ordering::SeqCst);
            maybe_queue_compaction(&handle_, durable_txid);
        })
        .detach();
    }
}

fn compaction_config_from_env() -> anyhow::Result<Option<CompactionConfig>> {
    // On by default. A mixed fleet must set `0` until every node can read
    // v0.5.2 block objects. An old reader cannot take over a cell after its
    // first L1 publication.
    let enabled = crate::env_vars::flag("CELLD_LTX_COMPACTION", true)?;
    if !enabled {
        return Ok(None);
    }

    let min_txids = crate::env_vars::with_default("CELLD_LTX_COMPACTION_MIN_TXIDS", 256)?;
    let concurrency = crate::env_vars::with_default("CELLD_LTX_COMPACTIONS", 2)?;
    anyhow::ensure!(
        min_txids >= 2,
        "CELLD_LTX_COMPACTION_MIN_TXIDS must be at least 2"
    );
    anyhow::ensure!(concurrency > 0, "CELLD_LTX_COMPACTIONS must be positive");
    Ok(Some(CompactionConfig {
        min_txids: min_txids as u64,
        concurrency,
    }))
}

/// The node's background sync loop: wake on a dirty cell (or a slow tick) and
/// launch a sync for every cell whose committed position runs ahead of its
/// durable one. Each cell's sync is an independent, self-rescheduling task —
/// the loop does *not* wait for the batch to finish — so one slow cell's upload
/// never stalls the others (a cell keeps its own cadence up to the concurrency
/// bound). A cell's writes reported between its syncs still clear on one upload:
/// the batching win, without the cross-cell head-of-line blocking.
async fn sync_loop(
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    dirty: Arc<Notify>,
    slots: Arc<Semaphore>,
    shipper: Arc<Mutex<Option<Arc<dyn Shipper>>>>,
    bundle_sink: Arc<Mutex<Option<Arc<dyn BundleSink>>>>,
    flush_ms: u64,
) {
    loop {
        asyncrt::select! {
            _ = dirty.notified() => {},
            _ = asyncrt::sleep(Duration::from_millis(25)) => {},
        }
        // The upload-cadence dial. With a healthy shipper installed, acks
        // ride the followers, so uploads become tiering and are PACED: an
        // immediate upload would hold the replica mutex for a bucket round
        // trip and the ship capture would queue behind it, putting the
        // bucket back on the ack path — the lab measured exactly that.
        // Without a shipper (or degraded), uploads run immediately: they
        // are the ack path again.
        let paced = flush_ms > 0
            && shipper
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|shipper| shipper.active());
        // With an active bundle sink, the bundle loop owns paced tiering
        // entirely — one PUT per node-flush instead of one per cell. This
        // loop then serves only the unpaced (degraded) mode and the direct
        // sync_wait callers, which are the drain points.
        let bundling = paced
            && bundle_sink
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|sink| sink.active());
        let now = asyncrt::wall_ms().max(0) as u64;
        let work: Vec<CellHandle> = {
            let map = cells.lock().unwrap();
            map.values()
                .filter(|c| {
                    #[cfg(all(test, celld_internal_tests))]
                    if asyncrt::sabotage_active(crate::host_services::EngineSabotage::HideDirtyCell)
                    {
                        return false;
                    }
                    c.req_seq.load(Ordering::SeqCst) > c.synced_seq.load(Ordering::SeqCst)
                        && !bundling
                        && (!paced
                            || now.saturating_sub(c.last_sync_ms.load(Ordering::SeqCst))
                                >= flush_ms)
                })
                .cloned()
                .collect()
        };
        for cell in work {
            // Claim the cell; skip if a sync is already in flight for it.
            if cell
                .syncing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            let slots = slots.clone();
            let dirty = dirty.clone();
            asyncrt::spawn(async move {
                // Keep syncing this cell while it stays dirty, rather than
                // notifying the main loop to re-scan every completion — that made
                // the loop wake O(cells) times and starved throughput as cells
                // accumulated. This is not a busy loop: each iteration awaits an
                // object-store upload (~one round-trip). A *failed* sync would
                // not, so it backs off, keeping the only tight iterations the
                // ones that actually uploaded.
                loop {
                    let ok = {
                        let _permit = slots.acquire().await;
                        sync_cell(cell.clone()).await
                    };
                    if cell.req_seq.load(Ordering::SeqCst) <= cell.synced_seq.load(Ordering::SeqCst)
                    {
                        break;
                    }
                    // Under pacing, one upload per wake: the next round waits
                    // for the flush interval instead of re-syncing here.
                    if paced {
                        break;
                    }
                    if ok != Some(true) {
                        asyncrt::sleep(Duration::from_millis(50)).await;
                    }
                }
                cell.syncing.store(false, Ordering::SeqCst);
                // A write landing in the clear window is picked up next tick;
                // nudge the loop so it does not wait the full interval.
                if cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst) {
                    dirty.notify_one();
                }
            })
            .detach();
        }
    }
}

/// The bundle loop: paced like the per-cell tiering it replaces, but the
/// unit is the node, not the cell. Every dirty cell's captured L0 segments
/// go up as ONE object per flush interval — the Class A collapse — and the
/// per-cell prefixes stay untouched until a drain point needs them. The
/// crediting mirrors sync_cell: `durable_txid` means bucket-covered,
/// whether by a per-cell object or a bundle row; the replica's own
/// position deliberately does NOT advance, so the direct sync_wait drain
/// still knows exactly which frames lack per-cell objects.
async fn bundle_loop(
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    sink: Arc<Mutex<Option<Arc<dyn BundleSink>>>>,
    flush_ms: u64,
) {
    if flush_ms == 0 {
        return;
    }
    let mut tick = asyncrt::interval(Duration::from_millis(flush_ms));
    tick.set_missed_tick_behavior(asyncrt::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let installed = sink.lock().unwrap().clone();
        let Some(active) = installed.filter(|sink| sink.active()) else {
            continue;
        };
        let work: Vec<((String, u64), CellHandle)> = {
            let map = cells.lock().unwrap();
            map.iter()
                .filter(|(_, cell)| {
                    cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst)
                })
                .map(|(key, cell)| (key.clone(), cell.clone()))
                .collect()
        };
        if work.is_empty() {
            continue;
        }
        type Credits = Vec<(CellHandle, u64, u64)>;
        let (entries, credits): (Vec<celld_ltx::bundle::BundleEntry>, Credits) =
            asyncrt::blocking(move || {
                let mut entries = Vec::new();
                let mut credits = Vec::new();
                for ((cell, epoch), handle) in work {
                    let tickets = handle.req_seq.load(Ordering::SeqCst);
                    let mut replica = handle.replica.lock().unwrap();
                    let Some(db) = replica.db_mut() else { continue };
                    if db.sync().is_err() {
                        continue;
                    }
                    let Ok(pos) = db.pos() else { continue };
                    let from = handle.durable_txid.load(Ordering::SeqCst) + 1;
                    let mut complete = true;
                    for txid in from..=pos.txid.0 {
                        match db.read_ltx_file(0, TXID(txid), TXID(txid)) {
                            Ok(bytes) => entries.push(celld_ltx::bundle::BundleEntry {
                                cell: cell.clone(),
                                cell_epoch: epoch,
                                txid,
                                bytes,
                            }),
                            Err(error) => {
                                warn!(%error, txid, "read staged l0 for bundle failed");
                                complete = false;
                                break;
                            }
                        }
                    }
                    drop(replica);
                    if complete {
                        credits.push((handle, tickets, pos.txid.0));
                    }
                }
                (entries, credits)
            })
            .await
            .unwrap_or_default();
        if credits.is_empty() {
            continue;
        }
        let count = entries.len();
        if entries.is_empty() || active.put_bundle(entries).await {
            if count > 0 {
                info!(
                    event = "log_bundle_flush",
                    entries = count,
                    cells = credits.len(),
                    "flushed a bundle"
                );
            }
            for (handle, tickets, position) in credits {
                handle.durable_txid.fetch_max(position, Ordering::SeqCst);
                handle.synced_seq.fetch_max(tickets, Ordering::SeqCst);
                handle
                    .last_sync_ms
                    .store(asyncrt::wall_ms().max(0) as u64, Ordering::SeqCst);
                // The compactor's overlay client sees bundle rows, so
                // bundle credits queue compaction like per-cell uploads do
                // — compaction is the continuous drain into pure layout.
                maybe_queue_compaction(&handle, position);
                handle.ready.notify_waiters();
            }
        }
    }
}

/// The log tier's group-commit loop, `sync_loop`'s fleet twin: wake on a
/// gate ticket, capture every dirty cell's new L0 segments in one blocking
/// pass, ship them as one batch, and credit the tickets the capture
/// covered. One batch in flight is what keeps every follower's fragment
/// contiguous, and nothing on this path waits for the bucket.
async fn ship_loop(
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    dirty_ship: Arc<Notify>,
    shipper: Arc<Mutex<Option<Arc<dyn Shipper>>>>,
) {
    // The truncation ledger is a core decision
    // (celld_logic::log_tier::ShipLedger): outstanding batches carry the
    // cells and top TXIDs, a batch is covered once every cell's durable
    // position passes its top TXID — bundle credits do this within a
    // flush interval — and the covered watermark rides the next append as
    // the followers' truncate_to. This is what bounds follower disks, and
    // the ledger's epoch reset is what keeps a stale watermark from
    // truncating a fresh fragment.
    let mut ledger: celld_logic::log_tier::ShipLedger<Vec<(CellHandle, u64)>> =
        celld_logic::log_tier::ShipLedger::default();
    loop {
        asyncrt::select! {
            _ = dirty_ship.notified() => {},
            _ = asyncrt::sleep(Duration::from_millis(25)) => {},
        }
        let installed = shipper.lock().unwrap().clone();
        let Some(active) = installed.filter(|shipper| shipper.active()) else {
            continue;
        };
        #[cfg(all(test, celld_internal_tests))]
        let skip_observe_epoch =
            asyncrt::sabotage_active(crate::host_services::EngineSabotage::SkipObserveLogEpoch);
        #[cfg(not(all(test, celld_internal_tests)))]
        let skip_observe_epoch = false;
        if !skip_observe_epoch {
            ledger.observe_epoch(active.epoch());
        }
        let work: Vec<((String, u64), CellHandle)> = {
            let map = cells.lock().unwrap();
            map.iter()
                .filter(|(_, cell)| {
                    let req = cell.req_seq.load(Ordering::SeqCst);
                    req > cell.shipped_seq.load(Ordering::SeqCst)
                        && req > cell.synced_seq.load(Ordering::SeqCst)
                })
                .map(|(key, cell)| (key.clone(), cell.clone()))
                .collect()
        };
        if work.is_empty() {
            continue;
        }
        let round = asyncrt::mono_ms();
        type Credits = Vec<(CellHandle, u64, u64)>;
        let (entries, credits): (Vec<ShipEntry>, Credits) = asyncrt::blocking(move || {
            let mut entries = Vec::new();
            let mut credits = Vec::new();
            for ((cell, epoch), handle) in work {
                // Tickets taken before the capture are covered by it —
                // the same discipline as sync_cell.
                let tickets = handle.req_seq.load(Ordering::SeqCst);
                let mut replica = handle.replica.lock().unwrap();
                let Some(db) = replica.db_mut() else { continue };
                if db.sync().is_err() {
                    continue;
                }
                let Ok(pos) = db.pos() else { continue };
                let from = handle.shipped_txid.load(Ordering::SeqCst) + 1;
                let mut complete = true;
                for txid in from..=pos.txid.0 {
                    match db.read_ltx_file(0, TXID(txid), TXID(txid)) {
                        Ok(bytes) => entries.push(ShipEntry {
                            cell: cell.clone(),
                            epoch,
                            txid,
                            bytes,
                        }),
                        // A pruned L0 the bucket already holds is not a
                        // gap the followers need filled; anything else
                        // leaves the cell uncredited for this round.
                        Err(_) if txid <= handle.durable_txid.load(Ordering::SeqCst) => {}
                        Err(_) => {
                            complete = false;
                            break;
                        }
                    }
                }
                drop(replica);
                if complete {
                    credits.push((handle, tickets, pos.txid.0));
                }
            }
            (entries, credits)
        })
        .await
        .unwrap_or_default();
        if credits.is_empty() {
            continue;
        }
        ledger.advance(|cells| {
            cells
                .iter()
                .all(|(handle, txid)| handle.durable_txid.load(Ordering::SeqCst) >= *txid)
        });
        let covered_seq = ledger.covered_seq();
        let captured_ms = asyncrt::mono_ms().saturating_sub(round);
        let shipped = if entries.is_empty() {
            Some(covered_seq)
        } else {
            active.ship(&entries, covered_seq).await
        };
        if let Some(last_seq) = shipped {
            if last_seq > covered_seq {
                ledger.shipped(
                    last_seq,
                    credits
                        .iter()
                        .map(|(handle, _, position)| (handle.clone(), *position))
                        .collect(),
                );
            }
            info!(
                event = "log_ship_round",
                entries = entries.len(),
                cells = credits.len(),
                capture_ms = captured_ms,
                ship_ms = asyncrt::mono_ms()
                    .saturating_sub(round)
                    .saturating_sub(captured_ms),
                "shipped a log batch"
            );
            for (handle, tickets, position) in credits {
                handle.shipped_txid.fetch_max(position, Ordering::SeqCst);
                handle.shipped_seq.fetch_max(tickets, Ordering::SeqCst);
                handle.ready.notify_waiters();
            }
            active.batch_credited();
        }
    }
}

/// Node-level object-store config (no per-cell prefix). `build_store` on this
/// yields the one shared client; per-cell clients set only `path`.
fn node_config(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> ObjectStoreConfig {
    let endpoint = endpoint.unwrap_or_default().to_string();
    // Static credentials come from the managed control plane when present,
    // else the `AWS_*` env the node already carries. Without this,
    // `build_store` sees empty keys and object_store falls back to the
    // instance credential provider, which off-EC2 sends unsigned requests (R2
    // answers "404 page not found").
    let env = |key: &str| std::env::var(key).ok().filter(|value| !value.is_empty());
    let access_key_id = credentials
        .map(|c| c.access_key_id.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret_access_key = credentials
        .map(|c| c.secret_access_key.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    // Temporary R2/STS credentials require the session token, or signing fails.
    let session_token = credentials
        .and_then(|c| c.session_token.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SESSION_TOKEN"))
        .unwrap_or_default();
    ObjectStoreConfig {
        bucket: bucket.to_string(),
        path: String::new(),
        region: region.to_string(),
        // A custom endpoint (R2/MinIO) uses path-style addressing, matching
        // `ObjectStoreConfig::from_url`'s default for non-AWS hosts.
        force_path_style: !endpoint.is_empty(),
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        skip_verify: false,
        part_size: 0,
        timestamp_metadata_key: TimestampMetadataKey::default(),
    }
}

fn production_ltx_host() -> LtxHost {
    execution_domain_ltx_host()
}

#[cfg(all(test, celld_internal_tests))]
fn deterministic_ltx_host() -> LtxHost {
    execution_domain_ltx_host().with_compaction_input_drop(|| {
        asyncrt::sabotage_active(crate::host_services::EngineSabotage::DropCompactionInput)
    })
}

fn execution_domain_ltx_host() -> LtxHost {
    let filesystem = asyncrt::fs();
    let age_filesystem = filesystem.clone();
    let read_filesystem = filesystem.clone();
    LtxHost::new(
        asyncrt::wall_ms,
        move |path| file_age(age_filesystem.as_ref(), path),
        move |path| {
            let filesystem = read_filesystem.clone();
            async move {
                asyncrt::blocking(move || filesystem.read(&path))
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?
            }
        },
        |job| async move {
            asyncrt::blocking(job)
                .await
                .map_err(|error| HostTaskError::new(error.to_string()))
        },
    )
    .with_filesystem(filesystem)
}

fn file_age(filesystem: &dyn celld_ltx::FileSystem, path: &Path) -> std::io::Result<Duration> {
    let modified = filesystem.metadata(path)?.modified_unix_millis;
    Ok(Duration::from_millis(
        asyncrt::wall_ms().saturating_sub(modified).max(0) as u64,
    ))
}
