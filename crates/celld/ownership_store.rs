// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Bucket ownership effect adapter, over either conditional-write dialect.
//!
//! This module deliberately contains serialization, wall-clock sampling, SDK
//! configuration and error classification only. Ownership decisions remain in
//! `celld-logic`.

use crate::bucket::Bucket;
use anyhow::Context;
use celld_logic::{
    CapacityPeer, CasGuard, CasOutcome, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord,
};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct OwnerWire<'a> {
    node: &'a str,
    epoch: u64,
}

#[derive(Deserialize)]
struct OwnerWireOwned {
    node: String,
    epoch: u64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct NodeLeaseWire {
    pub(crate) node: String,
    pub(crate) expires_ms: u64,
    #[serde(default)]
    pub(crate) addr: String,
    #[serde(default)]
    pub(crate) probe_public_key: String,
    #[serde(default)]
    pub(crate) peer_protocol: u16,
    #[serde(default, rename = "ownership_index_generation")]
    generation: String,
    #[serde(default)]
    pub(crate) load: NodeLoadWire,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct NodeLoadWire {
    pub sampled_ms: u64,
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    /// What the pressure classifier reads, published next to `rss_bytes` so a
    /// gap between the two is visible.
    ///
    /// `None` means the node did not report it, which a node before this field
    /// existed does not. That is not the same as zero, and a consumer that
    /// ranks nodes must not read a silent zero as the emptiest node in the
    /// fleet for the length of a rolling upgrade.
    #[serde(default)]
    pub in_use_bytes: Option<u64>,
    pub cpu_percent_x100: u64,
    pub open_fds: u64,
    pub fd_limit: u64,
    pub pressured: bool,
    pub shed_cells: u64,
    /// Cold demand queued behind the activation ceiling. Zero in steady
    /// state; positive only while a restore burst saturates the node, so a
    /// rollout waits it out before it restarts the next node.
    #[serde(default)]
    pub restoring: u64,
}

/// A node record older than this is not worth reading. Three lease
/// lifetimes, floored, so a fleet with a short TTL does not discard records
/// a slow renewal would have refreshed.
const CAPACITY_RECORD_RECENCY_FLOOR_MS: u64 = 60_000;

fn capacity_record_is_recent(last_modified_secs: i64, now_ms: u64, lease_ttl_ms: u64) -> bool {
    let window_ms = lease_ttl_ms
        .saturating_mul(3)
        .max(CAPACITY_RECORD_RECENCY_FLOOR_MS);
    last_modified_secs >= (now_ms.saturating_sub(window_ms) / 1_000) as i64
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The production-compatible conditional object store used by ownership
/// effects. A failed write is always reported to the core as ambiguous unless
/// the store definitively returned HTTP 412.
pub struct BucketOwnership {
    bucket: Bucket,
    lease_bucket: Bucket,
    node: String,
    probe_public_key: String,
    live: Arc<LiveLoad>,
    lease_ttl_ms: u64,
}

/// What this node currently looks like, for peers deciding where to place a
/// cell. The executor owns these numbers and publishes them on every lease
/// renewal; nothing here decides anything locally.
/// The shedding latch, readable without the core loop.
///
/// Admission on the stateless path is a relaxed atomic load, not a message:
/// that path is the hello-world path, and asking the core would reinstate the
/// round trip removed to reach its throughput. Set once at startup, alongside
/// the counters peers rank this node by.
static NODE_LOAD: std::sync::OnceLock<std::sync::Arc<LiveLoad>> = std::sync::OnceLock::new();

pub fn set_node_load(load: std::sync::Arc<LiveLoad>) {
    let _ = NODE_LOAD.set(load);
}

/// Is this node over a resource ceiling and recovering? False when nothing
/// publishes load (no bucket), which is also when there is no pressure
/// sampler to say otherwise.
pub fn node_is_shedding() -> bool {
    NODE_LOAD
        .get()
        .is_some_and(|load| load.pressured.load(std::sync::atomic::Ordering::Relaxed))
}

#[derive(Debug, Default)]
pub struct LiveLoad {
    pub resident_cells: AtomicUsize,
    pub host_websockets: AtomicUsize,
    pub cpu_percent_x100: AtomicU64,
    pub pressured: AtomicBool,
    /// Cells shed since this process started. Monotonic, and only ever read
    /// by a human or a diagnostic -- placement uses the levels, not the rate.
    pub shed_cells: AtomicU64,
    /// Cold demand queued behind the activation ceiling, republished on every
    /// lease renewal for a rollout to pace on.
    pub restoring: AtomicU64,
}

impl BucketOwnership {
    /// Creates an adapter with an isolated lease pool and the process key
    /// advertised for challenge-bound direct probes.
    pub fn new(
        bucket: Bucket,
        lease_bucket: Bucket,
        node: String,
        probe_public_key: String,
    ) -> Self {
        Self {
            bucket,
            lease_bucket,
            node,
            probe_public_key,
            live: Arc::new(LiveLoad::default()),
            lease_ttl_ms: 0,
        }
    }

    /// The lease lifetime this fleet renews on, used to decide which node
    /// records are still worth reading.
    pub fn with_lease_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.lease_ttl_ms = ttl_ms;
        self
    }

    /// The counters this node publishes to its peers.
    pub fn live(&self) -> Arc<LiveLoad> {
        self.live.clone()
    }

    /// The storage scheme this adapter coordinates through (`s3` or `gs`),
    /// for the startup banner.
    pub fn storage_scheme(&self) -> &'static str {
        self.bucket.scheme()
    }

    /// Stable identity for this exact lease-writing process.
    pub fn process_generation(&self) -> Option<&str> {
        (!self.probe_public_key.is_empty()).then_some(self.probe_public_key.as_str())
    }

    pub async fn read_owner(&self, cell: &str) -> anyhow::Result<Option<OwnerRecord>> {
        let key = format!("cells/{cell}/own.json");
        let Some((owner, etag)) = load_json::<OwnerWireOwned>(&self.bucket, &key).await? else {
            return Ok(None);
        };
        Ok(Some(OwnerRecord {
            node: (!owner.node.is_empty()).then_some(owner.node),
            epoch: owner.epoch,
            etag,
        }))
    }

    pub async fn read_node_lease(&self, owner: &str) -> anyhow::Result<Option<NodeLeaseRecord>> {
        load_node_lease(&self.bucket, owner).await
    }

    /// Read this process's authority record through the isolated lease pool.
    pub async fn read_self_node_lease(
        &self,
        owner: &str,
    ) -> anyhow::Result<Option<NodeLeaseRecord>> {
        load_node_lease(&self.lease_bucket, owner).await
    }

    /// Enumerate the fleet membership records used for advisory placement.
    /// The adapter owns pagination and bounded I/O concurrency; the core gets
    /// every decoded observation and owns all filtering and selection policy.
    pub async fn read_capacity_peers(&self) -> anyhow::Result<Vec<CapacityPeer>> {
        const READ_CONCURRENCY: usize = 16;
        let current_ms = now_ms();
        let mut nodes = Vec::new();
        for object in self.bucket.list("nodes/").await? {
            // A record nothing has rewritten in several lease lifetimes
            // belongs to a node that is not coming back. Skipping it here
            // is the difference between reading the live fleet and
            // reading every node that has ever run: the listing is what
            // the placement decision costs, and it is paid on every
            // unowned cell.
            if !capacity_record_is_recent(
                object.last_modified.timestamp(),
                current_ms,
                self.lease_ttl_ms,
            ) {
                continue;
            }
            let Some(node) = object
                .location
                .as_ref()
                .strip_prefix("nodes/")
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if !node.is_empty() {
                nodes.push(node.to_string());
            }
        }
        nodes.sort();
        nodes.dedup();

        let mut reads = stream::iter(nodes.into_iter().map(|node| async move {
            let key = format!("nodes/{node}.json");
            Ok::<_, anyhow::Error>(load_json::<NodeLeaseWire>(&self.bucket, &key).await?.map(
                |(lease, _)| CapacityPeer {
                    node: lease.node,
                    addr: lease.addr,
                    expires_ms: lease.expires_ms,
                    peer_protocol: lease.peer_protocol,
                    sampled_ms: lease.load.sampled_ms,
                    resident_cells: lease.load.resident_cells,
                    host_websockets: lease.load.host_websockets,
                    rss_bytes: lease.load.rss_bytes,
                    in_use_bytes: lease.load.in_use_bytes,
                    pressured: lease.load.pressured,
                },
            ))
        }))
        .buffer_unordered(READ_CONCURRENCY);
        let mut peers = Vec::new();
        while let Some(peer) = reads.next().await {
            if let Some(peer) = peer? {
                peers.push(peer);
            }
        }
        Ok(peers)
    }

    /// Publish a cell as unowned, keeping its epoch.
    ///
    /// Read-then-conditional-write, because the release is only safe against
    /// the exact record this node wrote: a takeover in the meantime means the
    /// cell is someone else's now, and blanking it would strip a live owner's
    /// claim. Rejection is an ordinary outcome, not an error -- the record
    /// keeps naming whoever it names, and nothing was lost.
    pub async fn release_owner(&self, cell: &str, epoch: u64) -> anyhow::Result<CasOutcome> {
        let Some(current) = self.read_owner(cell).await? else {
            return Ok(CasOutcome::Rejected);
        };
        if current.node.as_deref() != Some(self.node.as_str()) || current.epoch != epoch {
            return Ok(CasOutcome::Rejected);
        }
        let key = format!("cells/{cell}/own.json");
        let body = serde_json::to_vec(&OwnerWire { node: "", epoch })?;
        match self.bucket.put_cas(&key, body, Some(&current.etag)).await? {
            Some(_) => Ok(CasOutcome::Applied),
            None => Ok(CasOutcome::Rejected),
        }
    }

    pub async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> anyhow::Result<CasOutcome> {
        let key = format!("cells/{cell}/own.json");
        let body = serde_json::to_vec(&OwnerWire {
            node: &self.node,
            epoch,
        })?;
        let etag = match &guard {
            CasGuard::Absent => None,
            CasGuard::Match(etag) => Some(etag.as_str()),
        };
        match self.bucket.put_cas(&key, body, etag).await? {
            Some(_) => Ok(CasOutcome::Applied),
            None => Ok(CasOutcome::Rejected),
        }
    }

    pub async fn cas_node_lease(
        &self,
        guard: CasGuard,
        record: &NodeLeaseRecord,
    ) -> anyhow::Result<LeaseCasOutcome> {
        if self.probe_public_key.is_empty() {
            return Err(anyhow::anyhow!(
                "refusing to publish a node lease without a signed-probe key"
            ));
        }
        let key = format!("nodes/{}.json", self.node);
        let body = serde_json::to_vec(&NodeLeaseWire {
            node: record.node.clone(),
            expires_ms: record.expires_ms,
            addr: record.addr.clone(),
            probe_public_key: self.probe_public_key.clone(),
            peer_protocol: record.peer_protocol,
            generation: record.generation.clone(),
            load: process_load(&self.live),
        })?;
        let etag = match &guard {
            CasGuard::Absent => None,
            CasGuard::Match(etag) => Some(etag.as_str()),
        };
        match self.lease_bucket.put_cas(&key, body, etag).await? {
            Some(etag) => Ok(LeaseCasOutcome::Applied { etag }),
            None => Ok(LeaseCasOutcome::Rejected),
        }
    }
}

pub(crate) async fn load_node_lease(
    bucket: &Bucket,
    owner: &str,
) -> anyhow::Result<Option<NodeLeaseRecord>> {
    let key = format!("nodes/{owner}.json");
    Ok(load_json::<NodeLeaseWire>(bucket, &key)
        .await?
        .map(|(lease, etag)| NodeLeaseRecord {
            node: lease.node,
            addr: lease.addr,
            expires_ms: lease.expires_ms,
            peer_protocol: lease.peer_protocol,
            generation: lease.generation,
            etag,
        }))
}

async fn load_json<T: for<'de> Deserialize<'de>>(
    bucket: &Bucket,
    key: &str,
) -> anyhow::Result<Option<(T, String)>> {
    let Some((bytes, etag)) = bucket.get(key).await? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {}://{}/{key}", bucket.scheme(), bucket.name))?;
    Ok(Some((value, etag)))
}

fn process_load(live: &LiveLoad) -> NodeLoadWire {
    // One sample for both numbers. Reading the resident set size here and the
    // in-use figure from a value the actor wrote on its own timer would publish
    // a pair from two instants, and before the first sample it would publish a
    // real resident set size beside an in-use figure of zero -- which reads as
    // total allocator retention.
    let memory = crate::memory::sample();
    // A 1-byte floor is the sentinel a platform without /proc leaves behind.
    // Both numbers take it, so the in-use figure can never read as zero beside
    // a real resident set size.
    let rss_bytes = memory.rss_bytes.max(1);
    let in_use_bytes = memory.in_use_bytes.max(1).min(rss_bytes);

    #[cfg(target_os = "linux")]
    let open_fds = std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or_default();
    #[cfg(not(target_os = "linux"))]
    let open_fds = 0;

    #[cfg(unix)]
    let fd_limit = {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            limit.rlim_cur
        } else {
            0
        }
    };
    #[cfg(not(unix))]
    let fd_limit = 0;

    NodeLoadWire {
        sampled_ms: now_ms(),
        rss_bytes,
        in_use_bytes: Some(in_use_bytes),
        open_fds,
        fd_limit,
        cpu_percent_x100: live.cpu_percent_x100.load(Ordering::Relaxed),
        resident_cells: live.resident_cells.load(Ordering::Relaxed),
        host_websockets: live.host_websockets.load(Ordering::Relaxed),
        pressured: live.pressured.load(Ordering::Relaxed),
        shed_cells: live.shed_cells.load(Ordering::Relaxed),
        restoring: live.restoring.load(Ordering::Relaxed),
    }
}
