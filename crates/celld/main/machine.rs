// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What the process can learn about where it is running.
//!
//! Two kinds of fact, both read once at startup or sampled on a timer:
//! the environment the operator set, and the machine underneath — memory,
//! CPU ticks, page size — which is per-platform and therefore duplicated
//! behind `cfg` for each one.
use super::*;

pub(crate) fn random_node_session_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let suffix = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("node_{suffix}")
}

pub(crate) fn random_peer_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    key
}

pub(crate) fn random_process_generation() -> String {
    random_peer_key()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Host resource sampling, ported from celld unchanged.
///
/// These are the only measurements the pressure decision reads, and they stay
/// here rather than in the core for the obvious reason: `/proc` is I/O, and a
/// core that reads it cannot be replayed.
#[derive(Default)]
pub(crate) struct ProcessLoadSampler {
    previous_cpu_ticks: Option<u64>,
    previous_sample: Option<std::time::Instant>,
}

impl ProcessLoadSampler {
    pub(crate) fn sample_cpu_percent_x100(&mut self) -> u64 {
        let Some(ticks) = process_cpu_ticks() else {
            return 0;
        };
        let now = std::time::Instant::now();
        let value = match (self.previous_cpu_ticks, self.previous_sample) {
            (Some(previous_ticks), Some(previous_sample)) => {
                let elapsed = previous_sample.elapsed().as_secs_f64();
                let ticks_per_second = clock_ticks_per_second() as f64;
                if elapsed > 0.0 && ticks_per_second > 0.0 {
                    (((ticks.saturating_sub(previous_ticks)) as f64 / ticks_per_second / elapsed)
                        * 10_000.0) as u64
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.previous_cpu_ticks = Some(ticks);
        self.previous_sample = Some(now);
        value
    }
}

#[cfg(target_os = "linux")]
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ticks() -> Option<u64> {
    None
}

#[cfg(unix)]
fn clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(ticks)
        .ok()
        .filter(|ticks| *ticks > 0)
        .unwrap_or(100)
}

#[cfg(not(unix))]
fn clock_ticks_per_second() -> u64 {
    100
}

/// Watermarks from celld's public environment contract.
/// `CELLD_PRESSURE_OWNERSHIP`: `release` (the default) or `sticky`.
/// The lease lifetime, as the capacity listing needs it to decide which node
/// records are stale enough to skip reading. Same variable and default the
/// lease itself uses.
/// Concurrent outbound WebSockets one cell may hold
/// (`CELLD_MAX_OUTBOUND_WEBSOCKETS`).
pub(crate) const DEFAULT_MAX_OUTBOUND_WEBSOCKETS: usize = 32;

/// Matches the fleet and ownership-store clients. Long enough to survive a
/// slow but live peer, short enough that a stale address does not hold a
/// request for the kernel's own connect timeout.
pub(crate) const PEER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub(crate) fn lease_ttl_ms_from_environment() -> u64 {
    celld::env_vars::positive_or("CELLD_TTL_MS", 10_000).expect("validated CELLD_TTL_MS")
}

pub(crate) fn ownership_on_evict_from_environment() -> anyhow::Result<OwnershipOnEvict> {
    match std::env::var("CELLD_PRESSURE_OWNERSHIP") {
        Err(_) => Ok(OwnershipOnEvict::Release),
        Ok(value) => match value.trim() {
            "release" => Ok(OwnershipOnEvict::Release),
            "sticky" => Ok(OwnershipOnEvict::Sticky),
            other => Err(anyhow::anyhow!(
                "CELLD_PRESSURE_OWNERSHIP must be `release` or `sticky`, not `{other}`"
            )),
        },
    }
}

/// Total memory this process may use: the cgroup limit when one applies
/// (containers), the machine otherwise.
#[cfg(target_os = "linux")]
fn total_memory_bytes() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(raw) = std::fs::read_to_string(path) {
            if let Ok(limit) = raw.trim().parse::<u64>() {
                // cgroup v1 reports "no limit" as a huge page-rounded
                // number; anything at or above 1 PiB means unlimited.
                if limit < (1 << 50) {
                    return Some(limit);
                }
            }
        }
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb = meminfo
        .lines()
        .find(|line| line.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "macos")]
fn total_memory_bytes() -> Option<u64> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let ok = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&raw mut size).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (ok == 0 && size > 0).then_some(size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn total_memory_bytes() -> Option<u64> {
    None
}

pub(crate) fn pressure_config_from_environment(
) -> anyhow::Result<celld_logic::pressure::PressureConfig> {
    // Residency is a hard cap (`CELLD_MAX_RESIDENT_CELLS` -> `Config::max_resident`),
    // enforced at admission -- not a pressure watermark, so it is not built here.
    // Memory shedding is on by default: a node that runs into its memory ceiling
    // must give cells back, not be killed. The arithmetic lives in the core,
    // where it is tested; the shell supplies only the two facts it can read.
    let config = celld_logic::pressure::PressureConfig::from_limits(
        total_memory_bytes(),
        celld::env_vars::optional::<u64>("CELLD_MAX_RSS_MB")?,
    );
    if config.ceiling_above_cap() {
        tracing::warn!(
            high_bytes = config.high_bytes,
            rss_hard_bytes = config.rss_hard_bytes,
            "CELLD_MAX_RSS_MB is at or above the absolute cap, so the node \
             decides on its resident set size and cannot recover from allocator \
             retention alone"
        );
    }
    Ok(config)
}

pub(crate) fn local_cache_max_bytes_from_environment() -> anyhow::Result<Option<u64>> {
    let bytes = celld::env_vars::with_default(
        "CELLD_LOCAL_CACHE_MAX_BYTES",
        DEFAULT_LOCAL_CACHE_MAX_BYTES,
    )?;
    Ok((bytes > 0).then_some(bytes))
}
