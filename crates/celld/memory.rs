// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What the process holds, and how much of that a cell actually holds.
//!
//! Two numbers, and they must come from one instant. The resident set size is
//! what the operating system reports. The in-use bytes are that number less the
//! pages the allocator keeps after a free, which no cell holds and no eviction
//! returns. The pressure classifier decides on the second and reports the
//! first, so a caller that samples them apart can publish a pair that never
//! existed -- including an in-use figure above the resident set size.
//! [`sample`] is therefore the only way to obtain them.

/// A memory sample, taken at one instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sample {
    pub rss_bytes: u64,
    pub in_use_bytes: u64,
}

/// The resident set size and the memory a cell holds, together.
pub fn sample() -> Sample {
    let rss_bytes = resident_bytes();
    Sample {
        rss_bytes,
        in_use_bytes: rss_bytes.saturating_sub(allocator_slack_bytes()),
    }
}

#[cfg(target_os = "linux")]
pub fn resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages.saturating_mul(page_size()))
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn page_size() -> u64 {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(size).ok().filter(|s| *s > 0).unwrap_or(4_096)
}

#[cfg(target_os = "macos")]
pub fn resident_bytes() -> u64 {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let got = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            (&raw mut info).cast(),
            size,
        )
    };
    if got == size {
        info.pti_resident_size
    } else {
        0
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn resident_bytes() -> u64 {
    0
}

/// The pages jemalloc holds but nothing uses: `stats.resident` less
/// `stats.allocated`. The statistics are cached, so the epoch must advance
/// first. A failure gives zero, which makes the sample equal to RSS.
pub fn allocator_slack_bytes() -> u64 {
    if tikv_jemalloc_ctl::epoch::advance().is_err() {
        return 0;
    }
    let Ok(resident) = tikv_jemalloc_ctl::stats::resident::read() else {
        return 0;
    };
    let Ok(allocated) = tikv_jemalloc_ctl::stats::allocated::read() else {
        return 0;
    };
    (resident as u64).saturating_sub(allocated as u64)
}

/// Ask jemalloc to give freed pages back on a timer. Its 10-second decay runs
/// only when a thread next calls the allocator, which a node that just shed its
/// working set does not do. This repairs what RSS reports, not the decision.
///
/// macOS has no background thread, so the failure is expected there and is
/// logged rather than raised. It matters to an operator: without the thread,
/// retention is never purged, and the absolute cap in `PressureConfig` is the
/// only thing between the process and a kill by the operating system.
pub fn tune_allocator() {
    if let Err(error) = tikv_jemalloc_ctl::background_thread::write(true) {
        tracing::warn!(
            %error,
            "the allocator will not run a background thread, so freed pages \
             return only when a thread allocates again"
        );
    }
}
