// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Memory-pressure shedding as a pure classifier of a memory sample plus the
//! prior shedding state (the hysteresis latch). No I/O, no clock — the env
//! read that builds the config stays at the edge (`main.rs`), and so does the
//! measurement, which has to ask both the operating system and the allocator.
//!
//! Residency is deliberately *not* here. A node's cell count is a hard cap
//! enforced at admission ([`crate::State::has_capacity`]), self-limiting and
//! known exactly; it is not a resource that needs a proactive walk down. This
//! classifier answers only the other question — "is this node out of memory
//! and must give cells back to recover?" — which a cell count cannot answer.
//! Conflating the two produced the placement churn and the admission wedge;
//! splitting them is what keeps each decision small.

/// Resource watermarks. Built once from the environment by the caller; the
/// core never reads the environment itself. Residency has no watermark here —
/// it is capped at admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PressureConfig {
    /// The ceiling on the memory the cells hold. This is the ordinary limit,
    /// and `CELLD_MAX_RSS_MB` sets it.
    pub high_bytes: Option<u64>,
    /// An absolute ceiling on the resident set size: 95% of the machine.
    ///
    /// `high_bytes` applies to `Load::in_use_bytes`, which excludes the pages
    /// the allocator keeps. That exclusion is what lets a node recover, but it
    /// is not exact: the allocator also holds metadata and fragmentation
    /// behind live allocations, and an eviction does not return those either.
    /// A node could therefore keep admitting while its true resident set
    /// climbed, and trade a wedge for a kill by the operating system.
    ///
    /// This is the floor under that trade. It reads the resident set size
    /// directly, so nothing can hide from it.
    ///
    /// It is a fixed share of the machine and is never derived from
    /// `high_bytes`. A first attempt placed it "at least 125% of the ceiling",
    /// which put it at exactly 100% of the machine for the default ceiling of
    /// 80% -- above anything the kernel would let the process reach, so the
    /// floor did not exist in the configuration that ships. See
    /// [`PressureConfig::from_limits`].
    pub rss_hard_bytes: Option<u64>,
}

/// Which ceilings are currently latched. Each one engages at its own ceiling
/// and releases at its own low watermark, so one crossing can never hold the
/// node against the other's watermark.
///
/// This is two booleans rather than the reported reason, because the reason
/// names only the more serious crossing. Deriving the latches from it lets a
/// hard-cap crossing arm the ordinary ceiling, and then a node stays closed on
/// a ceiling it never crossed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Latches {
    pub memory: bool,
    pub rss_hard: bool,
}

/// Which measurement a walk down is working against.
///
/// The futility stop compares a sample with the one the last cut measured, so
/// it has to know that the two samples are the same measurement. They are not
/// interchangeable: eviction moves the in-use figure at once and may never move
/// the resident set size at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metric {
    InUse,
    Rss,
}

/// A resource sample — the only input the classifier reads. `resident_cells`
/// is carried so a resource trigger can size its walk down as a proportion of
/// what is actually resident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Load {
    pub resident_cells: usize,
    /// Reported to peers and operators, never decided on.
    pub rss_bytes: u64,
    /// RSS minus the pages the allocator keeps but nothing uses. Shedding can
    /// only return the memory a cell holds, so only this may hold the latch --
    /// a classifier reading RSS never releases once jemalloc retains, however
    /// many cells the node gives back.
    pub in_use_bytes: u64,
}

/// The latch engaged because the memory the cells hold crossed the ceiling.
/// This is the ordinary case, and shedding relieves it.
pub const SHED_MEMORY: &str = "memory";

/// The latch engaged because the resident set size crossed the absolute cap.
/// Shedding may not relieve this one, so it is named apart: an operator who
/// sees it is looking at allocator retention that did not come back, and the
/// node is close to a kill by the operating system.
pub const SHED_RSS_HARD: &str = "rss-hard";

impl PressureConfig {
    /// Build the watermarks from the machine and the operator's setting.
    ///
    /// The policy lives here, not at the edge, because it is arithmetic with a
    /// correctness argument and it needs tests. The shell only supplies the two
    /// facts it can read: how much memory the process may use, and what
    /// `CELLD_MAX_RSS_MB` says. `None` for the setting means the default, and
    /// `Some(0)` disables pressure shedding altogether.
    ///
    /// The absolute cap is 95% of the machine and is never derived from the
    /// ceiling. A cap derived from the ceiling either lands above the machine
    /// (so it never fires) or below the ceiling (so it fires first on every
    /// sample and the classifier is reading the resident set size again). Both
    /// have shipped in this file; neither is a floor.
    ///
    /// When the machine size is unknown, the cap falls back to 125% of an
    /// explicit ceiling. That is worse than a share of the machine and better
    /// than no floor at all.
    pub fn from_limits(total_memory_bytes: Option<u64>, max_rss_mb: Option<u64>) -> Self {
        // A watermark of zero is worse than no watermark: every sample crosses
        // it and none can clear it, so the node latches on its first sample and
        // refuses every cell for its lifetime. A machine that reports nothing
        // usable therefore gets no watermark at all. Linux reports 0 for a
        // cgroup limit of `0` and for an unreadable `MemTotal`.
        let usable = |bytes: u64| (bytes > 0).then_some(bytes);
        let high_bytes = match max_rss_mb {
            None => total_memory_bytes.and_then(|total| usable(total / 5 * 4)),
            Some(0) => None,
            Some(megabytes) => usable(megabytes.saturating_mul(1024 * 1024)),
        };
        let rss_hard_bytes = high_bytes.and_then(|high| {
            // `checked_mul`, because the fallback multiplies an operator's
            // number: a saturating multiply followed by a divide would return a
            // cap *below* the ceiling, which is the shape this avoids.
            let fallback = high.checked_mul(5).map_or(u64::MAX, |scaled| scaled / 4);
            let cap = total_memory_bytes
                .and_then(|total| usable(total / 100 * 95))
                .unwrap_or(fallback);
            usable(cap)
        });
        Self {
            high_bytes,
            rss_hard_bytes,
        }
    }

    /// Does the ceiling sit at or above the absolute cap?
    ///
    /// Then the cap is the effective limit and the node decides on its resident
    /// set size, not on the memory the cells hold. That is the operator's
    /// choice -- they asked for a ceiling above the safety floor -- but it
    /// gives up the recovery property of this module, so the shell reports it.
    pub fn ceiling_above_cap(self) -> bool {
        match (self.high_bytes, self.rss_hard_bytes) {
            (Some(high), Some(cap)) => high >= cap,
            _ => false,
        }
    }

    /// Fold a sample into the latches, and say why the node is shedding.
    ///
    /// Each ceiling latches on its own crossing: it engages at its ceiling and
    /// releases at its own low watermark, which is 80% of that ceiling. The
    /// hard cap is reported first, because it is the more serious of the two.
    ///
    /// The latches are carried separately and not derived from the reported
    /// reason. Sharing them lets one crossing hold the node against the other's
    /// watermark, in either direction: a crossing of the ordinary ceiling then
    /// holds the node on a resident-set watermark, which is issue #36 again,
    /// and a hard-cap crossing then holds it on a ceiling it never crossed.
    pub fn classify(self, s: Load, was: Latches) -> (Latches, Option<&'static str>) {
        let over = |ceiling: Option<u64>, sample: u64, latched: bool| {
            ceiling.is_some_and(|c| sample >= c || (latched && sample > Self::low_watermark(c)))
        };
        let latches = Latches {
            memory: over(self.high_bytes, s.in_use_bytes, was.memory),
            rss_hard: over(self.rss_hard_bytes, s.rss_bytes, was.rss_hard),
        };
        let reason = if latches.rss_hard {
            Some(SHED_RSS_HARD)
        } else if latches.memory {
            Some(SHED_MEMORY)
        } else {
            None
        };
        (latches, reason)
    }

    /// The sample a latched ceiling releases at: 80% of the ceiling.
    ///
    /// One definition, because two callers read it. `classify` decides whether
    /// the latch still holds, and the walk down's stopping condition asks
    /// whether shedding can still get the sample down here. Two copies of the
    /// arithmetic drift, and a walk down that aims at the wrong number either
    /// stops above the line it could have reached or never stops at all.
    fn low_watermark(ceiling: u64) -> u64 {
        ceiling.saturating_mul(4) / 5
    }

    /// Where the walk down against `metric` has to get the sample to.
    ///
    /// `None` means no ceiling is configured for that measurement, so nothing
    /// can latch on it and no walk down works against it.
    pub fn resume_line(self, metric: Metric) -> Option<u64> {
        let ceiling = match metric {
            Metric::InUse => self.high_bytes,
            Metric::Rss => self.rss_hard_bytes,
        };
        ceiling.map(Self::low_watermark)
    }

    /// Which measurement the walk down must work against, given the latches.
    ///
    /// The ordinary ceiling wins whenever it is latched, even if the cap is
    /// latched too. Eviction relieves the ordinary ceiling and may do nothing
    /// at all for the cap, so choosing the cap's measurement while the node is
    /// genuinely over its ordinary ceiling stops the walk down exactly when it
    /// is needed.
    pub fn walk_metric(latches: Latches) -> Metric {
        if latches.memory {
            Metric::InUse
        } else {
            Metric::Rss
        }
    }

    /// How far to shed down to. A memory trigger takes a proportion of what was
    /// just measured because the effect of an eviction is not visible until
    /// the next sample.
    pub fn release_target(resident_cells: usize) -> usize {
        resident_cells.saturating_sub((resident_cells / 10).max(1))
    }
}

pub const MAX_OUTBOUND_PIN_PERCENT: usize = 50;

/// May another cell be pinned resident by an outbound WebSocket?
///
/// An outbound socket is not hibernatable, so eviction refuses its cell for as
/// long as it is open. That is correct — a live host transport cannot survive
/// eviction — but it means every pinned cell is removed from the eviction
/// pool. Pin the whole ceiling and a resource walk down has nothing to
/// nominate. The budget is node-wide, counted in pinned *cells* rather than
/// sockets, because one socket is enough to pin. `ceiling` is the hard resident
/// cap.
pub fn may_pin_outbound(pinned_cells: usize, ceiling: Option<usize>) -> bool {
    ceiling.is_none_or(|cap| {
        // At least one, always. The share alone rounds to zero below a ceiling
        // of two, which would make an outbound socket impossible on a small
        // node rather than merely budgeted.
        pinned_cells < (cap.saturating_mul(MAX_OUTBOUND_PIN_PERCENT) / 100).max(1)
    })
}
