// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Durable types on the bucket contract between deployment tools and celld.
//! These objects are the interface; nothing else is exchanged.
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;

/// `deploy/<script>/<version>/manifest.json` — the normalized thing celld reads
/// to know what to run. The script name is deployment identity, not a fleet
/// selector: one fleet has one current application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default = "legacy_manifest_schema_version")]
    pub schema_version: u32,
    pub version: String,
    pub script_name: String,
    /// Absent for Wrangler asset-only deployments.
    pub main_module: Option<String>,
    /// Durable Object classes exported by the worker.
    pub do_classes: Vec<String>,
    /// Subset of `do_classes` that are SQLite-backed (from migrations).
    pub sqlite_classes: Vec<String>,
    pub modules: Vec<ModuleRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets: Option<AssetManifestRef>,
    /// Cron trigger expressions from the config's `triggers.crons`. They are
    /// deployment state rather than cell state: the reserved cron cell reads
    /// them from the manifest it is running under, so changing a schedule
    /// needs no migration of an already-armed alarm.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub crons: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_features: Vec<String>,
    /// wrangler's raw metadata, retained verbatim for anything we don't yet model.
    pub raw_metadata: serde_json::Value,
}

fn legacy_manifest_schema_version() -> u32 {
    1
}

/// Manifest `required_features` values this build can load. A manifest
/// requiring anything else must be rejected up front: `ModuleRef` tolerates
/// unknown fields, so an older node would otherwise deserialize the manifest
/// partially and fail (or misbehave) at worker load instead.
pub const SUPPORTED_DEPLOYMENT_FEATURES: &[&str] = &[
    FEATURE_ASSETS_V1,
    FEATURE_CRON_V1,
    FEATURE_D1_V1,
    FEATURE_SQLITE_VEC_V1,
    FEATURE_WASM_V1,
];

pub const FEATURE_ASSETS_V1: &str = "assets-v1";
/// A deployment with D1 databases. Required because a build without the
/// reserved `__D1Database` class would load the manifest and then fail every
/// `env.DB` call at request time, on a node the developer is not watching —
/// the gate moves that failure to the deploy.
pub const FEATURE_D1_V1: &str = "d1-v1";
/// A deployment with cron triggers. Required because a build without the
/// reserved cron cell would load the manifest, ignore `crons`, and silently
/// never fire — the quiet failure the gate exists to prevent.
pub const FEATURE_CRON_V1: &str = "cron-v1";
pub const FEATURE_SQLITE_VEC_V1: &str = "sqlite-vec-v1";
pub const FEATURE_WASM_V1: &str = "wasm-v1";

/// Reject a manifest requiring any feature this build does not support. Both
/// load paths (control-plane deployments and fleet pointer loads) must apply
/// the same gate, so it lives here beside the feature list.
pub fn validate_required_features(required: &[String]) -> anyhow::Result<()> {
    for feature in required {
        if !SUPPORTED_DEPLOYMENT_FEATURES.contains(&feature.as_str()) {
            anyhow::bail!(
                "deployment requires feature {feature:?} this celld build does not support; upgrade celld"
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleRef {
    pub name: String,
    pub bytes: usize,
    /// content hash of this module's bytes (hex, truncated)
    pub sha256: String,
    /// Absent means UTF-8 source: the main module is ESM, siblings become
    /// text modules. `wasm` bytes become a module whose default export is a
    /// compiled `WebAssembly.Module` (Wrangler's `CompiledWasm` rule).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ModuleKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModuleKind {
    Wasm,
}

/// Reference from a deploy manifest to its immutable, canonical asset index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetManifestRef {
    pub index: String,
    pub sha256: String,
    pub file_count: u32,
    pub total_bytes: u64,
}

/// `deploy/<script>/<version>/assets.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetIndex {
    pub schema_version: u32,
    pub entries: BTreeMap<String, AssetEntry>,
    pub config: AssetConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetEntry {
    /// Full SHA-256 of the exact response body, lowercase hexadecimal.
    pub sha256: String,
    pub bytes: u64,
    /// `None` means omit Content-Type, matching Wrangler's `application/null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html_handling: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_found_handling: Option<String>,
    #[serde(default)]
    pub run_worker_first: RunWorkerFirst,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirects: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_date: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compatibility_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunWorkerFirst {
    Bool(bool),
    Routes(Vec<String>),
}

impl Default for RunWorkerFirst {
    fn default() -> Self {
        Self::Bool(false)
    }
}

/// A fleet-wide immutable asset body key. The digest is validated before this
/// is called by the receiver and again by the applying node.
pub fn asset_blob_key(sha256: &str) -> Option<String> {
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    Some(format!(
        "deploy-blobs/assets/sha256/{}/{}",
        &sha256[..2],
        sha256
    ))
}

/// `deploy/current.json` — the fleet-wide pointer a node reads on startup.
/// Changing this is a deploy; nodes converge to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployPointer {
    /// Present on fleet-wide pointers. Older named pointers omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_name: Option<String>,
    pub version: String,
    pub prefix: String,
    pub rollout: Rollout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rollout {
    pub percent: u8,
}

/// Deployment identity: sorted module contents plus the serialized metadata,
/// never the raw upload framing (which is not deterministic). Every sender
/// must agree on this or identical code deploys as two versions depending on
/// the path used.
///
/// `metadata_json` is the exact byte serialization the sender stores as
/// `Manifest::raw_metadata`; callers pass the same bytes to both.
/// Cron trigger expressions are deliberately NOT an input. A version names
/// the code and its bindings; a schedule is configuration layered on top,
/// which is also how Cloudflare models it — schedules are their own resource,
/// set by their own API call after the script upload. Hashing them would make
/// the native and managed paths disagree about what a version is.
pub fn deployment_version(
    modules: &[(String, Vec<u8>)],
    metadata_json: &[u8],
    asset_index: Option<&[u8]>,
) -> String {
    let mut sorted = modules.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = Sha256::new();
    for (name, bytes) in sorted {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(bytes);
    }
    hasher.update(metadata_json);
    if let Some(index) = asset_index {
        hasher.update([0]);
        hasher.update(b"assets.json");
        hasher.update([0]);
        hasher.update(index);
    }
    format!("{:x}", hasher.finalize())[..16].to_string()
}
