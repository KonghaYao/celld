use anyhow::{Context, Result};
use std::path::PathBuf;

pub struct PinnedVersions {
    pub wasmtime: String,
    pub wasmtime_wasi: String,
    pub wasmtime_wasi_http: String,
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates| crates.parent())
        .expect("qualification crate lives under crates/")
        .to_path_buf()
}

pub fn pinned_versions() -> Result<PinnedVersions> {
    let lock = std::fs::read_to_string(workspace_root().join("Cargo.lock"))
        .context("read workspace Cargo.lock")?;
    Ok(PinnedVersions {
        wasmtime: package_version(&lock, "wasmtime")?,
        wasmtime_wasi: package_version(&lock, "wasmtime-wasi")?,
        wasmtime_wasi_http: package_version(&lock, "wasmtime-wasi-http")?,
    })
}

fn package_version(lock: &str, name: &str) -> Result<String> {
    let header = format!("name = \"{name}\"");
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() != header {
            continue;
        }
        for following in lines.by_ref() {
            let following = following.trim();
            if let Some(version) = following.strip_prefix("version = \"") {
                return Ok(version.trim_end_matches('"').to_string());
            }
            if following.is_empty() {
                break;
            }
        }
    }
    anyhow::bail!("Cargo.lock is missing {name}")
}
