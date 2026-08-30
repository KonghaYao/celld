// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! R2 binding branch: write overlay pointer on the child version bucket.

use crate::cell_branch::{branch_cli_project_id, validate_parent_bucket, ValidatedParentBucket};
use crate::cli_options::FleetFlags;
use crate::r2_overlay::{self, R2OverlayPointer, R2_BASE_JSON};

pub const R2_BRANCH_FAMILY: &str = "R2_BRANCH_ERROR";

pub async fn branch_r2_binding(
    bucket_name: &str,
    parent_bucket: &str,
    fleet: &FleetFlags,
) -> anyhow::Result<R2BranchOk> {
    let storage = fleet.clone().resolve("celld r2 branch")?;
    let project_id = branch_cli_project_id("", Some(&storage.bucket));
    let parent = validate_parent_bucket(parent_bucket, &project_id)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let child = crate::fleet::bucket_client(
        &storage.bucket,
        storage.endpoint.as_deref(),
        &storage.region,
    )?;
    if r2_overlay::read_overlay_pointer(&child, bucket_name)
        .await?
        .is_some()
    {
        anyhow::bail!("child R2 binding already has {R2_BASE_JSON}; compact first");
    }
    if r2_overlay::parent_has_overlay(&parent, bucket_name).await? {
        anyhow::bail!("parent R2 binding is already a branch; compact first");
    }
    let started = std::time::Instant::now();
    r2_overlay::write_overlay_pointer_cas(
        &child,
        bucket_name,
        &R2OverlayPointer {
            parent_bucket: parent.uri.clone(),
        },
    )
    .await?;
    let _ = r2_overlay::load_overlay(&child, bucket_name).await?;
    Ok(R2BranchOk {
        parent_bucket: parent.uri,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

pub struct R2BranchOk {
    pub parent_bucket: String,
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_parent_for_r2_branch() {
        let parsed = validate_parent_bucket("s3://cellp-celld/demo/v1", "demo").expect("valid");
        assert_eq!(parsed.key_prefix, "demo/v1/");
    }
}
