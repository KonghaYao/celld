// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! KV large-value blob read fallback to a branched parent bucket.

use bytes::Bytes;

use crate::bucket::Bucket;
use crate::cell_branch::{self, validate_parent_bucket, ValidatedParentBucket};

/// Open a validated parent version bucket using the node's fleet credentials.
pub fn open_parent_bucket(parent: &ValidatedParentBucket) -> anyhow::Result<Bucket> {
    let endpoint = std::env::var("S3_ENDPOINT")
        .ok()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
        .filter(|value| !value.is_empty());
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".into());
    crate::fleet::bucket_client(&parent.uri, endpoint.as_deref(), &region)
}

/// GET a KV blob key from the child bucket, then the parent bucket when branched.
pub async fn get_blob(
    child: &Bucket,
    cell: &str,
    object_key: &str,
) -> anyhow::Result<Option<Bytes>> {
    if let Some((bytes, _etag)) = child.get(object_key).await? {
        return Ok(Some(bytes));
    }
    let Some(base) = cell_branch::active_base_pointer(cell)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
    else {
        return Ok(None);
    };
    if base.parent_cell != cell {
        anyhow::bail!("branch parent_cell {:?} does not match {cell}", base.parent_cell);
    }
    let project_id = cell_branch::runtime_project_id().map_err(|error| anyhow::anyhow!("{error}"))?;
    let parent = validate_parent_bucket(&base.parent_bucket, &project_id)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let parent_store = open_parent_bucket(&parent)?;
    Ok(parent_store
        .get(object_key)
        .await?
        .map(|(bytes, _etag)| bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_parent_bucket_parses_cellp_uri() {
        let parent = validate_parent_bucket("s3://cellp-celld/demo-app/v-parent", "demo-app")
            .expect("valid parent");
        match open_parent_bucket(&parent) {
            Ok(_) => {}
            Err(error) => assert!(
                error.to_string().contains("bucket"),
                "unexpected error: {error}"
            ),
        }
    }
}
