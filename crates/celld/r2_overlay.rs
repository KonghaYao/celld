// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! R2 binding overlay: parent-bucket fallback, child writes, tombstone deletes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::bucket::{BlobEntry, BlobMeta, BlobPage, BlobRead, Bucket};
use crate::cell_branch::{self, validate_parent_bucket, ValidatedParentBucket};
use crate::kv_blob_branch::open_parent_bucket;

pub const R2_BASE_JSON: &str = "base.json";
pub const R2_TOMBSTONE_DIR: &str = ".tombstones/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R2OverlayPointer {
    pub parent_bucket: String,
}

pub fn r2_binding_prefix(bucket_name: &str) -> String {
    format!("r2/{bucket_name}/")
}

pub fn r2_base_json_key(bucket_name: &str) -> String {
    format!("r2/{bucket_name}/{R2_BASE_JSON}")
}

pub fn r2_object_key(bucket_name: &str, key: &str) -> String {
    format!("r2/{bucket_name}/{key}")
}

pub fn r2_tombstone_key(bucket_name: &str, object_key: &str) -> String {
    format!("r2/{bucket_name}/{R2_TOMBSTONE_DIR}{object_key}")
}

fn reserved_overlay_keys(bucket_name: &str) -> BTreeSet<String> {
    BTreeSet::from([
        r2_base_json_key(bucket_name),
        format!("r2/{bucket_name}/{R2_TOMBSTONE_DIR}"),
    ])
}

struct OverlayState {
    parent: ValidatedParentBucket,
    store: Bucket,
}

static OVERLAYS: OnceLock<Mutex<HashMap<String, OverlayState>>> = OnceLock::new();

fn overlays() -> &'static Mutex<HashMap<String, OverlayState>> {
    OVERLAYS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub async fn load_overlay(child: &Bucket, bucket_name: &str) -> anyhow::Result<Option<ValidatedParentBucket>> {
    if let Some(state) = overlays().lock().unwrap().get(bucket_name) {
        return Ok(Some(state.parent.clone()));
    }
    let base_key = r2_base_json_key(bucket_name);
    let Some((bytes, _etag)) = child.get(&base_key).await? else {
        return Ok(None);
    };
    let pointer: R2OverlayPointer = serde_json::from_slice(&bytes)?;
    let project_id = cell_branch::runtime_project_id().map_err(|error| anyhow::anyhow!("{error}"))?;
    let parent = validate_parent_bucket(&pointer.parent_bucket, &project_id)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let store = open_parent_bucket(&parent)?;
    overlays().lock().unwrap().insert(
        bucket_name.to_string(),
        OverlayState {
            parent: parent.clone(),
            store,
        },
    );
    Ok(Some(parent))
}

pub async fn parent_store(bucket_name: &str) -> Option<Bucket> {
    overlays().lock().unwrap().get(bucket_name).map(|state| state.store.clone())
}

pub async fn is_tombstoned(child: &Bucket, bucket_name: &str, object_key: &str) -> anyhow::Result<bool> {
    Ok(child
        .head(&r2_tombstone_key(bucket_name, object_key))
        .await?
        .is_some())
}

pub async fn write_tombstone(child: &Bucket, bucket_name: &str, object_key: &str) -> anyhow::Result<()> {
    child
        .put(
            &r2_tombstone_key(bucket_name, object_key),
            Bytes::from_static(b"1"),
        )
        .await
}

pub async fn read_overlay_pointer(
    store: &Bucket,
    bucket_name: &str,
) -> anyhow::Result<Option<R2OverlayPointer>> {
    let key = r2_base_json_key(bucket_name);
    match store.get(&key).await? {
        Some((bytes, _etag)) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

pub async fn write_overlay_pointer_cas(
    child: &Bucket,
    bucket_name: &str,
    pointer: &R2OverlayPointer,
) -> anyhow::Result<()> {
    let key = r2_base_json_key(bucket_name);
    let body = serde_json::to_vec_pretty(pointer)?;
    match child.put_cas(&key, body, None).await? {
        Some(_) => Ok(()),
        None => anyhow::bail!("R2 overlay pointer already exists on child prefix"),
    }
}

pub async fn parent_has_overlay(
    parent_uri: &ValidatedParentBucket,
    bucket_name: &str,
) -> anyhow::Result<bool> {
    let parent = open_parent_bucket(parent_uri)?;
    Ok(read_overlay_pointer(&parent, bucket_name).await?.is_some())
}

/// Merge one listing page from child and parent, hiding tombstones.
pub async fn list_overlay_page(
    child: &Bucket,
    bucket_name: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> anyhow::Result<BlobPage> {
    let scoped = r2_object_key(bucket_name, prefix);
    let after_key = after.map(|key| r2_object_key(bucket_name, key));
    let child_page = child
        .list_page(&scoped, after_key.as_deref(), limit, None)
        .await?;
    let Some(parent) = parent_store(bucket_name).await else {
        return Ok(child_page);
    };
    let parent_page = parent
        .list_page(&scoped, after_key.as_deref(), limit, None)
        .await?;
    let strip = r2_object_key(bucket_name, "");
    let mut merged: BTreeMap<String, BlobEntry> = BTreeMap::new();
    for entry in parent_page.objects {
        let logical = entry
            .key
            .strip_prefix(&strip)
            .unwrap_or(&entry.key)
            .to_string();
        if reserved_overlay_keys(bucket_name).contains(&entry.key)
            || is_tombstoned(child, bucket_name, &logical).await?
        {
            continue;
        }
        merged.insert(logical, entry);
    }
    for entry in child_page.objects {
        let logical = entry
            .key
            .strip_prefix(&strip)
            .unwrap_or(&entry.key)
            .to_string();
        if logical.starts_with(R2_TOMBSTONE_DIR) || logical == R2_BASE_JSON {
            continue;
        }
        if is_tombstoned(child, bucket_name, &logical).await? {
            merged.remove(&logical);
            continue;
        }
        merged.insert(logical, entry);
    }
    let mut objects = merged.into_values().collect::<Vec<_>>();
    objects.truncate(limit);
    let truncated = objects.len() >= limit;
    let cursor = objects.last().map(|entry| {
        entry
            .key
            .strip_prefix(&strip)
            .unwrap_or(&entry.key)
            .to_string()
    });
    Ok(BlobPage {
        objects,
        prefixes: child_page.prefixes,
        truncated,
        cursor,
    })
}

pub async fn overlay_head_blob(
    child: &Bucket,
    bucket_name: &str,
    object_key: &str,
) -> anyhow::Result<Option<BlobMeta>> {
    if is_tombstoned(child, bucket_name, object_key).await? {
        return Ok(None);
    }
    let key = r2_object_key(bucket_name, object_key);
    if let Some(meta) = child.head_blob(&key).await? {
        return Ok(Some(meta));
    }
    if let Some(parent) = parent_store(bucket_name).await {
        return parent.head_blob(&key).await;
    }
    Ok(None)
}

pub async fn overlay_get_blob(
    child: &Bucket,
    bucket_name: &str,
    object_key: &str,
    range: crate::bucket::BlobRange,
    conditions: &crate::bucket::BlobConditions,
) -> anyhow::Result<BlobRead> {
    if is_tombstoned(child, bucket_name, object_key).await? {
        return Ok(BlobRead::Missing);
    }
    let key = r2_object_key(bucket_name, object_key);
    match child.get_blob(&key, range, conditions).await? {
        BlobRead::Missing => {}
        other => return Ok(other),
    }
    if let Some(parent) = parent_store(bucket_name).await {
        return parent.get_blob(&key, range, conditions).await;
    }
    Ok(BlobRead::Missing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_key_preserves_object_key_slashes() {
        assert_eq!(
            r2_tombstone_key("assets", "dir/object.txt"),
            "r2/assets/.tombstones/dir/object.txt"
        );
    }

    #[test]
    fn overlay_pointer_roundtrip_json() {
        let pointer = R2OverlayPointer {
            parent_bucket: "s3://cellp-celld/demo/v1".to_string(),
        };
        let json = serde_json::to_string(&pointer).unwrap();
        let parsed: R2OverlayPointer = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, pointer);
    }
}
