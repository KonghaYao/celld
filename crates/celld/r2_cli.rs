// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Operator commands read the process environment and host files.
#![allow(clippy::disallowed_methods)]

//! `celld r2` — operator writes and branch overlays for R2 bindings.

use anyhow::{anyhow, bail, Context};
use bytes::Bytes;
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::note;

const DEFAULT_BULK_CONCURRENCY: usize = 50;
const MAX_BULK_CONCURRENCY: usize = 256;

struct BranchReply {
    parent_bucket: String,
    duration_ms: u64,
}

impl Record for BranchReply {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "parent_bucket": self.parent_bucket,
            "duration_ms": self.duration_ms,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "parent_bucket: {}\nduration_ms: {}",
            self.parent_bucket, self.duration_ms
        ))
    }
}

#[derive(Debug)]
struct BulkPutReply {
    bucket_name: String,
    objects: usize,
    bytes: u64,
    duration_ms: u64,
}

impl Record for BulkPutReply {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "bucket_name": self.bucket_name,
            "objects": self.objects,
            "bytes": self.bytes,
            "duration_ms": self.duration_ms,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "bucket_name: {}\nobjects: {}\nbytes: {}\nduration_ms: {}",
            self.bucket_name, self.objects, self.bytes, self.duration_ms
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkManifestEntry {
    key: String,
    file: PathBuf,
}

#[derive(Debug)]
struct BulkEntry {
    key: String,
    file: PathBuf,
    bytes: u64,
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    match command.action {
        Action::Branch {
            bucket_name,
            parent_bucket,
            fleet,
            json,
        } => {
            let result = crate::r2_branch::branch_r2_binding(&bucket_name, &parent_bucket, &fleet)
                .await
                .with_context(|| format!("branch R2 binding {bucket_name:?}"))?;
            let mut out = Output::new(if json { Format::Json } else { Format::Text });
            if json {
                out.row(&BranchReply {
                    parent_bucket: result.parent_bucket.clone(),
                    duration_ms: result.duration_ms,
                })?;
            } else {
                note!(
                    "Branched R2 binding {bucket_name:?} from {} in {}ms",
                    result.parent_bucket,
                    result.duration_ms
                );
            }
            out.finish()?;
        }
        Action::BulkPut {
            bucket_name,
            filename,
            concurrency,
            fleet,
            json,
        } => {
            let storage = fleet.resolve("celld r2 bulk put")?;
            let store = storage.open().await?;
            let result = bulk_put_manifest(&store, &bucket_name, &filename, concurrency).await?;
            let mut out = Output::new(if json { Format::Json } else { Format::Text });
            if json {
                out.row(&result)?;
            } else {
                note!(
                    "Uploaded {} object(s), {} bytes, to R2 binding {:?} in {}ms",
                    result.objects,
                    result.bytes,
                    result.bucket_name,
                    result.duration_ms
                );
            }
            out.finish()?;
        }
    }
    Ok(())
}

async fn bulk_put_manifest(
    store: &crate::bucket::Bucket,
    bucket_name: &str,
    filename: &Path,
    concurrency: usize,
) -> anyhow::Result<BulkPutReply> {
    validate_bucket_name(bucket_name)?;
    if concurrency == 0 || concurrency > MAX_BULK_CONCURRENCY {
        bail!("R2 bulk put concurrency must be between 1 and {MAX_BULK_CONCURRENCY}");
    }
    // Parse and resolve every entry before the first write. A malformed record
    // must not leave half of an otherwise valid manifest in the binding.
    let entries = read_bulk_manifest(filename)?;
    let overlay = crate::r2_overlay::read_overlay_pointer(store, bucket_name)
        .await
        .with_context(|| format!("inspect R2 overlay for {bucket_name:?}"))?
        .is_some();
    let objects = entries.len();
    let started = std::time::Instant::now();
    let bucket_name = bucket_name.to_string();
    let uploads = stream::iter(entries).map(|entry| {
        let store = store.clone();
        let bucket_name = bucket_name.clone();
        async move {
            let body = tokio::fs::read(&entry.file)
                .await
                .with_context(|| format!("read R2 bulk object {}", entry.file.display()))?;
            if body.len() as u64 != entry.bytes {
                bail!(
                    "R2 bulk object changed while being read: {}",
                    entry.file.display()
                );
            }
            let bytes = body.len() as u64;
            let object_key = crate::r2_overlay::r2_object_key(&bucket_name, &entry.key);
            store
                .put(&object_key, Bytes::from(body))
                .await
                .with_context(|| format!("upload R2 object {:?}", entry.key))?;
            if overlay {
                crate::r2_overlay::clear_tombstone(&store, &bucket_name, &entry.key)
                    .await
                    .with_context(|| format!("clear R2 tombstone for {:?}", entry.key))?;
            }
            Ok::<u64, anyhow::Error>(bytes)
        }
    });
    let uploaded = uploads
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let bytes = uploaded.into_iter().try_fold(0_u64, |total, bytes| {
        total
            .checked_add(bytes)
            .context("R2 bulk byte count overflow")
    })?;
    Ok(BulkPutReply {
        bucket_name,
        objects,
        bytes,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

fn read_bulk_manifest(filename: &Path) -> anyhow::Result<Vec<BulkEntry>> {
    let filename = std::fs::canonicalize(filename)
        .with_context(|| format!("resolve R2 bulk manifest {}", filename.display()))?;
    let metadata = std::fs::metadata(&filename)
        .with_context(|| format!("read R2 bulk manifest {}", filename.display()))?;
    if !metadata.is_file() {
        bail!("R2 bulk manifest {} is not a file", filename.display());
    }
    let source = std::fs::read_to_string(&filename)
        .with_context(|| format!("read R2 bulk manifest {}", filename.display()))?;
    let manifest: Vec<BulkManifestEntry> = serde_json::from_str(&source)
        .with_context(|| format!("parse {} as a Wrangler R2 bulk file", filename.display()))?;
    let base = filename
        .parent()
        .context("the R2 bulk manifest has no parent directory")?;
    let mut keys = BTreeSet::new();
    manifest
        .into_iter()
        .map(|entry| {
            let storage_key = validate_object_key(&entry.key)?;
            if !keys.insert(storage_key) {
                bail!("duplicate R2 bulk key: {:?}", entry.key);
            }
            let file = if entry.file.is_absolute() {
                entry.file
            } else {
                base.join(entry.file)
            };
            let file = std::fs::canonicalize(&file)
                .with_context(|| format!("resolve R2 bulk object {}", file.display()))?;
            let metadata = std::fs::metadata(&file)
                .with_context(|| format!("read R2 bulk object {}", file.display()))?;
            if !metadata.is_file() {
                bail!("R2 bulk object {} is not a file", file.display());
            }
            Ok(BulkEntry {
                key: entry.key,
                file,
                bytes: metadata.len(),
            })
        })
        .collect()
}

fn validate_bucket_name(name: &str) -> anyhow::Result<()> {
    let valid = name.len() <= 64
        && name
            .chars()
            .next()
            .is_some_and(|first| first == '_' || first.is_ascii_alphanumeric())
        && name
            .chars()
            .skip(1)
            .all(|value| value == '_' || value == '-' || value.is_ascii_alphanumeric());
    if !valid {
        bail!(
            "invalid R2 bucket name {name:?}: use letters, digits, `-` and `_` \
             (not starting with `-`), at most 64 characters"
        );
    }
    Ok(())
}

fn validate_object_key(key: &str) -> anyhow::Result<String> {
    let bytes = key.len();
    if bytes == 0 || bytes > 1024 {
        bail!("R2 bulk key must be 1 to 1024 bytes, not {bytes}: {key:?}");
    }
    let normalized = object_store::path::Path::from(key).to_string();
    let tombstones = crate::r2_overlay::R2_TOMBSTONE_DIR.trim_end_matches('/');
    if normalized == crate::r2_overlay::R2_BASE_JSON
        || normalized == tombstones
        || normalized.starts_with(crate::r2_overlay::R2_TOMBSTONE_DIR)
    {
        bail!("R2 bulk key is reserved for branch metadata: {key:?}");
    }
    if normalized != key {
        bail!("R2 bulk key must be canonical, use {normalized:?} instead of {key:?}");
    }
    Ok(normalized)
}

#[derive(Debug)]
enum Action {
    Branch {
        bucket_name: String,
        parent_bucket: String,
        fleet: FleetFlags,
        json: bool,
    },
    BulkPut {
        bucket_name: String,
        filename: PathBuf,
        concurrency: usize,
        fleet: FleetFlags,
        json: bool,
    },
}

#[derive(Debug)]
struct Command {
    action: Action,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let verb = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "branch" => "branch",
            "bulk" => match arguments.next().as_deref() {
                Some("put") => "bulk-put",
                Some("--help" | "-h" | "help") => return Ok(None),
                Some(other) if !other.starts_with('-') => {
                    bail!("unknown `celld r2 bulk` subcommand: {other}")
                }
                _ => bail!("`celld r2 bulk` needs `put`"),
            },
            other => bail!("unknown `celld r2` subcommand: {other}"),
        };
        let mut positional = Vec::new();
        let mut name = None;
        let mut parent_bucket = None;
        let mut filename = None;
        let mut concurrency = None;
        let mut fleet = FleetFlags::default();
        let mut json = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--name" => name = Some(value("--name")?),
                "--parent-bucket" => parent_bucket = Some(value("--parent-bucket")?),
                "--filename" => filename = Some(PathBuf::from(value("--filename")?)),
                "--concurrency" => {
                    let raw = value("--concurrency")?;
                    let parsed = raw.parse::<usize>().with_context(|| {
                        format!("--concurrency takes a positive integer, got {raw:?}")
                    })?;
                    if parsed == 0 || parsed > MAX_BULK_CONCURRENCY {
                        bail!("--concurrency must be between 1 and {MAX_BULK_CONCURRENCY}");
                    }
                    concurrency = Some(parsed);
                }
                "--json" => json = true,
                "--help" | "-h" => return Ok(None),
                other => {
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld r2 --help` for usage")
                    }
                    positional.push(other.to_string());
                }
            }
        }
        let fleet = fleet.with_environment();
        let action = match verb {
            "branch" => {
                if !positional.is_empty() {
                    bail!(
                        "`celld r2 branch` takes no positional argument: {:?}",
                        positional[0]
                    );
                }
                if filename.is_some() || concurrency.is_some() {
                    bail!("`celld r2 branch` takes no --filename or --concurrency");
                }
                let bucket_name = name.ok_or_else(|| anyhow!("celld r2 branch requires --name"))?;
                let parent_bucket = parent_bucket
                    .ok_or_else(|| anyhow!("celld r2 branch requires --parent-bucket"))?;
                Action::Branch {
                    bucket_name,
                    parent_bucket,
                    fleet,
                    json,
                }
            }
            "bulk-put" => {
                if name.is_some() || parent_bucket.is_some() {
                    bail!("`celld r2 bulk put` takes no --name or --parent-bucket");
                }
                let [bucket_name] = positional.as_slice() else {
                    bail!("celld r2 bulk put needs exactly one R2 bucket name");
                };
                validate_bucket_name(bucket_name)?;
                let filename =
                    filename.ok_or_else(|| anyhow!("celld r2 bulk put requires --filename"))?;
                Action::BulkPut {
                    bucket_name: bucket_name.clone(),
                    filename,
                    concurrency: concurrency.unwrap_or(DEFAULT_BULK_CONCURRENCY),
                    fleet,
                    json,
                }
            }
            _ => unreachable!("the verb was matched above"),
        };
        Ok(Some(Self { action }))
    }
}

pub fn print_help() {
    let text = format!(
        "celld r2 — write and branch deployed R2 bindings

USAGE
  celld r2 bulk put BUCKET_NAME --filename FILE [--concurrency N] [fleet options]
  celld r2 branch --name BUCKET_NAME --parent-bucket URI [--bucket CHILD] [fleet options]

BULK PUT
  FILE uses Wrangler's R2 bulk-put shape: a JSON array of {{key, file}}.
  Relative file paths resolve from the manifest directory; absolute paths are
  accepted. Every record is validated before upload starts. The default
  concurrency is {DEFAULT_BULK_CONCURRENCY}.

  The command writes into the selected version bucket under
  `r2/<BUCKET_NAME>/<key>`. It is a write-only import path, not an object
  browser.

BRANCH
  Writes `r2/<name>/base.json` on the child version bucket pointing at the
  parent version. Worker-visible overlay (GET miss -> parent, PUT -> child,
  DELETE -> tombstone under `r2/<name>/.tombstones/<key>`) is installed on
  the next node start after branch.

FLEET OPTIONS
{FLEET_HELP}
  --json                Print one JSON object"
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> crate::bucket::Bucket {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        crate::bucket::Bucket::with_stores(
            store.clone(),
            store,
            crate::bucket::StorageBackend::Local,
            "r2-bulk-test".to_string(),
            String::new(),
        )
    }

    #[test]
    fn branch_parse_requires_name_and_parent() {
        let error =
            Command::parse(vec!["branch".into(), "--name".into(), "assets".into()]).unwrap_err();
        assert!(error.to_string().contains("--parent-bucket"), "{error}");
    }

    #[test]
    fn branch_help_does_not_require_name() {
        assert!(Command::parse(vec!["branch".into(), "--help".into()])
            .unwrap()
            .is_none());
    }

    #[test]
    fn bulk_put_parse_matches_wrangler_shape() {
        let command = Command::parse(vec![
            "bulk".into(),
            "put".into(),
            "next-cache".into(),
            "--filename".into(),
            "cache.json".into(),
            "--concurrency".into(),
            "7".into(),
            "--bucket".into(),
            "cellp-celld/demo/v1".into(),
        ])
        .unwrap()
        .unwrap();
        let Action::BulkPut {
            bucket_name,
            filename,
            concurrency,
            fleet,
            ..
        } = command.action
        else {
            panic!("parsed the wrong R2 action");
        };
        assert_eq!(bucket_name, "next-cache");
        assert_eq!(filename, PathBuf::from("cache.json"));
        assert_eq!(concurrency, 7);
        assert_eq!(fleet.bucket.as_deref(), Some("cellp-celld/demo/v1"));
    }

    #[test]
    fn bulk_put_parse_rejects_invalid_concurrency() {
        for raw in ["0", "257", "many"] {
            let error = Command::parse(vec![
                "bulk".into(),
                "put".into(),
                "next-cache".into(),
                "--filename".into(),
                "cache.json".into(),
                "--concurrency".into(),
                raw.into(),
            ])
            .unwrap_err();
            assert!(error.to_string().contains("--concurrency"), "{error}");
        }
    }

    #[test]
    fn bulk_put_parse_requires_bucket_and_filename() {
        let error = Command::parse(vec!["bulk".into(), "put".into()]).unwrap_err();
        assert!(error.to_string().contains("bucket name"), "{error}");

        let error =
            Command::parse(vec!["bulk".into(), "put".into(), "next-cache".into()]).unwrap_err();
        assert!(error.to_string().contains("--filename"), "{error}");
    }

    #[test]
    fn bulk_manifest_resolves_relative_files() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("one.cache"), b"one").unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([
                {"key": "incremental-cache/build/hash.cache", "file": "one.cache"}
            ]))
            .unwrap(),
        )
        .unwrap();
        let entries = read_bulk_manifest(&manifest).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bytes, 3);
        assert_eq!(
            entries[0].file,
            std::fs::canonicalize(directory.path().join("one.cache")).unwrap()
        );
    }

    #[test]
    fn bulk_manifest_rejects_noncanonical_keys() {
        for key in ["/leading", "trailing/", "two//segments", "relative/../segment"] {
            let error = validate_object_key(key).unwrap_err();
            assert!(error.to_string().contains("must be canonical"), "{error}");
        }
    }

    #[test]
    fn bulk_manifest_rejects_unknown_fields() {
        let directory = tempfile::tempdir().unwrap();
        let object = directory.path().join("one.cache");
        std::fs::write(&object, b"one").unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([{
                "key": "valid",
                "file": object,
                "unexpected": true
            }]))
            .unwrap(),
        )
        .unwrap();
        let error = read_bulk_manifest(&manifest).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("unknown field `unexpected`"), "{message}");
    }

    #[test]
    fn bulk_manifest_rejects_duplicate_keys() {
        let directory = tempfile::tempdir().unwrap();
        let object = directory.path().join("one.cache");
        std::fs::write(&object, b"one").unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([
                {"key": "same", "file": object},
                {"key": "same", "file": object}
            ]))
            .unwrap(),
        )
        .unwrap();
        let error = read_bulk_manifest(&manifest).unwrap_err();
        assert!(
            error.to_string().contains("duplicate R2 bulk key"),
            "{error}"
        );
    }

    #[test]
    fn bulk_manifest_rejects_invalid_key_and_file() {
        for key in ["", "base.json", ".tombstones", ".tombstones/hidden"] {
            let directory = tempfile::tempdir().unwrap();
            let object = directory.path().join("one.cache");
            std::fs::write(&object, b"one").unwrap();
            let manifest = directory.path().join("objects.json");
            std::fs::write(
                &manifest,
                serde_json::to_vec(&serde_json::json!([{"key": key, "file": object}])).unwrap(),
            )
            .unwrap();
            let error = read_bulk_manifest(&manifest).unwrap_err();
            assert!(
                error.to_string().contains(if key.is_empty() {
                    "1 to 1024 bytes"
                } else {
                    "reserved for branch metadata"
                }),
                "{error}"
            );
        }

        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([{"key": "valid", "file": "missing"}])).unwrap(),
        )
        .unwrap();
        let error = read_bulk_manifest(&manifest).unwrap_err();
        assert!(
            error.to_string().contains("resolve R2 bulk object"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn bulk_put_writes_binding_prefix_and_contents() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("one.cache"), b"one").unwrap();
        std::fs::write(directory.path().join("two.cache"), b"second").unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([
                {"key": "incremental-cache/build/one.cache", "file": "one.cache"},
                {"key": "incremental-cache/build/two.cache", "file": "two.cache"}
            ]))
            .unwrap(),
        )
        .unwrap();
        let store = test_store();
        let result = bulk_put_manifest(&store, "next-cache", &manifest, 2)
            .await
            .unwrap();
        assert_eq!(result.objects, 2);
        assert_eq!(result.bytes, 9);
        let (one, _) = store
            .get("r2/next-cache/incremental-cache/build/one.cache")
            .await
            .unwrap()
            .unwrap();
        let (two, _) = store
            .get("r2/next-cache/incremental-cache/build/two.cache")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(one.as_ref(), b"one");
        assert_eq!(two.as_ref(), b"second");
    }

    #[tokio::test]
    async fn bulk_put_clears_overlay_tombstone() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("one.cache"), b"replacement").unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([
                {"key": "incremental-cache/build/one.cache", "file": "one.cache"}
            ]))
            .unwrap(),
        )
        .unwrap();
        let store = test_store();
        let bucket_name = "next-cache";
        let key = "incremental-cache/build/one.cache";
        let pointer = crate::r2_overlay::R2OverlayPointer {
            parent_bucket: "s3://cellp-celld/demo/parent".to_string(),
        };
        store
            .put(
                &crate::r2_overlay::r2_base_json_key(bucket_name),
                serde_json::to_vec(&pointer).unwrap(),
            )
            .await
            .unwrap();
        let tombstone = crate::r2_overlay::r2_tombstone_key(bucket_name, key);
        store.put(&tombstone, b"1".to_vec()).await.unwrap();

        bulk_put_manifest(&store, bucket_name, &manifest, 1)
            .await
            .unwrap();

        assert!(store.get(&tombstone).await.unwrap().is_none());
        let (body, _) = store
            .get(&crate::r2_overlay::r2_object_key(bucket_name, key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body.as_ref(), b"replacement");
    }

    #[tokio::test]
    async fn invalid_manifest_leaves_binding_untouched() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("one.cache"), b"one").unwrap();
        let manifest = directory.path().join("objects.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!([
                {"key": "valid", "file": "one.cache"},
                {"key": "missing", "file": "missing.cache"}
            ]))
            .unwrap(),
        )
        .unwrap();
        let store = test_store();
        let error = bulk_put_manifest(&store, "next-cache", &manifest, 2)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("resolve R2 bulk object"),
            "{error}"
        );
        assert!(store.get("r2/next-cache/valid").await.unwrap().is_none());
    }
}
