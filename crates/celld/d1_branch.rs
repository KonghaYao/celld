// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Owner-side D1 branch from a parent version bucket (D1-BRANCH-RPC.md).

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use celld_ltx::base::{self, BasePointer};
use celld_ltx::object_store::path::Path as ObjPath;
use celld_ltx::object_store::{Error as OsError, ObjectStore, PutMode, PutOptions};
use serde::{Deserialize, Serialize};

use crate::ltx_repl::LtxRepl;
use crate::storage;

static LTX: OnceLock<Arc<LtxRepl>> = OnceLock::new();

pub fn set_ltx(ltx: Arc<LtxRepl>) {
    let _ = LTX.set(ltx);
}

fn ltx() -> Result<Arc<LtxRepl>, String> {
    LTX.get()
        .cloned()
        .ok_or_else(|| "D1 branch requires fleet replication".to_string())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchRequest {
    pub parent_bucket: String,
    #[serde(default)]
    pub parent_epoch: u64,
}

#[derive(Debug, Serialize)]
pub struct D1BranchOk {
    pub fork_txid: u64,
    pub parent_bucket: String,
    pub bytes_parent: u64,
    pub duration_ms: u64,
}

pub struct D1BranchFailure {
    pub message: String,
}

fn d1_branch_error(message: impl Into<String>) -> D1BranchFailure {
    D1BranchFailure {
        message: message.into(),
    }
}

/// Parsed parent version location after SSRF validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedParentBucket {
    pub uri: String,
    pub key_prefix: String,
}

/// Validate `s3://cellp-celld/{project_id}/{parentVersion}`.
pub fn validate_parent_bucket(uri: &str, project_id: &str) -> Result<ValidatedParentBucket, String> {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        return Err("parent bucket must be s3://cellp-celld/{project}/{version}".into());
    }
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| "parent bucket must use s3:// scheme".to_string())?;
    if rest.contains("s3://") {
        return Err("parent bucket path must not embed s3://".into());
    }
    let mut parts = rest.split('/');
    let bucket = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "parent bucket is missing a bucket name".to_string())?;
    if bucket != "cellp-celld" {
        return Err(format!("parent bucket must be cellp-celld, not {bucket}"));
    }
    let project = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "parent bucket is missing a project id".to_string())?;
    if project != project_id {
        return Err(format!(
            "parent bucket project {project:?} does not match current project {project_id:?}"
        ));
    }
    let parent_version = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "parent bucket is missing a parent version id".to_string())?;
    if parent_version.contains("..") {
        return Err("parent version must not contain ..".into());
    }
    if parts.next().is_some() {
        return Err("parent bucket must be s3://cellp-celld/{project}/{version}".into());
    }
    Ok(ValidatedParentBucket {
        uri: uri.to_string(),
        key_prefix: format!("{project}/{parent_version}/"),
    })
}

/// Project id used by the CLI's copy of [`validate_parent_bucket`].
///
/// Wrangler `name` is the Worker script id (`d1-seed`), not the cellp
/// project in `s3://cellp-celld/{project}/{version}`. Prefer
/// `CELLD_VAR_PROJECT_ID`, then the child `--bucket` prefix.
pub fn branch_cli_project_id(wrangler_name: &str, child_bucket_spec: Option<&str>) -> String {
    if let Ok(id) = std::env::var("CELLD_VAR_PROJECT_ID") {
        if !id.is_empty() {
            return id;
        }
    }
    if let Some(spec) = child_bucket_spec {
        if let Some(project) = project_from_cellp_bucket(spec) {
            return project;
        }
    }
    wrangler_name.to_string()
}

fn project_from_cellp_bucket(spec: &str) -> Option<String> {
    let rest = spec.trim_start_matches("s3://");
    let mut parts = rest.split('/').filter(|part| !part.is_empty());
    let bucket = parts.next()?;
    if bucket != "cellp-celld" {
        return None;
    }
    parts.next().map(str::to_string)
}

pub fn runtime_project_id() -> Result<String, String> {
    std::env::var("CELLD_VAR_PROJECT_ID")
        .map_err(|_| "CELLD_VAR_PROJECT_ID is unset on this node".to_string())
}

pub struct PreparedBranch {
    scope: String,
    request: BranchRequest,
    epoch: u64,
    sqlite_vec: bool,
    ltx: Arc<LtxRepl>,
    started: std::time::Instant,
}

pub(crate) struct ReopenSpec {
    pub(crate) scope: String,
    pub(crate) db_path: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) sqlite_vec: bool,
}

pub fn prepare(
    scope: &str,
    request: BranchRequest,
    sqlite_vec: bool,
) -> Result<(PreparedBranch, ReopenSpec), D1BranchFailure> {
    let started = std::time::Instant::now();
    let ltx = ltx().map_err(d1_branch_error)?;
    let project_id = runtime_project_id().map_err(d1_branch_error)?;
    validate_parent_bucket(&request.parent_bucket, &project_id).map_err(d1_branch_error)?;
    let epoch = storage::activation_epoch(scope)
        .ok_or_else(|| d1_branch_error(format!("no active database for {scope}")))?;
    let db_path = ltx.cell_db_path(scope, epoch);
    storage::close(scope);
    Ok((
        PreparedBranch {
            scope: scope.to_string(),
            request,
            epoch,
            sqlite_vec,
            ltx,
            started,
        },
        ReopenSpec {
            scope: scope.to_string(),
            db_path,
            epoch,
            sqlite_vec,
        },
    ))
}

pub async fn branch_prepared(
    prepared: PreparedBranch,
) -> Result<D1BranchOk, D1BranchFailure> {
    let PreparedBranch {
        scope,
        request,
        epoch,
        ltx,
        started,
        ..
    } = prepared;
    let project_id = runtime_project_id().map_err(d1_branch_error)?;
    let parent = validate_parent_bucket(&request.parent_bucket, &project_id)
        .map_err(d1_branch_error)?;
    let result = ltx
        .branch_from_parent(
            &scope,
            epoch,
            &parent,
            request.parent_epoch,
        )
        .await
        .map_err(|error| d1_branch_error(error.to_string()))?;
    Ok(D1BranchOk {
        fork_txid: result.fork_txid,
        parent_bucket: request.parent_bucket,
        bytes_parent: result.bytes_parent,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

pub(crate) fn reopen(spec: &ReopenSpec) -> Result<(), D1BranchFailure> {
    storage::open_at_epoch(
        &spec.scope,
        &spec.db_path.to_string_lossy(),
        spec.epoch,
        spec.sqlite_vec,
    )
    .map_err(|error| d1_branch_error(error.to_string()))
}

pub(crate) async fn read_base_json(
    store: &dyn ObjectStore,
    prefix: &str,
    cell: &str,
    epoch: u64,
) -> Result<Option<BasePointer>, anyhow::Error> {
    let key = base::base_json_key(&format!("{prefix}cells/{cell}/ltx/e{epoch}"));
    match store.get(&ObjPath::from(key)).await {
        Ok(object) => {
            let bytes = object.bytes().await?;
            BasePointer::parse_json(&bytes).map(Some).map_err(Into::into)
        }
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(error) => Err(anyhow::anyhow!("read base.json: {error}")),
    }
}

pub(crate) async fn put_base_json_cas(
    store: &dyn ObjectStore,
    prefix: &str,
    cell: &str,
    epoch: u64,
    pointer: &BasePointer,
) -> Result<(), anyhow::Error> {
    let key = base::base_json_key(&format!("{prefix}cells/{cell}/ltx/e{epoch}"));
    let body = pointer.to_json()?;
    match store
        .put_opts(
            &ObjPath::from(key),
            body.into(),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
            anyhow::bail!("base.json already exists on child prefix")
        }
        Err(error) => Err(anyhow::anyhow!("create base.json: {error}")),
    }
}

pub fn verify_fork_checksum(ltx_bytes: &[u8], expected_hex: &str) -> Result<(), String> {
    let actual = base::post_apply_checksum_from_ltx(ltx_bytes).map_err(|error| error.to_string())?;
    let expected = base::parse_checksum_hex(expected_hex).map_err(|error| error.to_string())?;
    if actual != expected {
        return Err(format!(
            "fork_checksum mismatch: expected {expected_hex}, got {}",
            base::checksum_hex(actual)
        ));
    }
    Ok(())
}

pub struct BranchResult {
    pub fork_txid: u64,
    pub bytes_parent: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_parent_bucket_accepts_same_project() {
        let parsed = validate_parent_bucket("s3://cellp-celld/demo-app/v-parent", "demo-app")
            .expect("valid");
        assert_eq!(parsed.key_prefix, "demo-app/v-parent/");
    }

    #[test]
    fn validate_parent_bucket_rejects_http() {
        let error = validate_parent_bucket("http://cellp-celld/demo/v1", "demo").unwrap_err();
        assert!(error.contains("s3://"), "{error}");
    }

    #[test]
    fn validate_parent_bucket_rejects_other_project() {
        let error =
            validate_parent_bucket("s3://cellp-celld/other-app/v1", "demo-app").unwrap_err();
        assert!(error.contains("does not match"), "{error}");
    }

    #[test]
    fn validate_parent_bucket_rejects_other_bucket() {
        let error = validate_parent_bucket("s3://other/demo-app/v1", "demo-app").unwrap_err();
        assert!(error.contains("cellp-celld"), "{error}");
    }

    #[test]
    fn branch_request_rejects_client_fork_txid() {
        let error = serde_json::from_str::<BranchRequest>(
            r#"{"parent_bucket":"s3://cellp-celld/demo-app/v1","fork_txid":9}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    #[test]
    fn branch_cli_project_id_prefers_child_bucket_over_wrangler_name() {
        if std::env::var("CELLD_VAR_PROJECT_ID")
            .ok()
            .filter(|id| !id.is_empty())
            .is_some()
        {
            return;
        }
        let id = branch_cli_project_id("d1-seed", Some("cellp-celld/demo-app/v-child"));
        assert_eq!(id, "demo-app");
    }

    #[test]
    fn verify_fork_checksum_rejects_mismatch() {
        let len = celld_ltx::ltx::HEADER_SIZE + celld_ltx::ltx::TRAILER_SIZE;
        let mut bytes = vec![0u8; len];
        let trailer_start = len - celld_ltx::ltx::TRAILER_SIZE;
        bytes[trailer_start..trailer_start + 8]
            .copy_from_slice(&0x8000_0000_0000_0042u64.to_be_bytes());
        let error = verify_fork_checksum(&bytes, "8000000000000001").unwrap_err();
        assert!(error.contains("fork_checksum mismatch"), "{error}");
    }
}
