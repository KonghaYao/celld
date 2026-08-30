// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Owner-side D1 branch from a parent version bucket (D1-BRANCH-RPC.md).

pub(crate) use crate::cell_branch::{
    branch_cli_project_id, project_from_cellp_bucket, read_base_json, put_base_json_cas,
    runtime_project_id, set_ltx, validate_parent_bucket, verify_fork_checksum, BranchRequest,
    BranchResult, ValidatedParentBucket,
};

pub(crate) type ReopenSpec = crate::cell_branch::ReopenSpec;

#[derive(Debug, serde::Serialize)]
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

pub(crate) struct PreparedBranch(crate::cell_branch::PreparedBranch);

pub fn prepare(
    scope: &str,
    request: BranchRequest,
    sqlite_vec: bool,
) -> Result<(PreparedBranch, ReopenSpec), D1BranchFailure> {
    crate::cell_branch::prepare(scope, request, sqlite_vec)
        .map(|(prepared, reopen)| (PreparedBranch(prepared), reopen))
        .map_err(|failure| d1_branch_error(failure.message))
}

pub async fn branch_prepared(prepared: PreparedBranch) -> Result<D1BranchOk, D1BranchFailure> {
    crate::cell_branch::branch_prepared(prepared.0)
        .await
        .map(|ok| D1BranchOk {
            fork_txid: ok.fork_txid,
            parent_bucket: ok.parent_bucket,
            bytes_parent: ok.bytes_parent,
            duration_ms: ok.duration_ms,
        })
        .map_err(|failure| d1_branch_error(failure.message))
}

pub(crate) fn reopen(spec: &ReopenSpec) -> Result<(), D1BranchFailure> {
    crate::cell_branch::reopen(spec).map_err(|failure| d1_branch_error(failure.message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_request_rejects_client_fork_txid() {
        let error = serde_json::from_str::<BranchRequest>(
            r#"{"parent_bucket":"s3://cellp-celld/demo-app/v1","fork_txid":9}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }
}
