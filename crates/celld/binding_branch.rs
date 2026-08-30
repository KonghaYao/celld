// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! KV / Queue binding branch over generic LTX cell-branch.

use serde::Serialize;

pub(crate) use crate::cell_branch::{
    branch_cli_project_id, prepare, branch_prepared, BranchRequest, BranchOk, BranchFailure,
    ReopenSpec, validate_parent_bucket,
};

pub const KV_BRANCH_FAMILY: &str = "KV_BRANCH_ERROR";
pub const QUEUE_BRANCH_FAMILY: &str = "QUEUE_BRANCH_ERROR";

#[derive(Debug, Serialize)]
pub struct BindingBranchOk {
    pub fork_txid: u64,
    pub parent_bucket: String,
    pub bytes_parent: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct BindingBranchError<'a> {
    pub family: &'a str,
    pub message: String,
}

pub(crate) struct PreparedBinding(crate::cell_branch::PreparedBranch);

pub fn prepare_binding(
    scope: &str,
    request: BranchRequest,
    expected_scope: &str,
) -> Result<(PreparedBinding, ReopenSpec), BindingBranchError<'static>> {
    if scope != expected_scope {
        return Err(binding_error(
            KV_BRANCH_FAMILY,
            format!(
                "cell scope {scope:?} does not match binding identity {expected_scope:?}"
            ),
        ));
    }
    prepare(scope, request, false)
        .map(|(prepared, reopen)| (PreparedBinding(prepared), reopen))
        .map_err(|failure| binding_error(KV_BRANCH_FAMILY, failure.message))
}

pub fn prepare_queue_binding(
    scope: &str,
    request: BranchRequest,
    expected_scope: &str,
) -> Result<(PreparedBinding, ReopenSpec), BindingBranchError<'static>> {
    if scope != expected_scope {
        return Err(binding_error(
            QUEUE_BRANCH_FAMILY,
            format!(
                "cell scope {scope:?} does not match binding identity {expected_scope:?}"
            ),
        ));
    }
    prepare(scope, request, false)
        .map(|(prepared, reopen)| (PreparedBinding(prepared), reopen))
        .map_err(|failure| binding_error(QUEUE_BRANCH_FAMILY, failure.message))
}

pub async fn branch_binding_prepared(
    prepared: PreparedBinding,
    family: &'static str,
) -> Result<BindingBranchOk, BindingBranchError<'static>> {
    branch_prepared(prepared.0)
        .await
        .map(|ok| BindingBranchOk {
            fork_txid: ok.fork_txid,
            parent_bucket: ok.parent_bucket,
            bytes_parent: ok.bytes_parent,
            duration_ms: ok.duration_ms,
        })
        .map_err(|failure| binding_error(family, failure.message))
}

pub(crate) fn reopen(spec: &ReopenSpec) -> Result<(), BindingBranchError<'static>> {
    crate::cell_branch::reopen(spec).map_err(|failure| binding_error(KV_BRANCH_FAMILY, failure.message))
}

fn binding_error(family: &'static str, message: String) -> BindingBranchError<'static> {
    BindingBranchError { family, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_branch_request_parses_parent_epoch_default() {
        let request: BranchRequest =
            serde_json::from_str(r#"{"parent_bucket":"s3://cellp-celld/demo/v1"}"#).unwrap();
        assert_eq!(request.parent_epoch, 0);
    }
}
