//! Parent/child [`ReplicaClient`] overlay for D1 branch restore.

use std::collections::HashSet;

use async_trait::async_trait;

use crate::client::ReplicaClient;
use crate::error::Result;
use crate::ltx::FileInfo;
use crate::TXID;

/// Read parent prefix up to `fork_txid`, child incrementals above it; write only
/// to child.
#[derive(Debug, Clone)]
pub struct ChainedReplicaClient<P, C> {
    parent: P,
    child: C,
    fork_txid: TXID,
}

impl<P, C> ChainedReplicaClient<P, C> {
    pub fn new(parent: P, child: C, fork_txid: TXID) -> Self {
        Self {
            parent,
            child,
            fork_txid,
        }
    }

    pub fn fork_txid(&self) -> TXID {
        self.fork_txid
    }

    pub fn parent(&self) -> &P {
        &self.parent
    }

    pub fn child(&self) -> &C {
        &self.child
    }

    fn child_owns(min_txid: TXID, fork_txid: TXID) -> bool {
        min_txid > fork_txid
    }

    fn child_seek(seek: TXID, fork_txid: TXID) -> TXID {
        if seek > fork_txid {
            seek
        } else {
            TXID(fork_txid.0.saturating_add(1))
        }
    }
}

fn merge_ltx_files(
    parent_files: Vec<FileInfo>,
    child_files: Vec<FileInfo>,
) -> Vec<FileInfo> {
    let mut seen = HashSet::new();
    let mut merged = Vec::with_capacity(parent_files.len() + child_files.len());
    for info in parent_files.into_iter().chain(child_files) {
        let key = (info.level, info.min_txid, info.max_txid);
        if seen.insert(key) {
            merged.push(info);
        }
    }
    merged.sort_by(|left, right| {
        (left.level, left.min_txid.0, left.max_txid.0).cmp(&(
            right.level,
            right.min_txid.0,
            right.max_txid.0,
        ))
    });
    merged
}

#[async_trait]
impl<P, C> ReplicaClient for ChainedReplicaClient<P, C>
where
    P: ReplicaClient + Send + Sync,
    C: ReplicaClient + Send + Sync,
{
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
        let parent_files = self
            .parent
            .ltx_files(level, seek)
            .await?
            .into_iter()
            .filter(|info| info.max_txid <= self.fork_txid)
            .collect::<Vec<_>>();
        let child_files = self
            .child
            .ltx_files(level, Self::child_seek(seek, self.fork_txid))
            .await?
            .into_iter()
            .filter(|info| info.min_txid > self.fork_txid)
            .collect::<Vec<_>>();
        Ok(merge_ltx_files(parent_files, child_files))
    }

    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>> {
        if Self::child_owns(min_txid, self.fork_txid) {
            self.child.open_ltx_file(level, min_txid, max_txid).await
        } else {
            self.parent.open_ltx_file(level, min_txid, max_txid).await
        }
    }

    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo> {
        if !Self::child_owns(min_txid, self.fork_txid) {
            return Err(crate::Error::Other(
                format!(
                    "refusing parent-owned LTX write min={} max={} fork={}",
                    min_txid.0, max_txid.0, self.fork_txid.0
                )
                .into(),
            ));
        }
        self.child
            .write_ltx_file(level, min_txid, max_txid, data)
            .await
    }

    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()> {
        self.child.delete_ltx_files(files).await
    }

    async fn delete_all(&self) -> Result<()> {
        self.child.delete_all().await
    }
}
