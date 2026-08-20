//! client — the `ReplicaClient` trait + a generic conformance suite.
//!
//! Ported from litestream@v0.5.11 `replica_client.go`. The trait is the storage
//! abstraction every backend implements; `run_client_suite` is the reusable
//! conformance test that each backend (T6 file, T7 object_store) must pass.
//!
//! # DECISION: buffered I/O for the KEEP scope
//! Go's `ReplicaClient` uses `io.Reader`/`io.ReadCloser`. We take/return owned
//! byte buffers (`&[u8]` / `Vec<u8>`) instead. L0 files are bounded in size;
//! streaming large snapshots is a noted follow-on (logged in OPEN_QUESTIONS).

use crate::error::Result;
use crate::ltx::{self, FileInfo, Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use crate::TXID;
use async_trait::async_trait;

pub mod bundle;
pub mod file;
pub mod object_store;

/// Client for reading and writing LTX files on a replica backend.
///
/// Ported from the `ReplicaClient` interface (replica_client.go:19-51). Methods
/// take a compaction `level` (0 = L0, the only level in the one-shot scope).
#[async_trait]
pub trait ReplicaClient: Send + Sync {
    /// Returns all LTX files for `level`, sorted ascending by `min_txid`, that
    /// start at or after `seek`.
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>>;

    /// Returns at most `limit` LTX files at or after `seek`.
    ///
    /// Remote clients can stop their object listing after they collect the
    /// requested prefix. The default keeps compatibility with local and test
    /// clients.
    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> Result<Vec<FileInfo>> {
        let mut files = self.ltx_files(level, seek).await?;
        files.truncate(limit);
        Ok(files)
    }

    /// Reads an LTX file. Returns an `io::ErrorKind::NotFound` error (wrapped)
    /// if the file does not exist.
    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>>;

    /// Writes an LTX file to the replica and returns its metadata.
    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo>;

    /// Deletes the given LTX files.
    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()>;

    /// Deletes all files on the replica.
    async fn delete_all(&self) -> Result<()>;
}

/// Builds a small, valid L0 LTX file for tests. Files use the `NoChecksum` flag
/// (matching real litestream L0 WAL-segment files), so any `min_txid`/`max_txid`
/// is valid without a pre-apply checksum. One 512-byte page filled with `seed`.
pub fn make_test_ltx_file(min_txid: TXID, max_txid: TXID, seed: u8) -> Vec<u8> {
    let page_size: u32 = 512;
    let pages = vec![(1u32, vec![seed; page_size as usize])];
    let header = Header {
        version: VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit: 1,
        min_txid,
        max_txid,
        timestamp: 0,
        pre_apply_checksum: 0,
        wal_offset: 0,
        wal_size: 0,
        wal_salt1: 0,
        wal_salt2: 0,
        node_id: 0,
    };
    ltx::encode_file(&header, &pages, 0).expect("encode test ltx file")
}

/// Generic conformance suite every `ReplicaClient` backend must pass: empty
/// state, write, listing order, seek filtering, reads, not-found, and deletion.
/// Call from an async test; panics with a descriptive message on
/// the first violation.
///
/// Ported from the shared `TestReplicaClient_*` behaviors in replica_client_test.go.
pub async fn run_client_suite<C: ReplicaClient>(client: &C) {
    // 1. Starts empty.
    client.delete_all().await.expect("delete_all");
    let files = client.ltx_files(0, TXID(0)).await.expect("list empty");
    assert!(files.is_empty(), "replica should start empty");

    // 2. Write a snapshot (txid 1) plus four incremental L0 files.
    let mut written: Vec<(FileInfo, Vec<u8>)> = Vec::new();
    for i in 1u64..=5 {
        let data = make_test_ltx_file(TXID(i), TXID(i), i as u8);
        let info = client
            .write_ltx_file(0, TXID(i), TXID(i), &data)
            .await
            .unwrap_or_else(|e| panic!("write txid {i}: {e}"));
        assert_eq!(info.min_txid, TXID(i), "written min txid");
        assert_eq!(info.max_txid, TXID(i), "written max txid");
        assert_eq!(info.size, data.len() as i64, "written size");
        written.push((info, data));
    }

    // 3. Listing returns all files, ascending by min txid (the brief's sort
    //    invariant — must hold regardless of backend listing order).
    let listed = client.ltx_files(0, TXID(0)).await.expect("list all");
    assert_eq!(listed.len(), 5, "all files listed");
    let order: Vec<u64> = listed.iter().map(|f| f.min_txid.0).collect();
    assert_eq!(
        order,
        vec![1, 2, 3, 4, 5],
        "files sorted ascending by min txid"
    );

    // 4. Seek filters to files starting at or after the given txid.
    let seeked = client.ltx_files(0, TXID(3)).await.expect("seek list");
    let order: Vec<u64> = seeked.iter().map(|f| f.min_txid.0).collect();
    assert_eq!(order, vec![3, 4, 5], "seek=3 yields txids >= 3");

    let bounded = client
        .ltx_files_bounded(0, TXID(2), 2)
        .await
        .expect("bounded list");
    let order: Vec<u64> = bounded.iter().map(|f| f.min_txid.0).collect();
    assert_eq!(order, vec![2, 3], "bounded listing stops at its limit");

    // 5. Full read returns the exact written bytes, which still decode + verify.
    for (info, data) in &written {
        let got = client
            .open_ltx_file(0, info.min_txid, info.max_txid)
            .await
            .expect("full read");
        assert_eq!(&got, data, "full read matches written bytes");
        ltx::decode_file(&got).expect("round-tripped file still verifies");
    }

    // 6. Opening a missing file is an error.
    let missing = client.open_ltx_file(0, TXID(99), TXID(99)).await;
    assert!(missing.is_err(), "missing file must error");

    // 7. Deleting a subset removes exactly those files.
    let to_delete: Vec<FileInfo> = listed.iter().take(2).cloned().collect();
    client
        .delete_ltx_files(&to_delete)
        .await
        .expect("delete subset");
    let after = client
        .ltx_files(0, TXID(0))
        .await
        .expect("list after delete");
    assert_eq!(after.len(), 3, "two files deleted");
    let order: Vec<u64> = after.iter().map(|f| f.min_txid.0).collect();
    assert_eq!(order, vec![3, 4, 5], "the first two were removed");

    // 8. delete_all clears everything.
    client.delete_all().await.expect("delete_all");
    let empty = client.ltx_files(0, TXID(0)).await.expect("list empty");
    assert!(empty.is_empty(), "delete_all clears the replica");
}
