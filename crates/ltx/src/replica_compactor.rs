//! Additive LTX compaction through a [`ReplicaClient`].
//!
//! This module ports the storage-independent part of Litestream v0.5.16's
//! `Compactor`. It creates a destination object but never deletes a source.

use crate::client::ReplicaClient;
use crate::compaction_level::SNAPSHOT_LEVEL;
use crate::compactor::Compactor;
use crate::error::{Error, Result};
use crate::ltx::{FileInfo, HEADER_FLAG_NO_CHECKSUM};
use crate::ltx_file_path;
use crate::LtxHost;
use crate::TXID;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;

/// The immutable object and source volume from one compaction attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutput {
    /// The published destination object.
    pub info: FileInfo,
    /// The number of source objects in the merge.
    pub input_files: usize,
    /// The sum of the source object sizes.
    pub input_bytes: u64,
    /// The number of source objects read from the local LTX directory.
    pub local_input_files: usize,
}

/// Compacts LTX objects between two adjacent replica levels.
pub struct ReplicaCompactor<'a, C> {
    client: &'a C,
    verify: bool,
    max_files: usize,
    max_input_bytes: u64,
    local_path: Option<PathBuf>,
    host: LtxHost,
}

impl<'a, C: ReplicaClient> ReplicaCompactor<'a, C> {
    pub fn new(client: &'a C) -> Self {
        Self {
            client,
            verify: false,
            max_files: usize::MAX,
            max_input_bytes: u64::MAX,
            local_path: None,
            host: LtxHost::default(),
        }
    }

    /// Enables a destination-level continuity check after publication.
    pub fn with_verification(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    /// Limits one compaction attempt to a contiguous source prefix.
    pub fn with_limits(mut self, max_files: usize, max_input_bytes: u64) -> Self {
        self.max_files = max_files;
        self.max_input_bytes = max_input_bytes;
        self
    }

    /// Uses the local LTX directory before it reads an object from the replica.
    pub fn with_local_path(mut self, path: impl AsRef<Path>) -> Self {
        self.local_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Uses an injected clock and executor host.
    pub fn with_host(mut self, host: LtxHost) -> Self {
        self.host = host;
        self
    }

    /// Compacts one new object prefix from `destination_level - 1`.
    ///
    /// The method returns `Ok(None)` when the destination already covers every
    /// available source object. It publishes one immutable destination object
    /// and leaves every source object intact.
    pub async fn compact(&self, destination_level: i32) -> Result<Option<CompactionOutput>> {
        if !(1..SNAPSHOT_LEVEL).contains(&destination_level) {
            return Err(invalid("the destination compaction level is invalid"));
        }
        if self.max_files == 0 || self.max_input_bytes == 0 {
            return Err(invalid("the compaction limits must be positive"));
        }

        let destination = self.client.ltx_files(destination_level, TXID(0)).await?;
        let previous_max = destination
            .iter()
            .map(|file| file.max_txid)
            .max()
            .unwrap_or(TXID(0));
        let seek = TXID(previous_max.0.wrapping_add(1));
        let source_level = destination_level - 1;
        let available = self
            .client
            .ltx_files_bounded(source_level, seek, self.max_files)
            .await?;
        let mut source = Vec::new();
        let mut input_bytes = 0u64;
        for file in available {
            let size = u64::try_from(file.size)
                .map_err(|_| invalid("a compaction source has a negative size"))?;
            if source.len() == self.max_files {
                break;
            }
            let total = input_bytes
                .checked_add(size)
                .ok_or_else(|| invalid("the compaction source size overflows"))?;
            if total > self.max_input_bytes {
                if source.is_empty() {
                    return Err(invalid("a compaction source exceeds the byte limit"));
                }
                break;
            }
            input_bytes = total;
            source.push(file);
        }
        if source.is_empty() {
            return Ok(None);
        }

        let min_txid = source
            .iter()
            .map(|file| file.min_txid)
            .min()
            .ok_or_else(|| invalid("the compaction source is empty"))?;
        let max_txid = source
            .iter()
            .map(|file| file.max_txid)
            .max()
            .ok_or_else(|| invalid("the compaction source is empty"))?;
        if min_txid != seek {
            return Err(invalid(
                "the compaction source does not continue the destination level",
            ));
        }
        let mut readers = Vec::with_capacity(source.len());
        let mut local_input_files = 0usize;
        for file in &source {
            let bytes = match &self.local_path {
                Some(path) => {
                    let filename = ltx_file_path(
                        &path.to_string_lossy(),
                        file.level as u32,
                        file.min_txid,
                        file.max_txid,
                    );
                    match self.host.read_file(filename).await {
                        Ok(bytes) => {
                            local_input_files += 1;
                            bytes
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            self.client
                                .open_ltx_file(file.level, file.min_txid, file.max_txid)
                                .await?
                        }
                        Err(error) => return Err(Error::Io(error)),
                    }
                }
                None => {
                    self.client
                        .open_ltx_file(file.level, file.min_txid, file.max_txid)
                        .await?
                }
            };
            readers.push(Cursor::new(bytes));
        }

        // The merge is pure CPU over as much as 64 MiB and must not hold a
        // runtime worker: a restart drain runs rounds back-to-back, and on a
        // 4-vCPU node two pegged workers starved co-owned cells' durable
        // writes (2026-08-12 fleet roll).
        #[cfg(celld_internal_tests)]
        let drop_input = self.host.drop_compaction_input() && readers.len() >= 3;
        #[cfg(celld_internal_tests)]
        if drop_input {
            let interior = readers.len() / 2;
            readers.remove(interior);
        }
        let (header, output) = self
            .host
            .run_blocking(move || {
                let mut compactor = Compactor::new(Vec::new(), readers);
                compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
                #[cfg(celld_internal_tests)]
                if drop_input {
                    compactor.allow_non_contiguous_txids = true;
                }
                compactor.compact()?;
                Ok::<_, Error>((compactor.header(), compactor.into_writer()))
            })
            .await
            .map_err(|_| invalid("the compaction merge task panicked"))??;
        if header.min_txid != min_txid || header.max_txid != max_txid {
            return Err(invalid(
                "a compaction source key does not match its LTX header",
            ));
        }
        let info = self
            .client
            .write_ltx_file(destination_level, min_txid, max_txid, &output)
            .await?;

        if self.verify {
            self.verify_level(destination_level).await?;
        }
        Ok(Some(CompactionOutput {
            info,
            input_files: source.len(),
            input_bytes,
            local_input_files,
        }))
    }

    /// Verifies that a destination level has neither gaps nor overlaps.
    pub async fn verify_level(&self, level: i32) -> Result<()> {
        let files = self.client.ltx_files(level, TXID(0)).await?;
        for pair in files.windows(2) {
            let expected = pair[0].max_txid.0.wrapping_add(1);
            if pair[1].min_txid != TXID(expected) {
                return Err(invalid("the compaction level is not contiguous"));
            }
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> Error {
    Error::Other(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::file::FileReplicaClient;
    use crate::client::{make_test_ltx_file, ReplicaClient};
    use crate::ltx::{decode_file, decode_file_pages, PAGE_HEADER_FLAG_SIZE};

    async fn write_l0(client: &FileReplicaClient, txid: u64, seed: u8) {
        let data = make_test_ltx_file(TXID(txid), TXID(txid), seed);
        client
            .write_ltx_file(0, TXID(txid), TXID(txid), &data)
            .await
            .expect("write L0");
    }

    #[tokio::test]
    async fn compacts_only_the_uncovered_source_tail() {
        let directory = tempfile::tempdir().unwrap();
        let client = FileReplicaClient::new(directory.path().to_string_lossy().into_owned());
        for txid in 1..=3 {
            write_l0(&client, txid, txid as u8).await;
        }

        let compactor = ReplicaCompactor::new(&client).with_verification(true);
        let first = compactor.compact(1).await.unwrap().expect("first output");
        assert_eq!(
            (first.info.min_txid, first.info.max_txid),
            (TXID(1), TXID(3))
        );
        assert_eq!(first.input_files, 3);
        assert_eq!(first.local_input_files, 0);

        assert!(compactor.compact(1).await.unwrap().is_none());
        for txid in 4..=5 {
            write_l0(&client, txid, txid as u8).await;
        }
        let second = compactor.compact(1).await.unwrap().expect("second output");
        assert_eq!(
            (second.info.min_txid, second.info.max_txid),
            (TXID(4), TXID(5))
        );

        let level = client.ltx_files(1, TXID(0)).await.unwrap();
        assert_eq!(level.len(), 2);
        assert_eq!(client.ltx_files(0, TXID(0)).await.unwrap().len(), 5);

        let bytes = client.open_ltx_file(1, TXID(1), TXID(3)).await.unwrap();
        let decoded = decode_file(&bytes).unwrap();
        assert_eq!(decoded.header.flags, HEADER_FLAG_NO_CHECKSUM);
        assert_eq!(
            u16::from_be_bytes([bytes[104], bytes[105]]),
            PAGE_HEADER_FLAG_SIZE
        );
        assert_eq!(decode_file_pages(&bytes).unwrap(), vec![(1, vec![3; 512])]);
    }

    #[tokio::test]
    async fn rejects_invalid_destinations_and_inconsistent_levels() {
        let directory = tempfile::tempdir().unwrap();
        let client = FileReplicaClient::new(directory.path().to_string_lossy().into_owned());
        let compactor = ReplicaCompactor::new(&client);
        assert!(compactor.compact(0).await.is_err());
        assert!(compactor.compact(SNAPSHOT_LEVEL).await.is_err());

        let first = make_test_ltx_file(TXID(1), TXID(1), 1);
        let third = make_test_ltx_file(TXID(3), TXID(3), 3);
        client
            .write_ltx_file(1, TXID(1), TXID(1), &first)
            .await
            .unwrap();
        client
            .write_ltx_file(1, TXID(3), TXID(3), &third)
            .await
            .unwrap();
        assert!(compactor.verify_level(1).await.is_err());
    }

    #[tokio::test]
    async fn limits_each_attempt_to_a_contiguous_source_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let client = FileReplicaClient::new(directory.path().to_string_lossy().into_owned());
        for txid in 1..=5 {
            write_l0(&client, txid, txid as u8).await;
        }

        let compactor = ReplicaCompactor::new(&client)
            .with_limits(2, u64::MAX)
            .with_verification(true);
        let first = compactor.compact(1).await.unwrap().expect("first output");
        let second = compactor.compact(1).await.unwrap().expect("second output");
        let third = compactor.compact(1).await.unwrap().expect("third output");

        assert_eq!(
            (first.info.min_txid, first.info.max_txid),
            (TXID(1), TXID(2))
        );
        assert_eq!(
            (second.info.min_txid, second.info.max_txid),
            (TXID(3), TXID(4))
        );
        assert_eq!(
            (third.info.min_txid, third.info.max_txid),
            (TXID(5), TXID(5))
        );
        assert!(compactor.compact(1).await.unwrap().is_none());
        assert_eq!(client.ltx_files(0, TXID(0)).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn prefers_matching_local_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let client = FileReplicaClient::new(directory.path().to_string_lossy().into_owned());
        write_l0(&client, 1, 1).await;
        write_l0(&client, 2, 2).await;

        let output = ReplicaCompactor::new(&client)
            .with_local_path(directory.path())
            .compact(1)
            .await
            .unwrap()
            .expect("output");
        assert_eq!(output.local_input_files, 2);
    }

    #[tokio::test]
    async fn refuses_to_publish_a_gap_after_the_destination_tail() {
        let directory = tempfile::tempdir().unwrap();
        let client = FileReplicaClient::new(directory.path().to_string_lossy().into_owned());
        write_l0(&client, 1, 1).await;
        let compactor = ReplicaCompactor::new(&client).with_verification(true);
        compactor.compact(1).await.unwrap().expect("first output");

        write_l0(&client, 3, 3).await;
        assert!(compactor.compact(1).await.is_err());
        assert_eq!(client.ltx_files(1, TXID(0)).await.unwrap().len(), 1);
    }
}
