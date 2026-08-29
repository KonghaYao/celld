use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use celld_ltx::replica::calc_restore_plan;
use celld_ltx::ChainedReplicaClient;
use celld_ltx::Error;
use celld_ltx::FileInfo;
use celld_ltx::ReplicaClient;
use celld_ltx::TXID;
use celld_ltx::compaction_level::SNAPSHOT_LEVEL;
use tokio::sync::Mutex;

#[derive(Default)]
struct MockStore {
    files: BTreeMap<i32, Vec<FileInfo>>,
}

impl MockStore {
    fn insert(&mut self, info: FileInfo) {
        let level = info.level;
        self.files.entry(level).or_default().push(info);
        if let Some(level_files) = self.files.get_mut(&level) {
            level_files.sort_by(|left, right| {
                (left.min_txid.0, left.max_txid.0).cmp(&(right.min_txid.0, right.max_txid.0))
            });
        }
    }
}

#[derive(Clone)]
struct MockClient {
    store: Arc<Mutex<MockStore>>,
}

#[async_trait]
impl ReplicaClient for MockClient {
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>, celld_ltx::Error> {
        let store = self.store.lock().await;
        Ok(store
            .files
            .get(&level)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|info| info.min_txid >= seek)
            .collect())
    }

    async fn open_ltx_file(
        &self,
        _level: i32,
        _min_txid: TXID,
        _max_txid: TXID,
    ) -> Result<Vec<u8>, celld_ltx::Error> {
        Ok(Vec::new())
    }

    async fn write_ltx_file(
        &self,
        _level: i32,
        _min_txid: TXID,
        _max_txid: TXID,
        _data: &[u8],
    ) -> Result<FileInfo, celld_ltx::Error> {
        Err(celld_ltx::Error::Other("mock write".into()))
    }

    async fn delete_ltx_files(&self, _files: &[FileInfo]) -> Result<(), celld_ltx::Error> {
        Ok(())
    }

    async fn delete_all(&self) -> Result<(), celld_ltx::Error> {
        Ok(())
    }
}

fn file(level: i32, min: u64, max: u64) -> FileInfo {
    FileInfo {
        level,
        min_txid: TXID(min),
        max_txid: TXID(max),
        size: 4096,
        ..Default::default()
    }
}

#[tokio::test]
async fn chained_restore_plan_parent_snapshot_plus_child_incremental() {
    let parent_store = Arc::new(Mutex::new(MockStore::default()));
    parent_store
        .lock()
        .await
        .insert(file(SNAPSHOT_LEVEL, 1, 42));
    parent_store
        .lock()
        .await
        .insert(file(0, 1, 42));

    let child_store = Arc::new(Mutex::new(MockStore::default()));
    child_store
        .lock()
        .await
        .insert(file(0, 43, 45));

    let parent = MockClient {
        store: parent_store,
    };
    let child = MockClient {
        store: child_store,
    };
    let chained = ChainedReplicaClient::new(parent, child, TXID(42));

    let plan = calc_restore_plan(&chained, TXID(0)).await.expect("plan");
    assert!(plan.len() >= 2);
    assert_eq!(plan[0].level, SNAPSHOT_LEVEL);
    assert_eq!(plan[0].max_txid, TXID(42));
    assert_eq!(plan.last().unwrap().max_txid, TXID(45));
}

#[tokio::test]
async fn chained_restore_plan_empty_child_restores_to_fork_txid() {
    let parent_store = Arc::new(Mutex::new(MockStore::default()));
    parent_store
        .lock()
        .await
        .insert(file(SNAPSHOT_LEVEL, 1, 10));
    parent_store.lock().await.insert(file(0, 1, 10));

    let child_store = Arc::new(Mutex::new(MockStore::default()));
    let chained = ChainedReplicaClient::new(
        MockClient {
            store: parent_store,
        },
        MockClient {
            store: child_store,
        },
        TXID(10),
    );

    let plan = calc_restore_plan(&chained, TXID(10)).await.expect("plan");
    assert_eq!(plan.last().unwrap().max_txid, TXID(10));
}

#[tokio::test]
async fn chained_restore_plan_gap_returns_tx_not_available() {
    let parent_store = Arc::new(Mutex::new(MockStore::default()));
    parent_store.lock().await.insert(file(0, 1, 5));

    let child_store = Arc::new(Mutex::new(MockStore::default()));
    child_store.lock().await.insert(file(0, 8, 9));

    let chained = ChainedReplicaClient::new(
        MockClient {
            store: parent_store,
        },
        MockClient {
            store: child_store,
        },
        TXID(5),
    );

    let error = calc_restore_plan(&chained, TXID(0))
        .await
        .expect_err("gap");
    assert!(
        matches!(error, Error::TxNotAvailable | Error::Other(_)),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn chained_write_rejects_parent_owned_txid() {
    let chained = ChainedReplicaClient::new(
        MockClient {
            store: Arc::new(Mutex::new(MockStore::default())),
        },
        MockClient {
            store: Arc::new(Mutex::new(MockStore::default())),
        },
        TXID(10),
    );
    let error = chained
        .write_ltx_file(9, TXID(1), TXID(10), &[])
        .await
        .expect_err("parent-owned snapshot");
    let message = error.to_string();
    assert!(
        message.contains("parent-owned"),
        "unexpected error: {message}"
    );
}
