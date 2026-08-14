//! integration_s3 — runs the T5 generic `run_client_suite` against the T7
//! `ObjectStoreClient` through a real S3-compatible HTTP endpoint.
//!
//! The test also verifies provider behavior that the `ReplicaClient` contract
//! does not expose: byte ranges, object metadata, multipart uploads, and
//! conditional writes. CI supplies a required fixture. Local runs skip with an
//! explicit reason when the optional fixture is absent.
//!
//! Configuration:
//!   * `CELLD_LTX_S3_ENDPOINT` (default `http://127.0.0.1:7070`)
//!   * `CELLD_LTX_S3_BUCKET` (default `celld-ltx`)
//!   * `CELLD_LTX_S3_REQUIRED=1` (fail instead of skipping)
//!   * `AWS_ACCESS_KEY_ID` (default `celld-ltx-access-key`)
//!   * `AWS_SECRET_ACCESS_KEY` (default `celld-ltx-secret-key`)
//!   * `AWS_REGION` (default `us-east-1`)

#![cfg(feature = "s3")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celld_ltx::client::object_store::{ObjectStoreClient, ObjectStoreConfig};
use celld_ltx::client::{run_client_suite, ReplicaClient};
use celld_ltx::ltx::{self, Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use celld_ltx::TXID;
use futures_util::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{
    Attribute, Error as ObjectStoreError, GetOptions, ObjectStore, PutMode, PutPayload,
};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7070";
const DEFAULT_BUCKET: &str = "celld-ltx";
const DEFAULT_ACCESS_KEY: &str = "celld-ltx-access-key";
const DEFAULT_SECRET_KEY: &str = "celld-ltx-secret-key";
const DEFAULT_REGION: &str = "us-east-1";
const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const TIMESTAMP_MILLIS: i64 = 1_609_459_200_123;
const TIMESTAMP_RFC3339: &str = "2021-01-01T00:00:00.123Z";

static PREFIX_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct S3Fixture {
    config: ObjectStoreConfig,
    store: Arc<dyn ObjectStore>,
}

impl S3Fixture {
    fn client(&self) -> ObjectStoreClient {
        ObjectStoreClient::with_store(self.config.clone(), self.store.clone())
    }
}

fn env_or_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn endpoint() -> String {
    env_or_default("CELLD_LTX_S3_ENDPOINT", DEFAULT_ENDPOINT)
}

fn bucket() -> String {
    env_or_default("CELLD_LTX_S3_BUCKET", DEFAULT_BUCKET)
}

fn required() -> bool {
    std::env::var("CELLD_LTX_S3_REQUIRED").as_deref() == Ok("1")
}

/// A unique path prefix keeps parallel and repeated runs isolated. All cleanup
/// operations stay below this prefix.
fn unique_path(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let sequence = PREFIX_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "celld-ltx-test/{tag}/{}-{nanos}-{sequence}",
        std::process::id()
    )
}

fn s3_config(tag: &str) -> ObjectStoreConfig {
    ObjectStoreConfig {
        bucket: bucket(),
        path: unique_path(tag),
        region: env_or_default("AWS_REGION", DEFAULT_REGION),
        endpoint: endpoint(),
        access_key_id: env_or_default("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY),
        secret_access_key: env_or_default("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY),
        // The local fixture has no wildcard DNS name, so requests use bucket paths.
        force_path_style: true,
        ..Default::default()
    }
}

fn fixture_unavailable(
    test: &str,
    config: &ObjectStoreConfig,
    reason: impl std::fmt::Display,
) -> Option<S3Fixture> {
    if required() {
        panic!(
            "required S3 fixture for {test} is unavailable at endpoint {} bucket {}: {reason}",
            config.endpoint, config.bucket
        );
    }

    eprintln!(
        "SKIP {test}: optional S3 fixture is unavailable at endpoint {} bucket {}: {reason}",
        config.endpoint, config.bucket
    );
    None
}

/// Perform a signed, bucket-scoped LIST through the same adapter as the tests.
/// A TCP listener or an unauthenticated health response is not sufficient.
async fn ready_fixture(test: &str, tag: &str) -> Option<S3Fixture> {
    let config = s3_config(tag);
    let store = match config.build_store() {
        Ok(store) => store,
        Err(error) => return fixture_unavailable(test, &config, error),
    };

    let readiness = tokio::time::timeout(READINESS_TIMEOUT, async {
        let mut objects = store.list(None);
        objects.try_next().await
    })
    .await;

    match readiness {
        Ok(Ok(_)) => Some(S3Fixture { config, store }),
        Ok(Err(error)) => fixture_unavailable(test, &config, error),
        Err(_) => fixture_unavailable(
            test,
            &config,
            format_args!("S3 LIST exceeded the {READINESS_TIMEOUT:?} timeout"),
        ),
    }
}

#[tokio::test]
async fn object_store_passes_s3_conformance_suite() {
    let Some(fixture) =
        ready_fixture("object_store_passes_s3_conformance_suite", "conformance").await
    else {
        return;
    };
    let client = fixture.client();

    run_client_suite(&client).await;

    client.delete_all().await.expect("final cleanup");
}

#[tokio::test]
async fn object_store_preserves_metadata_and_supports_range_reads() {
    let Some(fixture) = ready_fixture(
        "object_store_preserves_metadata_and_supports_range_reads",
        "metadata-range",
    )
    .await
    else {
        return;
    };
    let client = fixture.client();
    let data = make_timestamped_ltx(TXID(1), TXID(1), TIMESTAMP_MILLIS);

    client
        .write_ltx_file(0, TXID(1), TXID(1), &data)
        .await
        .expect("write LTX object");

    let key = ObjPath::from(format!(
        "{}/0000/{}",
        fixture.config.path,
        ltx::format_filename(TXID(1), TXID(1))
    ));
    let head = fixture
        .store
        .get_opts(
            &key,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .expect("HEAD LTX object");
    let timestamp = head
        .attributes
        .get(&Attribute::Metadata("litestream-timestamp".into()))
        .expect("litestream timestamp metadata");
    assert_eq!(timestamp.as_ref(), TIMESTAMP_RFC3339);

    let range = (data.len() / 4)..(data.len() * 3 / 4);
    assert!(range.len() >= 32, "range must cover a nontrivial window");
    let ranged = fixture
        .store
        .get_range(&key, range.clone())
        .await
        .expect("range GET LTX object");
    assert_eq!(ranged.as_ref(), &data[range]);

    client.delete_all().await.expect("cleanup");
}

/// A file at or above the 5 MiB threshold uses multipart upload and reads back
/// byte-for-byte. This keeps the boundary from the upstream S3 client test.
#[tokio::test]
async fn object_store_multipart_upload_round_trips() {
    let Some(fixture) =
        ready_fixture("object_store_multipart_upload_round_trips", "multipart").await
    else {
        return;
    };
    let client = fixture.client();
    client.delete_all().await.expect("clean start");

    // Pad a real, decodable LTX file past the multipart threshold.
    let data = make_large_ltx(TXID(1), TXID(1), 6 * 1024 * 1024);
    assert!(
        data.len() >= 5 * 1024 * 1024,
        "test file must exceed the multipart threshold (got {} bytes)",
        data.len()
    );

    let info = client
        .write_ltx_file(0, TXID(1), TXID(1), &data)
        .await
        .expect("multipart write");
    assert_eq!(info.size, data.len() as i64);

    let got = client
        .open_ltx_file(0, TXID(1), TXID(1))
        .await
        .expect("read back multipart object");
    assert_eq!(got, data, "multipart round-trip is byte-exact");

    client.delete_all().await.expect("cleanup");
}

#[tokio::test]
async fn object_store_rejects_duplicate_create_and_stale_update() {
    let Some(fixture) = ready_fixture(
        "object_store_rejects_duplicate_create_and_stale_update",
        "conditional-put",
    )
    .await
    else {
        return;
    };
    let key = ObjPath::from(format!("{}/conditional", fixture.config.path));

    let first = fixture
        .store
        .put_opts(
            &key,
            PutPayload::from(b"first".to_vec()),
            PutMode::Create.into(),
        )
        .await
        .expect("conditional create");
    let first_etag = first
        .e_tag
        .as_deref()
        .filter(|etag| !etag.is_empty())
        .expect("conditional create must return an ETag")
        .to_string();

    let duplicate_error = fixture
        .store
        .put_opts(
            &key,
            PutPayload::from(b"duplicate".to_vec()),
            PutMode::Create.into(),
        )
        .await
        .expect_err("duplicate conditional create must fail");
    assert!(
        matches!(duplicate_error, ObjectStoreError::AlreadyExists { .. }),
        "duplicate create returned {duplicate_error}"
    );

    let second = fixture
        .store
        .put_opts(
            &key,
            PutPayload::from(b"current".to_vec()),
            PutMode::Update(first.clone().into()).into(),
        )
        .await
        .expect("update with current ETag");
    let second_etag = second
        .e_tag
        .as_deref()
        .filter(|etag| !etag.is_empty())
        .expect("conditional update must return an ETag");
    assert_ne!(first_etag, second_etag, "the update must change the ETag");

    let stale_error = fixture
        .store
        .put_opts(
            &key,
            PutPayload::from(b"stale".to_vec()),
            PutMode::Update(first.into()).into(),
        )
        .await
        .expect_err("update with stale ETag must fail");
    assert!(
        matches!(stale_error, ObjectStoreError::Precondition { .. }),
        "stale update returned {stale_error}"
    );

    let final_value = fixture
        .store
        .get(&key)
        .await
        .expect("read final conditional value")
        .bytes()
        .await
        .expect("collect final conditional value");
    assert_eq!(final_value.as_ref(), b"current");

    fixture.store.delete(&key).await.expect("cleanup");
}

fn header(min_txid: TXID, max_txid: TXID, page_size: u32, commit: u32, timestamp: i64) -> Header {
    Header {
        version: VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit,
        min_txid,
        max_txid,
        timestamp,
        pre_apply_checksum: 0,
        wal_offset: 0,
        wal_size: 0,
        wal_salt1: 0,
        wal_salt2: 0,
        node_id: 0,
    }
}

fn make_timestamped_ltx(min_txid: TXID, max_txid: TXID, timestamp: i64) -> Vec<u8> {
    let page_size = 512;
    let page = (0..page_size).map(|offset| offset as u8).collect();
    ltx::encode_file(
        &header(min_txid, max_txid, page_size, 1, timestamp),
        &[(1, page)],
        0,
    )
    .expect("encode timestamped LTX")
}

/// Build a real LTX file whose encoded size is at least `min_bytes`.
fn make_large_ltx(min_txid: TXID, max_txid: TXID, min_bytes: usize) -> Vec<u8> {
    let page_size: u32 = 4096;
    let lock = ltx::lock_pgno(page_size);
    let n_pages = (min_bytes / page_size as usize) + 8;

    let mut pages: Vec<(u32, Vec<u8>)> = Vec::with_capacity(n_pages);
    let mut pgno: u32 = 1;
    let mut commit: u32 = 0;
    while pages.len() < n_pages {
        if pgno == lock {
            pgno += 1;
            continue;
        }
        // SplitMix64-filled pages are deterministic and resist compression.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(pgno as u64 + 1);
        let mut buf = vec![0u8; page_size as usize];
        let mut i = 0;
        while i < buf.len() {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let bytes = z.to_le_bytes();
            let take = bytes.len().min(buf.len() - i);
            buf[i..i + take].copy_from_slice(&bytes[..take]);
            i += take;
        }
        pages.push((pgno, buf));
        commit = pgno;
        pgno += 1;
    }

    let hdr = header(min_txid, max_txid, page_size, commit, 0);
    ltx::encode_file(&hdr, &pages, 0).expect("encode large LTX")
}
