//! client::object_store — S3/R2/MinIO `ReplicaClient` via the `object_store` crate.
//!
//! Ported (behavior only) from litestream@v0.5.11 `s3/replica_client.go`. We do
//! **not** use the Go AWS SDK; instead the five `ReplicaClient` operations are
//! mapped onto `object_store::ObjectStore` (the `s3` cargo feature wires the
//! `object_store::aws::AmazonS3Builder` backend). The behavioral invariants that
//! let this client pass the T5 conformance suite against a real MinIO are kept:
//!
//!   * key/path scheme `{path}/{level:04x}/{min}-{max}.ltx`
//!     (s3/replica_client.go:629, 677, 1040-1042);
//!   * the 5 MiB single-PUT vs multipart threshold
//!     (s3/replica_client.go:99 + the Go uploader default);
//!   * list + seek-skip on `min_txid < seek`, ascending TXID order
//!     (s3/replica_client.go:1530-1533);
//!   * `NoSuchKey` → `os.ErrNotExist` error mapping
//!     (s3/replica_client.go:647-649, 1662-1668);
//!   * batch DELETE up to 1000 keys per call with per-key error surfacing
//!     (s3/replica_client.go:1028-1101).
//!
//! The provider-defaults table (`ParseHost`, path-style flags, endpoint env
//! var) is a faithful port of `NewReplicaClientFromURL`
//! (s3/replica_client.go:133-314) so a `s3://…` URL configures the same way.
//!
//! This whole module is gated behind `#[cfg(feature = "s3")]` because it needs
//! `object_store`'s AWS backend.

#![cfg(feature = "s3")]

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat};
use futures_util::stream::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{
    Attribute, AttributeValue, Attributes, ClientOptions, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload,
};

use crate::error::{Error, Result};
use crate::ltx::{self, FileInfo};
use crate::replica_url::{
    self, bool_query_value, ensure_endpoint_scheme, region_from_s3_arn, ParsedReplicaUrl,
};
use crate::TXID;

use super::ReplicaClient;

/// The standard Litestream S3 metadata key for an LTX header timestamp.
const METADATA_KEY_TIMESTAMP: &str = "litestream-timestamp";

/// Max keys S3 operates on per batch DELETE. `MaxKeys` (s3/replica_client.go:56).
pub const MAX_KEYS: usize = 1000;

/// Region used when none is specified. `DefaultRegion` (s3/replica_client.go:59).
pub const DEFAULT_REGION: &str = "us-east-1";

/// Multipart upload threshold: data at or above this size is uploaded with
/// `put_multipart`; below it, a single `put`. Matches the Go uploader's 5 MiB
/// `PartSize` default (s3/replica_client.go:99).
pub const MULTIPART_THRESHOLD: usize = 5 * 1024 * 1024;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the S3/R2/MinIO backend.
///
/// Maps to the public fields of Go's `ReplicaClient` struct
/// (s3/replica_client.go:78-116). Zero/`None` values mean "use the backend
/// default".
#[derive(Debug, Clone, Default)]
pub struct ObjectStoreConfig {
    /// Bucket name (required).
    pub bucket: String,
    /// Key prefix within the bucket.
    pub path: String,
    /// AWS region.
    pub region: String,
    /// Custom endpoint (MinIO, R2, …); empty = native AWS.
    pub endpoint: String,
    /// Static access key id; empty = ambient credential chain.
    pub access_key_id: String,
    /// Static secret access key; empty = ambient credential chain.
    pub secret_access_key: String,
    /// Session token for temporary/scoped credentials (STS, R2 API tokens);
    /// empty = none. Required alongside temporary keys or signing fails.
    pub session_token: String,
    /// Force path-style addressing (required for MinIO/Backblaze/Supabase/Filebase).
    pub force_path_style: bool,
    /// Skip TLS verification (allows self-signed endpoints).
    pub skip_verify: bool,
    /// Multipart part size in bytes; 0 = default (5 MiB).
    pub part_size: u64,
}

impl ObjectStoreConfig {
    /// Construct from a parsed `s3://` URL, mirroring `NewReplicaClientFromURL`
    /// (s3/replica_client.go:133-314): host → bucket/region/endpoint/path-style
    /// (or ARN), query-param overrides (camelCase ↔ hyphenated aliases), the
    /// `AWS_*`/`LITESTREAM_*` env credentials, the `LITESTREAM_S3_ENDPOINT` env
    /// fallback, and the provider-specific path-style defaults for
    /// MinIO/Backblaze/Filebase/Supabase.
    pub fn from_url(parsed: &ParsedReplicaUrl) -> Result<Self> {
        let host = &parsed.host;
        let query = &parsed.query;

        // Host → bucket/region/endpoint/forcePathStyle (or ARN access point).
        let (bucket, mut region, mut endpoint, mut force_path_style) = if host.starts_with("arn:") {
            (host.clone(), region_from_s3_arn(host), String::new(), false)
        } else {
            parse_host(host)
        };

        let q = Some(query);

        // endpoint query param: ensure scheme, default to path-style for custom
        // endpoints unless force-path-style is explicitly set to false.
        let q_endpoint = query.get("endpoint");
        if !q_endpoint.is_empty() {
            let (ep, _) = ensure_endpoint_scheme(q_endpoint);
            endpoint = ep;
            match bool_query_value(q, &["forcePathStyle", "force-path-style"]) {
                Some(false) => {}
                _ => force_path_style = true,
            }
        }
        let q_region = query.get("region");
        if !q_region.is_empty() {
            region = q_region.to_string();
        }
        if let Some(v) = bool_query_value(q, &["forcePathStyle", "force-path-style"]) {
            force_path_style = v;
        }
        let mut skip_verify = false;
        if let Some(v) = bool_query_value(q, &["skipVerify", "skip-verify"]) {
            skip_verify = v;
        }

        let mut part_size: u64 = 0;
        let v = query.get("partSize");
        let v2 = query.get("part-size");
        if !v.is_empty() {
            if let Ok(n) = v.parse::<u64>() {
                if n > 0 {
                    part_size = n;
                }
            }
        } else if !v2.is_empty() {
            if let Ok(n) = v2.parse::<u64>() {
                if n > 0 {
                    part_size = n;
                }
            }
        }

        if bucket.is_empty() {
            return Err(Error::Other("bucket required for s3 replica URL".into()));
        }

        // Track whether forcePathStyle was explicitly set via query param
        // (s3/replica_client.go:208) — this gates the env-var/provider defaults.
        let force_path_style_set =
            !query.get("forcePathStyle").is_empty() || !query.get("force-path-style").is_empty();

        // Static credentials from env (AWS_* preferred, then LITESTREAM_*).
        let mut access_key_id = String::new();
        let mut secret_access_key = String::new();
        if let Some(v) = nonempty_env("AWS_ACCESS_KEY_ID") {
            access_key_id = v;
        } else if let Some(v) = nonempty_env("LITESTREAM_ACCESS_KEY_ID") {
            access_key_id = v;
        }
        if let Some(v) = nonempty_env("AWS_SECRET_ACCESS_KEY") {
            secret_access_key = v;
        } else if let Some(v) = nonempty_env("LITESTREAM_SECRET_ACCESS_KEY") {
            secret_access_key = v;
        }
        let session_token = nonempty_env("AWS_SESSION_TOKEN")
            .or_else(|| nonempty_env("LITESTREAM_SESSION_TOKEN"))
            .unwrap_or_default();

        // LITESTREAM_S3_ENDPOINT env fallback (only when no endpoint yet).
        if endpoint.is_empty() {
            if let Some(v) = nonempty_env("LITESTREAM_S3_ENDPOINT") {
                let (ep, _) = ensure_endpoint_scheme(&v);
                endpoint = ep;
                if !force_path_style_set {
                    force_path_style = true;
                }
            }
        }

        // Provider detection for applying defaults.
        let is_filebase = replica_url::is_filebase_endpoint(&endpoint);
        let is_backblaze = replica_url::is_backblaze_endpoint(&endpoint);
        let is_minio = replica_url::is_minio_endpoint(&endpoint);
        let is_supabase = replica_url::is_supabase_endpoint(&endpoint);
        if !force_path_style_set && (is_filebase || is_backblaze || is_minio || is_supabase) {
            force_path_style = true;
        }

        Ok(ObjectStoreConfig {
            bucket,
            path: parsed.path.clone(),
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            force_path_style,
            skip_verify,
            part_size,
        })
    }

    /// Build the backing `Arc<dyn ObjectStore>` for this config. Public so a host
    /// can build one store for a bucket and share it across many
    /// [`ObjectStoreClient::with_store`] clients that differ only by key prefix
    /// — one connection pool for every cell on a node.
    pub fn build_store(&self) -> Result<Arc<dyn ObjectStore>> {
        if self.bucket.is_empty() {
            return Err(Error::Other("s3: bucket name is required".into()));
        }

        let region = if self.region.is_empty() {
            DEFAULT_REGION.to_string()
        } else {
            self.region.clone()
        };

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(region)
            // Path-style ⇔ NOT virtual-hosted-style (s3/replica_client.go:258-263).
            .with_virtual_hosted_style_request(!self.force_path_style)
            // object_store ships with conditional puts DISABLED for S3 and
            // answers PutMode::Create with NotImplemented. The epoch seal is a
            // conditional create (first restorer wins), and every provider on
            // the support matrix speaks the If-None-Match/If-Match headers
            // this enables.
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

        if !self.endpoint.is_empty() {
            // A plaintext or local endpoint must allow HTTP. skip_verify applies
            // only to TLS certificate validation.
            let allow_http = self.endpoint.starts_with("http://")
                || replica_url::is_local_endpoint(&self.endpoint);
            let client_options = ClientOptions::new()
                .with_allow_http(allow_http)
                .with_allow_invalid_certificates(self.skip_verify);
            builder = builder
                .with_endpoint(&self.endpoint)
                .with_client_options(client_options);
        }

        if !self.access_key_id.is_empty() {
            builder = builder.with_access_key_id(&self.access_key_id);
        }
        if !self.secret_access_key.is_empty() {
            builder = builder.with_secret_access_key(&self.secret_access_key);
        }
        if !self.session_token.is_empty() {
            builder = builder.with_token(&self.session_token);
        }

        let store = builder
            .build()
            .map_err(|e| Error::Other(format!("s3: build store: {e}").into()))?;
        Ok(Arc::new(store))
    }

    /// Effective multipart part size (`part_size`, or the 5 MiB default).
    fn effective_part_size(&self) -> usize {
        if self.part_size > 0 {
            self.part_size as usize
        } else {
            MULTIPART_THRESHOLD
        }
    }
}

/// Returns `Some(value)` for a non-empty env var, else `None`. Mirrors Go's
/// `if v := os.Getenv(k); v != ""` pattern (s3/replica_client.go:211-224).
fn nonempty_env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

// ── ParseHost ─────────────────────────────────────────────────────────────────

/// Parse an S3 host into `(bucket, region, endpoint, force_path_style)`.
///
/// Direct port of `ParseHost` (s3/replica_client.go:1608-1652): MinIO-style
/// `bucket.host:port`, then the AWS / DigitalOcean / Backblaze / Filebase /
/// Scaleway provider patterns, falling back to "host *is* the bucket".
pub fn parse_host(host: &str) -> (String, String, String, bool) {
    // MinIO-style hosts: `bucket.host:port` (a colon and not a ".com").
    if host.contains(':') && !host.contains(".com") {
        // SplitN(host, ".", 2)
        if let Some((bucket, rest)) = host.split_once('.') {
            return (
                bucket.to_string(),
                DEFAULT_REGION.to_string(),
                format!("http://{rest}"),
                true,
            );
        }
        // No bucket in host, just host:port.
        return (String::new(), String::new(), format!("http://{host}"), true);
    }

    // AWS S3: `^(.+)\.s3(?:\.([^.]+))?\.amazonaws\.com$`
    if let Some((bucket, region)) = match_aws_s3(host) {
        return (bucket, region, String::new(), false);
    }
    // DigitalOcean: `^(?:(.+)\.)?([^.]+)\.digitaloceanspaces.com$`
    if let Some((bucket, region)) = match_two_label_suffix(host, ".digitaloceanspaces.com") {
        return (
            bucket,
            region.clone(),
            format!("https://{region}.digitaloceanspaces.com"),
            false,
        );
    }
    // Backblaze: `^(?:(.+)\.)?s3.([^.]+)\.backblazeb2.com$`
    if let Some((bucket, region)) = match_s3_region_suffix(host, ".backblazeb2.com") {
        return (
            bucket,
            region.clone(),
            format!("https://s3.{region}.backblazeb2.com"),
            true,
        );
    }
    // Filebase: `^(?:(.+)\.)?s3.filebase.com$`
    if let Some(bucket) = match_filebase(host) {
        return (bucket, String::new(), "s3.filebase.com".to_string(), false);
    }
    // Scaleway: `^(?:(.+)\.)?s3.([^.]+)\.scw\.cloud$`
    if let Some((bucket, region)) = match_s3_region_suffix(host, ".scw.cloud") {
        return (
            bucket,
            region.clone(),
            format!("s3.{region}.scw.cloud"),
            false,
        );
    }

    // Standard S3: the host is the bucket name.
    (host.to_string(), String::new(), String::new(), false)
}

/// `^(.+)\.s3(?:\.([^.]+))?\.amazonaws\.com$` → (bucket, region).
fn match_aws_s3(host: &str) -> Option<(String, String)> {
    let rest = host.strip_suffix(".amazonaws.com")?;
    // rest = "<bucket>.s3" or "<bucket>.s3.<region>"
    if let Some(bucket) = rest.strip_suffix(".s3") {
        if bucket.is_empty() {
            return None;
        }
        return Some((bucket.to_string(), String::new()));
    }
    // "<bucket>.s3.<region>": find the ".s3." separator; region is a single
    // label ([^.]+) — i.e. the remainder after ".s3." must contain no dot.
    let idx = rest.find(".s3.")?;
    let bucket = &rest[..idx];
    let region = &rest[idx + 4..];
    if bucket.is_empty() || region.is_empty() || region.contains('.') {
        return None;
    }
    Some((bucket.to_string(), region.to_string()))
}

/// `^(?:(.+)\.)?([^.]+)\.<suffix>$` → (bucket, region). `suffix` starts with '.'.
fn match_two_label_suffix(host: &str, suffix: &str) -> Option<(String, String)> {
    let rest = host.strip_suffix(suffix)?;
    if rest.is_empty() {
        return None;
    }
    // The last label before the suffix is the region; anything before it
    // (optionally) is the bucket.
    match rest.rfind('.') {
        Some(i) => {
            let bucket = &rest[..i];
            let region = &rest[i + 1..];
            if region.is_empty() {
                return None;
            }
            Some((bucket.to_string(), region.to_string()))
        }
        None => Some((String::new(), rest.to_string())),
    }
}

/// `^(?:(.+)\.)?s3.([^.]+)\.<suffix>$` → (bucket, region). `suffix` starts with '.'.
fn match_s3_region_suffix(host: &str, suffix: &str) -> Option<(String, String)> {
    let rest = host.strip_suffix(suffix)?;
    // rest = "[bucket.]s3.<region>"; region is one label ([^.]+).
    // Bucket-less form: rest == "s3.<region>".
    if let Some(region) = rest.strip_prefix("s3.") {
        if region.is_empty() || region.contains('.') {
            return None;
        }
        return Some((String::new(), region.to_string()));
    }
    // Bucketed form: rest == "<bucket>.s3.<region>".
    let sep = rest.find(".s3.")?;
    let bucket = &rest[..sep];
    let region = &rest[sep + 4..];
    if bucket.is_empty() || region.is_empty() || region.contains('.') {
        return None;
    }
    Some((bucket.to_string(), region.to_string()))
}

/// `^(?:(.+)\.)?s3.filebase.com$` → bucket.
fn match_filebase(host: &str) -> Option<String> {
    if host == "s3.filebase.com" {
        return Some(String::new());
    }
    let bucket = host.strip_suffix(".s3.filebase.com")?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket.to_string())
    }
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Concrete S3/R2/MinIO backend, wrapping a lazily-initialised
/// `Arc<dyn ObjectStore>`. The config-driven path builds an S3 store, but
/// [`Self::with_store`] accepts any prebuilt `ObjectStore` — the five
/// replica operations are provider-neutral, and a host can inject e.g. a
/// GCS or in-memory store.
///
/// Mirrors Go `ReplicaClient` (s3/replica_client.go:78-116). The inner store is
/// created on the first call that needs it (`OnceCell`, mirroring `Init`,
/// s3/replica_client.go:322-477), so construction is infallible and race-free.
pub struct ObjectStoreClient {
    store: tokio::sync::OnceCell<Arc<dyn ObjectStore>>,
    config: ObjectStoreConfig,
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreClient")
            .field("config", &self.config)
            .field("initialized", &self.store.initialized())
            .finish()
    }
}

impl ObjectStoreClient {
    /// Create a client from config (no I/O; the store is built on first use).
    pub fn new(config: ObjectStoreConfig) -> Self {
        ObjectStoreClient {
            store: tokio::sync::OnceCell::new(),
            config,
        }
    }

    /// Create a client directly from an already-built `ObjectStore` (e.g. an
    /// in-memory store for tests, or a pre-configured backend).
    pub fn with_store(config: ObjectStoreConfig, store: Arc<dyn ObjectStore>) -> Self {
        let cell = tokio::sync::OnceCell::new();
        cell.set(store).ok();
        ObjectStoreClient {
            store: cell,
            config,
        }
    }

    /// Get-or-build the inner store, once.
    async fn store(&self) -> Result<&Arc<dyn ObjectStore>> {
        self.store
            .get_or_try_init(|| async { self.config.build_store() })
            .await
    }

    /// Build the S3 key for an LTX file: `{path}/{level:04x}/{min}-{max}.ltx`.
    /// Ported from s3/replica_client.go:629, 677, 1040-1042.
    fn ltx_key(&self, level: i32, min_txid: TXID, max_txid: TXID) -> String {
        let filename = ltx::format_filename(min_txid, max_txid);
        format!("{}/{:04x}/{}", self.config.path, level, filename)
    }

    /// Prefix for listing a level: `{path}/{level:04x}/`.
    /// Ported from s3/replica_client.go:1363.
    fn level_prefix(&self, level: i32) -> String {
        format!("{}/{:04x}/", self.config.path, level)
    }

    /// Root prefix for delete-all: `{path}/`. (s3/replica_client.go:1114).
    fn root_prefix(&self) -> String {
        format!("{}/", self.config.path)
    }
}

/// Map an `object_store::Error` to `crate::Error`, preserving NotFound as
/// `io::ErrorKind::NotFound` so callers keep working with the std error kind.
/// Mirrors `isNotExists` → `os.ErrNotExist` (s3/replica_client.go:647-649,
/// 1662-1668).
fn map_os_error(e: object_store::Error) -> Error {
    match e {
        object_store::Error::NotFound { .. } => {
            Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, e))
        }
        other => Error::Other(Box::new(other)),
    }
}

#[async_trait::async_trait]
impl ReplicaClient for ObjectStoreClient {
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
        self.ltx_files_bounded(level, seek, usize::MAX).await
    }

    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> Result<Vec<FileInfo>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let store = self.store().await?;
        let prefix = ObjPath::from(self.level_prefix(level));
        // The offset ends immediately before every filename with this minimum
        // TXID. S3 and GCS can push it into the listing request, so an additive
        // compaction does not scan the complete retained L0 history. The
        // supported production stores return each page in lexical order. The
        // compactor validates continuity and fails closed for an unordered
        // custom store.
        let offset = ObjPath::from(format!("{}{:016x}", self.level_prefix(level), seek.0));
        let mut listed = store.list_with_offset(Some(&prefix), &offset);
        let mut infos = Vec::with_capacity(limit.min(256));
        while let Some(meta) = listed.try_next().await.map_err(map_os_error)? {
            let name = meta.location.filename().unwrap_or("");
            let (min_txid, max_txid) = match ltx::parse_filename(name) {
                Ok(t) => t,
                Err(_) => continue, // skip non-LTX keys
            };
            if min_txid < seek {
                continue;
            }
            infos.push(FileInfo {
                level,
                min_txid,
                max_txid,
                size: meta.size as i64,
                created_at: Some(std::time::SystemTime::from(meta.last_modified)),
                ..Default::default()
            });
            if infos.len() == limit {
                break;
            }
        }

        // Iterator contract: ascending by (level, min_txid, max_txid).
        infos.sort_by(|a, b| {
            (a.level, a.min_txid.0, a.max_txid.0).cmp(&(b.level, b.min_txid.0, b.max_txid.0))
        });
        Ok(infos)
    }

    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>> {
        let store = self.store().await?;
        let key = ObjPath::from(self.ltx_key(level, min_txid, max_txid));

        let result = match store.get(&key).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("replica: get object {key}: not found"),
                )));
            }
            Err(e) => return Err(map_os_error(e)),
        };

        let bytes = result.bytes().await.map_err(map_os_error)?;
        Ok(bytes.to_vec())
    }

    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo> {
        let store = self.store().await?;

        // Preserve the LTX header timestamp in the standard Litestream object
        // metadata. This costs no extra request and lets Litestream perform an
        // accurate timestamp restore without downloading every candidate file.
        let header = ltx::Header::parse(data)?;
        let created_at = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(header.timestamp.max(0) as u64);
        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::Metadata(METADATA_KEY_TIMESTAMP.into()),
            AttributeValue::from(format_rfc3339_nano(header.timestamp)?),
        );
        let key = ObjPath::from(self.ltx_key(level, min_txid, max_txid));

        // Multipart threshold: < 5 MiB → single PUT; ≥ 5 MiB → multipart with
        // fixed-size parts. Ported from the Go uploader's 5 MiB PartSize default
        // (s3/replica_client.go:99, brief §5.1).
        let part_size = self.config.effective_part_size();
        if data.len() < MULTIPART_THRESHOLD {
            let payload = PutPayload::from(data.to_vec());
            let options = PutOptions {
                attributes,
                ..Default::default()
            };
            store
                .put_opts(&key, payload, options)
                .await
                .map_err(|e| Error::Other(format!("replica: upload to {key}: {e}").into()))?;
        } else {
            let options = PutMultipartOpts {
                attributes,
                ..Default::default()
            };
            let mut upload = store
                .put_multipart_opts(&key, options)
                .await
                .map_err(|e| Error::Other(format!("replica: upload to {key}: {e}").into()))?;
            // Upload in fixed-size parts (each ≥ 5 MiB except possibly the last,
            // matching object_store's part-size requirement).
            for chunk in data.chunks(part_size.max(MULTIPART_THRESHOLD)) {
                upload
                    .put_part(PutPayload::from(chunk.to_vec()))
                    .await
                    .map_err(|e| {
                        Error::Other(format!("replica: upload part to {key}: {e}").into())
                    })?;
            }
            upload.complete().await.map_err(|e| {
                Error::Other(format!("replica: complete upload to {key}: {e}").into())
            })?;
        }

        Ok(FileInfo {
            level,
            min_txid,
            max_txid,
            size: data.len() as i64,
            created_at: Some(created_at),
            ..Default::default()
        })
    }

    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let store = self.store().await?;

        // Build the key list, then delete in batches of MAX_KEYS via the
        // store's delete_stream, surfacing per-key errors (brief §5.5).
        let keys: Vec<ObjPath> = files
            .iter()
            .map(|info| ObjPath::from(self.ltx_key(info.level, info.min_txid, info.max_txid)))
            .collect();

        for batch in keys.chunks(MAX_KEYS) {
            delete_batch(store.as_ref(), batch, /*ignore_missing=*/ true).await?;
        }
        Ok(())
    }

    async fn delete_all(&self) -> Result<()> {
        let store = self.store().await?;
        let prefix = ObjPath::from(self.root_prefix());

        // List everything under the path prefix, then batch-delete.
        // (s3/replica_client.go:1104-1148).
        let keys: Vec<ObjPath> = store
            .list(Some(&prefix))
            .map_ok(|m| m.location)
            .map_err(map_os_error)
            .try_collect()
            .await?;

        for batch in keys.chunks(MAX_KEYS) {
            delete_batch(store.as_ref(), batch, /*ignore_missing=*/ true).await?;
        }
        Ok(())
    }
}

/// Delete a batch of keys via `delete_stream`, surfacing per-key errors.
///
/// When `ignore_missing` is set, `NotFound` is tolerated (delete is idempotent —
/// the file client swallows ENOENT the same way), but every other per-key error
/// is returned (brief §5.5: do not silently swallow partial failures).
async fn delete_batch(
    store: &dyn ObjectStore,
    keys: &[ObjPath],
    ignore_missing: bool,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let owned: Vec<ObjPath> = keys.to_vec();
    let stream = futures_util::stream::iter(owned.into_iter().map(Ok));
    let mut results = store.delete_stream(stream.boxed());
    while let Some(res) = results.next().await {
        match res {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) if ignore_missing => {}
            Err(e) => return Err(map_os_error(e)),
        }
    }
    Ok(())
}

/// Format a Unix-millisecond timestamp like Go's `time.RFC3339Nano`.
fn format_rfc3339_nano(unix_millis: i64) -> Result<String> {
    let mut timestamp = DateTime::from_timestamp_millis(unix_millis.max(0))
        .ok_or_else(|| Error::Other("LTX timestamp is outside the RFC3339 range".into()))?
        .to_rfc3339_opts(SecondsFormat::AutoSi, true);

    // Chrono keeps millisecond precision as three digits. Go removes trailing
    // zeros, so `.500Z` becomes `.5Z`.
    if timestamp.contains('.') {
        // The pop must stay outside debug_assert!, which vanishes in release
        // builds and would leave the Z in place to be doubled below.
        let suffix = timestamp.pop();
        debug_assert_eq!(suffix, Some('Z'));
        while timestamp.ends_with('0') {
            timestamp.pop();
        }
        timestamp.push('Z');
    }
    Ok(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica_url::parse_replica_url_with_query;

    #[test]
    fn timestamp_metadata_matches_go_rfc3339_nano() {
        assert_eq!(
            format_rfc3339_nano(1_609_459_200_000).unwrap(),
            "2021-01-01T00:00:00Z"
        );
        assert_eq!(
            format_rfc3339_nano(1_609_459_200_500).unwrap(),
            "2021-01-01T00:00:00.5Z"
        );
        assert_eq!(
            format_rfc3339_nano(1_609_459_200_123).unwrap(),
            "2021-01-01T00:00:00.123Z"
        );
        assert!(format_rfc3339_nano(i64::MAX).is_err());
    }

    #[tokio::test]
    async fn write_preserves_the_litestream_timestamp_metadata() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let client = ObjectStoreClient::with_store(
            ObjectStoreConfig {
                bucket: "bucket".into(),
                path: "replica".into(),
                ..Default::default()
            },
            store.clone(),
        );
        let data = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: 512,
            commit: 1,
            min_txid: TXID(1),
            max_txid: TXID(1),
            timestamp: 1_609_459_200_123,
            pre_apply_checksum: 0,
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        }
        .marshal();

        client
            .write_ltx_file(0, TXID(1), TXID(1), &data)
            .await
            .expect("write LTX object");

        let result = store
            .get_opts(
                &ObjPath::from("replica/0000/0000000000000001-0000000000000001.ltx"),
                object_store::GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read object metadata");
        let value = result
            .attributes
            .get(&Attribute::Metadata(METADATA_KEY_TIMESTAMP.into()))
            .expect("litestream timestamp metadata");
        assert_eq!(value.as_ref(), "2021-01-01T00:00:00.123Z");
    }

    // ── ParseHost (port of TestParseHost, s3/replica_client_test.go:1071) ──────
    #[test]
    fn parse_host_table() {
        let cases: &[(&str, &str, &str, &str, bool)] = &[
            (
                "my-space.sgp1.digitaloceanspaces.com",
                "my-space",
                "sgp1",
                "https://sgp1.digitaloceanspaces.com",
                false,
            ),
            (
                "test-bucket.nyc3.digitaloceanspaces.com",
                "test-bucket",
                "nyc3",
                "https://nyc3.digitaloceanspaces.com",
                false,
            ),
            (
                "mybucket.s3.us-east-1.amazonaws.com",
                "mybucket",
                "us-east-1",
                "",
                false,
            ),
            ("mybucket.s3.amazonaws.com", "mybucket", "", "", false),
            (
                "mybucket.s3.us-west-004.backblazeb2.com",
                "mybucket",
                "us-west-004",
                "https://s3.us-west-004.backblazeb2.com",
                true,
            ),
            (
                "mybucket.localhost:9000",
                "mybucket",
                "us-east-1",
                "http://localhost:9000",
                true,
            ),
        ];
        for (host, b, r, e, fps) in cases {
            let (bucket, region, endpoint, force) = parse_host(host);
            assert_eq!(&bucket, b, "bucket for {host}");
            assert_eq!(&region, r, "region for {host}");
            assert_eq!(&endpoint, e, "endpoint for {host}");
            assert_eq!(force, *fps, "force_path_style for {host}");
        }
    }

    #[test]
    fn parse_host_standard_s3_is_bucket() {
        let (bucket, region, endpoint, force) = parse_host("mybucket");
        assert_eq!(bucket, "mybucket");
        assert_eq!(region, "");
        assert_eq!(endpoint, "");
        assert!(!force);
    }

    fn cfg_from_url(url: &str) -> ObjectStoreConfig {
        let parsed = parse_replica_url_with_query(url).unwrap();
        ObjectStoreConfig::from_url(&parsed).unwrap()
    }

    // ── URL query param aliases (port of
    //    TestNewReplicaClientFromURL_QueryParamAliases, test:1940) ───────────────
    #[test]
    fn query_param_aliases() {
        let c = cfg_from_url("s3://mybucket/path?forcePathStyle=true");
        assert!(c.force_path_style);

        let c = cfg_from_url("s3://mybucket/path?force-path-style=true");
        assert!(c.force_path_style);

        let c = cfg_from_url(
            "s3://mybucket/path?endpoint=http://localhost:9000&force-path-style=false",
        );
        assert!(!c.force_path_style, "explicit force-path-style=false wins");

        let c = cfg_from_url("s3://mybucket/path?skipVerify=true");
        assert!(c.skip_verify);
        let c = cfg_from_url("s3://mybucket/path?skip-verify=true");
        assert!(c.skip_verify);

        let c = cfg_from_url("s3://mybucket/path?part-size=10485760");
        assert_eq!(c.part_size, 10_485_760);
        let c = cfg_from_url("s3://mybucket/path?partSize=10485760");
        assert_eq!(c.part_size, 10_485_760);

        let c = cfg_from_url(
            "s3://mybucket/path?force-path-style=true&skip-verify=true&part-size=8388608",
        );
        assert!(c.force_path_style);
        assert!(c.skip_verify);
        assert_eq!(c.part_size, 8_388_608);
    }

    // ── Endpoint env var (port of TestNewReplicaClientFromURL_EndpointEnvVar,
    //    test:2023). These mutate a process-global env var, so they run
    //    sequentially under one #[test] with save/restore to avoid cross-test
    //    interference. ───────────────────────────────────────────────────────────
    #[test]
    fn endpoint_env_var() {
        let saved = std::env::var("LITESTREAM_S3_ENDPOINT").ok();

        let set = |v: &str| {
            if v.is_empty() {
                std::env::remove_var("LITESTREAM_S3_ENDPOINT");
            } else {
                std::env::set_var("LITESTREAM_S3_ENDPOINT", v);
            }
        };

        set("http://localhost:9000");
        let c = cfg_from_url("s3://mybucket/path");
        assert_eq!(c.endpoint, "http://localhost:9000");
        assert!(c.force_path_style, "env endpoint forces path-style");

        set("s3.example.com");
        let c = cfg_from_url("s3://mybucket/path");
        assert_eq!(
            c.endpoint, "https://s3.example.com",
            "env endpoint gets https"
        );
        assert!(c.force_path_style);

        set("http://localhost:9000");
        let c = cfg_from_url("s3://mybucket/path?endpoint=http://other:9000");
        assert_eq!(
            c.endpoint, "http://other:9000",
            "query endpoint overrides env"
        );
        assert!(c.force_path_style);

        set("http://localhost:9000");
        let c = cfg_from_url("s3://mybucket/path?force-path-style=false");
        assert_eq!(c.endpoint, "http://localhost:9000");
        assert!(
            !c.force_path_style,
            "explicit force-path-style=false respected with env endpoint"
        );

        set("");
        let c = cfg_from_url("s3://mybucket/path");
        assert_eq!(c.endpoint, "");
        assert!(!c.force_path_style);

        // Restore.
        match saved {
            Some(v) => std::env::set_var("LITESTREAM_S3_ENDPOINT", v),
            None => std::env::remove_var("LITESTREAM_S3_ENDPOINT"),
        }
    }

    // ── Key construction (wire-compat requirement D-1, test:629/677/1040) ─────
    #[test]
    fn ltx_key_scheme() {
        let client = ObjectStoreClient::new(ObjectStoreConfig {
            bucket: "b".into(),
            path: "replica".into(),
            ..Default::default()
        });
        assert_eq!(
            client.ltx_key(0, TXID(1), TXID(1)),
            "replica/0000/0000000000000001-0000000000000001.ltx"
        );
        assert_eq!(
            client.ltx_key(0, TXID(1), TXID(6)),
            "replica/0000/0000000000000001-0000000000000006.ltx"
        );
        assert_eq!(client.level_prefix(0), "replica/0000/");
        assert_eq!(client.root_prefix(), "replica/");
    }

    // ── isNotExists mapping (port of TestIsNotExists, test:53) ────────────────
    #[test]
    fn not_found_maps_to_io_not_found() {
        let e = map_os_error(object_store::Error::NotFound {
            path: "k".into(),
            source: "missing".into(),
        });
        match e {
            Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {other:?}"),
        }
    }
}
