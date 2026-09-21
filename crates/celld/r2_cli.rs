// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The R2 CLI reads operator input and object bodies outside node storage.
#![allow(clippy::disallowed_methods)]

//! `celld r2` — read and write the objects behind an `r2_buckets` binding.
//!
//! A release pipeline publishes a build artifact with
//! `wrangler r2 object put` and the Worker serves it from its `r2_buckets`
//! binding. celld served the binding but had no command, so the only way to
//! seed a bucket was `aws s3 cp` straight into the fleet bucket. That works
//! for the bytes and loses the record: an R2 object carries
//! `customMetadata`, a `cacheExpiry`, and its checksums in a celld-private
//! envelope that a plain copy does not write. The result reads back with no
//! custom metadata, which is a silent difference rather than a failure.
//!
//! So this command does not implement R2. It installs the fleet bucket the
//! same way a node does and calls [`crate::js::r2_ops`] — the functions the
//! binding's ops call. The key scoping, the envelope, the checksum set and
//! the single-request/multipart choice therefore have one implementation,
//! and an object written here is an object the binding could have written.
//!
//! Unlike `celld d1`, `celld kv` and `celld queue`, this command needs no
//! node. A binding reads the fleet bucket directly, so an operator with the
//! bucket credentials can publish into an empty fleet — which is what a
//! release pipeline that runs before a deployment needs.

use crate::cli_options::FLEET_HELP;
use crate::cli_options::LISTING_HELP;
use crate::cli_output::list;
use crate::cli_output::Bounds;
use crate::cli_output::Format;
use crate::cli_output::Output;
use crate::cli_output::Page;
use crate::cli_output::Record;
use crate::cli_output::Resumable;
use crate::cli_output::Resume;
use crate::note;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use bytes::Bytes;
use futures_util::stream;
use futures_util::StreamExt as _;
use futures_util::TryStreamExt as _;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

/// How much of a body the command reads at once. The write buffers what it
/// is given and cuts its own parts, so this only bounds the resident copy
/// of a body that can be many gigabytes.
const READ_CHUNK: usize = 1 << 20;

/// How many objects `celld r2 bulk put` uploads at once unless told otherwise.
/// The bounded stream is what keeps a large manifest from opening every body
/// at the same time.
const DEFAULT_BULK_CONCURRENCY: usize = 50;
const MAX_BULK_CONCURRENCY: usize = 256;

/// The branch a child version's R2 binding was pointed at, as the operator
/// sees it. `parent_bucket` names the version the child reads through.
struct BranchReply {
    parent_bucket: String,
    duration_ms: u64,
}

impl Record for BranchReply {
    fn json(&self) -> Value {
        json!({
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

/// What one bulk import moved. The manifest is validated whole before the
/// first write, so a failure here means nothing was uploaded.
struct BulkPutReply {
    bucket_name: String,
    objects: usize,
    bytes: u64,
    duration_ms: u64,
}

impl Record for BulkPutReply {
    fn json(&self) -> Value {
        json!({
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

/// One record of Wrangler's bulk-put file. Unknown keys are refused so a
/// manifest written for another tool cannot half-apply here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkManifestEntry {
    key: String,
    file: PathBuf,
}

/// One record of that manifest, resolved and measured before any upload.
#[derive(Debug)]
struct BulkEntry {
    key: String,
    file: PathBuf,
    bytes: u64,
}

/// One object in a listing.
struct Object {
    key: String,
    size: u64,
    etag: Option<String>,
    uploaded_ms: i64,
}

impl Record for Object {
    fn json(&self) -> Value {
        json!({
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
            "uploaded": self.uploaded_ms,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.key)
    }
}

impl Resumable for Object {
    fn cursor(&self) -> &str {
        &self.key
    }
}

/// One object's complete record, as `celld r2 head` reports it.
///
/// The JSON carries the binding's object record before the JS conversion.
/// Its `http` and `custom` fields become `httpMetadata` and
/// `customMetadata` in an `R2Object`.
struct Head(Value);

impl Record for Head {
    fn json(&self) -> Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        let field = |name: &str| self.0.get(name).cloned().unwrap_or(Value::Null);
        let scalar = |value: &Value| match value {
            Value::Null => "-".to_string(),
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let mut lines = vec![
            format!("key:      {}", scalar(&field("key"))),
            format!("size:     {}", scalar(&field("size"))),
            format!("etag:     {}", scalar(&field("etag"))),
            format!("uploaded: {}", scalar(&field("uploaded"))),
        ];
        // The two maps print only when they hold something. An object
        // written by a plain `aws s3 cp` has neither, and an empty block
        // for each would hide that difference in a wall of dashes.
        for (label, name) in [("http", "http"), ("custom", "custom")] {
            if let Some(map) = field(name).as_object().filter(|map| !map.is_empty()) {
                for (key, value) in map {
                    lines.push(format!("{label}.{key}: {}", scalar(value)));
                }
            }
        }
        Cow::Owned(lines.join("\n"))
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    let storage = command.fleet.clone().resolve("celld r2")?;
    // The binding's store, installed exactly as a node installs it, so
    // every call below reaches the same keys the Worker reads.
    let store = storage.open().await?;
    crate::js::set_r2_store(store.clone());
    let bucket = command.bucket.as_str();

    match command.action {
        Action::Get { key } => {
            let read = crate::js::r2_ops::read(
                bucket,
                &key,
                crate::bucket::BlobRange::Whole,
                &crate::bucket::BlobConditions::default(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
            match read {
                crate::bucket::BlobRead::Hit(blob) => {
                    // Streamed rather than collected: a release artifact is
                    // routinely larger than the memory a CI runner gives
                    // this process.
                    Output::new(Format::Text).stream(blob.body).await?;
                }
                // An unconditional read cannot be refused, so the only
                // other answer is an absent key.
                crate::bucket::BlobRead::Missing | crate::bucket::BlobRead::Unmet(_) => {
                    bail!("no object {key:?} in r2 bucket {bucket:?}")
                }
            }
        }
        Action::Head { key } => {
            let meta = crate::js::r2_ops::head(bucket, &key)
                .await
                .map_err(anyhow::Error::msg)?
                .ok_or_else(|| anyhow!("no object {key:?} in r2 bucket {bucket:?}"))?;
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            out.row(&Head(crate::js::r2_ops::object_json(&key, &meta, None)))?;
            out.finish()?;
        }
        Action::Put {
            key,
            file,
            http,
            custom,
        } => {
            let request = json!({ "http": http, "custom": custom }).to_string();
            let mut put = crate::js::r2_ops::open_put(bucket.to_string(), key.clone(), &request)
                .map_err(anyhow::Error::msg)?;
            let mut source = Source::open(file.as_deref())?;
            let mut buffer = vec![0u8; READ_CHUNK];
            loop {
                let read = source.read(&mut buffer).context("read the object body")?;
                if read == 0 {
                    break;
                }
                put.push(buffer[..read].to_vec())
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            let answer = put.finish().await.map_err(anyhow::Error::msg)?;
            let answer: Value = serde_json::from_str(&answer).context("read the write's answer")?;
            let size = answer
                .pointer("/object/size")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            note!("wrote {key:?} ({size} bytes)");
        }
        Action::Delete { keys } => {
            crate::js::r2_ops::delete(bucket, &keys)
                .await
                .map_err(anyhow::Error::msg)?;
            note!("deleted {} object(s)", keys.len());
        }
        Action::List { prefix, bounds } => {
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            let listed = list(&mut out, &bounds, |resume, want| {
                let prefix = prefix.clone();
                // An operator's `--after` is a key and the driver's own
                // continuation is the cursor a truncated page answered
                // with. R2 reads the cursor first and both name the same
                // place, so either one resumes strictly after that key.
                let (cursor, after) = match resume {
                    Resume::From(after) => (String::new(), after.unwrap_or_default()),
                    Resume::Token(token) => (token, String::new()),
                };
                async move {
                    let page = crate::js::r2_ops::list(
                        bucket,
                        &json!({
                            "prefix": prefix,
                            "cursor": cursor,
                            "startAfter": after,
                            "limit": want,
                        })
                        .to_string(),
                    )
                    .await
                    .map_err(anyhow::Error::msg)?;
                    let objects = page
                        .get("objects")
                        .and_then(Value::as_array)
                        .ok_or_else(|| anyhow!("the listing carried no objects: {page}"))?;
                    let rows = objects
                        .iter()
                        .map(|object| Object {
                            key: object
                                .get("key")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            size: object
                                .get("size")
                                .and_then(Value::as_u64)
                                .unwrap_or_default(),
                            etag: object
                                .get("etag")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            uploaded_ms: object
                                .get("uploaded")
                                .and_then(Value::as_i64)
                                .unwrap_or_default(),
                        })
                        .collect();
                    // The store's own truncation signal, never a row
                    // count: a page filled exactly to its bound is not
                    // evidence that another object exists.
                    let next = page
                        .get("cursor")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    Ok(Page { rows, next })
                }
            })
            .await?;
            out.finish()?;
            listed.report("object", "--after");
        }
        // A branch of a whole binding: the child version's bucket is pointed
        // at the parent's, and every later read through this CLI or a Worker
        // goes through the overlay the pointer installs.
        Action::Branch {
            bucket_name,
            parent_bucket,
        } => {
            let result =
                crate::r2_branch::branch_r2_binding(&bucket_name, &parent_bucket, &command.fleet)
                    .await
                    .with_context(|| format!("branch R2 binding {bucket_name:?}"))?;
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            if command.json {
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
        } => {
            let result = bulk_put_manifest(&store, &bucket_name, &filename, concurrency).await?;
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            if command.json {
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

/// Import a Wrangler bulk-put manifest into one binding's key space.
///
/// Every record is resolved and validated before the first write, so a
/// malformed manifest cannot leave half of an otherwise valid import behind.
/// A child version's tombstone is cleared for each key written, because the
/// import is a write and must be visible to the Worker.
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

/// `bucket_name` reaches a key prefix, so it obeys the same rule the
/// deployment applies. Anything else could address another binding's keys.
fn validate_bucket_name(name: &str) -> anyhow::Result<()> {
    if !crate::deploy::valid_resource_name(name) {
        bail!(
            "invalid R2 bucket name {name:?}: use letters, digits, `-` and `_` \
             (not starting with `-`), at most 64 characters"
        );
    }
    Ok(())
}

/// A bulk key lands in the object key space, so the branch's own metadata
/// names are reserved and the key must already be canonical: the store
/// canonicalizes a path, and importing the other spelling would make the
/// same object addressable two ways.
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

/// Where a `put` reads its body. A named file and standard input are both
/// read in bounded pieces, so neither has to fit in memory.
enum Source {
    File(std::io::BufReader<std::fs::File>),
    Stdin(std::io::Stdin),
}

impl Source {
    fn open(path: Option<&std::path::Path>) -> anyhow::Result<Self> {
        match path {
            None => Ok(Self::Stdin(std::io::stdin())),
            Some(path) => {
                let file = std::fs::File::open(path)
                    .with_context(|| format!("open {}", path.display()))?;
                Ok(Self::File(std::io::BufReader::new(file)))
            }
        }
    }

    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buffer),
            Self::Stdin(stdin) => stdin.lock().read(buffer),
        }
    }
}

#[derive(Debug)]
enum Action {
    Get {
        key: String,
    },
    Head {
        key: String,
    },
    Put {
        key: String,
        /// `None` reads the body from standard input.
        file: Option<PathBuf>,
        /// R2's `httpMetadata`, already in the binding's spelling.
        http: BTreeMap<String, Value>,
        /// R2's `customMetadata`.
        custom: BTreeMap<String, String>,
    },
    Delete {
        keys: Vec<String>,
    },
    List {
        prefix: String,
        bounds: Bounds,
    },
    /// Point a child version's binding at a parent version's bucket. Writes
    /// `r2/<name>/base.json` on the child; the overlay it names is installed
    /// the next time a node starts on that bucket.
    Branch {
        bucket_name: String,
        parent_bucket: String,
    },
    /// Import a Wrangler bulk-put file into one binding's key space.
    BulkPut {
        bucket_name: String,
        filename: PathBuf,
        concurrency: usize,
    },
}

#[derive(Debug)]
struct Command {
    action: Action,
    bucket: String,
    fleet: crate::cli_options::FleetFlags,
    json: bool,
}

/// The `httpMetadata` field each flag sets, in R2's spelling.
const HTTP_FLAGS: [(&str, &str); 5] = [
    ("--content-type", "contentType"),
    ("--content-language", "contentLanguage"),
    ("--content-disposition", "contentDisposition"),
    ("--content-encoding", "contentEncoding"),
    ("--cache-control", "cacheControl"),
];

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let verb = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            verb @ ("get" | "head" | "put" | "delete" | "list") => verb.to_string(),
            "branch" => "branch".to_string(),
            // Wrangler spells the bulk import as a subcommand pair, so the
            // verb is settled here and the rest of the flags are shared.
            "bulk" => match arguments.next().as_deref() {
                Some("put") => "bulk-put".to_string(),
                Some("--help" | "-h" | "help") => return Ok(None),
                Some(other) if !other.starts_with('-') => {
                    bail!("unknown `celld r2 bulk` subcommand: {other}")
                }
                _ => bail!("`celld r2 bulk` needs `put`"),
            },
            other => bail!("unknown `celld r2` subcommand: {other}"),
        };
        // The bucket is the `bucket_name` from `r2_buckets`, not the
        // binding name: the name is what places the keys in the fleet
        // bucket, and it is the one an `env.BUCKET` call resolves to.
        //
        // `branch` names its binding with `--name` and `bulk put` takes its
        // bucket as the one positional argument, so neither reads it here.
        let bucket = match verb.as_str() {
            "branch" | "bulk-put" => String::new(),
            _ => {
                let bucket = arguments
                    .next()
                    .filter(|value| !value.starts_with('-'))
                    .ok_or_else(|| anyhow!("celld r2 needs an r2 bucket name"))?;
                // The same rule the deployment applies to `bucket_name`.
                // Without it a name with a `/` in it would write under
                // another binding's prefix, or outside the reserved `r2/`
                // space entirely.
                validate_bucket_name(&bucket)?;
                bucket
            }
        };

        let mut positional = Vec::new();
        let mut file = None;
        let mut pipe = false;
        let mut metadata = None;
        let mut prefix = String::new();
        let mut http = BTreeMap::new();
        let mut cache_expiry = None;
        let mut name = None;
        let mut parent_bucket = None;
        let mut filename = None;
        let mut concurrency = None;
        let mut fleet = crate::cli_options::FleetFlags::default();
        let mut bounds = Bounds::default();
        let mut json = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--path" | "--file" => file = Some(PathBuf::from(value("--path")?)),
                "--pipe" => pipe = true,
                "--metadata" => metadata = Some(value("--metadata")?),
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
                "--cache-expiry" => {
                    let raw = value("--cache-expiry")?;
                    cache_expiry = Some(raw.parse::<i64>().with_context(|| {
                        format!("--cache-expiry takes unix milliseconds, got {raw:?}")
                    })?);
                }
                "--prefix" => prefix = value("--prefix")?,
                "--json" => json = true,
                // celld has neither concept, and a flag that silently means
                // nothing is the gap the compatibility page forbids.
                flag @ ("--local" | "--remote" | "--jurisdiction") => bail!(
                    "celld r2 does not take {flag}: celld runs neither a local \
                     miniflare store nor a Cloudflare account, and it has no \
                     jurisdiction. An r2 bucket name addresses one key space in \
                     one fleet."
                ),
                "--help" | "-h" => return Ok(None),
                other => {
                    if let Some((_, field)) = HTTP_FLAGS.iter().find(|(flag, _)| *flag == other) {
                        let text = value(other)?;
                        http.insert((*field).to_string(), Value::String(text));
                        continue;
                    }
                    // The shared flags first, so `--bucket`, `--limit` and
                    // their siblings mean here exactly what they mean in
                    // every other command.
                    if fleet.consume(other, &mut value)? || bounds.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld r2 --help` for usage")
                    }
                    positional.push(other.to_string());
                }
            }
        }
        if let Some(expiry) = cache_expiry {
            http.insert("cacheExpiry".to_string(), Value::from(expiry));
        }
        // A flag the verb does not read must be a refusal. Accepting
        // `--content-type` on a `get` would tell an operator they set a
        // header that nothing wrote.
        let refuse = |flag: &str, set: bool| -> anyhow::Result<()> {
            if set {
                bail!("`celld r2 {verb}` takes no {flag}");
            }
            Ok(())
        };
        let write_flags = !http.is_empty() || metadata.is_some();
        // `branch` and `bulk put` own `--name`, `--parent-bucket`,
        // `--filename` and `--concurrency`; every other verb refuses them
        // rather than ignore a flag that wrote nothing.
        let branch_flags = name.is_some()
            || parent_bucket.is_some()
            || filename.is_some()
            || concurrency.is_some();
        let one_key = |what: &str| -> anyhow::Result<String> {
            let [key] = positional.as_slice() else {
                bail!("celld r2 {what} needs exactly one key");
            };
            Ok(key.clone())
        };
        let action = match verb.as_str() {
            "get" | "head" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse(
                    "--name, --parent-bucket, --filename or --concurrency",
                    branch_flags,
                )?;
                let key = one_key(&verb)?;
                match verb.as_str() {
                    "get" => Action::Get { key },
                    _ => Action::Head { key },
                }
            }
            "put" => {
                refuse(
                    "--name, --parent-bucket, --filename or --concurrency",
                    branch_flags,
                )?;
                let key = one_key("put")?;
                // Reading a body from a terminal by default would hang
                // with no output, so standard input is opt-in.
                if file.is_some() == pipe {
                    bail!("celld r2 put requires exactly one of --path FILE or --pipe");
                }
                let custom = match &metadata {
                    None => BTreeMap::new(),
                    Some(text) => parse_metadata(text)?,
                };
                Action::Put {
                    key,
                    file,
                    http,
                    custom,
                }
            }
            "delete" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse(
                    "--name, --parent-bucket, --filename or --concurrency",
                    branch_flags,
                )?;
                if positional.is_empty() {
                    bail!("celld r2 delete needs at least one key");
                }
                Action::Delete { keys: positional }
            }
            "list" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse(
                    "--name, --parent-bucket, --filename or --concurrency",
                    branch_flags,
                )?;
                if !positional.is_empty() {
                    bail!("celld r2 list takes no positional arguments; use --prefix");
                }
                Action::List {
                    prefix,
                    bounds: bounds.clone(),
                }
            }
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
                validate_bucket_name(&bucket_name)?;
                let parent_bucket = parent_bucket
                    .ok_or_else(|| anyhow!("celld r2 branch requires --parent-bucket"))?;
                Action::Branch {
                    bucket_name,
                    parent_bucket,
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
                }
            }
            _ => unreachable!("the verb was matched above"),
        };
        bounds.validate()?;
        Ok(Some(Self {
            action,
            bucket,
            fleet: fleet.with_environment(),
            json,
        }))
    }
}

/// `--metadata` as R2's `customMetadata`: a flat object of strings.
///
/// A nested value is refused rather than stringified. R2 stores strings, so
/// accepting an object would write `[object Object]` on one side of the
/// binding and a JSON document on the other.
fn parse_metadata(text: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let parsed: Value = serde_json::from_str(text).context("parse --metadata as JSON")?;
    let object = parsed
        .as_object()
        .ok_or_else(|| anyhow!("--metadata takes a JSON object of strings"))?;
    object
        .iter()
        .map(|(name, value)| match value {
            Value::String(text) => Ok((name.clone(), text.clone())),
            _ => Err(anyhow!(
                "--metadata value for {name:?} must be a string, because R2 \
                 stores customMetadata as strings"
            )),
        })
        .collect()
}

pub fn print_help() {
    // The whole block is laid out here, and the content flags are generated
    // from the table the parser reads, so a flag cannot be accepted and
    // undocumented, or documented and rejected. The column is wide enough
    // for `--content-disposition VALUE`, which is the longest of them.
    let row = |flag: String, text: &str| format!("  {flag:<27} {text}");
    let flags = [
        row("--path FILE".to_string(), "read the body from a file"),
        row("--pipe".to_string(), "read the body from standard input"),
        row(
            "--metadata JSON".to_string(),
            "customMetadata, as a JSON object of strings",
        ),
    ]
    .into_iter()
    .chain(HTTP_FLAGS.iter().map(|(flag, field)| {
        row(
            format!("{flag} VALUE"),
            &format!("set httpMetadata.{field}"),
        )
    }))
    .chain(std::iter::once(row(
        "--cache-expiry MS".to_string(),
        "set httpMetadata.cacheExpiry, in unix milliseconds",
    )))
    .collect::<Vec<_>>()
    .join("\n");
    let text = format!(
        "celld r2 — read and write the objects behind an `r2_buckets` binding

USAGE
  celld r2 get    <bucket-name> <key>    [fleet options]
  celld r2 head   <bucket-name> <key>    [--json] [fleet options]
  celld r2 put    <bucket-name> <key>    --path FILE | --pipe [put options]
  celld r2 delete <bucket-name> <key>... [fleet options]
  celld r2 list   <bucket-name>          [--prefix P] [listing options]
  celld r2 bulk put BUCKET_NAME --filename FILE [--concurrency N] [fleet options]
  celld r2 branch --name BUCKET_NAME --parent-bucket URI [fleet options]

The bucket name is the `bucket_name` from the project's `r2_buckets` entry,
not the binding name. `get` writes the object to stdout as bytes, so an
artifact comes back byte for byte.

This command reads the fleet bucket directly and needs no running node, so
a release pipeline can publish an object before it deploys the Worker.

PUT OPTIONS
{flags}

LISTING OPTIONS (list)
  --prefix P          list only the keys under this prefix
{LISTING_HELP}

BULK PUT
  FILE uses Wrangler's bulk-put shape: a JSON array of {{key, file}}.
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

An object written here carries the same record as one a Worker writes:
customMetadata, cacheExpiry and the checksums travel in a celld envelope
beside the five content headers. A copy made with another tool still reads
through the binding, and its user metadata becomes its customMetadata."
    );
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn test_store() -> crate::bucket::Bucket {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        crate::bucket::Bucket::with_stores(
            store.clone(),
            store,
            crate::bucket::StorageBackend::Local,
            "r2-cli-test".to_string(),
            String::new(),
        )
    }

    #[test]
    fn branch_parse_requires_name_and_parent() {
        let error = Command::parse(vec!["branch".into()]).unwrap_err();
        assert!(error.to_string().contains("--name"), "{error}");
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
        .expect("a parsed command");
        let Action::BulkPut {
            bucket_name,
            filename,
            concurrency,
        } = command.action
        else {
            panic!("expected a bulk put");
        };
        assert_eq!(bucket_name, "next-cache");
        assert_eq!(filename, PathBuf::from("cache.json"));
        assert_eq!(concurrency, 7);
        assert_eq!(command.fleet.bucket.as_deref(), Some("cellp-celld/demo/v1"));
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
        for key in [
            "/leading",
            "trailing/",
            "two//segments",
            "relative/../segment",
        ] {
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
        let error = match bulk_put_manifest(&store, "next-cache", &manifest, 2).await {
            Ok(reply) => panic!("expected a failure, got {} objects", reply.objects),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("resolve R2 bulk object"), "{error}");
        assert!(store.get("r2/next-cache/valid").await.unwrap().is_none());
    }
}
