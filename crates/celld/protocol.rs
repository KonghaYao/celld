// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Durable types on the bucket contract between deployment tools and celld.
//! These objects are the interface; nothing else is exchanged.
use anyhow::Context;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;

/// `deploy/<script>/<version>/manifest.json` — the normalized thing celld reads
/// to know what to run. The script name is deployment identity, not a fleet
/// selector: one fleet has one current application.
#[derive(Debug, Clone, Serialize)]
pub struct Manifest {
    #[serde(default = "legacy_manifest_schema_version")]
    pub schema_version: u32,
    pub version: String,
    pub script_name: String,
    /// Historical default. Omitted from serialization when `js-v8`.
    #[serde(
        default = "default_execution_profile",
        skip_serializing_if = "is_default_execution_profile"
    )]
    pub execution_profile: String,
    /// Native Component descriptor. Absent for JS / `wasm-v1` deployments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_component: Option<NativeComponentDescriptor>,
    /// Absent for Wrangler asset-only deployments.
    pub main_module: Option<String>,
    /// Durable Object classes exported by the worker.
    pub do_classes: Vec<String>,
    /// Subset of `do_classes` that are SQLite-backed (from migrations).
    pub sqlite_classes: Vec<String>,
    pub modules: Vec<ModuleRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets: Option<AssetManifestRef>,
    /// Cron trigger expressions from the config's `triggers.crons`. They are
    /// deployment state rather than cell state: the reserved cron cell reads
    /// them from the manifest it is running under, so changing a schedule
    /// needs no migration of an already-armed alarm.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub crons: Vec<String>,
    /// Push consumers attached to this Worker deployment. Producer bindings
    /// remain in `raw_metadata.bindings`, because they become values in `env`;
    /// consumers drive the broker and have no environment binding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queue_consumers: Vec<QueueConsumerConfig>,
    /// The container classes of this deployment: class, image id, and the
    /// instance shape. Each image tar is at `deploy/images/<id>.tar`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<crate::container::ContainerSpec>,
    /// The `celld-fence` image `celld deploy` built beside the container
    /// images: the node runs it once, privileged, to fence its container
    /// bridges before any container starts. `None` in a deployment without
    /// containers, and in one made by a celld that predates the fence,
    /// which a node then refuses to start containers for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence_image: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_features: Vec<String>,
    /// wrangler's raw metadata, retained verbatim for anything we don't yet model.
    pub raw_metadata: serde_json::Value,
}

impl Manifest {
    pub fn validate_execution(&self) -> anyhow::Result<()> {
        validate_required_features(&self.required_features)?;
        match self.execution_profile.as_str() {
            EXECUTION_PROFILE_JS_V8 => {
                anyhow::ensure!(
                    self.native_component.is_none(),
                    "js-v8 deployment cannot carry a native_component descriptor"
                );
                anyhow::ensure!(
                    !self
                        .required_features
                        .iter()
                        .any(|feature| feature == FEATURE_NATIVE_HTTP_V1),
                    "js-v8 deployment cannot require {FEATURE_NATIVE_HTTP_V1}"
                );
            }
            EXECUTION_PROFILE_NATIVE_HTTP_V1 => {
                let descriptor = self
                    .native_component
                    .as_ref()
                    .context("native-http-v1 deployment is missing native_component")?;
                descriptor.validate()?;
                anyhow::ensure!(
                    self.required_features
                        .iter()
                        .any(|feature| feature == FEATURE_NATIVE_HTTP_V1),
                    "native-http-v1 deployment does not require {FEATURE_NATIVE_HTTP_V1}"
                );
                anyhow::ensure!(
                    !self
                        .required_features
                        .iter()
                        .any(|feature| feature == FEATURE_WASM_V1),
                    "native-http-v1 cannot mix with {FEATURE_WASM_V1}"
                );
                anyhow::ensure!(
                    self.main_module.is_none(),
                    "native-http-v1 cannot mix a JS main_module"
                );
                let has_kv = self
                    .raw_metadata
                    .get("bindings")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .any(|binding| {
                        binding.get("type").and_then(serde_json::Value::as_str) == Some("kv")
                    });
                let admits_kv = descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == NATIVE_CAPABILITY_KV);
                anyhow::ensure!(
                    has_kv == admits_kv,
                    "native KV capability must match declared KV bindings exactly"
                );
                anyhow::ensure!(
                    self.assets.is_none()
                        && self.crons.is_empty()
                        && self.queue_consumers.is_empty(),
                    "native-http-v1 does not support assets, cron, or queue consumers"
                );
                let unsupported = self
                    .raw_metadata
                    .get("bindings")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|binding| binding.get("type").and_then(serde_json::Value::as_str))
                    .find(|kind| !matches!(*kind, "kv" | "plain_text"));
                if let Some(kind) = unsupported {
                    anyhow::bail!("native-http-v1 does not support {kind} bindings");
                }
            }
            other => anyhow::bail!("unknown execution_profile {other:?}"),
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct ManifestWire {
    #[serde(default = "legacy_manifest_schema_version")]
    schema_version: u32,
    version: String,
    script_name: String,
    #[serde(default = "default_execution_profile")]
    execution_profile: String,
    #[serde(default)]
    native_component: Option<NativeComponentDescriptor>,
    main_module: Option<String>,
    do_classes: Vec<String>,
    sqlite_classes: Vec<String>,
    modules: Vec<ModuleRef>,
    #[serde(default)]
    assets: Option<AssetManifestRef>,
    #[serde(default)]
    crons: Vec<String>,
    #[serde(default)]
    queue_consumers: Vec<QueueConsumerConfig>,
    #[serde(default)]
    containers: Vec<crate::container::ContainerSpec>,
    #[serde(default)]
    fence_image: Option<String>,
    #[serde(default)]
    required_features: Vec<String>,
    raw_metadata: serde_json::Value,
}

impl TryFrom<ManifestWire> for Manifest {
    type Error = anyhow::Error;

    fn try_from(wire: ManifestWire) -> Result<Self, Self::Error> {
        let manifest = Self {
            schema_version: wire.schema_version,
            version: wire.version,
            script_name: wire.script_name,
            execution_profile: wire.execution_profile,
            native_component: wire.native_component,
            main_module: wire.main_module,
            do_classes: wire.do_classes,
            sqlite_classes: wire.sqlite_classes,
            modules: wire.modules,
            assets: wire.assets,
            crons: wire.crons,
            queue_consumers: wire.queue_consumers,
            containers: wire.containers,
            fence_image: wire.fence_image,
            required_features: wire.required_features,
            raw_metadata: wire.raw_metadata,
        };
        manifest.validate_execution()?;
        Ok(manifest)
    }
}

impl<'de> Deserialize<'de> for Manifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        ManifestWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

fn legacy_manifest_schema_version() -> u32 {
    1
}

/// Manifest `required_features` values this build can load. A manifest
/// requiring anything else must be rejected up front: `ModuleRef` tolerates
/// unknown fields, so an older node would otherwise deserialize the manifest
/// partially and fail (or misbehave) at worker load instead.
pub const SUPPORTED_DEPLOYMENT_FEATURES: &[&str] = &[
    FEATURE_ASSETS_V1,
    FEATURE_CONTAINERS_V1,
    FEATURE_CRON_V1,
    FEATURE_D1_V1,
    FEATURE_KV_V1,
    FEATURE_QUEUES_V1,
    FEATURE_SQLITE_VEC_V1,
    FEATURE_R2_V1,
    FEATURE_IMAGES_V1,
    FEATURE_WASM_V1,
    FEATURE_BINARY_V1,
    FEATURE_WORKFLOWS_V1,
    FEATURE_NATIVE_HTTP_V1,
];

pub const FEATURE_ASSETS_V1: &str = "assets-v1";
/// A deployment with `containers`. Required because a build without the
/// container engine would load the manifest, build a `ctx` with no
/// `container`, and fail only when the object first touches it.
pub const FEATURE_CONTAINERS_V1: &str = "containers-v1";
/// A deployment with D1 databases. Required because a build without the
/// reserved `__D1Database` class would load the manifest and then fail every
/// `env.DB` call at request time, on a node the developer is not watching —
/// the gate moves that failure to the deploy.
pub const FEATURE_D1_V1: &str = "d1-v1";
/// A deployment with cron triggers. Required because a build without the
/// reserved cron cell would load the manifest, ignore `crons`, and silently
/// never fire — the quiet failure the gate exists to prevent.
pub const FEATURE_CRON_V1: &str = "cron-v1";
/// A deployment with `r2_buckets` bindings. Required because a build without
/// the R2 binding would load the manifest and then throw on every method the
/// application calls, at request time, on a node the developer is not
/// watching — the gate moves that failure to the deploy.
pub const FEATURE_R2_V1: &str = "r2-v1";
/// A deployment with an `images` binding. Required because a build without
/// the host transform would load the manifest and then throw on the first
/// `env.IMAGES.input()` call, at request time.
pub const FEATURE_IMAGES_V1: &str = "images-v1";
pub const FEATURE_SQLITE_VEC_V1: &str = "sqlite-vec-v1";
pub const FEATURE_WASM_V1: &str = "wasm-v1";
pub const FEATURE_NATIVE_HTTP_V1: &str = "native-http-v1";
pub const EXECUTION_PROFILE_JS_V8: &str = "js-v8";
pub const EXECUTION_PROFILE_NATIVE_HTTP_V1: &str = "native-http-v1";
pub const NATIVE_BASELINE_1: &str = "cellp-native-baseline-1";
pub const NATIVE_HTTP_WORLD: &str = "wasi:http/proxy@0.2";
pub const NATIVE_CAPABILITY_CONFIG: &str = "cellp:config@0.1";
pub const NATIVE_CAPABILITY_KV: &str = "cellp:kv@0.1";

fn default_execution_profile() -> String {
    EXECUTION_PROFILE_JS_V8.to_string()
}

fn is_default_execution_profile(profile: &str) -> bool {
    profile == EXECUTION_PROFILE_JS_V8
}

/// Immutable Native Component admission descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeComponentDescriptor {
    pub path: String,
    pub sha256: String,
    pub baseline: String,
    pub world: String,
    pub capabilities: Vec<String>,
}

impl NativeComponentDescriptor {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.path.is_empty()
                && !self.path.contains('\\')
                && !self.path.split('/').any(|part| part.is_empty()
                    || part == "."
                    || part == ".."
                    || part.contains(':')),
            "native component path is invalid"
        );
        anyhow::ensure!(
            self.sha256.len() == 64
                && self
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "native component digest must be a full lowercase SHA-256"
        );
        anyhow::ensure!(
            self.baseline == NATIVE_BASELINE_1,
            "unsupported native baseline {:?}",
            self.baseline
        );
        anyhow::ensure!(
            self.world == NATIVE_HTTP_WORLD,
            "unsupported native world {:?}",
            self.world
        );
        let mut seen = std::collections::BTreeSet::new();
        for capability in &self.capabilities {
            anyhow::ensure!(
                capability == NATIVE_CAPABILITY_CONFIG || capability == NATIVE_CAPABILITY_KV,
                "unsupported native capability {capability:?}"
            );
            anyhow::ensure!(
                seen.insert(capability.as_str()),
                "native capability {capability:?} is repeated"
            );
        }
        anyhow::ensure!(
            seen.contains(NATIVE_CAPABILITY_CONFIG),
            "native component must admit {NATIVE_CAPABILITY_CONFIG}"
        );
        Ok(())
    }
}
/// Wrangler `Data` rule sibling modules (`.bin` default export is an `ArrayBuffer`).
pub const FEATURE_BINARY_V1: &str = "binary-v1";
/// A deployment with `workflows` bindings. Required because a build without
/// the reserved workflow cell would load the manifest, build an `env` missing
/// the binding, and fail only when the application first calls `create()` —
/// in production, with an error that blames the application.
pub const FEATURE_WORKFLOWS_V1: &str = "workflows-v1";

/// A deployment with `kv_namespaces` bindings. Gated for the same reason
/// workflows are, and not for D1's reason: a build without KV would load the
/// manifest, build an `env` with no `KV` on it, and fail only when the
/// application first reads a key. D1's absence fails loudly at load instead,
/// because its reserved class is in `do_classes` and the loader refuses a
/// class it does not know.
pub const FEATURE_KV_V1: &str = "kv-v1";

/// A deployment with a Queue producer or consumer. A node without the broker
/// would otherwise omit a producer binding or silently stop consuming.
pub const FEATURE_QUEUES_V1: &str = "queues-v1";

pub const QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION: u32 = 1;

/// `deploy/queues/<queue>/consumer.json` — the one deployment allowed to
/// consume a fleet-global queue. The record keeps an exact deployment
/// reference, because the script's named pointer can move independently
/// between the attachment read and the Worker load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConsumerAttachment {
    pub schema_version: u32,
    pub queue: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<QueueConsumerDeployment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConsumerDeployment {
    pub script_name: String,
    pub version: String,
    pub prefix: String,
}

pub fn queue_consumer_attachment_key(queue: &str) -> Option<String> {
    celld_logic::cell::valid_cell_scope(queue)
        .then(|| format!("deploy/queues/{queue}/consumer.json"))
}

pub fn validate_queue_consumer_attachment(
    expected_queue: &str,
    attachment: &QueueConsumerAttachment,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        attachment.schema_version == QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION,
        "queue consumer attachment for {expected_queue:?} has unsupported schema version {}",
        attachment.schema_version
    );
    anyhow::ensure!(
        attachment.queue == expected_queue,
        "queue consumer attachment for {expected_queue:?} names queue {:?}",
        attachment.queue
    );
    if let Some(consumer) = &attachment.consumer {
        anyhow::ensure!(
            !consumer.script_name.is_empty()
                && !consumer.version.is_empty()
                && consumer.prefix
                    == format!("deploy/{}/{}", consumer.script_name, consumer.version),
            "queue consumer attachment for {expected_queue:?} has an invalid deployment reference"
        );
    }
    Ok(())
}

/// The normalized push-consumer settings stored in a deployment manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConsumerConfig {
    pub queue: String,
    pub max_batch_size: u16,
    pub max_batch_timeout: u16,
    pub max_retries: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_queue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay: Option<u32>,
}

/// Validate every Queue value before a deployment becomes runtime state.
///
/// A native deploy validates its source configuration before it writes a
/// manifest. A managed deploy has a separate TypeScript producer, so the node
/// repeats the authoritative checks from `celld-logic` when it loads either
/// manifest. This prevents a malformed producer from becoming a missing
/// binding and prevents an invalid consumer policy from reaching the broker.
#[doc(hidden)]
pub fn validate_queue_manifest(manifest: &Manifest) -> anyhow::Result<()> {
    let queue_bindings = manifest
        .raw_metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|binding| binding.get("type").and_then(serde_json::Value::as_str) == Some("queue"))
        .collect::<Vec<_>>();
    if queue_bindings.is_empty() && manifest.queue_consumers.is_empty() {
        return Ok(());
    }

    anyhow::ensure!(
        manifest
            .required_features
            .iter()
            .any(|feature| feature == FEATURE_QUEUES_V1),
        "Queue deployment does not require {FEATURE_QUEUES_V1}"
    );
    anyhow::ensure!(
        manifest
            .do_classes
            .iter()
            .any(|class| class == celld_logic::queue::RESERVED_CLASS)
            && manifest
                .sqlite_classes
                .iter()
                .any(|class| class == celld_logic::queue::RESERVED_CLASS),
        "Queue deployment does not install the reserved SQLite Queue class"
    );

    for binding in queue_bindings {
        let environment = binding
            .get("name")
            .and_then(serde_json::Value::as_str)
            .filter(|name| valid_binding_name(name))
            .context("Queue binding has an invalid environment name")?;
        let queue = binding
            .get("queue")
            .and_then(serde_json::Value::as_str)
            .filter(|queue| celld_logic::cell::valid_cell_scope(queue))
            .with_context(|| format!("Queue binding {environment:?} has an invalid queue name"))?;
        let delivery_delay_seconds = match binding.get("delivery_delay") {
            None => 0,
            Some(value) => value
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .with_context(|| {
                    format!("Queue binding {environment:?} has an invalid delivery_delay")
                })?,
        };
        celld_logic::queue::validate_config(&celld_logic::queue::QueueConfig {
            max_batch_size: celld_logic::queue::DEFAULT_MAX_BATCH_SIZE,
            max_batch_timeout_seconds: celld_logic::queue::DEFAULT_MAX_BATCH_TIMEOUT_SECONDS,
            max_retries: celld_logic::queue::DEFAULT_MAX_RETRIES,
            max_concurrency: None,
            delivery_delay_seconds,
            retry_delay_seconds: None,
        })
        .map_err(anyhow::Error::new)
        .with_context(|| format!("Queue binding {environment:?} for {queue:?} is invalid"))?;
    }

    anyhow::ensure!(
        manifest.queue_consumers.is_empty() || manifest.main_module.is_some(),
        "Queue consumer deployment has no Worker module"
    );
    let mut consumed = std::collections::BTreeSet::new();
    for consumer in &manifest.queue_consumers {
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&consumer.queue),
            "Queue consumer has an invalid queue name {:?}",
            consumer.queue
        );
        anyhow::ensure!(
            consumed.insert(&consumer.queue),
            "Queue consumer deployment repeats queue {:?}",
            consumer.queue
        );
        if let Some(dead_letter_queue) = &consumer.dead_letter_queue {
            anyhow::ensure!(
                celld_logic::cell::valid_cell_scope(dead_letter_queue)
                    && dead_letter_queue != &consumer.queue,
                "Queue consumer for {:?} has an invalid dead-letter queue {:?}",
                consumer.queue,
                dead_letter_queue
            );
        }
        celld_logic::queue::validate_config(&celld_logic::queue::QueueConfig {
            max_batch_size: consumer.max_batch_size,
            max_batch_timeout_seconds: consumer.max_batch_timeout,
            max_retries: consumer.max_retries,
            max_concurrency: consumer.max_concurrency,
            delivery_delay_seconds: 0,
            retry_delay_seconds: consumer.retry_delay,
        })
        .map_err(anyhow::Error::new)
        .with_context(|| format!("Queue consumer for {:?} is invalid", consumer.queue))?;
    }
    Ok(())
}

fn valid_binding_name(name: &str) -> bool {
    name.len() <= 128
        && name
            .chars()
            .next()
            .is_some_and(|value| value == '_' || value == '$' || value.is_ascii_alphabetic())
        && name
            .chars()
            .skip(1)
            .all(|value| value == '_' || value == '$' || value.is_ascii_alphanumeric())
}

/// Reject a manifest requiring any feature this build does not support. Both
/// load paths (control-plane deployments and fleet pointer loads) must apply
/// the same gate, so it lives here beside the feature list.
pub fn validate_required_features(required: &[String]) -> anyhow::Result<()> {
    for feature in required {
        if !SUPPORTED_DEPLOYMENT_FEATURES.contains(&feature.as_str()) {
            anyhow::bail!(
                "deployment requires feature {feature:?} this celld build does not support; upgrade celld"
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleRef {
    pub name: String,
    pub bytes: usize,
    /// Full SHA-256 of this module's exact bytes, as lowercase hexadecimal.
    ///
    /// The loader also accepts the 16-character prefix written by celld and
    /// the managed control plane before full module digests were introduced.
    pub sha256: String,
    /// Absent means UTF-8 source. Non-`.js` siblings become Wrangler Text
    /// modules; `.js`/`.mjs`/`.cjs` siblings compile as ES modules (Wrangler
    /// `no_bundle` outdirs). `wasm` bytes become a module whose default export
    /// is a compiled `WebAssembly.Module` (Wrangler's `CompiledWasm` rule).
    /// `binary` bytes become a module whose default export is an `ArrayBuffer`
    /// (Wrangler's `Data` rule for `.bin` siblings).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ModuleKind>,
}

impl ModuleRef {
    pub(crate) fn verify(&self, body: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(
            body.len() == self.bytes,
            "deployment module size mismatch for {:?}: expected {}, got {}",
            self.name,
            self.bytes,
            body.len()
        );
        anyhow::ensure!(
            matches!(self.sha256.len(), 16 | 64)
                && self
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "deployment module {:?} has an invalid SHA-256 digest",
            self.name
        );
        let actual = format!("{:x}", Sha256::digest(body));
        anyhow::ensure!(
            actual.starts_with(&self.sha256),
            "deployment module digest mismatch for {:?}: expected {}, got {}",
            self.name,
            self.sha256,
            actual
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModuleKind {
    Wasm,
    Binary,
}

/// Reference from a deploy manifest to its immutable, canonical asset index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetManifestRef {
    pub index: String,
    pub sha256: String,
    pub file_count: u32,
    pub total_bytes: u64,
}

/// `deploy/<script>/<version>/assets.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AssetIndex {
    pub schema_version: u32,
    pub entries: BTreeMap<String, AssetEntry>,
    pub config: AssetConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetEntry {
    /// Full SHA-256 of the exact response body, lowercase hexadecimal.
    pub sha256: String,
    pub bytes: u64,
    /// `None` means omit Content-Type, matching Wrangler's `application/null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html_handling: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_found_handling: Option<String>,
    #[serde(default)]
    pub run_worker_first: RunWorkerFirst,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirects: Option<String>,
    /// Cloudflare `_routes.json` (`include` / `exclude`) captured at deploy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routes: Option<AssetRoutesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_date: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compatibility_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunWorkerFirst {
    Bool(bool),
    Routes(Vec<String>),
}

impl Default for RunWorkerFirst {
    fn default() -> Self {
        Self::Bool(false)
    }
}

/// Workers static-assets `_routes.json` (version 1).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetRoutesConfig {
    pub version: u32,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// A fleet-wide immutable asset body key. The digest is validated before this
/// is called by the receiver and again by the applying node.
pub fn asset_blob_key(sha256: &str) -> Option<String> {
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    Some(format!(
        "deploy-blobs/assets/sha256/{}/{}",
        &sha256[..2],
        sha256
    ))
}

/// `deploy/current.json` — the fleet-wide pointer a node reads on startup.
/// Changing this is a deploy; nodes converge to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployPointer {
    /// Present on fleet-wide pointers. Older named pointers omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_name: Option<String>,
    pub version: String,
    pub prefix: String,
    pub rollout: Rollout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rollout {
    pub percent: u8,
}

/// Deployment identity: sorted module contents plus the serialized metadata,
/// never the raw upload framing (which is not deterministic). Every sender
/// must agree on this or identical code deploys as two versions depending on
/// the path used.
///
/// `metadata_json` is the exact byte serialization the sender stores as
/// `Manifest::raw_metadata`; callers pass the same bytes to both.
/// Cron trigger expressions are deliberately NOT an input. A version names
/// the code and its bindings; a schedule is configuration layered on top,
/// which is also how Cloudflare models it — schedules are their own resource,
/// set by their own API call after the script upload. Hashing them would make
/// the native and managed paths disagree about what a version is.
pub fn deployment_version(
    modules: &[(String, Vec<u8>)],
    metadata_json: &[u8],
    asset_index: Option<&[u8]>,
) -> String {
    let mut sorted = modules.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = Sha256::new();
    for (name, bytes) in sorted {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(bytes);
    }
    hasher.update(metadata_json);
    if let Some(index) = asset_index {
        hasher.update([0]);
        hasher.update(b"assets.json");
        hasher.update([0]);
        hasher.update(index);
    }
    format!("{:x}", hasher.finalize())[..16].to_string()
}

#[cfg(test)]
mod native_manifest_tests {
    use super::*;

    fn js_manifest() -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "version": "abc",
            "script_name": "app",
            "main_module": "index.js",
            "do_classes": [],
            "sqlite_classes": [],
            "modules": [],
            "raw_metadata": {}
        })
    }

    #[test]
    fn missing_profile_defaults_to_js_v8_and_omits_on_serialize() {
        let manifest: Manifest = serde_json::from_value(js_manifest()).unwrap();
        assert_eq!(manifest.execution_profile, EXECUTION_PROFILE_JS_V8);
        let encoded = serde_json::to_value(&manifest).unwrap();
        assert!(encoded.get("execution_profile").is_none());
        assert!(encoded.get("native_component").is_none());
    }

    #[test]
    fn native_requires_descriptor_and_feature() {
        let mut value = js_manifest();
        value["execution_profile"] = "native-http-v1".into();
        value["main_module"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Manifest>(value.clone()).is_err());
        value["required_features"] = serde_json::json!([FEATURE_NATIVE_HTTP_V1]);
        value["native_component"] = serde_json::json!({
            "path": "component.wasm",
            "sha256": "a".repeat(64),
            "baseline": NATIVE_BASELINE_1,
            "world": NATIVE_HTTP_WORLD,
            "capabilities": [NATIVE_CAPABILITY_CONFIG]
        });
        serde_json::from_value::<Manifest>(value).unwrap();
    }

    #[test]
    fn native_rejects_js_main_and_wasm_v1_mix() {
        let mut value = js_manifest();
        value["execution_profile"] = "native-http-v1".into();
        value["required_features"] = serde_json::json!([FEATURE_NATIVE_HTTP_V1, FEATURE_WASM_V1]);
        value["native_component"] = serde_json::json!({
            "path": "component.wasm",
            "sha256": "a".repeat(64),
            "baseline": NATIVE_BASELINE_1,
            "world": NATIVE_HTTP_WORLD,
            "capabilities": [NATIVE_CAPABILITY_CONFIG]
        });
        assert!(serde_json::from_value::<Manifest>(value).is_err());
    }

    #[test]
    fn native_kv_capability_must_match_bindings() {
        let mut value = js_manifest();
        value["execution_profile"] = "native-http-v1".into();
        value["main_module"] = serde_json::Value::Null;
        value["required_features"] = serde_json::json!([FEATURE_NATIVE_HTTP_V1]);
        value["native_component"] = serde_json::json!({
            "path": "component.wasm",
            "sha256": "a".repeat(64),
            "baseline": NATIVE_BASELINE_1,
            "world": NATIVE_HTTP_WORLD,
            "capabilities": [NATIVE_CAPABILITY_CONFIG, NATIVE_CAPABILITY_KV]
        });
        assert!(serde_json::from_value::<Manifest>(value.clone()).is_err());
        value["raw_metadata"] = serde_json::json!({
            "bindings": [{"type":"kv","name":"VALUES","id":"example-values"}]
        });
        serde_json::from_value::<Manifest>(value).unwrap();
    }

    #[test]
    fn unknown_profile_fails_closed() {
        let mut value = js_manifest();
        value["execution_profile"] = "mystery".into();
        assert!(serde_json::from_value::<Manifest>(value).is_err());
    }
}
