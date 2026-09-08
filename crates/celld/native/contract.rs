use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::js::HttpResponse;

pub const DEFAULT_REQUEST_DEADLINE: Duration = Duration::from_secs(30);
pub const DEFAULT_CANCELLATION_GRACE: Duration = Duration::from_millis(250);
pub const DEFAULT_MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: u64 = 10 * 1024 * 1024;
pub const DEFAULT_MAX_RESPONSE_BODY_BYTES: u64 = 10 * 1024 * 1024;
pub const DEFAULT_MAX_HOSTCALLS: u32 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeErrorCode {
    InvalidContract,
    ArtifactIntegrity,
    InstantiateFailed,
    CapabilityDenied,
    InvalidRequest,
    HostcallFailed,
    GuestTrap,
    DeadlineExceeded,
    ResourceExhausted,
    Cancelled,
}

impl NativeErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidContract => "invalid_contract",
            Self::ArtifactIntegrity => "artifact_integrity",
            Self::InstantiateFailed => "instantiate_failed",
            Self::CapabilityDenied => "capability_denied",
            Self::InvalidRequest => "invalid_request",
            Self::HostcallFailed => "hostcall_failed",
            Self::GuestTrap => "guest_trap",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn status(self) -> u16 {
        match self {
            Self::InvalidRequest | Self::InvalidContract => 400,
            Self::CapabilityDenied => 403,
            Self::Cancelled => 499,
            Self::DeadlineExceeded => 504,
            _ => 500,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NativeError {
    pub code: NativeErrorCode,
}

impl NativeError {
    pub fn new(code: NativeErrorCode) -> Self {
        Self { code }
    }

    pub fn invalid_contract(_detail: impl Into<String>) -> Self {
        Self::new(NativeErrorCode::InvalidContract)
    }

    pub fn artifact_integrity() -> Self {
        Self::new(NativeErrorCode::ArtifactIntegrity)
    }

    pub fn instantiate_failed() -> Self {
        Self::new(NativeErrorCode::InstantiateFailed)
    }

    pub fn capability_denied() -> Self {
        Self::new(NativeErrorCode::CapabilityDenied)
    }

    pub fn invalid_request() -> Self {
        Self::new(NativeErrorCode::InvalidRequest)
    }

    pub fn hostcall_failed() -> Self {
        Self::new(NativeErrorCode::HostcallFailed)
    }

    pub fn guest_trap() -> Self {
        Self::new(NativeErrorCode::GuestTrap)
    }

    pub fn deadline_exceeded() -> Self {
        Self::new(NativeErrorCode::DeadlineExceeded)
    }

    pub fn resource_exhausted() -> Self {
        Self::new(NativeErrorCode::ResourceExhausted)
    }

    pub fn cancelled() -> Self {
        Self::new(NativeErrorCode::Cancelled)
    }

    pub fn into_http_response(self) -> HttpResponse {
        HttpResponse {
            status: self.code.status(),
            body: format!("{}\n", self.code.as_str()).into_bytes(),
            stream: None,
            headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
            websocket: None,
            write_position: None,
        }
    }
}

impl fmt::Display for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code.as_str())
    }
}

impl std::error::Error for NativeError {}

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub request_id: String,
    pub version_id: String,
    pub generation: u64,
}

impl RequestContext {
    pub fn new(request_id: String, version_id: String, generation: u64) -> Result<Self, NativeError> {
        if request_id.is_empty() || version_id.is_empty() || generation == 0 {
            return Err(NativeError::invalid_request());
        }
        Ok(Self {
            request_id,
            version_id,
            generation,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResourceLimits {
    pub request_deadline: Duration,
    pub cancellation_grace: Duration,
    pub max_memory_bytes: u64,
    pub max_request_body_bytes: u64,
    pub max_response_body_bytes: u64,
    pub max_hostcalls: u32,
}

pub type NativeLimits = ResourceLimits;

impl ResourceLimits {
    pub fn from_vars(vars: &BTreeMap<String, String>) -> Result<Self, NativeError> {
        Ok(Self {
            request_deadline: duration_var(
                vars,
                "CELLP_NATIVE_REQUEST_DEADLINE_MS",
                DEFAULT_REQUEST_DEADLINE,
            )?,
            cancellation_grace: duration_var(
                vars,
                "CELLP_NATIVE_CANCELLATION_GRACE_MS",
                DEFAULT_CANCELLATION_GRACE,
            )?,
            max_memory_bytes: u64_var(vars, "CELLP_NATIVE_MAX_MEMORY_BYTES", DEFAULT_MAX_MEMORY_BYTES)?,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_response_body_bytes: DEFAULT_MAX_RESPONSE_BODY_BYTES,
            max_hostcalls: u32_var(vars, "CELLP_NATIVE_MAX_HOSTCALLS", DEFAULT_MAX_HOSTCALLS)?,
        })
    }
}

fn duration_var(
    vars: &BTreeMap<String, String>,
    name: &str,
    default: Duration,
) -> Result<Duration, NativeError> {
    match vars.get(name) {
        None => Ok(default),
        Some(value) => {
            let millis: u64 = value.parse().map_err(|_| NativeError::invalid_contract(name))?;
            if millis == 0 {
                return Err(NativeError::invalid_contract(name));
            }
            Ok(Duration::from_millis(millis))
        }
    }
}

fn u64_var(vars: &BTreeMap<String, String>, name: &str, default: u64) -> Result<u64, NativeError> {
    match vars.get(name) {
        None => Ok(default),
        Some(value) => {
            let parsed = value.parse().map_err(|_| NativeError::invalid_contract(name))?;
            if parsed == 0 {
                return Err(NativeError::invalid_contract(name));
            }
            Ok(parsed)
        }
    }
}

fn u32_var(vars: &BTreeMap<String, String>, name: &str, default: u32) -> Result<u32, NativeError> {
    match vars.get(name) {
        None => Ok(default),
        Some(value) => {
            let parsed = value.parse().map_err(|_| NativeError::invalid_contract(name))?;
            if parsed == 0 {
                return Err(NativeError::invalid_contract(name));
            }
            Ok(parsed)
        }
    }
}

#[derive(Clone)]
pub struct CapabilitySnapshot {
    vars: BTreeMap<String, String>,
    kv: BTreeMap<String, String>,
}

impl CapabilitySnapshot {
    pub fn from_runtime(
        vars: Vec<(String, String)>,
        kv_bindings: Vec<(String, String)>,
        kv_admitted: bool,
    ) -> Result<Self, NativeError> {
        if !kv_admitted && !kv_bindings.is_empty() {
            return Err(NativeError::invalid_contract(
                "KV bindings present without admitted KV interface",
            ));
        }
        Ok(Self {
            vars: vars.into_iter().collect(),
            kv: if kv_admitted {
                kv_bindings.into_iter().collect()
            } else {
                BTreeMap::new()
            },
        })
    }

    pub fn vars(&self) -> &BTreeMap<String, String> {
        &self.vars
    }

    pub fn var(&self, name: &str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    pub fn kv_namespace(&self, binding: &str) -> Result<&str, NativeError> {
        self.kv
            .get(binding)
            .map(String::as_str)
            .ok_or_else(NativeError::capability_denied)
    }
}

impl fmt::Debug for CapabilitySnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilitySnapshot")
            .field("vars", &self.vars.keys().cloned().collect::<Vec<_>>())
            .field("kv", &self.kv.keys().cloned().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_snapshot_rejects_bindings_without_admitted_kv_interface() {
        let error = CapabilitySnapshot::from_runtime(
            Vec::new(),
            vec![("VALUES".into(), "example-values".into())],
            false,
        )
        .unwrap_err();
        assert_eq!(error.code, NativeErrorCode::InvalidContract);
    }

    #[test]
    fn debug_redacts_values_and_resource_ids() {
        let snapshot = CapabilitySnapshot::from_runtime(
            vec![("GREETING".into(), "secret".into())],
            vec![("VALUES".into(), "example-values".into())],
            true,
        )
        .unwrap();
        let debug = format!("{snapshot:?}");
        assert!(debug.contains("GREETING"));
        assert!(debug.contains("VALUES"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("example-values"));
    }
}
