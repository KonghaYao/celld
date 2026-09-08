use std::fmt;

const QUALIFIED_WORLD: &str = "cellp:qualification/echo@0.1.0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterRequest {
    pub value: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterResponse {
    pub value: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdapterErrorCode {
    AbiUnsupported,
    ImportMissing,
    InstantiateFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterError {
    pub code: AdapterErrorCode,
    message: String,
}

impl AdapterError {
    fn new(code: AdapterErrorCode, message: &str) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AdapterError {}

pub fn invoke_echo(
    component_bytes: &[u8],
    declared_world: &str,
    request: AdapterRequest,
) -> Result<AdapterResponse, AdapterError> {
    if declared_world != QUALIFIED_WORLD {
        return Err(AdapterError::new(
            AdapterErrorCode::AbiUnsupported,
            "declared world is not the qualification echo world",
        ));
    }
    invoke_inner(component_bytes, request)
}

fn invoke_inner(
    component_bytes: &[u8],
    request: AdapterRequest,
) -> Result<AdapterResponse, AdapterError> {
    use wasmtime::component::{Component, Linker};
    use wasmtime::{Config, Engine, Store};

    let mut config = Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config).map_err(|_| {
        AdapterError::new(
            AdapterErrorCode::InstantiateFailed,
            "engine configuration failed",
        )
    })?;
    let component = Component::new(&engine, component_bytes).map_err(|_| {
        AdapterError::new(
            AdapterErrorCode::InstantiateFailed,
            "component bytes are not a valid Component Model artifact",
        )
    })?;
    let mut linker = Linker::new(&engine);
    let mut store = Store::new(&engine, ());
    let instance = linker.instantiate(&mut store, &component).map_err(|error| {
        let text = error.to_string().to_ascii_lowercase();
        if text.contains("import") || text.contains("unknown import") {
            AdapterError::new(
                AdapterErrorCode::ImportMissing,
                "required import is unavailable",
            )
        } else {
            AdapterError::new(
                AdapterErrorCode::InstantiateFailed,
                "component instantiation failed",
            )
        }
    })?;
    let echo = instance
        .get_typed_func::<(u32,), (u32,)>(&mut store, "echo")
        .map_err(|_| {
            AdapterError::new(
                AdapterErrorCode::AbiUnsupported,
                "component does not export the echo function",
            )
        })?;
    let (value,) = echo.call(&mut store, (request.value,)).map_err(|_| {
        AdapterError::new(
            AdapterErrorCode::InstantiateFailed,
            "echo invocation failed",
        )
    })?;
    Ok(AdapterResponse { value })
}

pub fn echo_wat() -> &'static str {
    r#"(component
  (core module $m
    (func (export "echo") (param i32) (result i32)
      local.get 0))
  (core instance $i (instantiate $m))
  (func (export "echo") (param "value" u32) (result u32)
    (canon lift (core func $i "echo"))))"#
}

pub fn import_missing_wat() -> &'static str {
    r#"(component
  (import "missing:host/api@0.1.0" (func $host (param "value" u32) (result u32)))
  (core func $lower (canon lower (func $host)))
  (core module $m
    (import "" "host" (func $host (param i32) (result i32)))
    (func (export "echo") (param i32) (result i32)
      local.get 0
      call $host))
  (core instance $i (instantiate $m (with "" (instance (export "host" (func $lower))))))
  (func (export "echo") (param "value" u32) (result u32)
    (canon lift (core func $i "echo"))))"#
}
