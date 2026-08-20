// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Isolate bootstrap: what a fresh context needs before user code runs.
//!
//! The prelude and the harness compile once per process and are cached, so a
//! new isolate pays for execution but not for compilation. On top of those go
//! the bindings — `env`, the routing table, the compatibility flags — which
//! differ per worker and are therefore built every time.
use super::*;

/// Per-process compiled-bytecode cache for the fixed bootstrap scripts.
/// Every cell wake pays a fresh isolate, and without this each one re-parses
/// and re-compiles the same ~23k lines of prelude + harness — the single
/// largest slice of cold-wake latency. The first isolate compiles
/// eagerly and publishes the cache; every later isolate consumes it and only
/// executes.
type BootstrapCache = std::collections::HashMap<&'static str, std::sync::Arc<Vec<u8>>>;

fn bootstrap_code_cache() -> &'static std::sync::Mutex<BootstrapCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<BootstrapCache>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

fn run_bootstrap_script(scope: &mut v8::PinScope, name: &'static str, src: &str) -> Result<()> {
    use v8::script_compiler::CompileOptions;
    use v8::script_compiler::NoCacheReason;
    let code = v8::String::new(scope, src).unwrap();
    let cached = bootstrap_code_cache().lock().unwrap().get(name).cloned();
    let unbound = match cached {
        // `CachedData` borrows `bytes`; the Arc binding outlives the Source.
        Some(bytes) => {
            let data = v8::script_compiler::CachedData::new(&bytes);
            let mut source = v8::script_compiler::Source::new_with_cached_data(code, None, data);
            let unbound = v8::script_compiler::compile_unbound_script(
                scope,
                &mut source,
                CompileOptions::ConsumeCodeCache,
                NoCacheReason::NoReason,
            )
            .ok_or_else(|| anyhow!("bootstrap compile {name}"))?;
            debug_assert!(
                !source.get_cached_data().is_some_and(|data| data.rejected()),
                "code cache rejected for {name}"
            );
            unbound
        }
        None => {
            let mut source = v8::script_compiler::Source::new(code, None);
            // Eager: a lazily-compiled cache would push function-body compile
            // cost back into every consumer's first request.
            let unbound = v8::script_compiler::compile_unbound_script(
                scope,
                &mut source,
                CompileOptions::EagerCompile,
                NoCacheReason::NoReason,
            )
            .ok_or_else(|| anyhow!("bootstrap compile {name}"))?;
            if let Some(data) = unbound.create_code_cache() {
                bootstrap_code_cache()
                    .lock()
                    .unwrap()
                    .insert(name, std::sync::Arc::new(data.to_vec()));
            }
            unbound
        }
    };
    unbound
        .bind_to_current_context(scope)
        .run(scope)
        .ok_or_else(|| anyhow!("bootstrap run {name}"))?;
    Ok(())
}

/// WPT-conformant Web APIs: TextEncoder, URL/URLSearchParams, atob/btoa, and
/// Headers. Run once per isolate before the Cells/Cloudflare-specific harness.
pub(super) fn install_prelude(scope: &mut v8::PinScope) -> Result<()> {
    const PRELUDE: &[(&str, &str)] = &[
        ("text_encoding.js", include_str!("text_encoding.js")),
        ("url_search_params.js", include_str!("url_search_params.js")),
        ("url.js", include_str!("url.js")),
        ("atob_btoa.js", include_str!("atob_btoa.js")),
        ("headers.js", include_str!("headers.js")),
        ("streams.js", include_str!("streams.js")),
        ("writable_stream.js", include_str!("writable_stream.js")),
        (
            "text_encoding_streams.js",
            include_str!("text_encoding_streams.js"),
        ),
    ];
    for (name, src) in PRELUDE {
        run_bootstrap_script(scope, name, src)?;
    }
    Ok(())
}

pub(super) fn install_harness(scope: &mut v8::PinScope) -> Result<()> {
    run_bootstrap_script(scope, "harness.js", include_str!("harness.js"))?;
    run_bootstrap_script(scope, "crypto.js", include_str!("crypto.js"))
}

pub(super) fn register_class(
    scope: &mut v8::PinScope,
    name: &str,
    cls: v8::Local<v8::Value>,
) -> Result<()> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let ck = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, ck.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let clk = v8::String::new(scope, "classes").unwrap();
    let classes = cell
        .get(scope, clk.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let nk = v8::String::new(scope, name).unwrap();
    classes.set(scope, nk.into(), cls);
    Ok(())
}

/// Populate `cloudflare:workers` `exports` with the worker module's own exports:
/// a Durable Object class is exposed as its namespace (idFromName/get/RPC), any
/// other export by value. `default` is omitted (it is the fetch entrypoint, not
/// an importable export). Mirrors Workerd's `import { exports }` surface.
pub(super) fn populate_cf_exports(
    scope: &mut v8::PinScope,
    ns: v8::Local<v8::Object>,
    do_classes: &[String],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cf = global
        .get(scope, v8::String::new(scope, "__cf").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf runtime state"))?;
    let exports = cf
        .get(scope, v8::String::new(scope, "exports").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf.exports"))?;
    let cell = global
        .get(scope, v8::String::new(scope, "__cell").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let make_ns: v8::Local<v8::Function> = cell
        .get(
            scope,
            v8::String::new(scope, "makeNamespace").unwrap().into(),
        )
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| anyhow!("missing __cell.makeNamespace"))?;
    let do_set: std::collections::HashSet<&str> = do_classes.iter().map(String::as_str).collect();
    let names = ns
        .get_own_property_names(scope, Default::default())
        .ok_or_else(|| anyhow!("no module export names"))?;
    for index in 0..names.length() {
        let name_value = names.get_index(scope, index).unwrap();
        let name = name_value.to_rust_string_lossy(scope);
        if name == "default" {
            continue;
        }
        let export_value = if do_set.contains(name.as_str()) {
            let arg = v8::String::new(scope, &name).unwrap().into();
            make_ns
                .call(scope, cell.into(), &[arg])
                .ok_or_else(|| anyhow!("makeNamespace({name}) failed"))?
        } else {
            ns.get(scope, name_value)
                .ok_or_else(|| anyhow!("export {name}"))?
        };
        let key = v8::String::new(scope, &name).unwrap();
        exports.set(scope, key.into(), export_value);
    }
    Ok(())
}

pub(super) fn inject_namespace_keys(
    scope: &mut v8::PinScope,
    script_name: &str,
    do_classes: &[String],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let keys_key = v8::String::new(scope, "namespaceKeys").unwrap();
    let keys = cell
        .get(scope, keys_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object namespace registry"))?;
    let script_key = v8::String::new(scope, "script").unwrap();
    let script_value = v8::String::new(scope, script_name).unwrap();
    cell.set(scope, script_key.into(), script_value.into());
    for class_name in do_classes {
        let class_key = v8::String::new(scope, class_name).unwrap();
        let namespace_key = super::namespace_key(script_name, class_name);
        let namespace_key = v8::String::new(scope, &namespace_key).unwrap();
        if !keys
            .set(scope, class_key.into(), namespace_key.into())
            .unwrap_or(false)
        {
            anyhow::bail!("could not register namespace key for {class_name}");
        }
    }
    Ok(())
}

/// `__cell.crons`: the deployment's cron trigger expressions, verbatim as the
/// developer wrote them. The reserved cron cell reads them at each activation,
/// so a schedule change arrives with the deployment and needs no migration of
/// an alarm the previous deployment armed. Absent `triggers.crons`, the list
/// is empty and the cron cell retires itself.
pub(super) fn inject_crons(scope: &mut v8::PinScope, crons: &[String]) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell = global
        .get(scope, v8::String::new(scope, "__cell").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let list = v8::Array::new(scope, crons.len() as i32);
    for (index, cron) in crons.iter().enumerate() {
        let value = v8::String::new(scope, cron).ok_or_else(|| anyhow!("cron expression"))?;
        list.set_index(scope, index as u32, value.into());
    }
    let key = v8::String::new(scope, "crons").unwrap();
    cell.set(scope, key.into(), list.into());
    Ok(())
}

/// `globalThis.Cloudflare.compatibilityFlags`. Only flags Cells actually
/// honours are listed; a flag Cells does not model is absent (falsy) rather
/// than reported as enabled.
///
/// Built through the V8 object API rather than by compiling a snippet: this
/// runs on every isolate load, and a separate `Script::compile` there cost
/// ~100us per worker.
pub(super) fn inject_compatibility_flags(scope: &mut v8::PinScope, compat: Compat) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let flags = v8::Object::new(scope);
    let set_flag = |scope: &mut v8::PinScope, name: &str, value: bool| {
        let key = v8::String::new(scope, name).unwrap();
        let value = v8::Boolean::new(scope, value);
        flags.set(scope, key.into(), value.into());
    };
    set_flag(scope, "upper_case_all_http_methods", true);
    // Cells' async stream/pipe methods return rejected promises
    // rather than throwing synchronously, which is exactly the
    // behavior this flag names.
    set_flag(scope, "capture_async_api_throws", true);
    set_flag(
        scope,
        "delete_all_deletes_alarm",
        compat.delete_all_deletes_alarm,
    );
    set_flag(scope, "js_rpc", compat.js_rpc);
    set_flag(scope, "sqlite_vec", compat.sqlite_vec);
    set_flag(
        scope,
        "fetcher_no_get_put_delete",
        !compat.fetcher_get_put_delete,
    );
    let cloudflare = v8::Object::new(scope);
    let key = v8::String::new(scope, "compatibilityFlags").unwrap();
    cloudflare.set(scope, key.into(), flags.into());
    let key = v8::String::new(scope, "Cloudflare").unwrap();
    global.set(scope, key.into(), cloudflare.into());
    Ok(())
}

pub(super) fn build_env(scope: &mut v8::PinScope, config: &WorkerConfig) -> Result<()> {
    let script_name = config.script_name.as_str();
    let bindings = config.bindings.as_slice();
    let r2_bindings = config.r2_bindings.as_slice();
    let d1_bindings = config.d1_bindings.as_slice();
    let ai_binding = config.ai_binding.as_deref();
    let vars = config.vars.as_slice();
    let services = config.services.as_slice();
    let asset_binding = config.asset_binding.as_deref();
    // call the harness: for each binding, env[bindingName] = makeNamespace(className)
    let src = {
        let mut lines = String::from("(() => { const e = __cell.env;\n");
        for (bname, cname) in bindings {
            lines.push_str(&format!(
                "e[{:?}] = __cell.makeNamespace({:?});\n",
                bname, cname
            ));
        }
        for name in r2_bindings {
            lines.push_str(&format!("e[{:?}] = __makeR2Bucket({:?});\n", name, name));
        }
        for (name, database) in d1_bindings {
            lines.push_str(&format!(
                "e[{:?}] = __makeD1Database({:?});\n",
                name, database
            ));
        }
        if let (Some(name), Ok(url)) = (ai_binding, std::env::var("CELLD_AI_URL")) {
            lines.push_str(&format!("e[{:?}] = __makeAiBinding({:?});\n", name, url));
        }
        for (binding, script, entrypoint) in services {
            let entrypoint = match entrypoint {
                Some(name) => format!("{name:?}"),
                None => "null".to_string(),
            };
            lines.push_str(&format!(
                "e[{:?}] = __makeServiceBinding({:?}, {});\n",
                binding, script, entrypoint
            ));
        }
        for (name, value) in vars {
            lines.push_str(&format!("e[{:?}] = {:?};\n", name, value));
        }
        if let Some(name) = asset_binding {
            lines.push_str(&format!(
                "e[{:?}] = __makeAssetsBinding({:?});\n",
                name, script_name
            ));
        }
        if let Some(name) = config.loader_binding.as_deref() {
            lines.push_str(&format!("e[{:?}] = __makeLoader();\n", name));
        }
        // A loaded worker's caller-supplied `env` (plain JSON values only in
        // the walking skeleton) merges last, over the declared bindings.
        if let Some(env) = config.loader_env.as_deref() {
            lines.push_str(&format!("Object.assign(e, {});\n", env));
        }
        lines.push_str("})();");
        lines
    };
    let code = v8::String::new(scope, &src).unwrap();
    let s = v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("env compile"))?;
    s.run(scope).ok_or_else(|| anyhow!("env run"))?;
    Ok(())
}

/// Record every exported class deriving from `WorkerEntrypoint` in
/// `__cell.entrypoints`, so a service binding with `entrypoint = "Name"` can
/// resolve it, and every export deriving from `DurableObject` in
/// `__cell.doExports`, so misrouting a DO class as a stateless entrypoint is
/// reported as such (Workerd getExportedHandler()). Runs once per load; the
/// check is a prototype walk, not a call into user code.
pub(super) fn register_entrypoints(
    scope: &mut v8::PinScope,
    ns: v8::Local<v8::Object>,
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let key = v8::String::new(scope, "entrypoints").unwrap();
    let entrypoints = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing entrypoint registry"))?;
    let key = v8::String::new(scope, "objectEntrypoints").unwrap();
    let object_entrypoints = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing object-entrypoint registry"))?;
    let key = v8::String::new(scope, "doExports").unwrap();
    let do_exports = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing doExports registry"))?;
    let cf_key = v8::String::new(scope, "__cf").unwrap();
    let cf = global
        .get(scope, cf_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf"))?;
    let key = v8::String::new(scope, "WorkerEntrypoint").unwrap();
    let entrypoint_base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing WorkerEntrypoint base"))?;
    let key = v8::String::new(scope, "DurableObject").unwrap();
    let durable_base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing DurableObject base"))?;
    let Some(names) = ns.get_own_property_names(scope, Default::default()) else {
        return Ok(());
    };
    let yes = v8::Boolean::new(scope, true);
    let extends = compile_fn(scope, EXTENDS_SRC)?;
    for index in 0..names.length() {
        let Some(name) = names.get_index(scope, index) else {
            continue;
        };
        let Some(value) = ns.get(scope, name) else {
            continue;
        };
        if !value.is_function() {
            // A plain-object export is Workerd's non-class entrypoint; its
            // handler functions dispatch as fn(arg, env, ctx).
            if value.is_object() {
                object_entrypoints.set(scope, name, value);
            }
            continue;
        }
        // Walk the prototype chain rather than calling anything. The walk
        // runs in JS: Reflect.getPrototypeOf honors Proxy exports (SDK shims
        // like workers-rs wrap their classes), which the host-side
        // Object::GetPrototype does not.
        if call_extends(scope, extends, value, entrypoint_base)? {
            entrypoints.set(scope, name, value);
        } else if call_extends(scope, extends, value, durable_base)? {
            do_exports.set(scope, name, yes.into());
        }
    }
    Ok(())
}

/// `(cls, base) => base is on cls's prototype chain`, via Reflect so Proxy
/// wrappers report the prototype their handler exposes. A getPrototypeOf trap
/// also makes the chain user-controlled (a cycle, or a fresh Proxy per hop,
/// is spec-legal on an extensible target), so track visited links and cap the
/// walk rather than let it hang the load.
const EXTENDS_SRC: &str = "((cls, base) => { \
    const seen = new Set(); \
    for (let p = Reflect.getPrototypeOf(cls); p; p = Reflect.getPrototypeOf(p)) { \
        if (p === base) return true; \
        if (seen.has(p) || seen.size >= 1000) return false; \
        seen.add(p); \
    } \
    return false; })";

/// The walk crosses into user code when an export is a Proxy, so it can throw
/// (a revoked Proxy, a throwing getPrototypeOf trap). Pin a TryCatch and
/// surface that as a load error rather than misclassifying the export.
fn call_extends(
    scope: &mut v8::PinScope,
    extends: v8::Local<v8::Function>,
    class: v8::Local<v8::Value>,
    base: v8::Local<v8::Value>,
) -> Result<bool> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut tc.init();
    let recv = v8::undefined(scope).into();
    match extends.call(scope, recv, &[class, base]) {
        Some(result) => Ok(result.is_true()),
        None => Err(anyhow!("inspect export prototype chain: {}", exc!(scope))),
    }
}

/// Mark which cell scopes are local to this node and record the node id. Every
/// other scope routes cross-node via `__do_call`.
pub(super) fn inject_routing(scope: &mut v8::PinScope, owned: &[String], node: &str) -> Result<()> {
    let mut src = format!("(() => {{ __cell.node = {node:?};\n");
    for s in owned {
        src.push_str(&format!("__cell.owned[{s:?}] = true;\n"));
    }
    src.push_str("})();");
    let code = v8::String::new(scope, &src).unwrap();
    let s = v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("routing compile"))?;
    s.run(scope).ok_or_else(|| anyhow!("routing run"))?;
    Ok(())
}

/// Amend `inject_routing` for one cell, release its instance, and restore its
/// id name.
///
/// The isolate-side half of taking a cell in or giving it back. Kept beside
/// `inject_routing` because it edits the table that function builds.
pub(super) fn adopt_cell(
    tc: &mut v8::PinScope,
    cell: &str,
    db_path: Option<&str>,
    owned: bool,
    compat: Compat,
) -> Result<Option<i64>> {
    if owned {
        if let Some(path) = db_path {
            storage::open_with_compat(cell, path, compat.sqlite_vec)
                .context("cell storage open failed")?;
        }
    }
    // NOT marked local, deliberately. `__cell.owned[scope]` turns a cell
    // dispatch into an in-isolate call, and with one cell per isolate the
    // only reachable case was a cell calling itself. Now that cells share an
    // isolate the same flag makes A->B run B's handler *nested inside* A's,
    // and a cycle back to A nests again on one thread — which times out
    // (`actor lab self and cycle paths`).
    //
    // So every cell dispatch goes back out through the host and arrives as
    // an ordinary reentrant `CellJob::Fetch`, which the run loop already
    // supports. That costs a round trip and is the behaviour cells had
    // before they shared isolates. Re-enabling the fast path needs the
    // target checked against what is already on the stack; correctness
    // first.
    //
    // The script also releases everything the isolate holds for this cell's
    // residency, on both edges: the give-back frees what the application left
    // behind — memory the node cannot shed, because the cell has left
    // residency — and the take-in stops state spanning two epochs, which
    // corrupts rather than leaks. `__cell.release` lives in the harness
    // because what a residency owns is decided by the maps that hold it; this
    // side only says when.
    //
    // The release loses nothing the cell needs back: `register_actor_name`
    // persists the id name before writing it here, so the take-in below reads
    // it back.
    let source = format!(
        "(() => {{ const s = {cell:?}; \
         delete __cell.owned[s]; \
         __cell.release(s); }})();"
    );
    let code = v8::String::new(tc, &source).unwrap();
    let script = v8::Script::compile(tc, code, None).ok_or_else(|| anyhow!("adopt compile"))?;
    script.run(tc).ok_or_else(|| anyhow!("adopt run"))?;
    if !owned {
        storage::close(cell);
        return Ok(None);
    }
    let name = storage::get_actor_name(cell).context("read actor name")?;
    register_actor_name(tc, cell, name.as_deref())?;
    Ok(storage::get_alarm(cell))
}

pub(super) fn inject_storage_compatibility(scope: &mut v8::PinScope, compat: Compat) -> Result<()> {
    let source = format!(
        "__cell.deleteAllDeletesAlarm = {};\n\
         __cell.compat.jsRpc = {};\n\
         __cell.compat.fetcherGetPutDelete = {};\n\
         __cell.compat.websocketStandardBinaryType = {};",
        compat.delete_all_deletes_alarm,
        compat.js_rpc,
        compat.fetcher_get_put_delete,
        compat.websocket_standard_binary_type,
    );
    let code = v8::String::new(scope, &source).unwrap();
    let script =
        v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("compatibility compile"))?;
    script
        .run(scope)
        .ok_or_else(|| anyhow!("compatibility run"))?;
    Ok(())
}

pub(super) fn harness_env<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Value>> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let ck = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, ck.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let ek = v8::String::new(scope, "env").unwrap();
    Ok(cell.get(scope, ek.into()).unwrap())
}

pub(super) fn begin_event_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Value>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__beginEvent").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no __beginEvent"))?
        .try_into()
        .map_err(|_| anyhow!("__beginEvent is not a function"))?;
    let recv = v8::undefined(scope).into();
    function
        .call(scope, recv, &[])
        .ok_or_else(|| anyhow!("__beginEvent threw"))
}

pub(super) fn end_event_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<Option<v8::Local<'s, v8::Promise>>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__endEvent").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no __endEvent"))?
        .try_into()
        .map_err(|_| anyhow!("__endEvent is not a function"))?;
    let recv = v8::undefined(scope).into();
    let value = function
        .call(scope, recv, &[])
        .ok_or_else(|| anyhow!("__endEvent threw"))?;
    if value.is_null_or_undefined() {
        Ok(None)
    } else {
        Ok(Some(value.try_into().map_err(|_| {
            anyhow!("__endEvent did not return a promise")
        })?))
    }
}

// ---- module compilation + request/response marshalling ----
