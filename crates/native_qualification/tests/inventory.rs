use native_qualification::inventory::pinned_versions;
use std::process::Command;

#[test]
fn inventory_metadata() {
    let pins = pinned_versions().expect("lock pins");
    assert_eq!(pins.wasmtime, "48.0.1");
    assert_eq!(pins.wasmtime_wasi, "48.0.1");
    assert_eq!(pins.wasmtime_wasi_http, "48.0.1");

    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--locked"])
        .current_dir(native_qualification::inventory::workspace_root())
        .output()
        .expect("cargo metadata");
    assert!(output.status.success(), "{:?}", output);
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
    let packages = metadata["packages"].as_array().expect("packages");
    for name in ["wasmtime", "wasmtime-wasi", "wasmtime-wasi-http"] {
        let package = packages
            .iter()
            .find(|package| package["name"] == name && package["version"] == "48.0.1")
            .unwrap_or_else(|| panic!("missing {name} 48.0.1"));
        let license = package["license"].as_str().unwrap_or_default();
        assert!(
            license.contains("Apache-2.0"),
            "{name} license {license}"
        );
    }
}
