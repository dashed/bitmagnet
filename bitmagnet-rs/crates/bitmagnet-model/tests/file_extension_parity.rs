//! Shared Go ↔ Rust parity test for the FB-A0/G1 file-extension contract (dv4).
//!
//! Loads `testdata/file-extension-fixtures.json` — the SAME corpus the Go test
//! `internal/blobmigration/file_extension_fixtures_test.go` consumes — and asserts
//! [`bitmagnet_model::file_extension_from_path`] reproduces `expected_extension` for
//! every case. This locks the Rust derivation byte-identical to Go's
//! `model.FileExtensionFromPath` and the Postgres generated column
//! `substring(lower(path) from '[^/.]\.([a-z0-9]+)$')`, which the G1 backfill relies
//! on (the stored blob `e` must equal the path-derived extension on every consumer).
//!
//! An empty `expected_extension` means "no extension" (the function returns `None`).

use bitmagnet_model::file_extension_from_path;

const FIXTURES_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/file-extension-fixtures.json"
));

#[test]
fn file_extension_from_path_matches_shared_fixtures() {
    let doc: serde_json::Value =
        serde_json::from_str(FIXTURES_JSON).expect("fixtures JSON should deserialize");
    let cases = doc["cases"].as_array().expect("`cases` is a JSON array");
    assert!(!cases.is_empty(), "no fixture cases loaded");

    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let name = case["name"].as_str().unwrap_or("<unnamed>");
        let path = case["path"].as_str().expect("`path` is a string");
        let expected = case["expected_extension"]
            .as_str()
            .expect("`expected_extension` is a string");

        let got = file_extension_from_path(path);
        let got_ref = got.as_deref().unwrap_or("");
        if got_ref != expected {
            failures.push(format!(
                "case {name:?} path {path:?}: expected {expected:?}, got {got_ref:?}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "file-extension parity failures vs shared fixtures:\n{}",
        failures.join("\n")
    );
}
