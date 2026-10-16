use std::{fs, path::PathBuf};

use bitmagnet_graphql::{normalize::normalize_sdl, schema};

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/schema.graphql")
}

/// The G1 0-diff gate: the code-first schema, rendered to SDL and canonicalized
/// by the shared normalizer, must equal the Go gqlgen SDL golden byte-for-byte.
/// The golden is Lane P's normalized concatenation of the source `.graphqls`
/// files (introspection root fields stripped symmetrically with `__`-typed
/// builtins), so only declared types/fields/enums/scalars/nullability are compared.
#[test]
fn schema_zero_diff_full_golden() {
    let golden = fs::read_to_string(golden_path()).expect("read schema SDL golden");
    let actual = normalize_sdl(&schema().sdl()).expect("normalize generated schema SDL");

    assert_eq!(
        actual, golden,
        "generated SDL diverges from the gqlgen golden"
    );
}
