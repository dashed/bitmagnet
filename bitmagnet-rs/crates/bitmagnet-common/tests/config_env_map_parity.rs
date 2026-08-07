use bitmagnet_common::{config, strcase};

/// A copy of the Go-side golden at `testdata/parity/config-env-map.golden`,
/// which is generated from Go's config surface by `TestConfigEnvMapGolden`.
///
/// The two files are meant to be byte-identical: this one is the ground truth
/// Rust's key derivation is checked against, so letting it lag means the port
/// stops being validated against keys Go has and Rust has not seen. When a Go
/// config field is added, copy the file across and bump `EXPECTED_KEYS` below in
/// the same commit.
const GOLDEN: &str = include_str!("fixtures/config-env-map.golden");

/// Line count of `GOLDEN`, asserted so a truncated or partially-copied fixture
/// is caught rather than silently shrinking the test's coverage. It is expected
/// to change whenever the Go config surface does; see [`GOLDEN`].
const EXPECTED_KEYS: usize = 124;

#[test]
fn go_config_env_map_golden_is_preserved() {
    assert!(!GOLDEN.contains('\r'), "golden must use LF line endings");
    assert!(GOLDEN.ends_with('\n'), "golden must end with a newline");

    let body = GOLDEN
        .strip_suffix('\n')
        .expect("trailing newline was checked");
    assert!(
        !body.ends_with('\n'),
        "golden must have exactly one trailing newline"
    );

    let lines = body.split('\n').collect::<Vec<_>>();
    assert_eq!(
        lines.len(),
        EXPECTED_KEYS,
        "golden key count changed: if you re-synced this fixture from \
         testdata/parity/config-env-map.golden, bump EXPECTED_KEYS to match"
    );
    for adjacent in lines.windows(2) {
        assert!(
            adjacent[0] < adjacent[1],
            "golden lines must be strictly sorted and unique: {:?}",
            adjacent
        );
    }

    for line in lines {
        let mut fields = line.split('\t');
        let expected_env_key = fields.next().expect("line has an env-key field");
        let dotpath = fields.next().expect("line has a dot-path field");
        assert!(
            fields.next().is_none(),
            "golden line must contain exactly two tab-separated fields: {line:?}"
        );

        assert_eq!(
            config::env_key_for_dotpath(dotpath),
            expected_env_key,
            "env key mismatch for {dotpath}"
        );
        for segment in dotpath.split('.') {
            assert_eq!(
                strcase::to_snake(segment),
                segment,
                "Go-produced path segment is not a strcase fixed point: {segment}"
            );
        }
    }
}
