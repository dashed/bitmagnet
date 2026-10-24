//! Byte-parity between [`bitmagnet_tape::marshal`] and Go's `tape.Marshal`.
//!
//! The fixture is generated FROM Go by
//! `go test ./internal/tape -run TestCanonicalEscapeFixture -update-canonical-fixture`,
//! and carries both the input and Go's exact output. Restating the inputs on
//! this side would let the two drift apart and still agree with themselves —
//! which is the failure mode a parity test exists to prevent.
//!
//! This matters because [`bitmagnet_tape::Session::next`] compares requests as
//! BYTES. An encoder that differs from Go by one escape desyncs on every
//! observation containing that character, and the failure would look like the
//! port asking the wrong question rather than like an encoding bug.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    input: String,
    encoded: String,
}

fn fixture() -> BTreeMap<String, Case> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/tape-canonical/escapes.json");
    let bytes = std::fs::read(&path).unwrap_or_else(|err| {
        panic!(
            "read {} (regenerate with `go test ./internal/tape -run TestCanonicalEscapeFixture -update-canonical-fixture`): {err}",
            path.display()
        )
    });

    serde_json::from_slice(&bytes).expect("fixture is valid JSON")
}

#[test]
fn matches_go_byte_for_byte_on_every_case() {
    let cases = fixture();
    assert!(!cases.is_empty(), "fixture must not be empty");

    let mut mismatches = Vec::new();

    for (name, case) in &cases {
        let got = bitmagnet_tape::marshal(&case.input).expect("marshal");

        if got != case.encoded {
            mismatches.push(format!("  {name}: go={:?} rust={:?}", case.encoded, got));
        }
    }

    assert!(
        mismatches.is_empty(),
        "canonical encoding diverges from Go in {} of {} cases:\n{}",
        mismatches.len(),
        cases.len(),
        mismatches.join("\n")
    );
}

/// The one divergence this module exists to repair. Guarded explicitly so that
/// if someone replaces the custom formatter with `serde_json::to_string`, the
/// failure names the reason instead of surfacing as a mysterious desync.
#[test]
fn escapes_the_unicode_line_separators_like_go_and_unlike_serde_json() {
    let input = "a\u{2028}b\u{2029}c";

    let ours = bitmagnet_tape::marshal(input).expect("marshal");
    let plain = serde_json::to_string(input).expect("serde_json");

    assert_eq!(
        ours, r#""a\u2028b\u2029c""#,
        "must escape U+2028/U+2029 as Go does"
    );
    assert_ne!(
        ours, plain,
        "if these now agree, serde_json changed and the custom formatter may be removable — \
         verify against Go before doing so"
    );
}

/// Go emits struct fields in declaration order and map keys sorted. Round-
/// tripping a struct through `serde_json::Value` would silently sort the fields
/// instead, so this pins the ordering the tape depends on.
#[test]
fn preserves_struct_field_order_and_sorts_map_keys() {
    #[derive(serde::Serialize)]
    struct Declared {
        zebra: u8,
        alpha: u8,
    }

    assert_eq!(
        bitmagnet_tape::marshal(&Declared { zebra: 1, alpha: 2 }).expect("marshal"),
        r#"{"zebra":1,"alpha":2}"#,
        "struct fields must stay in DECLARATION order"
    );

    let mut map = BTreeMap::new();
    map.insert("zebra", 1);
    map.insert("alpha", 2);

    assert_eq!(
        bitmagnet_tape::marshal(&map).expect("marshal"),
        r#"{"alpha":2,"zebra":1}"#,
        "map keys must be sorted, as Go sorts them"
    );
}

/// `null` and absent are different in the tape format, and the encoder must not
/// blur them: `Option::None` encodes as `null`, matching a Go pointer field.
#[test]
fn encodes_none_as_null_like_a_go_pointer() {
    #[derive(serde::Serialize)]
    struct WithOption {
        year: Option<u16>,
    }

    assert_eq!(
        bitmagnet_tape::marshal(&WithOption { year: None }).expect("marshal"),
        r#"{"year":null}"#
    );
}
