use bitmagnet_graphql::{normalize::normalize_sdl, spike::spike_schema_sdl};
use std::fs;

const FULL_GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/schema.graphql"
);
const SUBSET_GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/graphql/schema_subset.graphql"
);

fn normalized_spike() -> String {
    normalize_sdl(&spike_schema_sdl()).expect("spike SDL should normalize")
}

fn block_named<'a>(sdl: &'a str, name: &str) -> &'a str {
    sdl.trim_end()
        .split("\n\n")
        .find(|block| block.split_whitespace().nth(1) == Some(name))
        .unwrap_or_else(|| panic!("normalized SDL should contain {name}"))
}

#[test]
fn normalizer_is_idempotent_on_full_golden() {
    let golden = fs::read_to_string(FULL_GOLDEN).expect("full golden should be readable");
    let normalized = normalize_sdl(&golden).expect("full golden should normalize");
    assert_eq!(normalized, golden);
}

#[test]
fn g0_subset_is_zero_diff() {
    const NAMES: [&str; 12] = [
        "Hash20",
        "Hash32",
        "Date",
        "DateTime",
        "Duration",
        "Year",
        "Void",
        "ContentType",
        "FacetLogic",
        "ContentTypeFacetInput",
        "SizeRangeInput",
        "TorrentReprocessInput",
    ];

    let normalized = normalized_spike();
    let selected = normalized
        .trim_end()
        .split("\n\n")
        .filter(|block| {
            block
                .split_whitespace()
                .nth(1)
                .is_some_and(|name| NAMES.contains(&name))
        })
        .collect::<Vec<_>>();
    let actual = format!("{}\n", selected.join("\n\n"));
    let golden = fs::read_to_string(SUBSET_GOLDEN).expect("subset golden should be readable");
    assert_eq!(actual, golden);
}

#[test]
fn nullable_wrapper_is_sdl_agnostic() {
    let normalized = normalized_spike();
    let block = block_named(&normalized, "WrapperPinInput");
    let fields = block
        .lines()
        .skip(1)
        .take_while(|line| *line != "}")
        .collect::<Vec<_>>();

    assert_eq!(
        fields,
        [
            "  viaMaybeUndefined: Boolean",
            "  viaOption: Boolean",
            "  viaOptionOption: Boolean",
        ]
    );
    assert!(!block.contains('!'));
}

#[test]
fn nullable_list_required_elem_shape() {
    let normalized = normalized_spike();
    assert!(normalized.contains("hashes: [Hash20!]"));
}
