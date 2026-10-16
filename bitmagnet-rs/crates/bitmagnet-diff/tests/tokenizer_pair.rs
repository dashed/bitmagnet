use anyhow::Result;
use bitmagnet_diff::{
    driver::Driver,
    fixture::load_file,
    runner::{run, Options},
};
use bitmagnet_search::tokenizer::tokenize_flat;
use serde_json::{json, Value};

struct TokenizerDriver;

impl Driver for TokenizerDriver {
    fn subsystem(&self) -> &str {
        "tokenizer"
    }

    fn run(&self, input: &Value) -> Result<Value> {
        let text = input.get("text").and_then(Value::as_str).unwrap_or("");
        let tokens = tokenize_flat(text);
        Ok(json!({ "tokens": tokens }))
    }
}

#[test]
fn tokenizer_parity_via_harness() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/tokenizer/corpus.jsonl"
    );
    let fixtures = load_file(path).expect("load corpus");
    let report = run(&fixtures, &TokenizerDriver, Options::default());

    assert!(
        report.ran > 1000,
        "expected large corpus, ran {}",
        report.ran
    );
    assert!(report.ok(), "tokenizer parity diverged:\n{report}");
}
