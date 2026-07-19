//! Phase-3 Lane C: the 119,991-name real-name replay parity gate (contract
//! §2.6, decision #2). Runs the flags-off classifier over the production-sampled
//! replay inputs and compares to `oracle.golden.jsonl` (the pure Go classifier's
//! output). Gate: **≥0.999 agreement**.
//!
//! Heavy (120k fixtures) — `#[ignore]` by default; run explicitly with
//! `cargo test -p bitmagnet-diff --test classifier_replay --release -- --ignored --nocapture`.
//! `REPLAY_LIMIT=<n>` truncates to the first n fixtures for a quick estimate.

use std::collections::BTreeMap;
use std::time::Instant;

use bitmagnet_classifier::{Classifier, ClassifierInput};
use bitmagnet_diff::{canonical, fixture::load_file, Fixture};

const REPLAY_SUBSYSTEM: &str = "classifier-replay";
const GATE: f64 = 0.999;

fn golden_path() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/classifier-replay/oracle.golden.jsonl"
    )
    .to_string()
}

#[test]
#[ignore = "heavy 120k-fixture replay gate; run with --release --ignored --nocapture"]
fn classifier_replay_agreement() {
    let mut fixtures: Vec<Fixture> = load_file(golden_path()).expect("load replay golden");
    assert!(
        fixtures.iter().all(|f| f.subsystem == REPLAY_SUBSYSTEM),
        "unexpected subsystem in replay golden"
    );
    if let Ok(limit) = std::env::var("REPLAY_LIMIT") {
        let n: usize = limit.parse().expect("REPLAY_LIMIT must be a number");
        fixtures.truncate(n);
    }

    let classifier = Classifier::from_core().expect("compile classifier.core.yml");
    let flags = Classifier::flags_off();

    let started = Instant::now();
    let mut matched = 0usize;
    let mut field_tally: BTreeMap<String, usize> = BTreeMap::new();
    let mut samples: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for fixture in &fixtures {
        let parsed: ClassifierInput =
            serde_json::from_value(fixture.input.clone()).expect("parse replay input");
        let got = canonical(&classifier.run("default", &flags, &parsed));
        let want = canonical(&fixture.expected);
        if got == want {
            matched += 1;
            continue;
        }
        let (go, gw) = (got.as_object().unwrap(), want.as_object().unwrap());
        let differing: Vec<String> = gw
            .keys()
            .filter(|k| go.get(*k) != gw.get(*k))
            .cloned()
            .collect();
        let key = differing.join("+");
        *field_tally.entry(key.clone()).or_default() += 1;
        let bucket = samples.entry(key).or_default();
        if bucket.len() < 5 {
            bucket.push(parsed.name.clone());
        }
    }

    let total = fixtures.len();
    let rate = matched as f64 / total.max(1) as f64;
    eprintln!(
        "replay: total={total} matched={matched} misses={} rate={rate:.6} ({:.1}s)",
        total - matched,
        started.elapsed().as_secs_f64(),
    );
    let mut pairs: Vec<_> = field_tally.into_iter().collect();
    pairs.sort_by_key(|p| std::cmp::Reverse(p.1));
    for (fields, n) in &pairs {
        eprintln!("  {n:>6}  {fields}");
        if let Some(bucket) = samples.get(fields) {
            for name in bucket.iter().take(3) {
                eprintln!("           e.g. {name:?}");
            }
        }
    }

    assert!(
        rate >= GATE,
        "replay agreement {rate:.6} < {GATE} gate ({} misses of {total})",
        total - matched
    );
}
