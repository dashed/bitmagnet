//! Action-entry tracing is intentionally above the dependency seam: an attach
//! action that returns before making I/O still has to count as entered.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bitmagnet_classifier::tape_corpus::{self, Verdict};
use bitmagnet_classifier::{
    Classifier, ClassifierInput, Flags, NullContentResolver, Outcome, Source,
    TAPE_EVIDENCE_ACTION_ENTRIES_WORKFLOW, TAPE_EVIDENCE_DELETED_WORKFLOW,
    TAPE_EVIDENCE_UNMATCHED_WORKFLOW,
};
use bitmagnet_tape::{Record, Replay};

const DIGEST: &str = "sha256:test-action-entries";

fn early_return_classifier() -> Classifier {
    let source = Source::parse(
        r#"
workflows:
  default:
    - add_tag:
        - z
        - a
        - z
    - find_match:
        - attach_local_content_by_id
        - attach_local_content_by_search
        - attach_tmdb_content_by_id
        - attach_tmdb_content_by_search
flag_definitions: {}
flags: {}
keywords: {}
extensions: {}
"#,
    )
    .expect("source parses");

    Classifier::compile(source, Arc::new(NullContentResolver)).expect("source compiles")
}

fn empty_input() -> ClassifierInput {
    serde_json::from_value(serde_json::json!({
        "id": "0000000000000000000000000000000000000001",
        "name": "unclassified",
        "size": 1,
        "filesStatus": "no_info"
    }))
    .expect("input decodes")
}

#[test]
fn tape_evidence_workflows_are_replay_only_and_exact() {
    let input = empty_input();
    let serving = Classifier::from_core().expect("core compiles");
    let (_, serving_outcome) = futures::executor::block_on(serving.classify(
        TAPE_EVIDENCE_DELETED_WORKFLOW,
        &Flags::new(),
        &input,
    ));
    assert!(matches!(serving_outcome, Outcome::Error(_)));

    let replay = Classifier::from_core_with_tape_evidence(Arc::new(NullContentResolver))
        .expect("evidence source compiles");
    let (_, outcome, actions) = futures::executor::block_on(replay.classify_with_action_entries(
        TAPE_EVIDENCE_ACTION_ENTRIES_WORKFLOW,
        &Flags::new(),
        &input,
    ));
    assert!(matches!(outcome, Outcome::Classified));
    assert_eq!(
        actions
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        [
            "attach_local_content_by_id",
            "attach_tmdb_content_by_id",
            "attach_local_content_by_search",
            "attach_tmdb_content_by_search",
        ]
    );

    let (_, unmatched) = futures::executor::block_on(replay.classify(
        TAPE_EVIDENCE_UNMATCHED_WORKFLOW,
        &Flags::new(),
        &input,
    ));
    assert!(matches!(unmatched, Outcome::Unmatched(_)));
    let (_, deleted) = futures::executor::block_on(replay.classify(
        TAPE_EVIDENCE_DELETED_WORKFLOW,
        &Flags::new(),
        &input,
    ));
    assert!(matches!(deleted, Outcome::Deleted(_)));
}

#[test]
fn attach_actions_are_traced_before_their_early_returns() {
    let classifier = early_return_classifier();
    // No hint, content type or base title: every action returns unmatched from
    // its own guard before consulting the resolver. The trace must still see
    // every attempted branch in exact order.
    let input: ClassifierInput = serde_json::from_value(serde_json::json!({
        "id": "early-return",
        "name": "unclassified",
        "size": 1,
        "filesStatus": "no_info"
    }))
    .expect("input decodes");

    let (result, outcome, entries) = futures::executor::block_on(
        classifier.classify_with_action_entries("default", &Flags::new(), &input),
    );

    assert!(matches!(outcome, Outcome::Classified));
    assert_eq!(result.tags.into_iter().collect::<Vec<_>>(), ["a", "z"]);
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        [
            "attach_local_content_by_id",
            "attach_local_content_by_search",
            "attach_tmdb_content_by_id",
            "attach_tmdb_content_by_search",
        ]
    );
}

struct TempTape(std::path::PathBuf);

impl TempTape {
    fn new(action_names: &[&str]) -> Self {
        Self::new_with_outcome(action_names, "completed")
    }

    fn new_with_outcome(action_names: &[&str], outcome: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "bitmagnet-action-tape-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create temporary tape dir");

        let action_entries: Vec<_> = action_names
            .iter()
            .map(|name| serde_json::json!({ "name": name }))
            .collect();
        let mut action_counts = BTreeMap::new();
        for name in action_names {
            *action_counts.entry(*name).or_insert(0usize) += 1;
        }

        let record = serde_json::json!({
            "subject": "s",
            "attempt": 0,
            "workflow": "default",
            "flags": {},
            "input": {
                "id": "s",
                "name": "unclassified",
                "size": 1,
                "filesStatus": "no_info"
            },
            "actionEntries": action_entries,
            "observations": [],
            "outcome": { "kind": outcome }
        });
        let manifest = serde_json::json!({
            "schema": bitmagnet_tape::SCHEMA,
            "effectiveConfigDigest": DIGEST,
            "generatedAt": "now",
            "recorder": "test",
            "recordCount": 1,
            "observationCount": 0,
            "incompleteRecordCount": 0,
            "authoritativeRecordCount": 1,
            "recordOutcomeCounts": { (outcome): 1 },
            "actionEntryCount": action_names.len(),
            "actionEntryCounts": action_counts,
            "truncated": false
        });

        std::fs::write(
            path.join(bitmagnet_tape::TAPE_FILE_NAME),
            format!("{record}\n"),
        )
        .expect("write tape");
        std::fs::write(
            path.join(bitmagnet_tape::MANIFEST_FILE_NAME),
            format!("{manifest}\n"),
        )
        .expect("write manifest");

        Self(path)
    }
}

impl Drop for TempTape {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn run_tape(action_names: &[&str]) -> tape_corpus::CorpusReport {
    let dir = TempTape::new(action_names);
    run_temp_tape(dir).await
}

async fn run_temp_tape(dir: TempTape) -> tape_corpus::CorpusReport {
    let replay = Replay::load(&dir.0, DIGEST).expect("synthetic tape loads");

    tape_corpus::run(
        &replay,
        |_| Ok(early_return_classifier()),
        |record: &Record| {
            record
                .input
                .as_ref()
                .map(|raw| serde_json::from_str(raw.get()).expect("input decodes"))
        },
        4,
    )
    .await
    .expect("gate runs")
}

#[tokio::test]
async fn production_gate_compares_the_exact_recorded_sequence() {
    let exact = [
        "attach_local_content_by_id",
        "attach_local_content_by_search",
        "attach_tmdb_content_by_id",
        "attach_tmdb_content_by_search",
    ];
    let report = run_tape(&exact).await;
    assert_eq!(report.matched, 1);

    let reordered = [
        "attach_local_content_by_search",
        "attach_local_content_by_id",
        "attach_tmdb_content_by_id",
        "attach_tmdb_content_by_search",
    ];
    let report = run_tape(&reordered).await;
    assert_eq!(report.desynced, 1);
    assert!(matches!(
        report.failures[0].verdict,
        Verdict::Desync { ref detail } if detail.contains("action-entry desync at sequence 0")
    ));
}

#[tokio::test]
async fn production_gate_compares_the_terminal_outcome() {
    let actions = [
        "attach_local_content_by_id",
        "attach_local_content_by_search",
        "attach_tmdb_content_by_id",
        "attach_tmdb_content_by_search",
    ];
    let report = run_temp_tape(TempTape::new_with_outcome(&actions, "deleted")).await;
    assert_eq!(report.desynced, 1);
    assert!(matches!(
        report.failures[0].verdict,
        Verdict::Desync { ref detail } if detail.contains("terminal outcome mismatch")
    ));
}
