//! Same-input, same-observation Rust half of the classifier tape rerun gate.
//!
//! The Go command `bitmagnet classifier tape-rerun` emits the same report
//! schema. A byte-for-byte comparison therefore proves both implementations
//! consumed the exact recorded dependency session, entered the same ordered
//! attach actions, reached the same deterministic terminal outcome, and
//! materialized the same classification-derived processor write set.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bitmagnet_classifier::resolver::tape::TapeContentResolver;
use bitmagnet_classifier::{
    core_config_digest, Classifier, ClassifierInput, ContentType, FlagValue, Flags, Outcome,
};
use bitmagnet_processor::{LoadedTorrent, Materializer, WriteSet};
use bitmagnet_tape::{marshal, ActionEntry, Record, Replay};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};

const REPORT_SCHEMA: &str = "bitmagnet.classifier-tape-rerun/v1";

#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-tape-rerun",
    about = "Replay a traced Go classifier tape through Rust and emit canonical processor write sets"
)]
struct Args {
    /// Directory holding manifest.json and tape.jsonl.
    #[arg(long)]
    tape_dir: PathBuf,

    /// Create-only JSON report path.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    schema: &'static str,
    effective_config_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    acquisition_plan_digest: Option<String>,
    record_count: usize,
    records: Vec<RerunRecord>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RerunRecord {
    subject: String,
    attempt: i64,
    workflow: String,
    input_sha256: String,
    processor_state: bitmagnet_tape::ProcessorState,
    observation_count: usize,
    action_entries: Vec<ActionEntry>,
    outcome: &'static str,
    write_set: WriteSet,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let digest = core_config_digest().context("compute embedded classifier config digest")?;
    let replay = Replay::load(&args.tape_dir, &digest).context("load classifier tape")?;
    let manifest = replay.manifest();

    if manifest.action_entry_count.is_none() {
        bail!("classifier tape has no action-entry trace");
    }
    if manifest.incomplete_record_count != 0 {
        bail!(
            "classifier tape has {} incomplete records; final rerun evidence must be quiescent",
            manifest.incomplete_record_count
        );
    }
    if manifest.authoritative_record_count != manifest.record_count {
        bail!(
            "classifier tape has {} authoritative records out of {}",
            manifest.authoritative_record_count,
            manifest.record_count
        );
    }

    let mut records: Vec<_> = replay.subjects().collect();
    records.sort_by(|a, b| (&a.subject, a.attempt).cmp(&(&b.subject, b.attempt)));
    if records.len() != manifest.record_count {
        bail!(
            "classifier tape exposes {} replayable records, manifest declares {}",
            records.len(),
            manifest.record_count
        );
    }

    let materializer = Materializer::from_core().context("compile processor materializer")?;
    let mut results = Vec::with_capacity(records.len());
    for record in records {
        results.push(rerun_record(&replay, &materializer, record)?);
    }

    let report = Report {
        schema: REPORT_SCHEMA,
        effective_config_digest: manifest.effective_config_digest.clone(),
        acquisition_plan_digest: manifest.acquisition_plan_digest.clone(),
        record_count: results.len(),
        records: results,
    };
    write_create_only_json(&args.output, &report)
}

fn rerun_record(
    replay: &Replay,
    materializer: &Materializer,
    record: &Record,
) -> Result<RerunRecord> {
    if !record.authoritative() {
        bail!(
            "classifier tape record {}#{} is not authoritative",
            record.subject,
            record.attempt
        );
    }
    let input_raw = record
        .input
        .as_ref()
        .context("classifier tape record has no embedded input")?
        .get();
    let input: ClassifierInput = serde_json::from_str(input_raw).with_context(|| {
        format!(
            "decode classifier input for {}#{}",
            record.subject, record.attempt
        )
    })?;
    if input.id != record.subject {
        bail!(
            "classifier tape record subject {:?} does not match input id {:?}",
            record.subject,
            input.id
        );
    }
    let processor_state = record.processor_state.as_ref().with_context(|| {
        format!(
            "classifier tape record {}#{} has no processorState",
            record.subject, record.attempt
        )
    })?;
    let recorded_actions = record.action_entries.clone().unwrap_or_default();
    let flags = flags_from_record(record)?;
    let resolver = Arc::new(TapeContentResolver::new(
        replay,
        &record.subject,
        record.attempt,
    ));
    let classifier = Classifier::from_core_with_tape_evidence(Arc::clone(&resolver) as Arc<_>)
        .context("compile classifier with tape resolver")?;
    let (classification, outcome, actual_actions) = futures::executor::block_on(
        classifier.classify_with_action_entries(&record.workflow, &flags, &input),
    );

    if actual_actions != recorded_actions {
        bail!(
            "classifier tape record {}#{} action-entry mismatch: recorded {:?}, Rust entered {:?}",
            record.subject,
            record.attempt,
            action_names(&recorded_actions),
            action_names(&actual_actions)
        );
    }
    if resolver.remaining() != 0 {
        bail!(
            "classifier tape record {}#{} left {} observations unconsumed",
            record.subject,
            record.attempt,
            resolver.remaining()
        );
    }

    let actual_outcome = tape_outcome(&outcome);
    let recorded_outcome = record
        .outcome
        .as_ref()
        .expect("authoritative record has an outcome")
        .kind
        .as_str();
    if actual_outcome != recorded_outcome {
        bail!(
            "classifier tape record {}#{} outcome mismatch: recorded {}, Rust produced {actual_outcome} ({outcome:?})",
            record.subject,
            record.attempt,
            recorded_outcome
        );
    }

    let write_set = materializer
        .materialize_replayed(
            LoadedTorrent {
                info_hash: record.subject.clone(),
                classifier_input: input,
                existing_content_ids: processor_state.existing_content_ids.clone(),
                attach_hint_unsupported: false,
            },
            classification,
            outcome,
        )
        .with_context(|| {
            format!(
                "materialize classifier tape record {}#{}",
                record.subject, record.attempt
            )
        })?;

    Ok(RerunRecord {
        subject: record.subject.clone(),
        attempt: record.attempt,
        workflow: record.workflow.clone(),
        input_sha256: format!("sha256:{:x}", Sha256::digest(input_raw.as_bytes())),
        processor_state: processor_state.clone(),
        observation_count: record.observations.len(),
        action_entries: actual_actions,
        outcome: actual_outcome,
        write_set,
    })
}

fn action_names(entries: &[ActionEntry]) -> Vec<&str> {
    entries.iter().map(|entry| entry.name.as_str()).collect()
}

fn tape_outcome(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Classified => "completed",
        Outcome::Deleted(_) => "deleted",
        Outcome::Unmatched(_) => "unmatched",
        Outcome::Error(_) => "error",
    }
}

fn flags_from_record(record: &Record) -> Result<Flags> {
    let mut flags = Flags::new();
    for (name, value) in &record.flags {
        let parsed = if let Some(value) = value.as_bool() {
            FlagValue::Bool(value)
        } else if let Some(values) = value.as_array() {
            let content_types = values
                .iter()
                .map(|value| {
                    let raw = value.as_str().with_context(|| {
                        format!("classifier flag {name:?} contains a non-string value")
                    })?;
                    ContentType::parse(raw).with_context(|| {
                        format!("classifier flag {name:?} has unknown content type {raw:?}")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            FlagValue::ContentTypeList(content_types)
        } else {
            bail!(
                "classifier flag {:?} has unsupported recorded value {}",
                name,
                value
            );
        };
        flags.insert(name.clone(), parsed);
    }
    Ok(flags)
}

fn write_create_only_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("output path has no UTF-8 file name")?;
    let encoded = marshal(value).context("encode rerun report")?;

    let mut temp_path = None;
    let mut temp = None;
    for sequence in 0..1000_u16 {
        let candidate = parent.join(format!(
            ".{file_name}.tmp-{}-{sequence}",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                temp_path = Some(candidate);
                temp = Some(file);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("create temporary rerun report"),
        }
    }
    let temp_path = temp_path.context("could not allocate temporary rerun report name")?;
    let mut temp = temp.expect("temporary path and file are set together");
    let result = (|| -> Result<()> {
        temp.write_all(encoded.as_bytes())
            .context("write rerun report")?;
        temp.write_all(b"\n").context("terminate rerun report")?;
        temp.sync_all().context("sync rerun report")?;
        drop(temp);
        fs::hard_link(&temp_path, path).with_context(|| {
            format!(
                "publish create-only rerun report {}",
                path.to_string_lossy()
            )
        })?;
        Ok(())
    })();
    let _ = fs::remove_file(&temp_path);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn create_only_report_does_not_replace_evidence() {
        let dir =
            std::env::temp_dir().join(format!("bitmagnet-tape-rerun-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir(&dir).expect("create test dir");
        let path = dir.join("report.json");

        write_create_only_json(&path, &BTreeMap::from([("value", "<exact>")]))
            .expect("write first report");
        assert!(
            write_create_only_json(&path, &BTreeMap::from([("value", "replacement")])).is_err()
        );
        assert_eq!(
            fs::read_to_string(&path).expect("read report"),
            "{\"value\":\"<exact>\"}\n"
        );

        fs::remove_dir_all(dir).expect("remove test dir");
    }

    #[test]
    fn report_escapes_unicode_line_separators_exactly_like_go() {
        let dir = std::env::temp_dir().join(format!(
            "bitmagnet-tape-rerun-unicode-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir(&dir).expect("create test dir");
        let path = dir.join("report.json");

        write_create_only_json(
            &path,
            &BTreeMap::from([("value", "a\u{2028}b\u{2029}c<&>")]),
        )
        .expect("write report");
        assert_eq!(
            fs::read_to_string(&path).expect("read report"),
            "{\"value\":\"a\\u2028b\\u2029c<&>\"}\n"
        );

        fs::remove_dir_all(dir).expect("remove test dir");
    }
}
