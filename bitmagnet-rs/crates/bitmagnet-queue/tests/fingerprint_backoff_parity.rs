//! Byte-identity parity against the frozen Go goldens
//! (`testdata/parity/queue/{fingerprints,backoff}.jsonl`, contract §1).
//!
//! Fingerprint: build the typed payload from each fixture's loose `input`,
//! serialize, and assert the exact `payload` bytes + `fingerprint` + the
//! constructor-injected `maxRetries` / `priority` / `archivalDurationNs`.
//!
//! Backoff: assert the deterministic base + jitter bounds for each fixture, and
//! exhaustively prove every valid jitter draw stays inside the envelope.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};
use bitmagnet_queue::backoff::{backoff_seconds_with_jitter, envelope, JITTER_MODULUS};
use bitmagnet_queue::message::{
    blob_migration_job, process_torrent_batch_job, process_torrent_job, BlobMigrationParams,
    ProcessTorrentBatchParams, ProcessTorrentParams,
};
use bitmagnet_queue::{ProtocolId, QueueJob, QueueJobOptions};
use serde_json::Value;

fn fixtures(name: &str) -> Vec<Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/queue")
        .join(name);
    let file = File::open(&path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    BufReader::new(file)
        .lines()
        .map(|l| l.expect("read line"))
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(&l).expect("parse fixture line"))
        .collect()
}

fn flags(input: &Value) -> Option<BTreeMap<String, Value>> {
    match input.get("classifierFlags") {
        Some(Value::Object(map)) => Some(
            map.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        ),
        _ => None,
    }
}

fn str_field(input: &Value, key: &str) -> String {
    input
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn i32_field(input: &Value, key: &str) -> i32 {
    input.get(key).and_then(Value::as_i64).unwrap_or(0) as i32
}

fn build_job(input: &Value) -> Result<QueueJob> {
    let job_type = input
        .get("jobType")
        .and_then(Value::as_str)
        .context("fixture missing jobType")?;

    match job_type {
        "process_torrent" => {
            let info_hashes = input
                .get("infoHashes")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|v| ProtocolId::from_hex(v.as_str().unwrap_or_default()))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()
                .context("parse infoHashes")?
                .unwrap_or_default();
            let params = ProcessTorrentParams {
                classify_mode: i32_field(input, "classifyMode"),
                classifier_workflow: str_field(input, "classifierWorkflow"),
                classifier_flags: flags(input),
                info_hashes,
            };
            let opts = QueueJobOptions::default().with_priority(i32_field(input, "priority"));
            Ok(process_torrent_job(&params, opts)?)
        }
        "process_torrent_batch" => {
            let info_hash_greater_than =
                match input.get("infoHashGreaterThan").and_then(Value::as_str) {
                    Some(hex) => ProtocolId::from_hex(hex).context("parse infoHashGreaterThan")?,
                    None => ProtocolId::zero(),
                };
            let content_types = input
                .get("contentTypes")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            let params = ProcessTorrentBatchParams {
                info_hash_greater_than,
                classify_mode: i32_field(input, "classifyMode"),
                classifier_workflow: str_field(input, "classifierWorkflow"),
                classifier_flags: flags(input),
                chunk_size: input.get("chunkSize").and_then(Value::as_u64).unwrap_or(0),
                batch_size: input.get("batchSize").and_then(Value::as_u64).unwrap_or(0),
                content_types,
                orphans: input
                    .get("orphans")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                ..ProcessTorrentBatchParams::default()
            };
            let opts = QueueJobOptions::default().with_priority(i32_field(input, "priority"));
            Ok(process_torrent_batch_job(&params, opts)?)
        }
        "blob_migration" => {
            let params = BlobMigrationParams {
                info_hash_greater_than: str_field(input, "infoHashGreaterThan"),
                info_hash_less_or_equal: str_field(input, "infoHashLessOrEqual"),
                range_id: input.get("rangeId").and_then(Value::as_i64).unwrap_or(0),
                num_ranges: input.get("numRanges").and_then(Value::as_i64).unwrap_or(0),
                chunk_size: input.get("chunkSize").and_then(Value::as_i64).unwrap_or(0),
            };
            let opts = QueueJobOptions::default().with_priority(i32_field(input, "priority"));
            Ok(blob_migration_job(&params, opts)?)
        }
        other => bail!("unknown jobType {other}"),
    }
}

#[test]
fn fingerprint_parity() {
    let fixtures = fixtures("fingerprints.jsonl");
    assert_eq!(fixtures.len(), 8, "expected 8 fingerprint fixtures");

    for fixture in &fixtures {
        let id = fixture.get("id").and_then(Value::as_str).unwrap_or("?");
        let input = fixture.get("input").expect("fixture input");
        let expected = fixture.get("expected").expect("fixture expected");

        let job = build_job(input).unwrap_or_else(|e| panic!("[{id}] build job: {e:#}"));

        let want_queue = expected["queue"].as_str().unwrap();
        let want_payload = expected["payload"].as_str().unwrap();
        let want_fingerprint = expected["fingerprint"].as_str().unwrap();
        let want_max_retries = expected["maxRetries"].as_u64().unwrap();
        let want_priority = expected["priority"].as_i64().unwrap();
        let want_archival = expected["archivalDurationNs"].as_u64().unwrap();

        assert_eq!(job.queue, want_queue, "[{id}] queue");
        assert_eq!(job.payload, want_payload, "[{id}] payload bytes");
        assert_eq!(job.fingerprint, want_fingerprint, "[{id}] fingerprint");
        assert_eq!(
            u64::from(job.max_retries),
            want_max_retries,
            "[{id}] maxRetries"
        );
        assert_eq!(i64::from(job.priority), want_priority, "[{id}] priority");
        assert_eq!(
            job.archival_duration.as_nanos(),
            u128::from(want_archival),
            "[{id}] archivalDurationNs"
        );

        // Independent cross-check: the fingerprint really is sha256(queue||payload).
        assert_eq!(
            job.fingerprint,
            bitmagnet_queue::fingerprint(want_queue, want_payload),
            "[{id}] fingerprint recompute"
        );
    }
}

#[test]
fn backoff_parity() {
    let fixtures = fixtures("backoff.jsonl");
    assert_eq!(fixtures.len(), 6, "expected 6 backoff fixtures");

    for fixture in &fixtures {
        let id = fixture.get("id").and_then(Value::as_str).unwrap_or("?");
        let retries = fixture["input"]["retries"].as_u64().unwrap() as u32;
        let expected = &fixture["expected"];

        let env = envelope(retries);
        assert_eq!(
            env.deterministic_seconds,
            expected["deterministicSeconds"].as_u64().unwrap(),
            "[{id}] deterministicSeconds"
        );
        assert_eq!(
            env.jitter_min_seconds,
            expected["jitterMinSeconds"].as_u64().unwrap(),
            "[{id}] jitterMinSeconds"
        );
        assert_eq!(
            env.jitter_max_seconds,
            expected["jitterMaxSeconds"].as_u64().unwrap(),
            "[{id}] jitterMaxSeconds"
        );

        // Exhaustively prove every valid jitter draw lands inside the envelope
        // (a stronger statement than sampling the nondeterministic runtime fn).
        for jitter in 0..JITTER_MODULUS {
            let secs = backoff_seconds_with_jitter(retries, jitter);
            assert!(
                secs >= env.min_seconds() && secs <= env.max_seconds(),
                "[{id}] jitter {jitter}: {secs} outside [{}, {}]",
                env.min_seconds(),
                env.max_seconds()
            );
        }
    }
}
