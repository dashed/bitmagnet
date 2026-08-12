use std::fs;
use std::process::Command;

#[test]
fn rust_report_matches_go_byte_for_byte() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-tape-rerun/example");
    let output_dir = std::env::temp_dir().join(format!(
        "bitmagnet-tape-rerun-cross-language-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&output_dir);
    fs::create_dir(&output_dir).expect("create output dir");
    let rust_report = output_dir.join("rust-report.json");

    let status = Command::new(env!("CARGO_BIN_EXE_bitmagnet-tape-rerun"))
        .arg("--tape-dir")
        .arg(fixture.join("tape"))
        .arg("--output")
        .arg(&rust_report)
        .status()
        .expect("run Rust tape rerun binary");
    assert!(status.success(), "Rust tape rerun command failed: {status}");

    let expected = fs::read(fixture.join("go-report.json")).expect("read Go report");
    let actual = fs::read(&rust_report).expect("read Rust report");
    assert_eq!(actual, expected, "Rust and Go rerun reports differ");

    let report: serde_json::Value = serde_json::from_slice(&expected).expect("decode Go report");
    let records = report["records"].as_array().expect("report records");
    assert_eq!(report["schema"], "bitmagnet.classifier-tape-rerun/v2");
    assert_eq!(
        report["acquisitionPlanDigest"],
        "sha256:c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe"
    );
    for workflow in [
        "tape_evidence_action_entries",
        "tape_evidence_unmatched",
        "tape_evidence_deleted",
    ] {
        assert!(
            records.iter().any(|record| record["workflow"] == workflow),
            "fixture must cover {workflow}"
        );
    }
    assert!(
        records.iter().any(|record| {
            record["actionEntries"].as_array().is_some_and(|entries| {
                entries
                    .iter()
                    .any(|entry| entry["name"] == "attach_local_content_by_id")
            })
        }),
        "fixture must cover the by-ID action path"
    );
    assert!(
        records.iter().any(|record| {
            record["writeSet"]["contents"]
                .as_array()
                .is_some_and(|contents| {
                    contents.iter().any(|content| {
                        content["identifiers"]["imdb"]
                            .as_str()
                            .is_some_and(|identifier| !identifier.is_empty())
                    })
                })
        }),
        "fixture must cover non-empty content identifiers"
    );
    assert!(
        records.iter().any(|record| {
            record["classification"]["baseTitle"] == "Cinderella"
                && record["classification"]["date"]["year"] == 1950
        }),
        "fixture must cover classifier-only title and date evidence"
    );
    for record in records
        .iter()
        .filter(|record| record["outcome"] == "deleted" || record["outcome"] == "unmatched")
    {
        assert_eq!(record["classification"]["contentType"], "");
        assert!(record["classification"]["baseTitle"].is_null());
        assert!(record["classification"]["date"].is_null());
        assert_eq!(
            record["classification"]["outcome"], record["outcome"],
            "terminal classifier normalization must agree with the tape outcome"
        );
        assert!(record["classification"]["error"].is_string());
    }

    fs::remove_dir_all(output_dir).expect("remove output dir");
}
