//! Public-API smoke tests for the Lane C classifier: source compilation plus a
//! couple of end-to-end classifications on the content-type-only path (which is
//! fully landed this milestone — no Lane-R dependency).

use bitmagnet_classifier::{Classifier, ClassifierInput};
use serde_json::json;

fn classify(input: serde_json::Value) -> serde_json::Value {
    let classifier = Classifier::from_core().expect("compile classifier.core.yml");
    let parsed: ClassifierInput = serde_json::from_value(input).expect("parse input");
    classifier.run("default", &Classifier::flags_off(), &parsed)
}

#[test]
fn compiles_core_source() {
    Classifier::from_core().expect("classifier.core.yml compiles");
}

#[test]
fn classifies_an_audiobook_by_extension_and_size() {
    // A single-file `.m4b` above the 50 MB audiobook threshold — the audiobook
    // extension branch (`classifier.core.yml`), no Lane R involved.
    let got = classify(json!({
        "id": "smoke-audiobook",
        "name": "Synthetic Story.m4b",
        "size": 60_000_000,
        "filesStatus": "single",
        "extension": "m4b",
    }));
    assert_eq!(got["contentType"], "audiobook");
    assert_eq!(got["outcome"], "classified");
    assert_eq!(got["baseTitle"], serde_json::Value::Null);
}

#[test]
fn deletes_a_banned_torrent_with_the_exact_error_string() {
    // The banned-keyword delete path — pins the compile-path `error` string that
    // the corpus golden freezes (contract §2.1).
    let got = classify(json!({
        "id": "smoke-banned",
        "name": "Synthetic paedo marker.txt",
        "size": 1000,
        "filesStatus": "single",
        "extension": "txt",
    }));
    assert_eq!(got["outcome"], "deleted");
    assert_eq!(got["contentType"], "");
    assert_eq!(
        got["error"],
        "runtime error at Path workflows.default.[0].if_else.if_action.delete: \
         workflow unmarshalError: delete_torrent"
    );
}

#[test]
fn boundary_below_audiobook_threshold_is_unclassified() {
    // Exactly at 50 MB the `> 50*mb` audiobook guard is false — content type
    // stays empty (matches corpus fixture `audiobook-0003-boundary-equal`).
    let got = classify(json!({
        "id": "smoke-boundary",
        "name": "Synthetic Boundary Equal.m4b",
        "size": 50_000_000,
        "filesStatus": "single",
        "extension": "m4b",
    }));
    assert_eq!(got["contentType"], "");
    assert_eq!(got["outcome"], "classified");
}
