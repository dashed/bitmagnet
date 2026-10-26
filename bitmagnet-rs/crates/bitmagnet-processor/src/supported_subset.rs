use bitmagnet_queue::ProcessTorrentParams;
use serde_json::Value;

const ATTACH_FLAGS: [&str; 3] = ["local_search_enabled", "apis_enabled", "tmdb_enabled"];

/// Go's `MessageParams` keys, spelled as its encoder writes them.
const PAYLOAD_KEYS: [&str; 4] = [
    "ClassifyMode",
    "ClassifierWorkflow",
    "ClassifierFlags",
    "InfoHashes",
];

/// Why a stored `process_torrent` payload is outside the keys Go's producers
/// write. It names the key, never a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PayloadKeySetError {
    #[error("not a JSON object")]
    NotAnObject,
    #[error("unexpected key {0:?}")]
    UnexpectedKey(String),
    #[error("ClassifierFlags is not an object")]
    FlagsNotAnObject,
    #[error("classifier flag {0:?} is not an attach flag")]
    UnexpectedFlag(String),
    #[error("attach flag {0:?} is not a boolean")]
    NonBooleanFlag(String),
}

/// Check a payload's keys before serde decodes it: only Go's four
/// `MessageParams` keys, spelled exactly, and in `ClassifierFlags`, if present,
/// only the three attach flags, each a JSON boolean.
///
/// serde drops a key [`ProcessTorrentParams`] does not declare. Go's
/// `encoding/json` binds a case-variant one (`classifierworkflow`) to the
/// field, the last in the stored order winning, and its flag map keeps any
/// name, so Go could run a job Rust never saw. Such a payload is left to Go.
/// An exact duplicate key cannot get here: `jsonb` keeps only the last.
///
/// # Errors
///
/// The first key, in sorted order, outside that set, or the value that is not
/// an object.
pub fn check_payload_key_set(payload: &str) -> Result<(), PayloadKeySetError> {
    let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(payload) else {
        return Err(PayloadKeySetError::NotAnObject);
    };
    if let Some(key) = fields
        .keys()
        .find(|key| !PAYLOAD_KEYS.contains(&key.as_str()))
    {
        return Err(PayloadKeySetError::UnexpectedKey(key.clone()));
    }
    let Some(flags) = fields.get("ClassifierFlags") else {
        return Ok(());
    };
    let Value::Object(flags) = flags else {
        return Err(PayloadKeySetError::FlagsNotAnObject);
    };
    for (name, value) in flags {
        if !ATTACH_FLAGS.contains(&name.as_str()) {
            return Err(PayloadKeySetError::UnexpectedFlag(name.clone()));
        }
        if !value.is_boolean() {
            return Err(PayloadKeySetError::NonBooleanFlag(name.clone()));
        }
    }
    Ok(())
}

pub(crate) fn has_explicit_default_workflow(params: &ProcessTorrentParams) -> bool {
    params.classifier_workflow == "default"
}

pub(crate) fn has_explicit_attach_flags_off(params: &ProcessTorrentParams) -> bool {
    has_explicit_attach_flags(params, false)
}

/// All three attach flags explicitly JSON `true`: the flags-on subset, which
/// needs a live content resolver.
pub(crate) fn has_explicit_attach_flags_on(params: &ProcessTorrentParams) -> bool {
    has_explicit_attach_flags(params, true)
}

/// A workflow Go resolves to `default`: absent (decoded as empty) or explicit.
pub(crate) fn has_organic_workflow(params: &ProcessTorrentParams) -> bool {
    params.classifier_workflow.is_empty() || has_explicit_default_workflow(params)
}

/// No flag but the three attach flags, each a JSON boolean. An absent flag
/// takes the core default, `true`.
pub(crate) fn has_only_boolean_attach_flags(params: &ProcessTorrentParams) -> bool {
    params.classifier_flags.as_ref().is_none_or(|flags| {
        flags
            .iter()
            .all(|(name, value)| ATTACH_FLAGS.contains(&name.as_str()) && value.is_boolean())
    })
}

fn has_explicit_attach_flags(params: &ProcessTorrentParams, value: bool) -> bool {
    let Some(flags) = params.classifier_flags.as_ref() else {
        return false;
    };
    ATTACH_FLAGS
        .iter()
        .all(|name| flags.get(*name).and_then(serde_json::Value::as_bool) == Some(value))
}

#[cfg(test)]
mod tests {
    use super::{check_payload_key_set, PayloadKeySetError};

    const HASH: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
    const OFF: &str = r#"{"apis_enabled":false,"local_search_enabled":false,"tmdb_enabled":false}"#;

    #[test]
    fn every_producer_shape_is_inside_the_key_set() {
        for payload in [
            // DHT, importer and retry jobs.
            format!(r#"{{"InfoHashes":["{HASH}"]}}"#),
            format!(r#"{{"ClassifyMode":1,"InfoHashes":["{HASH}"]}}"#),
            format!(r#"{{"InfoHashes":["{HASH}"],"ClassifierFlags":{{}}}}"#),
            // Batch and CLI jobs, and the explicit subsets.
            format!(
                r#"{{"ClassifyMode":1,"ClassifierWorkflow":"default","ClassifierFlags":{{"tmdb_enabled":false}},"InfoHashes":["{HASH}"]}}"#
            ),
            format!(
                r#"{{"ClassifierWorkflow":"default","ClassifierFlags":{OFF},"InfoHashes":["{HASH}"]}}"#
            ),
            format!(
                r#"{{"ClassifierWorkflow":"default","ClassifierFlags":{},"InfoHashes":["{HASH}"]}}"#,
                OFF.replace("false", "true")
            ),
            // Values are serde's and the validator's to judge.
            r#"{"ClassifierWorkflow":"custom","InfoHashes":null}"#.to_owned(),
            "{}".to_owned(),
        ] {
            assert_eq!(check_payload_key_set(&payload), Ok(()), "{payload}");
        }
    }

    #[test]
    fn the_first_key_outside_the_set_is_named() {
        use PayloadKeySetError::{
            FlagsNotAnObject, NonBooleanFlag, NotAnObject, UnexpectedFlag, UnexpectedKey,
        };
        let flags = |flags: &str| {
            format!(
                r#"{{"ClassifierWorkflow":"default","ClassifierFlags":{flags},"InfoHashes":["{HASH}"]}}"#
            )
        };
        let key = |name: &str| UnexpectedKey(name.to_owned());
        let flag = |name: &str| UnexpectedFlag(name.to_owned());
        for (payload, error) in [
            // Go reads "default", then "custom", and runs `custom`.
            (
                format!(
                    r#"{{"ClassifierWorkflow":"default","ClassifierFlags":{OFF},"InfoHashes":["{HASH}"],"classifierworkflow":"custom"}}"#
                ),
                key("classifierworkflow"),
            ),
            (
                format!(r#"{{"InfoHashes":["{HASH}"],"Extra":true}}"#),
                key("Extra"),
            ),
            (format!(r#"{{"infohashes":["{HASH}"]}}"#), key("infohashes")),
            (format!(r#"{{"InfoHashes":["{HASH}"],"":1}}"#), key("")),
            (
                format!(r#"{{"InfoHashes":["{HASH}"],"delete_xxx":true}}"#),
                key("delete_xxx"),
            ),
            (format!(r#"[{{"InfoHashes":["{HASH}"]}}]"#), NotAnObject),
            (format!(r#""{HASH}""#), NotAnObject),
            ("null".to_owned(), NotAnObject),
            ("{".to_owned(), NotAnObject),
            (flags("null"), FlagsNotAnObject),
            (flags("[]"), FlagsNotAnObject),
            (flags(r#""tmdb_enabled""#), FlagsNotAnObject),
            (
                flags(&OFF.replace('}', r#","delete_xxx":true}"#)),
                flag("delete_xxx"),
            ),
            (
                flags(&OFF.replace('}', r#","delete_xxx":false}"#)),
                flag("delete_xxx"),
            ),
            (
                flags(r#"{"delete_content_types":["movie"]}"#),
                flag("delete_content_types"),
            ),
            (flags(r#"{"TMDB_enabled":true}"#), flag("TMDB_enabled")),
            (flags(r#"{"":true}"#), flag("")),
            (
                flags(r#"{"tmdb_enabled":0}"#),
                NonBooleanFlag("tmdb_enabled".to_owned()),
            ),
            (
                flags(r#"{"tmdb_enabled":"true"}"#),
                NonBooleanFlag("tmdb_enabled".to_owned()),
            ),
            (
                flags(r#"{"apis_enabled":null}"#),
                NonBooleanFlag("apis_enabled".to_owned()),
            ),
        ] {
            assert_eq!(check_payload_key_set(&payload), Err(error), "{payload}");
        }
    }

    #[test]
    fn a_refusal_names_the_key_and_no_value() {
        for (payload, text) in [
            (
                r#"{"InfoHashes":["v4lue"],"Extra":"v4lue"}"#,
                r#"unexpected key "Extra""#,
            ),
            (
                r#"{"InfoHashes":["v4lue"],"ClassifierFlags":"v4lue"}"#,
                "ClassifierFlags is not an object",
            ),
            (
                r#"{"InfoHashes":["v4lue"],"ClassifierFlags":{"other_flag":"v4lue"}}"#,
                r#"classifier flag "other_flag" is not an attach flag"#,
            ),
            (
                r#"{"InfoHashes":["v4lue"],"ClassifierFlags":{"tmdb_enabled":"v4lue"}}"#,
                r#"attach flag "tmdb_enabled" is not a boolean"#,
            ),
        ] {
            let error = check_payload_key_set(payload).expect_err(payload);
            assert_eq!(error.to_string(), text);
            assert!(!text.contains("v4lue"), "{text}");
        }
    }
}
