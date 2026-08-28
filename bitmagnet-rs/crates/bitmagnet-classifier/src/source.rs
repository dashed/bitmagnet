//! The classifier `Source` (`source.go`) + the YAML loader for
//! `classifier.core.yml`. This milestone loads the embedded core source only;
//! the XDG/CWD/config merge layers (`source_provider.go`) land incrementally.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Number as JsonNumber;
use serde_yaml::Value as Yaml;
use sha2::{Digest, Sha256};

use crate::model::ContentType;

/// The embedded core classifier source — the byte-for-byte public contract
/// (`classifier.core.yml`, kept in sync with the Go embed).
pub(crate) const CORE_YAML: &str =
    include_str!("../../../../internal/classifier/classifier.core.yml");

const EFFECTIVE_CONFIG_DIGEST_VERSION: u8 = 1;
const CORE_DEFAULT_WORKFLOW: &str = "default";

/// Reserved workflows emitted only by the reviewed classifier-tape acquisition
/// executor. They are deliberately absent from [`Source::load_core`], so a
/// serving caller can never select the delete/unmatched evidence paths.
pub const TAPE_EVIDENCE_ACTION_ENTRIES_WORKFLOW: &str = "tape_evidence_action_entries";
pub const TAPE_EVIDENCE_UNMATCHED_WORKFLOW: &str = "tape_evidence_unmatched";
pub const TAPE_EVIDENCE_DELETED_WORKFLOW: &str = "tape_evidence_deleted";

const TAPE_EVIDENCE_WORKFLOWS_YAML: &str = r#"
workflows:
  tape_evidence_action_entries:
    - find_match: [attach_local_content_by_id]
    - find_match: [attach_tmdb_content_by_id]
    - find_match: [attach_local_content_by_search]
    - find_match: [attach_tmdb_content_by_search]
  tape_evidence_unmatched: unmatched
  tape_evidence_deleted: delete
"#;

/// A flag's declared type (`FlagType`). Only the two shapes `classifier.core.yml`
/// declares are modelled; string/int/string_list land with user config support.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlagType {
    Bool,
    ContentTypeList,
}

/// A resolved flag value bound into the CEL `flags` map.
#[derive(Clone, Debug)]
pub enum FlagValue {
    Bool(bool),
    ContentTypeList(Vec<ContentType>),
}

/// The parsed classifier source.
pub struct Source {
    /// Workflow name -> its action-tree YAML (a sequence of steps).
    pub workflows: BTreeMap<String, Yaml>,
    pub flag_definitions: BTreeMap<String, FlagType>,
    pub flags: BTreeMap<String, FlagValue>,
    pub keywords: BTreeMap<String, Vec<String>>,
    pub extensions: BTreeMap<String, Vec<String>>,
}

/// A source parse/compile failure.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("parse classifier source: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("serialize effective classifier configuration: {0}")]
    Json(#[from] serde_json::Error),
    #[error("classifier source: {0}")]
    Shape(String),
}

/// Digest the exact embedded classifier configuration that [`Source::load_core`]
/// parses and the default workflow that the shadow runtime supports.
pub fn core_config_digest() -> Result<String, SourceError> {
    effective_config_digest(CORE_YAML, CORE_DEFAULT_WORKFLOW)
}

fn effective_config_digest(yaml: &str, default_workflow: &str) -> Result<String, SourceError> {
    let root: Yaml = serde_yaml::from_str(yaml)?;
    let map = root
        .as_mapping()
        .ok_or_else(|| SourceError::Shape("root is not a mapping".into()))?;
    let empty = Yaml::Mapping(Default::default());
    let document = EffectiveConfigDocument {
        version: EFFECTIVE_CONFIG_DIGEST_VERSION,
        default_workflow,
        source: EffectiveConfigSource {
            workflows: canonical_yaml(map.get("workflows").unwrap_or(&empty))?,
            flag_definitions: canonical_yaml(map.get("flag_definitions").unwrap_or(&empty))?,
            flags: canonical_yaml(map.get("flags").unwrap_or(&empty))?,
            keywords: canonical_yaml(map.get("keywords").unwrap_or(&empty))?,
            extensions: canonical_yaml(map.get("extensions").unwrap_or(&empty))?,
        },
    };
    // Go's encoding/json always escapes the JSONP line/paragraph separators,
    // even with HTML escaping disabled. Make that explicit in v1 so equal
    // Unicode strings hash identically in both implementations.
    let encoded = serde_json::to_string(&document)?
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    let sum = Sha256::digest(encoded.as_bytes());

    Ok(format!("sha256:{}", hex::encode(sum)))
}

#[derive(Serialize)]
struct EffectiveConfigDocument<'a> {
    version: u8,
    default_workflow: &'a str,
    source: EffectiveConfigSource,
}

#[derive(Serialize)]
struct EffectiveConfigSource {
    workflows: CanonicalJson,
    flag_definitions: CanonicalJson,
    flags: CanonicalJson,
    keywords: CanonicalJson,
    extensions: CanonicalJson,
}

#[derive(Serialize)]
#[serde(untagged)]
enum CanonicalJson {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<CanonicalJson>),
    Object(BTreeMap<String, CanonicalJson>),
}

fn canonical_yaml(value: &Yaml) -> Result<CanonicalJson, SourceError> {
    match value {
        Yaml::Null => Ok(CanonicalJson::Null),
        Yaml::Bool(value) => Ok(CanonicalJson::Bool(*value)),
        Yaml::Number(value) => {
            let number = if let Some(value) = value.as_i64() {
                JsonNumber::from(value)
            } else if let Some(value) = value.as_u64() {
                JsonNumber::from(value)
            } else {
                return Err(SourceError::Shape(
                    "effective config digest v1 does not support floating-point values".into(),
                ));
            };
            Ok(CanonicalJson::Number(number))
        }
        Yaml::String(value) => Ok(CanonicalJson::String(value.clone())),
        Yaml::Sequence(values) => values
            .iter()
            .map(canonical_yaml)
            .collect::<Result<Vec<_>, _>>()
            .map(CanonicalJson::Array),
        Yaml::Mapping(values) => values
            .iter()
            .map(|(key, value)| {
                let key = key.as_str().ok_or_else(|| {
                    SourceError::Shape("effective config mapping key is not a string".into())
                })?;
                Ok((key.to_string(), canonical_yaml(value)?))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(CanonicalJson::Object),
        Yaml::Tagged(tagged) => canonical_yaml(&tagged.value),
    }
}

impl Source {
    /// Parse the embedded `classifier.core.yml`.
    pub fn load_core() -> Result<Source, SourceError> {
        Source::parse(CORE_YAML)
    }

    /// Load core plus the private acquisition-only workflows needed to replay
    /// a traced T1 tape. This does not alter [`core_config_digest`]: the plan
    /// workflows are evidence apparatus, not serving classifier configuration.
    pub fn load_core_with_tape_evidence() -> Result<Source, SourceError> {
        let mut core = Source::load_core()?;
        let evidence = Source::parse(TAPE_EVIDENCE_WORKFLOWS_YAML)?;
        for (name, workflow) in evidence.workflows {
            core.workflows.insert(name, workflow);
        }
        Ok(core)
    }

    /// Parse a classifier YAML source document.
    pub fn parse(yaml: &str) -> Result<Source, SourceError> {
        let root: Yaml = serde_yaml::from_str(yaml)?;
        let map = root
            .as_mapping()
            .ok_or_else(|| SourceError::Shape("root is not a mapping".into()))?;

        let workflows = map
            .get("workflows")
            .and_then(Yaml::as_mapping)
            .map(|wf| {
                wf.iter()
                    .filter_map(|(k, v)| k.as_str().map(|s| (s.to_string(), v.clone())))
                    .collect()
            })
            .unwrap_or_default();

        let flag_definitions = parse_flag_definitions(map.get("flag_definitions"))?;
        let flags = parse_flags(map.get("flags"), &flag_definitions)?;
        let keywords = parse_string_lists(map.get("keywords"));
        let extensions = parse_string_lists(map.get("extensions"));

        Ok(Source {
            workflows,
            flag_definitions,
            flags,
            keywords,
            extensions,
        })
    }
}

fn parse_flag_definitions(v: Option<&Yaml>) -> Result<BTreeMap<String, FlagType>, SourceError> {
    let mut out = BTreeMap::new();
    if let Some(map) = v.and_then(Yaml::as_mapping) {
        for (k, val) in map {
            let name = k
                .as_str()
                .ok_or_else(|| SourceError::Shape("non-string flag name".into()))?;
            let ty = val
                .as_str()
                .ok_or_else(|| SourceError::Shape("non-string flag type".into()))?;
            let ty = match ty {
                "bool" => FlagType::Bool,
                "content_type_list" => FlagType::ContentTypeList,
                other => {
                    return Err(SourceError::Shape(format!(
                        "unsupported flag type '{other}' (Lane C milestone supports bool + content_type_list)"
                    )))
                }
            };
            out.insert(name.to_string(), ty);
        }
    }
    Ok(out)
}

fn parse_flags(
    v: Option<&Yaml>,
    defs: &BTreeMap<String, FlagType>,
) -> Result<BTreeMap<String, FlagValue>, SourceError> {
    let mut out = BTreeMap::new();
    if let Some(map) = v.and_then(Yaml::as_mapping) {
        for (k, val) in map {
            let name = k
                .as_str()
                .ok_or_else(|| SourceError::Shape("non-string flag name".into()))?;
            let value =
                match defs.get(name) {
                    Some(FlagType::Bool) => FlagValue::Bool(val.as_bool().ok_or_else(|| {
                        SourceError::Shape(format!("flag '{name}' is not a bool"))
                    })?),
                    Some(FlagType::ContentTypeList) => {
                        let mut cts = Vec::new();
                        for item in val.as_sequence().into_iter().flatten() {
                            let s = item.as_str().ok_or_else(|| {
                                SourceError::Shape(format!("flag '{name}' item is not a string"))
                            })?;
                            // "unknown" maps to an invalid content type (skipped in
                            // the CEL list, matching `NewContentType` for unknown).
                            if s != "unknown" {
                                let ct = ContentType::parse(s).ok_or_else(|| {
                                    SourceError::Shape(format!("unknown content type '{s}'"))
                                })?;
                                cts.push(ct);
                            }
                        }
                        FlagValue::ContentTypeList(cts)
                    }
                    None => {
                        return Err(SourceError::Shape(format!(
                            "flag '{name}' has a value but no definition"
                        )))
                    }
                };
            out.insert(name.to_string(), value);
        }
    }
    Ok(out)
}

fn parse_string_lists(v: Option<&Yaml>) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    if let Some(map) = v.and_then(Yaml::as_mapping) {
        for (k, val) in map {
            if let Some(name) = k.as_str() {
                let list = val
                    .as_sequence()
                    .into_iter()
                    .flatten()
                    .filter_map(|i| i.as_str().map(str::to_string))
                    .collect();
                out.insert(name.to_string(), list);
            }
        }
    }
    out
}

#[cfg(test)]
mod config_digest_tests {
    use super::{effective_config_digest, CORE_DEFAULT_WORKFLOW, CORE_YAML};

    #[test]
    fn behavior_mutations_change_effective_config_digest() {
        let baseline =
            effective_config_digest(CORE_YAML, CORE_DEFAULT_WORKFLOW).expect("baseline digest");

        for (needle, replacement) in [
            ("set_content_type: audiobook", "set_content_type: comic"),
            ("tmdb_enabled: true", "tmdb_enabled: false"),
            ("- m4a", "- m4x"),
        ] {
            assert!(
                CORE_YAML.contains(needle),
                "missing mutation needle {needle}"
            );
            let mutated = CORE_YAML.replacen(needle, replacement, 1);
            let digest =
                effective_config_digest(&mutated, CORE_DEFAULT_WORKFLOW).expect("mutated digest");
            assert_ne!(digest, baseline, "mutation {needle} did not change digest");
        }
    }

    #[test]
    fn default_workflow_changes_effective_config_digest() {
        let baseline =
            effective_config_digest(CORE_YAML, CORE_DEFAULT_WORKFLOW).expect("baseline digest");
        let mutated = effective_config_digest(CORE_YAML, "audio").expect("mutated digest");
        assert_ne!(mutated, baseline);
    }

    #[test]
    fn unicode_edge_vector_matches_go() {
        let yaml = r#"
workflows:
  edge:
    value: "before\u2028between\u2029after"
    items: [null, true, -7, "<&>"]
flag_definitions: {}
flags: {}
keywords: {}
extensions: {}
"#;
        assert_eq!(
            effective_config_digest(yaml, "edge").expect("edge digest"),
            "sha256:61562ac973ee6a59d1e49d5dbdc555002f23b3a9c24358de5c423aef7edfb7bf"
        );
    }

    #[test]
    fn digest_rejects_floating_point_values() {
        let yaml = r#"
workflows:
  edge:
    value: 1.5
"#;
        let error = effective_config_digest(yaml, "edge").expect_err("float must be rejected");
        assert!(error
            .to_string()
            .contains("does not support floating-point"));
    }
}
