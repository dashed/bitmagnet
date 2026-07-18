//! The classifier `Source` (`source.go`) + the YAML loader for
//! `classifier.core.yml`. This milestone loads the embedded core source only;
//! the XDG/CWD/config merge layers (`source_provider.go`) land incrementally.

use std::collections::BTreeMap;

use serde_yaml::Value as Yaml;

use crate::model::ContentType;

/// The embedded core classifier source — the byte-for-byte public contract
/// (`classifier.core.yml`, kept in sync with the Go embed).
pub(crate) const CORE_YAML: &str =
    include_str!("../../../../internal/classifier/classifier.core.yml");

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
    #[error("classifier source: {0}")]
    Shape(String),
}

impl Source {
    /// Parse the embedded `classifier.core.yml`.
    pub fn load_core() -> Result<Source, SourceError> {
        Source::parse(CORE_YAML)
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
