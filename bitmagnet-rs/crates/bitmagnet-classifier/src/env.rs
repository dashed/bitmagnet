//! The CEL environment: builds the constant namespace variables from a
//! `Source` and evaluates compiled expression `Program`s against a
//! `torrent`/`result`/`flags` binding, registering the three custom functions
//! `classifier.core.yml` calls (`sum`, `join`, and a compiled-regex-caching
//! `matches`).
//!
//! Unlike cel-go (`cel_env.go`), the namespaces are bound as ordinary CEL
//! **maps** rather than dotted-name constants — cel-rust resolves `a.b` on a
//! map by key, and has no compile-time type-checker that needs the null-map
//! placeholder trick.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use bitmagnet_release::regex_pattern_from_keywords;
use cel::common::types::CelString;
use cel::extractors::This;
use cel::{to_value, Context, ExecutionError, FunctionContext, Program, Value};
use regex::Regex;

use crate::cel_value::build_cel_classification;
use crate::errors::FlowError;
use crate::model::{ContentType, FileType};
use crate::result::Classification;
use crate::source::{FlagValue, Source};

/// Precomputed, run-invariant CEL bindings (everything except `torrent`,
/// `result`, and `flags`).
pub(crate) struct Env {
    namespaces: Vec<(String, Value)>,
    /// cel-rust's standard overload table. `Context::default()` rebuilds it
    /// on every call, so it is built once and shared by every evaluation.
    stdlib: Arc<cel::Env>,
    regexes: Arc<RegexCache>,
}

/// Compiled `matches` patterns, keyed by pattern text.
///
/// cel-rust's stdlib `matches` compiles its pattern on every call, and the
/// `keywords.*` patterns are large Unicode alternations, so recompiling them
/// for every torrent dominated classification time. Patterns come from the
/// compiled configuration, so the set is small; the bound only guards against a
/// configuration that builds patterns from torrent data.
#[derive(Default)]
struct RegexCache(Mutex<HashMap<String, Regex>>);

const MAX_CACHED_REGEXES: usize = 256;

impl RegexCache {
    fn get(&self, pattern: &str) -> Result<Regex, regex::Error> {
        if let Some(regex) = self.lock().get(pattern) {
            return Ok(regex.clone());
        }
        let regex = Regex::new(pattern)?;
        let mut cache = self.lock();
        if cache.len() < MAX_CACHED_REGEXES {
            cache.insert(pattern.to_owned(), regex.clone());
        }
        Ok(regex)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Regex>> {
        // The map is only ever inserted into, so a poisoned lock is still valid.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Env construction failure.
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    #[error("compile keyword group '{group}': {source}")]
    Keyword {
        group: String,
        source: bitmagnet_release::KeywordError,
    },
    #[error("serialize namespace '{0}'")]
    Serialize(String),
}

impl Env {
    /// Build the namespace bindings from a source.
    pub(crate) fn build(source: &Source) -> Result<Env, EnvError> {
        let mut namespaces: Vec<(String, Value)> = Vec::new();

        // extensions: {group: [ext, ...]}
        push_ns(&mut namespaces, "extensions", &source.extensions)?;

        // keywords: {group: compiled-regex-pattern}
        let mut kw: BTreeMap<String, String> = BTreeMap::new();
        for (group, kws) in &source.keywords {
            let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
            let pattern =
                regex_pattern_from_keywords(&refs).map_err(|source| EnvError::Keyword {
                    group: group.clone(),
                    source,
                })?;
            kw.insert(group.clone(), pattern);
        }
        push_ns(&mut namespaces, "keywords", &kw)?;

        // contentType / fileType: {name: discriminant} (+ unknown = 0)
        let mut ct: BTreeMap<String, i64> = BTreeMap::from([("unknown".to_string(), 0)]);
        for c in ContentType::all() {
            ct.insert(c.as_str().to_string(), i64::from(c.proto_i32()));
        }
        push_ns(&mut namespaces, "contentType", &ct)?;

        let mut ft: BTreeMap<String, i64> = BTreeMap::from([("unknown".to_string(), 0)]);
        for f in FileType::all() {
            ft.insert(f.as_str().to_string(), i64::from(f.proto_i32()));
        }
        push_ns(&mut namespaces, "fileType", &ft)?;

        // size units
        namespaces.push(("kb".to_string(), Value::Int(1_000)));
        namespaces.push(("mb".to_string(), Value::Int(1_000_000)));
        namespaces.push(("gb".to_string(), Value::Int(1_000_000_000)));

        Ok(Env {
            namespaces,
            stdlib: Arc::new(cel::Env::stdlib()),
            regexes: Arc::default(),
        })
    }

    /// Evaluate a compiled boolean CEL expression against the current binding.
    pub(crate) fn eval_bool(
        &self,
        program: &Program,
        torrent: &Value,
        result: &Classification,
        flags: &Value,
    ) -> Result<bool, FlowError> {
        let mut ctx = Context::with_env(Arc::clone(&self.stdlib));
        register_functions(&mut ctx, &self.regexes);
        for (name, value) in &self.namespaces {
            ctx.add_variable_from_value(name.clone(), value.clone());
        }
        ctx.add_variable_from_value("flags", flags.clone());
        ctx.add_variable_from_value("torrent", torrent.clone());
        let result_val = to_value(build_cel_classification(result))
            .map_err(|e| FlowError::Cel(format!("serialize result: {e}")))?;
        ctx.add_variable_from_value("result", result_val);

        match program.execute(&ctx) {
            Ok(Value::Bool(b)) => Ok(b),
            // Mirrors `condition_expression.go`'s "not bool" guard.
            Ok(_) => Err(FlowError::Cel("not bool".to_string())),
            Err(e) => Err(FlowError::Cel(e.to_string())),
        }
    }
}

fn push_ns<T: serde::Serialize>(
    ns: &mut Vec<(String, Value)>,
    name: &str,
    value: &T,
) -> Result<(), EnvError> {
    let v = to_value(value).map_err(|_| EnvError::Serialize(name.to_string()))?;
    ns.push((name.to_string(), v));
    Ok(())
}

/// A serde-friendly flag value (`cel::Value` is not `Serialize`, so the CEL
/// `flags` map is built from this intermediate). Content-type lists become
/// lists of int discriminants (`FlagType.celVal`).
#[derive(serde::Serialize)]
#[serde(untagged)]
enum FlagSerde {
    Bool(bool),
    IntList(Vec<i64>),
}

/// Serialize the runtime flags into the CEL `flags` map value.
pub(crate) fn flags_value(flags: &BTreeMap<String, FlagValue>) -> Value {
    let map: BTreeMap<String, FlagSerde> = flags
        .iter()
        .map(|(name, value)| {
            let v = match value {
                FlagValue::Bool(b) => FlagSerde::Bool(*b),
                FlagValue::ContentTypeList(cts) => {
                    FlagSerde::IntList(cts.iter().map(|c| i64::from(c.proto_i32())).collect())
                }
            };
            (name.clone(), v)
        })
        .collect();
    to_value(map).unwrap_or(Value::Null)
}

/// Register the custom member functions `classifier.core.yml` invokes.
fn register_functions(ctx: &mut Context<'_>, regexes: &Arc<RegexCache>) {
    // `<list<int>>.sum()` — k8s.lists `sum` over the int overload (the only
    // summable type core.yml uses). Empty list -> 0 (`cel_lists.go`).
    ctx.add_function("sum", |This(list): This<Arc<Vec<Value>>>| -> i64 {
        list.iter()
            .map(|v| match v {
                Value::Int(n) => *n,
                _ => 0,
            })
            .sum()
    });
    // `<list<string>>.join(sep)` — ext.Strings `join`.
    ctx.add_function(
        "join",
        |This(list): This<Arc<Vec<Value>>>, sep: Arc<String>| -> Arc<String> {
            let parts: Vec<String> = list
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.to_string(),
                    _ => String::new(),
                })
                .collect();
            Arc::new(parts.join(sep.as_str()))
        },
    );
    // `<string>.matches(pattern)` — cel-rust's stdlib overload with the
    // compiled regex cached. The crate's `regex` feature is off so that this
    // registry function, rather than the stdlib overload, receives the call.
    // Any other call shape falls through exactly as it did when only the
    // stdlib member overload was declared.
    let regexes = Arc::clone(regexes);
    ctx.add_function(
        "matches",
        move |ftx: &FunctionContext<'_, '_>| -> Result<bool, ExecutionError> {
            let (Some(target), [pattern]) = (ftx.this.as_deref(), ftx.args.as_slice()) else {
                return Err(ExecutionError::undeclared_reference("matches"));
            };
            let (Some(target), Some(pattern)) = (
                target.downcast_ref::<CelString>(),
                pattern.downcast_ref::<CelString>(),
            ) else {
                return Err(ExecutionError::undeclared_reference("matches"));
            };
            match regexes.get(pattern.inner()) {
                Ok(regex) => Ok(regex.is_match(target.inner())),
                Err(err) => Err(ExecutionError::function_error(
                    "matches",
                    format!("'{}' not a valid regex:\n{err}", pattern.inner()),
                )),
            }
        },
    );
}
