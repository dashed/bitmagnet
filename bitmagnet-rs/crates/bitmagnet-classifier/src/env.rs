//! The CEL environment: builds the constant namespace variables from a
//! `Source` and evaluates compiled expression `Program`s against a
//! `torrent`/`result`/`flags` binding, registering the three custom functions
//! `classifier.core.yml` calls (`sum`, `join`, and a compiled-regex-caching
//! `matches`).
//!
//! cel-rust converts every bound [`Value`] into its runtime representation by
//! deep copy, so bindings are layered by lifetime: the namespaces and
//! functions live in one root context built with the classifier, each run binds
//! `torrent` and `flags` once in a child scope ([`Env::run_scope`]), and only
//! `result` is bound per evaluation — and, for an expression that only selects
//! its fields, bound as a [`LazyResult`] that converts just those fields
//! ([`eval_bool`]).
//!
//! Unlike cel-go (`cel_env.go`), the namespaces are bound as ordinary CEL
//! **maps** rather than dotted-name constants — cel-rust resolves `a.b` on a
//! map by key, and has no compile-time type-checker that needs the null-map
//! placeholder trick.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};

use bitmagnet_release::regex_pattern_from_keywords;
use cel::common::ast::{EntryExpr, Expr, IdedEntryExpr};
use cel::common::types::CelString;
use cel::extractors::This;
use cel::{
    to_value, Context, ExecutionError, FunctionContext, IdedExpr, Program, ResolveResult, Value,
};
use regex::Regex;

use crate::cel_value::{cel_classification, LazyResult};
use crate::errors::FlowError;
use crate::model::{ContentType, FileType};
use crate::result::Classification;
use crate::source::{FlagValue, Source};

/// Precomputed, run-invariant CEL bindings (everything except `torrent`,
/// `result`, and `flags`).
pub(crate) struct Env {
    /// cel-rust's standard overload table (built once rather than per
    /// `Context::default()`), the custom functions, and the namespaces.
    root: Context<'static>,
}

/// Compiled `matches` patterns, keyed by pattern text.
///
/// cel-rust's stdlib `matches` compiles its pattern on every call, and the
/// `keywords.*` patterns are large Unicode alternations, so recompiling them
/// for every torrent dominated classification time. Patterns come from the
/// compiled configuration, so the set is small; the bound only guards against a
/// configuration that builds patterns from torrent data. A list rather than a
/// hash map: hashing a multi-kilobyte pattern on every call costs far more
/// than comparing it against a handful of entries, where differing lengths
/// short-circuit and a hit is one `memcmp`. Entries are shared rather than
/// cloned because `Regex::clone` starts an empty match-cache pool, which would
/// rebuild the lazy DFA on every call.
///
/// Read-locked on a hit, because the processor classifies a job's torrents on
/// several threads and nearly every call hits. A miss compiles outside the
/// lock and looks again before inserting: threads that start together all miss
/// the same pattern, and inserting each copy filled the bound with duplicates
/// (13 entries for core.yml's 4 patterns on 4 threads), so with enough threads
/// a pattern would never be cached and would recompile on every call.
#[derive(Default)]
struct RegexCache(RwLock<Vec<(String, Arc<Regex>)>>);

const MAX_CACHED_REGEXES: usize = 64;

impl RegexCache {
    fn get(&self, pattern: &str) -> Result<Arc<Regex>, regex::Error> {
        // The list is only ever appended to, so a poisoned lock is still valid.
        let cached = |cache: &[(String, Arc<Regex>)]| {
            cache
                .iter()
                .find(|(cached, _)| cached == pattern)
                .map(|(_, regex)| Arc::clone(regex))
        };
        if let Some(regex) = cached(&self.0.read().unwrap_or_else(PoisonError::into_inner)) {
            return Ok(regex);
        }
        let regex = Arc::new(Regex::new(pattern)?);
        let mut cache = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(regex) = cached(&cache) {
            return Ok(regex);
        }
        if cache.len() < MAX_CACHED_REGEXES {
            cache.push((pattern.to_owned(), Arc::clone(&regex)));
        }
        Ok(regex)
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
        let mut root = Context::with_env(Arc::new(cel::Env::stdlib()));
        register_functions(&mut root);

        // extensions: {group: [ext, ...]}
        push_ns(&mut root, "extensions", &source.extensions)?;

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
        push_ns(&mut root, "keywords", &kw)?;

        // contentType / fileType: {name: discriminant} (+ unknown = 0)
        let mut ct: BTreeMap<String, i64> = BTreeMap::from([("unknown".to_string(), 0)]);
        for c in ContentType::all() {
            ct.insert(c.as_str().to_string(), i64::from(c.proto_i32()));
        }
        push_ns(&mut root, "contentType", &ct)?;

        let mut ft: BTreeMap<String, i64> = BTreeMap::from([("unknown".to_string(), 0)]);
        for f in FileType::all() {
            ft.insert(f.as_str().to_string(), i64::from(f.proto_i32()));
        }
        push_ns(&mut root, "fileType", &ft)?;

        // size units
        root.add_variable_from_value("kb", Value::Int(1_000));
        root.add_variable_from_value("mb", Value::Int(1_000_000));
        root.add_variable_from_value("gb", Value::Int(1_000_000_000));

        Ok(Env { root })
    }

    /// Bind one run's `torrent` and `flags` over the shared root.
    pub(crate) fn run_scope(&self, torrent: Value, flags: Value) -> Context<'_> {
        let mut scope = self.root.new_inner_scope();
        scope.add_variable_from_value("flags", flags);
        scope.add_variable_from_value("torrent", torrent);
        scope
    }
}

/// The variable conditions read the current classification through.
const RESULT: &str = "result";

/// A compiled condition expression, and how it reads `result`.
pub(crate) struct CelExpression {
    program: Program,
    result: ResultUse,
}

impl CelExpression {
    pub(crate) fn compile(source: &str) -> Result<Self, cel::ParseErrors> {
        let program = Program::compile(source)?;
        let result = result_use(program.expression());
        Ok(Self { program, result })
    }
}

/// How an expression reads `result`, which decides how [`eval_bool`] binds it.
///
/// An expression reaches its variables only through identifiers, so its use is
/// the strongest one any `result` identifier in it makes (the variants are
/// ordered weakest first).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ResultUse {
    /// It never names `result`, so nothing is bound.
    Unused,
    /// It names `result` only to select a field (`result.x`, `has(result.x)`),
    /// where all cel-rust asks of the value is its type and `get`/`contains`
    /// for that field name, so a [`LazyResult`] stands in for the map.
    Fields,
    /// It uses the object itself, so it gets the map.
    Whole,
}

fn result_use(expr: &IdedExpr) -> ResultUse {
    let children: Vec<&IdedExpr> = match &expr.expr {
        Expr::Ident(name) if name == RESULT => return ResultUse::Whole,
        Expr::Select(select) if matches!(&select.operand.expr, Expr::Ident(name) if name == RESULT) => {
            return ResultUse::Fields
        }
        Expr::Select(select) => vec![&select.operand],
        Expr::Call(call) => call
            .target
            .as_deref()
            .into_iter()
            .chain(&call.args)
            .collect(),
        Expr::Comprehension(comprehension) => vec![
            &comprehension.iter_range,
            &comprehension.accu_init,
            &comprehension.loop_cond,
            &comprehension.loop_step,
            &comprehension.result,
        ],
        Expr::List(list) => list.elements.iter().collect(),
        Expr::Map(map) => entry_exprs(&map.entries),
        Expr::Struct(strct) => entry_exprs(&strct.entries),
        Expr::Ident(_) | Expr::Literal(_) | Expr::Unspecified => Vec::new(),
    };
    children
        .into_iter()
        .map(result_use)
        .max()
        .unwrap_or(ResultUse::Unused)
}

fn entry_exprs(entries: &[IdedEntryExpr]) -> Vec<&IdedExpr> {
    entries
        .iter()
        .flat_map(|entry| match &entry.expr {
            EntryExpr::StructField(field) => vec![&field.value],
            EntryExpr::MapEntry(entry) => vec![&entry.key, &entry.value],
        })
        .collect()
}

/// Evaluate a compiled boolean CEL expression against a run scope from
/// [`Env::run_scope`] and the current result.
pub(crate) fn eval_bool(
    scope: &Context<'_>,
    expr: &CelExpression,
    result: &Arc<Classification>,
) -> Result<bool, FlowError> {
    match execute(scope, expr, result) {
        Ok(Value::Bool(b)) => Ok(b),
        // Mirrors `condition_expression.go`'s "not bool" guard.
        Ok(_) => Err(FlowError::Cel("not bool".to_string())),
        Err(e) => Err(FlowError::Cel(e.to_string())),
    }
}

/// Run `expr` with `result` bound the way its [`ResultUse`] calls for.
fn execute(
    scope: &Context<'_>,
    expr: &CelExpression,
    result: &Arc<Classification>,
) -> ResolveResult {
    if expr.result == ResultUse::Unused {
        return expr.program.execute(scope);
    }
    let mut ctx = scope.new_inner_scope();
    if expr.result == ResultUse::Fields {
        ctx.add_variable_as_val(RESULT, Box::new(LazyResult::new(Arc::clone(result))));
    } else {
        ctx.add_variable_from_value(RESULT, cel_classification(result));
    }
    expr.program.execute(&ctx)
}

fn push_ns<T: serde::Serialize>(
    ns: &mut Context<'_>,
    name: &str,
    value: &T,
) -> Result<(), EnvError> {
    let v = to_value(value).map_err(|_| EnvError::Serialize(name.to_string()))?;
    ns.add_variable_from_value(name, v);
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
fn register_functions(ctx: &mut Context<'_>) {
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
    let regexes = RegexCache::default();
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

#[cfg(test)]
mod result_binding_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// Target/pattern pairs beyond the keyword patterns: anchors, word
    /// boundaries, flags, Unicode classes and case folding, multi-byte text,
    /// the empty pattern, and patterns the `regex` crate rejects.
    const CASES: &[(&str, &str)] = &[
        ("abc", "^[a-z]*$"),
        ("abc1", "^[a-z]*$"),
        ("abc", "b"),
        ("abc", "^b"),
        ("abc", "c$"),
        ("line1\nline2", "^line2$"),
        ("line1\nline2", "(?m)^line2$"),
        ("The Matrix 1999", r"\b1999\b"),
        ("The Matrix 19999", r"\b1999\b"),
        ("ÉTÉ", r"(?i)été"),
        ("été", r"^\p{L}+$"),
        ("été", r"^(?-u:\w)+$"),
        ("日本語", r"^\p{Han}+$"),
        ("٣", r"^\d$"),
        ("٣", r"^(?-u:\d)$"),
        ("ǅ", r"(?i)ǆ"),
        ("a.b", r"a\.b"),
        ("axb", r"a\.b"),
        ("", ""),
        ("abc", ""),
        ("abc", "("),
        ("abc", "[a-"),
        ("abc", r"\p{NotAClass}"),
        ("abc", "a{2,1}"),
        ("abc", "(?=a)"),
        ("abc", r"\1"),
    ];

    fn root() -> Context<'static> {
        let mut ctx = Context::with_env(Arc::new(cel::Env::stdlib()));
        register_functions(&mut ctx);
        ctx
    }

    fn eval(ctx: &Context<'_>, expr: &str, target: &str, pattern: &str) -> cel::ResolveResult {
        let mut scope = ctx.new_inner_scope();
        scope.add_variable_from_value("s", target);
        scope.add_variable_from_value("p", pattern);
        Program::compile(expr).expect("compile").execute(&scope)
    }

    /// cel-rust's stdlib `matches` (behind the `regex` feature this crate
    /// leaves off) is `Regex::new(pattern)` then `is_match`, failing with this
    /// exact `FunctionError` when the pattern does not compile.
    fn stdlib_definition(target: &str, pattern: &str) -> cel::ResolveResult {
        match Regex::new(pattern) {
            Ok(regex) => Ok(Value::Bool(regex.is_match(target))),
            Err(err) => Err(ExecutionError::function_error(
                "matches",
                format!("'{pattern}' not a valid regex:\n{err}"),
            )),
        }
    }

    #[test]
    fn matches_agrees_with_the_cel_stdlib_definition_on_hits_and_misses() {
        let ctx = root();
        // Twice, so the second pass is served from the cache.
        for _ in 0..2 {
            for (target, pattern) in CASES {
                assert_eq!(
                    eval(&ctx, "s.matches(p)", target, pattern),
                    stdlib_definition(target, pattern),
                    "{target:?}.matches({pattern:?})"
                );
            }
        }
    }

    #[test]
    fn invalid_patterns_are_errors_and_never_cached() {
        let ctx = root();
        for _ in 0..2 {
            assert!(matches!(
                eval(&ctx, "s.matches(p)", "abc", "("),
                Err(ExecutionError::FunctionError { .. })
            ));
        }
        assert_eq!(
            eval(&ctx, "s.matches(p)", "abc", "a"),
            Ok(Value::Bool(true))
        );
    }

    #[test]
    fn concurrent_misses_cache_one_shared_regex() {
        let cache = RegexCache::default();
        let start = std::sync::Barrier::new(8);
        let regexes: Vec<Arc<Regex>> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        cache
                            .get(r"\b(?:\p{L}+ ){3}\d{4}\b")
                            .expect("valid pattern")
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().expect("worker"))
                .collect()
        });
        let cached = cache.0.read().expect("unpoisoned");
        assert_eq!(cached.len(), 1);
        assert!(regexes.iter().all(|regex| Arc::ptr_eq(regex, &cached[0].1)));
    }

    #[test]
    fn call_shapes_the_stdlib_overload_never_declared_stay_undeclared() {
        let ctx = root();
        for expr in [
            "matches(s, p)",
            "s.matches(1)",
            "s.matches(p, p)",
            "1.matches(p)",
        ] {
            assert_eq!(
                eval(&ctx, expr, "abc", "a"),
                Err(ExecutionError::undeclared_reference("matches")),
                "{expr}"
            );
        }
    }
}
