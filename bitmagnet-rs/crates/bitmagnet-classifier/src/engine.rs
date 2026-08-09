//! The action/condition tree compiler + executor — the Rust port of the Go
//! `compileAction`/`compileCondition` compilers (`action.go`, `condition.go`,
//! the `action_*.go` / `condition_*.go` families) and their runtime.
//!
//! Compile-time **path threading** reproduces Go's `compilerContext.path`
//! exactly (`workflows.default.[0].if_else.if_action.delete`), because the
//! corpus `error` field pins those strings verbatim (contract §2.1).

use std::collections::HashMap;

use cel::{Program, Value};
use futures::future::LocalBoxFuture;
use serde_yaml::Value as Yaml;

use crate::env::Env;
use crate::errors::FlowError;
use crate::model::{ClassifierInput, ContentType};
use crate::parsers::{parse_date, parse_video_content};
use crate::resolver::{tmdb, ContentResolver};
use crate::result::Classification;

/// Go `model.SourceTmdb` — the one source whose hinted id IS the TMDB id, so no
/// `/find` lookup is needed.
const TMDB_SOURCE: &str = "tmdb";

/// A compiled action node.
pub(crate) enum Action {
    /// A sequence of actions run in order, threading the result (`action.go`).
    List(Vec<Action>),
    SetContentType(ContentType),
    /// `delete` — raises `ErrDeleteTorrent` wrapped at its compile path.
    Delete(Vec<String>),
    /// `unmatched` — raises `ErrUnmatched` wrapped at its compile path.
    Unmatched(Vec<String>),
    /// `add_tag` — records tag names. `classifier.core.yml` never uses it and
    /// tags are not part of the corpus output surface, so the names are held
    /// (compiled + validated) but not yet consumed at runtime.
    #[allow(dead_code)]
    AddTag(Vec<String>),
    /// `find_match` — first non-unmatched sub-action wins; carries its path for
    /// wrapping non-unmatched errors.
    FindMatch {
        actions: Vec<Action>,
        path: Vec<String>,
    },
    IfElse {
        condition: Condition,
        if_action: Option<Box<Action>>,
        else_action: Option<Box<Action>>,
    },
    RunWorkflow(Vec<String>),
    ParseDate,
    ParseVideoContent,
    /// The four `attach_*` actions, each carrying which dependency it consults.
    ///
    /// Kept as distinct kinds rather than one fused "unmatched" variant because
    /// they consult different dependencies and fail for different reasons —
    /// collapsing them is what made the enrichment path invisible to the parity
    /// gate. Under the flags-off corpus they all still resolve to `unmatched`
    /// (contract §2.2), because the gates around them are false and the
    /// local-by-id branch's lookup misses.
    Attach(AttachKind),
}

/// A compiled condition node.
pub(crate) enum Condition {
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
    Expression(Box<Program>),
}

/// A source-compilation error.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("compiler error at path '{path}': {message}")]
    At { path: String, message: String },
    #[error("compile CEL expression '{expr}': {source}")]
    Cel {
        expr: String,
        source: cel::ParseErrors,
    },
}

fn err_at(path: &[String], message: impl Into<String>) -> CompileError {
    CompileError::At {
        path: path.join("."),
        message: message.into(),
    }
}

/// Compile every workflow in a source into an executable action map.
pub(crate) fn compile_workflows(
    workflows: &HashMap<String, Yaml>,
) -> Result<HashMap<String, Action>, CompileError> {
    let names: std::collections::HashSet<String> = workflows.keys().cloned().collect();
    let mut out = HashMap::new();
    for (name, src) in workflows {
        let path = vec!["workflows".to_string(), name.clone()];
        out.insert(name.clone(), compile_action(src, path, &names)?);
    }
    Ok(out)
}

/// `compileAction` — dispatches on array-vs-single, threading the array index.
fn compile_action(
    source: &Yaml,
    path: Vec<String>,
    workflows: &std::collections::HashSet<String>,
) -> Result<Action, CompileError> {
    if let Some(seq) = source.as_sequence() {
        let mut actions = Vec::with_capacity(seq.len());
        for (i, item) in seq.iter().enumerate() {
            let mut elem_path = path.clone();
            elem_path.push(format!("[{i}]"));
            actions.push(compile_dispatch(item, elem_path, workflows)?);
        }
        Ok(Action::List(actions))
    } else {
        compile_dispatch(source, path, workflows)
    }
}

/// Resolve a single action node's def-name, extend the path, and build it.
fn compile_dispatch(
    item: &Yaml,
    base_path: Vec<String>,
    workflows: &std::collections::HashSet<String>,
) -> Result<Action, CompileError> {
    // Literal string actions.
    if let Some(literal) = item.as_str() {
        let mut path = base_path;
        path.push(literal.to_string());
        return match literal {
            "delete" => Ok(Action::Delete(path)),
            "unmatched" => Ok(Action::Unmatched(path)),
            "parse_date" => Ok(Action::ParseDate),
            "parse_video_content" => Ok(Action::ParseVideoContent),
            "attach_local_content_by_id" => Ok(Action::Attach(AttachKind::LocalContentById)),
            "attach_local_content_by_search" => {
                Ok(Action::Attach(AttachKind::LocalContentBySearch))
            }
            "attach_tmdb_content_by_id" => Ok(Action::Attach(AttachKind::TmdbContentById)),
            "attach_tmdb_content_by_search" => Ok(Action::Attach(AttachKind::TmdbContentBySearch)),
            other => Err(err_at(
                &path,
                format!("no action matched literal '{other}'"),
            )),
        };
    }

    // Single-key mapping actions.
    let (key, value) = single_key(item, &base_path, "action")?;
    let mut path = base_path;
    path.push(key.clone());
    match key.as_str() {
        "set_content_type" => {
            let s = value
                .as_str()
                .ok_or_else(|| err_at(&path, "set_content_type value is not a string"))?;
            let ct = ContentType::parse(s)
                .ok_or_else(|| err_at(&path, format!("unknown content type '{s}'")))?;
            Ok(Action::SetContentType(ct))
        }
        "find_match" => {
            let seq = value
                .as_sequence()
                .ok_or_else(|| err_at(&path, "find_match value is not a list"))?;
            let mut actions = Vec::with_capacity(seq.len());
            for (i, sub) in seq.iter().enumerate() {
                let mut sub_path = path.clone();
                sub_path.push(format!("[{i}]"));
                actions.push(compile_action(sub, sub_path, workflows)?);
            }
            Ok(Action::FindMatch { actions, path })
        }
        "if_else" => compile_if_else(value, path, workflows),
        "add_tag" => Ok(Action::AddTag(string_list(value, &path, "add_tag")?)),
        "run_workflow" => {
            let names = string_list(value, &path, "run_workflow")?;
            for n in &names {
                if !workflows.contains(n) {
                    return Err(err_at(&path, format!("workflow {n} not found")));
                }
            }
            Ok(Action::RunWorkflow(names))
        }
        other => Err(err_at(&path, format!("no action matched '{other}'"))),
    }
}

fn compile_if_else(
    value: &Yaml,
    path: Vec<String>,
    workflows: &std::collections::HashSet<String>,
) -> Result<Action, CompileError> {
    let map = value
        .as_mapping()
        .ok_or_else(|| err_at(&path, "if_else value is not a mapping"))?;
    let condition_src = map
        .get("condition")
        .ok_or_else(|| err_at(&path, "if_else missing condition"))?;

    let mut cond_path = path.clone();
    cond_path.push("condition".to_string());
    let condition = compile_condition(condition_src, cond_path)?;

    let if_action = match map.get("if_action") {
        Some(a) => {
            let mut p = path.clone();
            p.push("if_action".to_string());
            Some(Box::new(compile_action(a, p, workflows)?))
        }
        None => None,
    };
    let else_action = match map.get("else_action") {
        Some(a) => {
            let mut p = path.clone();
            p.push("else_action".to_string());
            Some(Box::new(compile_action(a, p, workflows)?))
        }
        None => None,
    };

    Ok(Action::IfElse {
        condition,
        if_action,
        else_action,
    })
}

/// `compileCondition` — a raw string is a CEL expression; a single-key map is
/// `and`/`or`/`not`.
fn compile_condition(source: &Yaml, path: Vec<String>) -> Result<Condition, CompileError> {
    if let Some(expr) = source.as_str() {
        let program = Program::compile(expr).map_err(|source| CompileError::Cel {
            expr: expr.to_string(),
            source,
        })?;
        return Ok(Condition::Expression(Box::new(program)));
    }
    // The `{expression: "..."}` explicit form.
    if let Some((key, value)) = source.as_mapping().and_then(|m| {
        m.iter()
            .next()
            .and_then(|(k, v)| k.as_str().map(|s| (s.to_string(), v)))
    }) {
        let mut child = path.clone();
        child.push(key.clone());
        return match key.as_str() {
            "and" => Ok(Condition::And(compile_condition_list(value, &child)?)),
            "or" => Ok(Condition::Or(compile_condition_list(value, &child)?)),
            "not" => Ok(Condition::Not(Box::new(compile_condition(value, child)?))),
            "expression" => {
                let expr = value
                    .as_str()
                    .ok_or_else(|| err_at(&child, "expression value is not a string"))?;
                let program = Program::compile(expr).map_err(|source| CompileError::Cel {
                    expr: expr.to_string(),
                    source,
                })?;
                Ok(Condition::Expression(Box::new(program)))
            }
            other => Err(err_at(&child, format!("no condition matched '{other}'"))),
        };
    }
    Err(err_at(&path, "no condition matched"))
}

fn compile_condition_list(value: &Yaml, path: &[String]) -> Result<Vec<Condition>, CompileError> {
    let seq = value
        .as_sequence()
        .ok_or_else(|| err_at(path, "condition list is not a list"))?;
    let mut out = Vec::with_capacity(seq.len());
    for (i, sub) in seq.iter().enumerate() {
        let mut p = path.to_vec();
        p.push(format!("[{i}]"));
        out.push(compile_condition(sub, p)?);
    }
    Ok(out)
}

fn single_key<'y>(
    item: &'y Yaml,
    path: &[String],
    kind: &str,
) -> Result<(String, &'y Yaml), CompileError> {
    let map = item
        .as_mapping()
        .ok_or_else(|| err_at(path, format!("{kind} is neither a string nor a mapping")))?;
    let (k, v) = map
        .iter()
        .next()
        .ok_or_else(|| err_at(path, format!("empty {kind} mapping")))?;
    let key = k
        .as_str()
        .ok_or_else(|| err_at(path, format!("{kind} key is not a string")))?;
    Ok((key.to_string(), v))
}

fn string_list(value: &Yaml, path: &[String], kind: &str) -> Result<Vec<String>, CompileError> {
    let seq = value
        .as_sequence()
        .ok_or_else(|| err_at(path, format!("{kind} value is not a list")))?;
    seq.iter()
        .map(|i| {
            i.as_str()
                .map(str::to_string)
                .ok_or_else(|| err_at(path, format!("{kind} item is not a string")))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Run-invariant execution context (everything but the threaded result) —
/// Go's `executionContext`, which likewise carries the injected `dependencies`
/// (`dependencies.go`) alongside the CEL bindings.
pub(crate) struct ExecCtx<'a> {
    pub env: &'a Env,
    pub torrent_val: &'a Value,
    pub flags_val: &'a Value,
    pub input: &'a ClassifierInput,
    pub workflows: &'a HashMap<String, Action>,
    /// The B′-0 dependency seam. Held as a trait object so the same compiled
    /// workflows run against the live PG/TMDB backends or a recorded tape.
    ///
    /// Read by [`Action::Attach`]. The local actions are implemented; the TMDB
    /// ones still resolve to unmatched (see [`AttachKind`]).
    pub resolver: &'a dyn ContentResolver,
}

/// Run an action, threading `result`. Mirrors the Go action runtime; on error
/// the caller decides (only the top-level workflow observes the zeroed result,
/// which is why the normalizer uses `Classification::default` on any error).
///
/// Async because the four `attach_*` actions are I/O (a PostgreSQL query or a
/// TMDB HTTP call) once lane B′-4 wires them to [`ExecCtx::resolver`]. The
/// executor is mutually recursive, so each level returns a boxed future rather
/// than being a plain `async fn` (which would need an infinitely-sized state
/// machine).
///
/// The box is [`LocalBoxFuture`], not `BoxFuture`: `cel::Value` is not `Sync`,
/// so a `&ExecCtx` cannot be held across an await point in a `Send` future.
/// That is not a limitation in practice — the classifier is driven per-torrent
/// from a single task, and a caller that needs `Send` can drive it on a
/// current-thread runtime.
pub(crate) fn run_action<'a>(
    action: &'a Action,
    ctx: &'a ExecCtx<'a>,
    result: Classification,
) -> LocalBoxFuture<'a, Result<Classification, FlowError>> {
    Box::pin(async move {
        match action {
            Action::List(actions) => {
                let mut r = result;
                for a in actions {
                    r = run_action(a, ctx, r).await?;
                }
                Ok(r)
            }
            Action::SetContentType(ct) => {
                let mut r = result;
                r.content_type = Some(*ct);
                Ok(r)
            }
            Action::Delete(path) => Err(FlowError::runtime(path, FlowError::Delete)),
            Action::Unmatched(path) => Err(FlowError::runtime(path, FlowError::Unmatched)),
            Action::AddTag(_) => Ok(result),
            Action::FindMatch { actions, path } => {
                for a in actions {
                    match run_action(a, ctx, result.clone()).await {
                        Ok(res) => return Ok(res),
                        Err(e) if e.is_unmatched() => continue,
                        Err(e) => return Err(FlowError::runtime(path, e)),
                    }
                }
                Ok(result)
            }
            Action::IfElse {
                condition,
                if_action,
                else_action,
            } => {
                if eval_condition(condition, ctx, &result)? {
                    if let Some(a) = if_action {
                        return run_action(a, ctx, result).await;
                    }
                } else if let Some(a) = else_action {
                    return run_action(a, ctx, result).await;
                }
                Ok(result)
            }
            Action::RunWorkflow(names) => {
                let mut r = result;
                for name in names {
                    let wf = ctx
                        .workflows
                        .get(name)
                        .ok_or_else(|| FlowError::Cel(format!("workflow not found: {name}")))?;
                    r = run_action(wf, ctx, r).await?;
                }
                Ok(r)
            }
            Action::ParseDate => {
                let mut r = result;
                let parsed = parse_date(&ctx.input.name);
                if parsed.is_nil() {
                    return Err(FlowError::Unmatched);
                }
                r.date = parsed;
                Ok(r)
            }
            Action::ParseVideoContent => {
                let (attrs, err) = parse_video_content(ctx.input, &result);
                let mut r = result;
                if let Some(e) = err {
                    return Err(e);
                }
                if let Some(attrs) = attrs {
                    r.merge(attrs);
                }
                Ok(r)
            }
            Action::Attach(kind) => run_attach(*kind, ctx, result).await,
        }
    })
}

fn eval_condition(
    condition: &Condition,
    ctx: &ExecCtx<'_>,
    result: &Classification,
) -> Result<bool, FlowError> {
    match condition {
        Condition::And(conds) => {
            for c in conds {
                if !eval_condition(c, ctx, result)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Or(conds) => {
            for c in conds {
                if eval_condition(c, ctx, result)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Not(c) => Ok(!eval_condition(c, ctx, result)?),
        Condition::Expression(program) => {
            ctx.env
                .eval_bool(program, ctx.torrent_val, result, ctx.flags_val)
        }
    }
}

/// Which dependency an `attach_*` action consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttachKind {
    /// `attach_local_content_by_id` — Go
    /// `action_attach_local_content_by_id.go`.
    LocalContentById,
    /// `attach_local_content_by_search` — Go
    /// `action_attach_local_content_by_search.go`.
    LocalContentBySearch,
    /// `attach_tmdb_content_by_id` — Go `action_attach_tmdb_content_by_id.go`.
    TmdbContentById,
    /// `attach_tmdb_content_by_search` — Go
    /// `action_attach_tmdb_content_by_search.go`.
    TmdbContentBySearch,
}

/// Run one `attach_*` action against [`ExecCtx::resolver`].
///
/// Go's actions guard, call their dependency, and attach on success; a lookup
/// that finds nothing is `ErrUnmatched`, which `find_match` treats as "try the
/// next branch" rather than as a failure. A genuine backend error propagates.
///
/// All four kinds are implemented; each consults a different dependency and
/// fails differently, which is why they stay distinct rather than fusing into
/// one "attach" action.
async fn run_attach(
    kind: AttachKind,
    ctx: &ExecCtx<'_>,
    result: Classification,
) -> Result<Classification, FlowError> {
    match kind {
        AttachKind::LocalContentById => attach_local_by_id(ctx, result).await,
        AttachKind::LocalContentBySearch => attach_local_by_search(ctx, result).await,
        AttachKind::TmdbContentById => attach_tmdb_by_id(ctx, result).await,
        AttachKind::TmdbContentBySearch => attach_tmdb_by_search(ctx, result).await,
    }
}

/// Go `attach_local_content_by_id`: look the hinted content ref up by primary
/// key.
///
/// The guard is Go's exactly — a nil hint, or a hint without a content SOURCE,
/// is unmatched before any lookup. The hint's content type and id are only
/// meaningful alongside a source, so a source-less hint is not a lookup with
/// missing arguments; it is not a lookup at all.
async fn attach_local_by_id(
    ctx: &ExecCtx<'_>,
    result: Classification,
) -> Result<Classification, FlowError> {
    let Some(hint) = ctx.input.hint.as_ref() else {
        return Err(FlowError::Unmatched);
    };

    // Go: `Hint.IsNil() || !Hint.ContentSource.Valid`. Here both are plain
    // strings whose empty value is Go's nil, so emptiness is the same guard.
    if hint.content_type.is_empty() || hint.content_source.is_empty() {
        return Err(FlowError::Unmatched);
    }

    // An unparseable hint type is a malformed input, not a miss — but Go would
    // never have built such a hint, and treating it as unmatched keeps a bad
    // input from failing an otherwise-fine classification.
    let Ok(content_type) = hint.content_type.parse::<bitmagnet_model::ContentType>() else {
        return Err(FlowError::Unmatched);
    };

    match ctx
        .resolver
        .content_by_id(content_type, &hint.content_source, &hint.content_id)
        .await
    {
        // Go's ErrUnmatched: the row does not exist. A recoverable miss.
        Ok(None) => Err(FlowError::Unmatched),
        Ok(Some(content)) => {
            let mut result = result;
            result.attach_content(content);
            Ok(result)
        }
        // A backend failure is NOT a miss: it must surface as an error outcome
        // rather than letting `find_match` quietly try the next branch.
        Err(err) => Err(FlowError::Cel(err.to_string())),
    }
}

/// Go `attach_local_content_by_search`: full-text search, then a first-wins
/// Levenshtein pick over the candidate window.
///
/// 🚨 The split matters. The resolver returns the ORDERED candidate list and the
/// tie-break runs here, because `ts_rank` ties make the window's order a
/// database observation rather than a computable fact. Doing the selection
/// behind the seam would bake Go's coin-flip into the unobservable side of the
/// boundary and leave nothing to compare.
async fn attach_local_by_search(
    ctx: &ExecCtx<'_>,
    result: Classification,
) -> Result<Classification, FlowError> {
    // Go guards on both: without a content type there is nothing to search, and
    // without a base title there is nothing to search FOR.
    let (Some(content_type), Some(base_title)) =
        (result.content_type, result.base_title.as_deref())
    else {
        return Err(FlowError::Unmatched);
    };

    // Go's `model.Year` zero value is nil; the resolver takes an Option.
    let year = (result.date.year != 0).then_some(result.date.year);

    let Some(content_type) = to_model_content_type(content_type) else {
        return Err(FlowError::Unmatched);
    };

    let candidates = match ctx
        .resolver
        .content_by_search(content_type, base_title, year)
        .await
    {
        Ok(candidates) => candidates,
        Err(err) => return Err(FlowError::Cel(err.to_string())),
    };

    // Go scores each candidate against its title AND its original title, taking
    // the item's best of the two.
    let best = bitmagnet_textmatch::find_best_match(base_title, &candidates, |item| {
        let mut titles = vec![item.content.title.clone()];
        if let Some(original) = item.content.original_title.as_ref() {
            titles.push(original.clone());
        }
        titles
    });

    // Nothing within the distance threshold is Go's ErrUnmatched.
    let Some(best) = best else {
        return Err(FlowError::Unmatched);
    };

    let mut result = result;
    result.attach_content(best.content.clone());
    Ok(result)
}

/// Go `attach_tmdb_content_by_id`: resolve the hinted ref to a TMDB id, then
/// fetch that title's details.
///
/// 🚨 The guard is Go's, and it is **not** the same as
/// [`attach_local_by_id`]'s. `TorrentHint.ContentRef()` is valid when the hint
/// has a content TYPE and a content ID; the SOURCE may be absent. A source-less
/// ref then falls to the external-id branch, where `tmdb.ExternalSource` finds no
/// mapping for `""` and returns unmatched. Tightening the guard to require a
/// source would reach the same verdict by a different route — and would skip the
/// `/find` request Go makes whenever the source is a non-TMDB one.
async fn attach_tmdb_by_id(
    ctx: &ExecCtx<'_>,
    result: Classification,
) -> Result<Classification, FlowError> {
    let Some(hint) = ctx.input.hint.as_ref() else {
        return Err(FlowError::Unmatched);
    };

    // Go `TorrentHint.ContentRef()`: `!IsNil() && ContentID.Valid`, where
    // `IsNil()` is an absent content type.
    if hint.content_type.is_empty() || hint.content_id.is_empty() {
        return Err(FlowError::Unmatched);
    }

    // Go: the ref's type is overridden by the classification's when it has one.
    let ref_type = result
        .content_type
        .and_then(to_model_content_type)
        .or_else(|| hint.content_type.parse().ok());

    let Some(ref_type) = ref_type else {
        return Err(FlowError::Unmatched);
    };

    let tmdb_id = if hint.content_source == TMDB_SOURCE {
        // Go uses strconv.Atoi and treats a parse failure as unmatched.
        match hint.content_id.parse::<i64>() {
            Ok(id) => id,
            Err(_) => return Err(FlowError::Unmatched),
        }
    } else {
        let Some(external_source) = tmdb::external_source(ref_type, &hint.content_source) else {
            return Err(FlowError::Unmatched);
        };

        let response = ctx
            .resolver
            .tmdb_find_by_external_id(&tmdb::FindByIdRequest {
                external_source: external_source.to_owned(),
                external_id: hint.content_id.clone(),
                language: None,
            })
            .await
            .map_err(|err| FlowError::Cel(err.to_string()))?;

        // Go takes the FIRST entry of the array matching the ref's type; an
        // empty array is unmatched.
        let first = match ref_type {
            bitmagnet_model::ContentType::Movie | bitmagnet_model::ContentType::Xxx => {
                response.movie_results.first().map(|item| item.id)
            }
            bitmagnet_model::ContentType::TvShow => response.tv_results.first().map(|item| item.id),
            _ => None,
        };

        match first {
            Some(id) => id,
            None => return Err(FlowError::Unmatched),
        }
    };

    let content = match ref_type {
        bitmagnet_model::ContentType::Movie | bitmagnet_model::ContentType::Xxx => {
            tmdb_movie_by_id(ctx, tmdb_id).await?
        }
        bitmagnet_model::ContentType::TvShow => tmdb_tv_show_by_id(ctx, tmdb_id).await?,
        // Go's switch has no other arms.
        _ => return Err(FlowError::Unmatched),
    };

    let mut result = result;
    result.attach_content(content);
    Ok(result)
}

/// Go `attach_tmdb_content_by_search`: search TMDB for the base title, pick a
/// winner by first-wins Levenshtein, then fetch that title's details.
///
/// The same split as [`attach_local_by_search`] applies: the resolver hands back
/// TMDB's ordered `results` array and the tie-break runs here.
async fn attach_tmdb_by_search(
    ctx: &ExecCtx<'_>,
    result: Classification,
) -> Result<Classification, FlowError> {
    let Some(base_title) = result.base_title.as_deref() else {
        return Err(FlowError::Unmatched);
    };

    let year = (result.date.year != 0).then_some(result.date.year);
    let is_tv_show = result.content_type.and_then(to_model_content_type)
        == Some(bitmagnet_model::ContentType::TvShow);

    let content = if is_tv_show {
        let response = ctx
            .resolver
            .tmdb_search_tv(&tmdb::SearchTvRequest {
                query: base_title.to_owned(),
                include_adult: true,
                first_air_date_year: year,
                ..Default::default()
            })
            .await
            .map_err(|err| FlowError::Cel(err.to_string()))?;

        let best = bitmagnet_textmatch::find_best_match(base_title, &response.results, |item| {
            vec![item.name.clone(), item.original_name.clone()]
        });

        let Some(best) = best else {
            return Err(FlowError::Unmatched);
        };

        tmdb_tv_show_by_id(ctx, best.id).await?
    } else {
        // Go's `default` arm: anything that is not a tv_show, including an
        // unknown content type. A title with parsed episodes is a series even
        // when the type says otherwise, and Go refuses to call it a movie.
        if !result.episodes.is_empty() {
            return Err(FlowError::Unmatched);
        }

        let response = ctx
            .resolver
            .tmdb_search_movie(&tmdb::SearchMovieRequest {
                query: base_title.to_owned(),
                include_adult: true,
                year,
                ..Default::default()
            })
            .await
            .map_err(|err| FlowError::Cel(err.to_string()))?;

        let best = bitmagnet_textmatch::find_best_match(base_title, &response.results, |item| {
            vec![item.title.clone(), item.original_title.clone()]
        });

        let Some(best) = best else {
            return Err(FlowError::Unmatched);
        };

        tmdb_movie_by_id(ctx, best.id).await?
    };

    let mut result = result;
    result.attach_content(content);
    Ok(result)
}

/// Go `tmdbGetMovieByTMDBID`: `GET /movie/{id}`, with 404 mapped to unmatched.
async fn tmdb_movie_by_id(
    ctx: &ExecCtx<'_>,
    id: i64,
) -> Result<bitmagnet_model::Content, FlowError> {
    let details = ctx
        .resolver
        .tmdb_movie_details(&tmdb::MovieDetailsRequest {
            id,
            ..Default::default()
        })
        .await
        .map_err(|err| FlowError::Cel(err.to_string()))?;

    // Go: `errors.Is(err, tmdb.ErrNotFound)` becomes ErrUnmatched, so
    // `find_match` moves on. Any other failure stays an error.
    let Some(details) = details else {
        return Err(FlowError::Unmatched);
    };

    // A date the transform rejects is an error outcome in Go, not a miss.
    details
        .into_content()
        .map_err(|err| FlowError::Cel(err.to_string()))
}

/// Go `tmdbGetTVShowByTMDBID`: `GET /tv/{id}?append_to_response=external_ids`.
async fn tmdb_tv_show_by_id(
    ctx: &ExecCtx<'_>,
    id: i64,
) -> Result<bitmagnet_model::Content, FlowError> {
    let details = ctx
        .resolver
        .tmdb_tv_details(&tmdb::TvDetailsRequest {
            series_id: id,
            // Go always asks for this, and the transform reads imdb/tvdb ids out
            // of it. Omitting it would be a different request AND lose the ids.
            append_to_response: vec!["external_ids".to_owned()],
            ..Default::default()
        })
        .await
        .map_err(|err| FlowError::Cel(err.to_string()))?;

    let Some(details) = details else {
        return Err(FlowError::Unmatched);
    };

    details
        .into_content()
        .map_err(|err| FlowError::Cel(err.to_string()))
}

/// Bridge the classifier's own [`ContentType`] to the model crate's.
///
/// The two crates each own an enum for the same closed vocabulary, and the
/// string form is what they agree on — it is also what the tape and Go's JSON
/// carry, so routing through it keeps a single spelling authoritative instead of
/// adding a second hand-maintained mapping that could silently drift.
fn to_model_content_type(content_type: ContentType) -> Option<bitmagnet_model::ContentType> {
    content_type.as_str().parse().ok()
}
