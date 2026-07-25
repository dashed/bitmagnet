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
use crate::resolver::ContentResolver;
use crate::result::Classification;

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
    /// The four `attach_*` actions, still fused into one variant. Under the
    /// flags-off corpus every one of them resolves to `unmatched` (contract
    /// §2.2): the local-by-id branch's `LocalSearch.ContentByID` is mocked to
    /// `ErrUnmatched`, and the other three are behind `flags` gates that are
    /// false, so their `if_else` runs the `unmatched` else-branch and they are
    /// never entered.
    ///
    /// 🔜 Lane B′-4 splits this into `AttachLocalContentById`,
    /// `AttachLocalContentBySearch`, `AttachTmdbContentById` and
    /// `AttachTmdbContentBySearch`, each reading [`ExecCtx::resolver`]. B′-0
    /// deliberately leaves the fusion in place so that the async/`Content`
    /// refactor is provably behaviour-preserving: with the null resolver this
    /// variant short-circuits exactly as before, keeping the 330 goldens and the
    /// 119,991-name replay bit-identical.
    AttachUnmatched,
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
            "attach_local_content_by_id"
            | "attach_local_content_by_search"
            | "attach_tmdb_content_by_id"
            | "attach_tmdb_content_by_search" => Ok(Action::AttachUnmatched),
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
    /// Unread in this lane: [`Action::AttachUnmatched`] still short-circuits
    /// before consulting it. Lane B′-4 splits that variant into the four real
    /// attach actions, and they read it from here.
    #[allow(dead_code)]
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
            Action::AttachUnmatched => Err(FlowError::Unmatched),
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
