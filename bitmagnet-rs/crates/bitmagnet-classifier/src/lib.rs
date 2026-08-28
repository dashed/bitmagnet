//! Phase-3 Lane C — content classifier (CEL-driven).
//!
//! Ports the Go `internal/classifier` engine: the YAML workflow DSL is compiled
//! into an action/condition tree, conditions evaluate CEL expressions against a
//! serde-bound `torrent`/`result`/`flags` binding, and the terminal result is
//! normalized to the frozen corpus `expected` schema. Contract:
//! `docs/dev/rust-rewrite/phase3-contracts.md` §2.
//!
//! Scope: the CEL engine (env binding, the YAML action-tree loader, the 13
//! actions + 4 conditions framework, the three custom CEL functions core.yml
//! calls — `sum`, `join`, `matches`), the date parser, and `parse_video_content`
//! via Lane R's parser (title/year/episode/language/video attributes). The Rust
//! classifier matches all 330 flags-off golden fixtures exactly (see the
//! `bitmagnet-diff` `classifier_pair` gate). The four `attach_*` actions resolve
//! to `unmatched` on the flags-off corpus path (contract §2.2).
//!
//! # The dependency seam (B′-0)
//!
//! Production Go runs this classifier with `local_search_enabled` /
//! `apis_enabled` / `tmdb_enabled` all **true**, so the `attach_*` actions do
//! real I/O against PostgreSQL and the TMDB API. [`ContentResolver`] is the
//! injection point for that I/O: it is passed to [`Classifier::compile`],
//! carried on the executor's context, and consumed by the attach actions. This
//! lane lands the seam and nothing else — [`NullContentResolver`] is the
//! default, so every attach still misses and the flags-off parity evidence is
//! unchanged. See [`resolver`] for why `content_by_search` must return an
//! *ordered candidate list*.

mod cel_value;
mod engine;
mod env;
mod errors;
mod model;
mod parsers;
pub mod resolver;
mod result;
mod source;
pub mod tape_corpus;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use bitmagnet_tape::ActionEntry;
use cel::to_value;
use serde_json::Value as Json;

pub use errors::FlowError;
pub use model::{ClassifierInput, ContentType, InputContent, InputFile, InputHint};
pub use resolver::{ContentResolver, ContentResultItem, NullContentResolver, ResolveError};
pub use result::{Classification, NormalizedClassifierDate, NormalizedClassifierResult, Outcome};
pub use source::{
    core_config_digest, FlagType, FlagValue, Source, SourceError,
    TAPE_EVIDENCE_ACTION_ENTRIES_WORKFLOW, TAPE_EVIDENCE_DELETED_WORKFLOW,
    TAPE_EVIDENCE_UNMATCHED_WORKFLOW,
};

use cel_value::build_cel_torrent;
use engine::{compile_workflows, run_action, Action, CompileError, ExecCtx};
use env::{flags_value, Env, EnvError};
use result::to_expected_json;

/// Runtime flag overrides passed to [`Classifier::run`] (`classifier.Flags`).
pub type Flags = BTreeMap<String, FlagValue>;

/// A compiled classifier: the CEL env, the compiled workflows, the flag
/// definitions + compiled defaults, and the injected [`ContentResolver`].
pub struct Classifier {
    env: Env,
    workflows: std::collections::HashMap<String, Action>,
    flag_definitions: BTreeMap<String, FlagType>,
    default_flags: BTreeMap<String, FlagValue>,
    /// Go's `classifier.dependencies` (`LocalSearch` + `tmdb.Client`), collapsed
    /// into one trait object. `Arc` because a compiled classifier is shared
    /// across the processor's worker tasks.
    resolver: Arc<dyn ContentResolver>,
}

/// Any failure building a [`Classifier`].
#[derive(Debug, thiserror::Error)]
pub enum ClassifierError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Env(#[from] EnvError),
    #[error(transparent)]
    Compile(#[from] CompileError),
}

/// Go `runner.Run`'s pre-attach (`runner.go:53-68`): reuse an already-known
/// content row instead of looking it up again.
///
/// 🚨 This is T9, and it is not an optimisation — it is a behavioural fork.
/// Attaching here makes `result.hasAttachedContent` true, and
/// `classifier.core.yml:92` gates the whole enrichment branch on
/// `!result.hasAttachedContent`. So a torrent whose content is already attached
/// performs **no local search and no TMDB call at all**, while one without
/// performs the full chain. A port that skipped this would re-derive content the
/// original classification simply reused: a different write set, and a different
/// set of dependency calls.
///
/// The hint that reaches here usually did NOT come from the `torrent_hints` row
/// as stored. `processor.go:119-134` synthesises it from the first sourced
/// `torrent_contents` association whenever the stored hint has **no** content
/// source — so a NULL source in the database is the precondition for this path,
/// not evidence against it.
///
/// Go's guards, all of them:
/// * the hint must carry a content SOURCE (a bare content type is not enough,
///   which is what makes this distinct from `attach_local_content_by_id`);
/// * the association must match the hint on type, source AND id;
/// * `tc.Content.Source == tc.ContentSource` — the association's content must
///   actually be hydrated. An unloaded association has a zero-valued `Content`
///   whose source is empty, and attaching that would blank the result.
///
/// First match wins, mirroring Go's `break`.
fn pre_attach_existing_content(
    result: &mut Classification,
    hint: &InputHint,
    contents: &[InputContent],
) {
    if hint.content_source.is_empty() {
        return;
    }

    for association in contents {
        if association.content_type != hint.content_type
            || association.content_source != hint.content_source
            || association.content_id != hint.content_id
        {
            continue;
        }

        let Some(content) = association.content.as_ref() else {
            continue;
        };

        // Go's hydration check.
        if content.source != association.content_source {
            continue;
        }

        result.attach_content(content.clone());
        return;
    }
}

impl Classifier {
    /// Compile the embedded `classifier.core.yml` with the flags-off
    /// [`NullContentResolver`].
    ///
    /// This is the parity-gate entry point: every lookup misses, so no content
    /// is ever attached.
    pub fn from_core() -> Result<Classifier, ClassifierError> {
        Classifier::compile(Source::load_core()?, Arc::new(NullContentResolver))
    }

    /// Compile the embedded `classifier.core.yml` against a specific
    /// [`ContentResolver`] — the live PG/TMDB backends, or a recording/replaying
    /// tape.
    pub fn from_core_with(
        resolver: Arc<dyn ContentResolver>,
    ) -> Result<Classifier, ClassifierError> {
        Classifier::compile(Source::load_core()?, resolver)
    }

    /// Compile core plus the reserved T1 acquisition workflows for tape replay.
    /// Normal serving constructors intentionally omit these workflows.
    pub fn from_core_with_tape_evidence(
        resolver: Arc<dyn ContentResolver>,
    ) -> Result<Classifier, ClassifierError> {
        Classifier::compile(Source::load_core_with_tape_evidence()?, resolver)
    }

    /// Compile a parsed source against a [`ContentResolver`].
    pub fn compile(
        source: Source,
        resolver: Arc<dyn ContentResolver>,
    ) -> Result<Classifier, ClassifierError> {
        let env = Env::build(&source)?;
        let workflows = compile_workflows(
            &source
                .workflows
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )?;
        Ok(Classifier {
            env,
            workflows,
            flag_definitions: source.flag_definitions,
            default_flags: source.flags,
            resolver,
        })
    }

    /// The flags-off corpus flag set (`local_search_enabled` / `apis_enabled` /
    /// `tmdb_enabled` all false), for the parity gate.
    #[must_use]
    pub fn flags_off() -> Flags {
        BTreeMap::from([
            ("local_search_enabled".to_string(), FlagValue::Bool(false)),
            ("apis_enabled".to_string(), FlagValue::Bool(false)),
            ("tmdb_enabled".to_string(), FlagValue::Bool(false)),
        ])
    }

    /// Run a workflow over an input and normalize to the corpus `expected` JSON.
    ///
    /// Async because the `attach_*` actions become I/O against
    /// [`Self::resolver`]. With the [`NullContentResolver`] the returned future
    /// never yields, so a caller that is still synchronous can drive it to
    /// completion with `futures::executor::block_on` without a runtime.
    #[must_use]
    /// Classify, returning the normalized corpus `expected` object.
    ///
    /// A thin wrapper over [`Self::classify`]; the two must stay in step, which
    /// is why this delegates rather than duplicating the run.
    pub async fn run(&self, workflow: &str, flags: &Flags, input: &ClassifierInput) -> Json {
        let (result, outcome) = self.classify(workflow, flags, input).await;
        to_expected_json(&result, &outcome)
    }

    /// Classify and return the ordered attach actions entered along the way.
    ///
    /// This is the action-level half of the production tape gate. It is kept as
    /// a sibling of [`Self::run`] so normal classifier callers do not acquire a
    /// tape-shaped return type merely because the evidence harness needs one.
    pub async fn run_with_action_entries(
        &self,
        workflow: &str,
        flags: &Flags,
        input: &ClassifierInput,
    ) -> (Json, Vec<ActionEntry>) {
        let (result, outcome, action_entries) = self
            .classify_with_action_entries(workflow, flags, input)
            .await;
        (to_expected_json(&result, &outcome), action_entries)
    }

    /// Classify, returning the STRUCTURED result.
    ///
    /// 🔑 Callers that need the attached content must use this, not
    /// [`Self::run`]. The normalized object is the frozen corpus `expected`
    /// schema (contract §2.1/§2.3): it reports `contentAttached` as a bare
    /// boolean and carries no content row, because the flags-off corpus can
    /// never attach one. Going through it therefore loses exactly the
    /// information the write-set materializer needs, which is what made
    /// attached content "unsupported" there.
    pub async fn classify(
        &self,
        workflow: &str,
        flags: &Flags,
        input: &ClassifierInput,
    ) -> (Classification, Outcome) {
        let (result, outcome, _) = self
            .classify_with_action_entries(workflow, flags, input)
            .await;
        (result, outcome)
    }

    /// Classify and return both the structured result and the exact ordered
    /// attach-action trace. The same-input processor rerun gate needs both;
    /// callers that only need the normalized corpus object should use
    /// [`Self::run_with_action_entries`].
    pub async fn classify_with_action_entries(
        &self,
        workflow: &str,
        flags: &Flags,
        input: &ClassifierInput,
    ) -> (Classification, Outcome, Vec<ActionEntry>) {
        let action_entries = RefCell::new(Vec::new());

        // Merge runtime flags over the compiled defaults, resolving one value
        // per defined flag (`runner.Run`).
        let mut merged: BTreeMap<String, FlagValue> = BTreeMap::new();
        for name in self.flag_definitions.keys() {
            if let Some(v) = flags.get(name).or_else(|| self.default_flags.get(name)) {
                merged.insert(name.clone(), v.clone());
            }
        }
        let flags_val = flags_value(&merged);

        let torrent_val = match to_value(build_cel_torrent(input)) {
            Ok(v) => v,
            Err(e) => {
                return (
                    Classification::default(),
                    Outcome::Error(format!("serialize torrent: {e}")),
                    action_entries.into_inner(),
                )
            }
        };

        let Some(wf) = self.workflows.get(workflow) else {
            return (
                Classification::default(),
                Outcome::Error(format!("workflow not found: {workflow}")),
                action_entries.into_inner(),
            );
        };

        // Initial result, exactly as `runner.Run` builds it: apply the hint,
        // then pre-attach an already-known content row. Order is load-bearing —
        // `AttachContent` overwrites the content type, so a pre-attach must be
        // able to override what the hint just set.
        let mut result = Classification::default();
        if let Some(hint) = &input.hint {
            // Go guards `if !t.Hint.IsNil()`, i.e. the hint has a content type.
            if !hint.content_type.is_empty() {
                result.apply_hint(hint);
                pre_attach_existing_content(&mut result, hint, &input.contents);
            }
        }

        let ctx = ExecCtx {
            env: &self.env,
            torrent_val: &torrent_val,
            flags_val: &flags_val,
            input,
            workflows: &self.workflows,
            action_entries: &action_entries,
            resolver: self.resolver.as_ref(),
        };

        let classified = match run_action(wf, &ctx, result).await {
            Ok(r) => (r, Outcome::Classified),
            Err(e) if e.is_delete() => (Classification::default(), Outcome::Deleted(e.to_string())),
            Err(e) if e.is_unmatched() => {
                (Classification::default(), Outcome::Unmatched(e.to_string()))
            }
            Err(e) => (Classification::default(), Outcome::Error(e.to_string())),
        };

        (classified.0, classified.1, action_entries.into_inner())
    }
}
