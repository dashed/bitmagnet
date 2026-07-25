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

use std::collections::BTreeMap;
use std::sync::Arc;

use cel::to_value;
use serde_json::Value as Json;

pub use errors::FlowError;
pub use model::{ClassifierInput, InputFile, InputHint};
pub use resolver::{ContentResolver, ContentResultItem, NullContentResolver, ResolveError};
pub use result::{Classification, Outcome};
pub use source::{core_config_digest, FlagType, FlagValue, Source, SourceError};

use cel_value::build_cel_torrent;
use engine::{compile_workflows, run_action, Action, CompileError, ExecCtx};
use env::{flags_value, Env, EnvError};
use model::ContentType;
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
    pub async fn run(&self, workflow: &str, flags: &Flags, input: &ClassifierInput) -> Json {
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
                return to_expected_json(
                    &Classification::default(),
                    &Outcome::Error(format!("serialize torrent: {e}")),
                )
            }
        };

        let Some(wf) = self.workflows.get(workflow) else {
            return to_expected_json(
                &Classification::default(),
                &Outcome::Error(format!("workflow not found: {workflow}")),
            );
        };

        // Initial result — apply the hint (`runner.Run`: `cl.ApplyHint`). The
        // corpus hints carry only a content type (no episode/language/video
        // attributes), and the torrents carry no attachable `Contents`.
        let mut result = Classification::default();
        if let Some(hint) = &input.hint {
            if let Some(ct) = ContentType::parse(&hint.content_type) {
                result.content_type = Some(ct);
            }
        }

        let ctx = ExecCtx {
            env: &self.env,
            torrent_val: &torrent_val,
            flags_val: &flags_val,
            input,
            workflows: &self.workflows,
            resolver: self.resolver.as_ref(),
        };

        let (final_result, outcome) = match run_action(wf, &ctx, result).await {
            Ok(r) => (r, Outcome::Classified),
            Err(e) if e.is_delete() => (Classification::default(), Outcome::Deleted(e.to_string())),
            Err(e) if e.is_unmatched() => {
                (Classification::default(), Outcome::Unmatched(e.to_string()))
            }
            Err(e) => (Classification::default(), Outcome::Error(e.to_string())),
        };

        to_expected_json(&final_result, &outcome)
    }
}
