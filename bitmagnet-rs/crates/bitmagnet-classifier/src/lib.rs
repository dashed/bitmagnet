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
//! to `unmatched` on the flags-off corpus path (contract §2.2); the TMDB client
//! (decision #3) lands as a later milestone.

mod cel_value;
mod engine;
mod env;
mod errors;
mod model;
mod parsers;
mod result;
mod source;

use std::collections::BTreeMap;

use cel::to_value;
use serde_json::Value as Json;

pub use errors::FlowError;
pub use model::{ClassifierInput, InputFile, InputHint};
pub use result::{Classification, Outcome};
pub use source::{FlagType, FlagValue, Source, SourceError};

use cel_value::build_cel_torrent;
use engine::{compile_workflows, run_action, Action, CompileError, ExecCtx};
use env::{flags_value, Env, EnvError};
use model::ContentType;
use result::to_expected_json;

/// Runtime flag overrides passed to [`Classifier::run`] (`classifier.Flags`).
pub type Flags = BTreeMap<String, FlagValue>;

/// A compiled classifier: the CEL env, the compiled workflows, and the
/// flag definitions + compiled defaults.
pub struct Classifier {
    env: Env,
    workflows: std::collections::HashMap<String, Action>,
    flag_definitions: BTreeMap<String, FlagType>,
    default_flags: BTreeMap<String, FlagValue>,
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
    /// Compile the embedded `classifier.core.yml`.
    pub fn from_core() -> Result<Classifier, ClassifierError> {
        Classifier::compile(Source::load_core()?)
    }

    /// Compile a parsed source.
    pub fn compile(source: Source) -> Result<Classifier, ClassifierError> {
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
    #[must_use]
    pub fn run(&self, workflow: &str, flags: &Flags, input: &ClassifierInput) -> Json {
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
        };

        let (final_result, outcome) = match run_action(wf, &ctx, result) {
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
