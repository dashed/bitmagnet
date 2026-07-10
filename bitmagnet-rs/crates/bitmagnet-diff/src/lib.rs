//! Language-agnostic differential parity harness for shared JSONL fixtures,
//! pluggable subsystem drivers, canonical normalization, and bounded diffs.
//!
//! Each non-blank fixture line is one JSON object with this schema:
//!
//! ```json
//! {"id":"<string>","subsystem":"<string>","input":<json>,"expected":<json>}
//! ```
//!
//! A driver consumes `input` for its named subsystem. The runner canonicalizes
//! both the driver's output and `expected`, compares them, and records counts
//! plus the first configured number of differences.

pub mod fixture;
pub mod driver;
pub mod normalize;
pub mod runner;

pub use driver::Driver;
pub use fixture::{load_file, load_jsonl, Fixture};
pub use normalize::{canonical, Normalizer};
pub use runner::{run, Diff, Options, Report};
