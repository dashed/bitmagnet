use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One differential test case from a JSONL fixture corpus.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Fixture {
    pub id: String,
    pub subsystem: String,
    pub input: Value,
    pub expected: Value,
}

/// Load newline-delimited fixtures from a buffered reader.
///
/// Blank lines are ignored, while read and parse errors include their
/// one-based line number.
pub fn load_jsonl<R: BufRead>(reader: R) -> Result<Vec<Fixture>> {
    let mut fixtures = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.with_context(|| format!("read JSONL line {line_number}"))?;
        if line.trim().is_empty() {
            continue;
        }

        let fixture = serde_json::from_str(&line)
            .with_context(|| format!("parse JSONL line {line_number}"))?;
        fixtures.push(fixture);
    }

    Ok(fixtures)
}

/// Load newline-delimited fixtures from a file path.
pub fn load_file<P: AsRef<Path>>(path: P) -> Result<Vec<Fixture>> {
    let path = path.as_ref();
    let file = File::open(path).with_context(|| format!("open fixture file {}", path.display()))?;

    load_jsonl(BufReader::new(file))
        .with_context(|| format!("load fixture file {}", path.display()))
}
