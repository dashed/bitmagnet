use std::fmt;

use crate::driver::Driver;
use crate::fixture::Fixture;
use crate::normalize::{canonical, Normalizer};

/// Controls normalization and diagnostic collection.
pub struct Options {
    /// Function used to canonicalize actual and expected JSON values.
    pub normalizer: Normalizer,
    /// Maximum number of errored or mismatched fixtures retained in `diffs`.
    pub max_diffs: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            normalizer: canonical,
            max_diffs: 20,
        }
    }
}

/// One mismatched or errored fixture.
#[derive(Debug, Clone)]
pub struct Diff {
    pub id: String,
    pub expected: String,
    pub got: String,
    pub err: Option<String>,
}

/// Summary of a differential harness run.
#[derive(Debug, Default)]
pub struct Report {
    pub total: usize,
    pub ran: usize,
    pub matched: usize,
    pub mismatched: usize,
    pub errored: usize,
    pub diffs: Vec<Diff>,
}

impl Report {
    /// Whether every fixture run matched its expected value.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.mismatched == 0 && self.errored == 0
    }
}

impl fmt::Display for Report {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "total={} ran={} matched={} mismatched={} errored={}",
            self.total, self.ran, self.matched, self.mismatched, self.errored
        )?;

        for (index, diff) in self.diffs.iter().enumerate() {
            write!(formatter, "\n\ndiff {}: id={:?}", index + 1, diff.id)?;
            if let Some(err) = &diff.err {
                write!(formatter, "\n  error: {err}")?;
            }
            if !diff.expected.is_empty() {
                write!(formatter, "\n  expected: {}", diff.expected)?;
            }
            if !diff.got.is_empty() {
                write!(formatter, "\n  got: {}", diff.got)?;
            }
        }

        let failed = self.mismatched.saturating_add(self.errored);
        let omitted = failed.saturating_sub(self.diffs.len());
        if omitted > 0 {
            write!(formatter, "\n\n... {omitted} additional diffs omitted")?;
        }

        Ok(())
    }
}

/// Run a driver against every fixture for its subsystem.
#[must_use]
pub fn run<D: Driver>(fixtures: &[Fixture], driver: &D, opts: Options) -> Report {
    let mut report = Report {
        total: fixtures.len(),
        ..Report::default()
    };

    let subsystem = driver.subsystem();
    for fixture in fixtures {
        if fixture.subsystem != subsystem {
            continue;
        }

        report.ran += 1;
        let got = match driver.run(&fixture.input) {
            Ok(got) => got,
            Err(err) => {
                report.errored += 1;
                if report.diffs.len() < opts.max_diffs {
                    report.diffs.push(Diff {
                        id: fixture.id.clone(),
                        expected: String::new(),
                        got: String::new(),
                        err: Some(err.to_string()),
                    });
                }
                continue;
            }
        };

        let normalized_got = (opts.normalizer)(&got);
        let normalized_expected = (opts.normalizer)(&fixture.expected);
        if normalized_got == normalized_expected {
            report.matched += 1;
            continue;
        }

        report.mismatched += 1;
        if report.diffs.len() < opts.max_diffs {
            report.diffs.push(Diff {
                id: fixture.id.clone(),
                expected: normalized_expected.to_string(),
                got: normalized_got.to_string(),
                err: None,
            });
        }
    }

    report
}
