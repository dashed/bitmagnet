//! Port of `internal/classifier/parsers/date.go` — the date lexer. This is
//! classifier-owned (it lives under `internal/classifier/parsers`), so Lane C
//! ports it directly rather than depending on Lane R.
//!
//! The word/non-word char predicate is Lane R's `goclass::is_word_char`, so the
//! two parsers agree on tokenization AND both agree with Go. Rust's
//! `is_alphabetic() || is_numeric()` is 12,322 code points wider than Go's
//! `unicode.IsLetter || unicode.IsDigit` — it accepts `² ³ ¹ ¼ ½ ¾ ①②③` and the
//! rest of `No`/`Nl`/`Other_Alphabetic`, which Go treats as separators. This
//! runs on every torrent name at classify time and feeds `tv_show` inference
//! through `Date::is_valid`, so it is the highest-volume of the divergences.

use crate::model::Date;

/// A minimal char-cursor lexer (`internal/lexer/lexer.go`) — just the two
/// `ReadWhile` variants + EOF the date parser needs.
struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(s: &str) -> Self {
        Lexer {
            chars: s.chars().collect(),
            pos: 0,
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.chars.len()
    }

    fn read_while(&mut self, f: impl Fn(char) -> bool) -> String {
        let mut out = String::new();
        while self.pos < self.chars.len() && f(self.chars[self.pos]) {
            out.push(self.chars[self.pos]);
            self.pos += 1;
        }
        out
    }
}

fn is_word_char(c: char) -> bool {
    bitmagnet_release::goclass::is_word_char(c)
}

/// `ParseDate` — scan the string for the first valid embedded date.
#[must_use]
pub(crate) fn parse_date(input: &str) -> Date {
    let parts = lex_date_parts(input);
    lex_date(&parts)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PartFormat {
    Digit1,
    Digits2,
    Digits4,
    StrMonth,
    WordChars,
    NonWordChars,
}

struct DatePart {
    date: Date,
    format: PartFormat,
    literal: String,
}

impl DatePart {
    fn is_nil(&self) -> bool {
        self.date.is_nil()
    }
}

fn str_month(s: &str) -> Option<u8> {
    Some(match s.to_lowercase().as_str() {
        "jan" | "january" => 1,
        "feb" | "february" => 2,
        "mar" | "march" => 3,
        "apr" | "april" => 4,
        "may" => 5,
        "jun" | "june" => 6,
        "jul" | "july" => 7,
        "aug" | "august" => 8,
        "sep" | "september" => 9,
        "oct" | "october" => 10,
        "nov" | "november" => 11,
        "dec" | "december" => 12,
        _ => return None,
    })
}

fn is_separator(s: &str) -> bool {
    matches!(s, "." | "-" | "/" | " ")
}

fn lex_date_parts(input: &str) -> Vec<DatePart> {
    let mut lexer = Lexer::new(input);
    let mut parts = Vec::new();
    while !lexer.is_eof() {
        parts.push(lex_date_part(&mut lexer));
    }
    parts
}

fn lex_date_part(lexer: &mut Lexer) -> DatePart {
    let word = lexer.read_while(is_word_char);
    if word.is_empty() {
        let non = lexer.read_while(|c| !is_word_char(c));
        return DatePart {
            date: Date::default(),
            format: PartFormat::NonWordChars,
            literal: non,
        };
    }

    if let Some(m) = str_month(&word) {
        return DatePart {
            date: Date {
                month: m,
                ..Date::default()
            },
            format: PartFormat::StrMonth,
            literal: word,
        };
    }

    let is_ascii_digits = |s: &str| s.chars().all(|c| c.is_ascii_digit());
    if word.len() == 1 && is_ascii_digits(&word) {
        let i: u8 = word.parse().unwrap_or(0);
        return DatePart {
            date: Date {
                day: i,
                month: i,
                ..Date::default()
            },
            format: PartFormat::Digit1,
            literal: word,
        };
    }
    if word.len() == 2 && is_ascii_digits(&word) {
        let i: u16 = word.parse().unwrap_or(0);
        let mut date = Date {
            year: 2000 + i,
            ..Date::default()
        };
        if (1..=12).contains(&i) {
            date.month = i as u8;
        }
        if (1..=31).contains(&i) {
            date.day = i as u8;
        }
        return DatePart {
            date,
            format: PartFormat::Digits2,
            literal: word,
        };
    }
    if word.len() == 4 && is_ascii_digits(&word) {
        let i: u16 = word.parse().unwrap_or(0);
        return DatePart {
            date: Date {
                year: i,
                ..Date::default()
            },
            format: PartFormat::Digits4,
            literal: word,
        };
    }

    DatePart {
        date: Date::default(),
        format: PartFormat::WordChars,
        literal: word,
    }
}

const MIN_PARTS: usize = 5;

/// Port of `dateLexer.lexDate` — the index-walking state machine. Faithfully
/// reproduces the Go `for` loop with in-body index advances.
fn lex_date(parts: &[DatePart]) -> Date {
    if parts.len() < MIN_PARTS {
        return Date::default();
    }
    let bound = parts.len() - MIN_PARTS + 1;
    let mut is_start_or_word_break = true;
    let mut i = 0;

    while i < bound {
        let part1 = &parts[i];
        if !is_start_or_word_break {
            if part1.format == PartFormat::NonWordChars {
                is_start_or_word_break = true;
            }
            i += 1;
            continue;
        }

        if !part1.is_nil() {
            i += 1;
            let sep = &parts[i];
            if sep.format == PartFormat::NonWordChars {
                if is_separator(&sep.literal) {
                    i += 1;
                    let part2 = &parts[i];
                    if !part2.is_nil() {
                        i += 1;
                        let sep2 = &parts[i];
                        if sep2.literal != sep.literal {
                            is_start_or_word_break = sep2.format == PartFormat::NonWordChars;
                            i += 1;
                            continue;
                        }
                        i += 1;
                        let part3 = &parts[i];
                        if !part3.is_nil()
                            && (i == parts.len() - 1
                                || parts[i + 1].format == PartFormat::NonWordChars)
                        {
                            let date = find_first_valid_date(part1.date, part2.date, part3.date);
                            if !date.is_nil() {
                                return date;
                            }
                            is_start_or_word_break = false;
                            i += 1;
                            continue;
                        }
                        is_start_or_word_break = part3.format == PartFormat::NonWordChars;
                        i += 1;
                        continue;
                    }
                    is_start_or_word_break = part2.format == PartFormat::NonWordChars;
                    i += 1;
                    continue;
                }
                is_start_or_word_break = true;
                i += 1;
                continue;
            }
            is_start_or_word_break = false;
            i += 1;
            continue;
        }
        is_start_or_word_break = part1.format == PartFormat::NonWordChars;
        i += 1;
    }

    Date::default()
}

/// `findFirstValidDate` — Y-M-D, then D-M-Y, then M-D-Y.
fn find_first_valid_date(p1: Date, p2: Date, p3: Date) -> Date {
    if p1.year != 0 && p2.month != 0 && p3.day != 0 {
        let d = Date {
            year: p1.year,
            month: p2.month,
            day: p3.day,
        };
        if d.is_valid() {
            return d;
        }
    }
    if p1.day != 0 && p2.month != 0 && p3.year != 0 {
        let d = Date {
            year: p3.year,
            month: p2.month,
            day: p1.day,
        };
        if d.is_valid() {
            return d;
        }
    }
    if p1.month != 0 && p2.day != 0 && p3.year != 0 {
        let d = Date {
            year: p3.year,
            month: p1.month,
            day: p2.day,
        };
        if d.is_valid() {
            return d;
        }
    }
    Date::default()
}

#[cfg(test)]
mod tests {
    use super::parse_date;
    use crate::model::Date;

    fn ymd(year: u16, month: u8, day: u8) -> Date {
        Date { year, month, day }
    }

    #[test]
    fn parses_the_date_corpus_formats() {
        // Cases taken verbatim from testdata/parity/classifier date fixtures.
        assert_eq!(
            parse_date("Daily Probe 2020-01-15 1080p HDTV x264-DATE.mkv"),
            ymd(2020, 1, 15)
        );
        assert_eq!(
            parse_date("Daily Probe 15-01-2020 1080p HDTV x264-DATE.mkv"),
            ymd(2020, 1, 15)
        );
        assert_eq!(
            parse_date("Daily Probe 15-Jan-2020 1080p HDTV x264-DATE.mkv"),
            ymd(2020, 1, 15)
        );
        assert_eq!(
            parse_date("Daily Probe Jan-15-2020 1080p HDTV x264-DATE.mkv"),
            ymd(2020, 1, 15)
        );
        assert_eq!(
            parse_date("Daily.Probe.2021.09.11.720p.WEBDL.x264-DATE.mkv"),
            ymd(2021, 9, 11)
        );
        assert_eq!(
            parse_date("Daily Probe 2022 12 31 1080p WEBRip x264-DATE.mkv"),
            ymd(2022, 12, 31)
        );
        assert_eq!(
            parse_date("Daily Probe 2019/07/16 1080p TV x264-DATE.mkv"),
            ymd(2019, 7, 16)
        );
        assert_eq!(
            parse_date("Daily.Probe.23.02.01.1080p.WEBRip.x265-DATE.mkv"),
            ymd(2023, 2, 1)
        );
    }

    #[test]
    fn returns_nil_for_names_without_a_date() {
        assert!(parse_date("Synthetic Story.m4b").is_nil());
        assert!(parse_date("Some Album Discography FLAC").is_nil());
        assert!(parse_date("").is_nil());
    }

    /// The audit's headline date defect, spelled out so a regression is legible
    /// without decoding the fixture: a divergent char gluing the title to the
    /// year made the wider Rust predicate merge what Go splits into two words,
    /// and the whole date vanished (which then fed `tv_show` inference through
    /// `Date::is_valid`). The go-oracle probes cover these shapes exhaustively;
    /// keep a legible handful inline.
    #[test]
    fn divergent_chars_no_longer_swallow_dates() {
        for input in [
            "Show²2019-12-31",
            "2019-12-31½Show",
            "Show①2019-12-31",
            "2019-12-31¾Show",
        ] {
            assert_eq!(parse_date(input), ymd(2019, 12, 31), "input = {input:?}");
        }
        // Agreement in both directions: shapes Go rejects must stay rejected
        // (Go's date grammar does not assemble `2020 01 02` from separator
        // splits, so neither may the port).
        assert!(parse_date("2020²01²02").is_nil());
        assert!(parse_date("2020½01½02").is_nil());
    }

    /// Behavioural parity for the Go-pinned word-char predicate, against
    /// results captured from the production Go binary.
    ///
    /// The date lexer splits on word/non-word boundaries, so a predicate that
    /// is wider than Go's merges what Go sees as `<digits><sep><digits>` into a
    /// single word run and the date stops parsing. `² ³ ¼ ½ ①` and the other
    /// 12,322 code points Rust calls alphanumeric are exactly that case. This
    /// runs on every torrent name at classify time and feeds `tv_show`
    /// inference through `Date::is_valid`.
    #[test]
    fn parse_date_matches_go_on_every_probe() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/unicode/go-oracle.jsonl"
        );
        let raw = std::fs::read_to_string(path).expect("read go-oracle.jsonl");
        let mut failures = Vec::new();
        let mut n = 0usize;
        for line in raw.lines().filter(|l| !l.trim().is_empty()) {
            n += 1;
            let v: serde_json::Value = serde_json::from_str(line).expect("valid oracle json");
            let input = v["input"].as_str().expect("input");
            let want = ymd(
                u16::try_from(v["date_year"].as_u64().expect("year")).expect("u16"),
                u8::try_from(v["date_month"].as_u64().expect("month")).expect("u8"),
                u8::try_from(v["date_day"].as_u64().expect("day")).expect("u8"),
            );
            let got = parse_date(input);
            if got != want {
                failures.push(format!("{input:?}: want {want:?}, got {got:?}"));
            }
            assert_eq!(
                got.is_valid(),
                v["date_valid"].as_bool().expect("date_valid"),
                "IsValid disagrees for {input:?}"
            );
        }
        assert!(n > 300, "oracle looks truncated");
        assert!(
            failures.is_empty(),
            "{} of {n} probes diverge from Go:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}
