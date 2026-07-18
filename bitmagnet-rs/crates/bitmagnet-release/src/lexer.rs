//! A faithful port of `internal/lexer/lexer.go` — the rune-cursor the keyword
//! DSL compiler drives. Go reads UTF-8 runes; a `Vec<char>` cursor is the
//! rune-exact analog (a Rust `char` is a Unicode scalar value == a Go rune).
//!
//! Only the subset the keyword compiler uses is ported (`read`, `backup`,
//! `read_char`, `pos`); the numeric/`ReadWhile` helpers are unused here.

pub(crate) struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub(crate) fn new(s: &str) -> Self {
        Lexer {
            chars: s.chars().collect(),
            pos: 0,
        }
    }

    /// Current position (in runes consumed). Mirrors `Lexer.Pos`.
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    /// Read the next rune, advancing the cursor. `None` at EOF (mirrors the
    /// `(rune, bool)` return).
    pub(crate) fn read(&mut self) -> Option<char> {
        let c = self.chars.get(self.pos).copied();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    /// Un-read the last rune. Go panics on an invalid unread; here an underflow
    /// is impossible because `read` only advances when a rune was returned.
    pub(crate) fn backup(&mut self) {
        debug_assert!(self.pos > 0, "backup underflow");
        self.pos -= 1;
    }

    /// Consume the next rune iff it equals `want`. Mirrors `Lexer.ReadChar`.
    pub(crate) fn read_char(&mut self, want: char) -> bool {
        match self.read() {
            Some(c) if c == want => true,
            Some(_) => {
                self.backup();
                false
            }
            None => false,
        }
    }
}

/// Mirrors `lexer.IsWordChar` — a rune is a "word char" iff it is a Unicode
/// letter or a Unicode digit. Go uses `unicode.IsLetter || unicode.IsDigit`.
/// NOTE: this classifies the *keyword DSL source*, not the match-time regex
/// (whose word-char class is `[\p{L}0-9]`, ASCII-digit; see `regexutil`).
pub(crate) fn is_word_char(c: char) -> bool {
    c.is_alphabetic() || c.is_numeric()
}
