package tape

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var updateCanonicalFixture = flag.Bool(
	"update-canonical-fixture",
	false,
	"rewrite testdata/parity/tape-canonical/escapes.json from this Go build",
)

// canonicalFixturePath is read by the Rust port's byte-parity test.
const canonicalFixturePath = "../../testdata/parity/tape-canonical/escapes.json"

// canonicalEscapeCases covers every construct where Go's encoder and
// serde_json's are known or suspected to differ, plus the ones where they agree
// (so a future divergence in an "agreeing" case is caught too).
//
// The names are stable identifiers the Rust test indexes by; do not rename.
var canonicalEscapeCases = map[string]string{
	"plain":              "Cinderella",
	"html_chars":         "a<b>c&d",
	"solidus":            "path/to/thing",
	"quote_backslash":    `a"b\c`,
	"tab_newline_return": "a\tb\nc\rd",
	"backspace":          "a\bb",
	"formfeed":           "a\fb",
	"control_01":         "a\x01b",
	"control_1f":         "a\x1fb",
	"del_7f":             "a\x7fb",
	"line_separator":     "a b",
	"paragraph_sep":      "a b",
	"both_separators":    "  ",
	"cjk":                "\u65e5\u672c\u8a9e",
	"emoji":              "🎬 clapper",
	"combining":          "é", // e + combining acute
	"nul":                "a\x00b",
	"empty":              "",
	"only_quotes":        `""`,
	"mixed":              "Amélie<>& \t🎬/\\\"",
}

// TestCanonicalEscapeFixture pins Go's canonical string encoding so the Rust
// port can assert against it. The fixture is generated FROM Go rather than
// hand-written, because a hand-written expectation would only encode what
// someone believed Go does.
func TestCanonicalEscapeFixture(t *testing.T) {
	// Both halves are stored: the Rust port must not restate the inputs, or the
	// two sides could drift and still agree with themselves.
	type fixtureCase struct {
		Input   string `json:"input"`
		Encoded string `json:"encoded"`
	}

	got := make(map[string]fixtureCase, len(canonicalEscapeCases))

	for name, input := range canonicalEscapeCases {
		encoded, err := Marshal(input)
		if err != nil {
			t.Fatalf("marshal %q: %v", name, err)
		}

		got[name] = fixtureCase{Input: input, Encoded: string(encoded)}
	}

	// Indent for reviewability; the VALUES are what matter and they are exact.
	serialized, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}

	serialized = append(serialized, '\n')

	if *updateCanonicalFixture {
		if err := os.MkdirAll(filepath.Dir(canonicalFixturePath), 0o755); err != nil {
			t.Fatalf("create fixture dir: %v", err)
		}

		if err := os.WriteFile(canonicalFixturePath, serialized, 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}

		t.Logf("wrote %s", canonicalFixturePath)

		return
	}

	want, err := os.ReadFile(canonicalFixturePath)
	if err != nil {
		t.Fatalf("read fixture (regenerate with -update-canonical-fixture): %v", err)
	}

	if string(want) != string(serialized) {
		t.Fatalf(
			"canonical encoding drifted from the fixture the Rust port asserts against.\n"+
				"If this Go change is intended, regenerate with -update-canonical-fixture AND\n"+
				"re-run the Rust byte-parity test, which will now fail until it is updated too.\n"+
				"want:\n%s\ngot:\n%s",
			want, serialized,
		)
	}
}
