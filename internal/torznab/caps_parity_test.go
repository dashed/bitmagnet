package torznab_test

// Parity golden: the default-profile Torznab caps XML contract (Phase 1 Lane G1).
//
// NORMALIZATION (shared with the future Rust service): drop the XML declaration;
// parse and re-serialize with two-space indentation, one element per line, LF,
// and one trailing newline; sort attributes by qualified name; preserve child
// order and literal namespace prefixes; drop whitespace-only text; canonically
// escape decoded text/attributes; and render empty elements as `<name/>` without
// synthesizing or removing elements.
//
// Regenerate after an intentional default-profile caps change:
//
//	go test ./internal/torznab/ -run TestTorznabCapsGolden -update

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/parity"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab"
)

var updateTorznabGoldens = flag.Bool("update", false, "update Torznab parity golden files")

const torznabParityDir = "testdata/parity/torznab"

// TestTorznabCapsGolden regenerates (with -update) or asserts the caps golden.
func TestTorznabCapsGolden(t *testing.T) {
	got := normalizedDefaultCaps(t)
	path := filepath.Join(repoRootTorznab(t), torznabParityDir, "caps.golden.xml")

	writeOrAssertTorznabGolden(t, path, got)
}

// TestTorznabCapsKnownGood pins load-bearing caps anchors so the generator can
// never go green while silently dropping a consumer-visible capability.
func TestTorznabCapsKnownGood(t *testing.T) {
	caps := string(normalizedDefaultCaps(t))
	mustContain := []string{
		"<caps",
		"<server",
		"supportedParams",
		`id="2000"`,
		`id="5000"`,
		`id="7000"`,
		"<tags",
	}

	for _, fragment := range mustContain {
		if !strings.Contains(caps, fragment) {
			t.Errorf("Torznab caps golden missing required fragment %q (consumer contract regression)", fragment)
		}
	}
}

func normalizedDefaultCaps(t *testing.T) []byte {
	t.Helper()

	raw, err := torznab.ProfileDefault.Caps().XML()
	if err != nil {
		t.Fatalf("marshal default Torznab caps: %v", err)
	}
	normalized, err := parity.NormalizeTorznabXML(raw)
	if err != nil {
		t.Fatalf("normalize default Torznab caps: %v", err)
	}

	return normalized
}

func writeOrAssertTorznabGolden(t *testing.T, path string, got []byte) {
	t.Helper()

	if *updateTorznabGoldens {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir Torznab golden directory: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write Torznab golden %s: %v", path, err)
		}

		t.Logf("wrote Torznab golden (%d bytes, sha256 %s) to %s", len(got), shortTorznabSHA(got), path)
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Torznab golden %s (run with -update to create): %v", path, err)
	}
	if bytes.Equal(want, got) {
		return
	}

	line, wantLine, gotLine := firstTorznabGoldenDifference(want, got)
	t.Fatalf(
		"Torznab golden %s differs at line %d (run with -update to regenerate)\nwant: %s\n got: %s",
		path,
		line,
		wantLine,
		gotLine,
	)
}

func firstTorznabGoldenDifference(want, got []byte) (int, string, string) {
	wantLines := strings.Split(strings.TrimSuffix(string(want), "\n"), "\n")
	gotLines := strings.Split(strings.TrimSuffix(string(got), "\n"), "\n")
	lineCount := min(len(wantLines), len(gotLines))
	for i := 0; i < lineCount; i++ {
		if wantLines[i] != gotLines[i] {
			return i + 1, wantLines[i], gotLines[i]
		}
	}
	if len(wantLines) != len(gotLines) {
		return lineCount + 1,
			fmt.Sprintf("<EOF after %d lines>", len(wantLines)),
			fmt.Sprintf("<EOF after %d lines>", len(gotLines))
	}

	return lineCount + 1, "<different line ending>", "<different line ending>"
}

func shortTorznabSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:12]
}

func repoRootTorznab(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	dir := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repo root (go.mod)")
		}

		dir = parent
	}
}
