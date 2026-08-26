package parity

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTorznabReplayOffline runs the G2 set/count/order gate against a captured
// corpus response tree (see docs/bitmagnet/torznab-g2-gate-runbook.md). Skips
// unless CAPTURE_DIR is set. CAPTURE_DIR must contain go/<id>.xml + rust/<id>.xml.
func TestTorznabReplayOffline(t *testing.T) {
	dir := os.Getenv("CAPTURE_DIR")
	if dir == "" {
		t.Skip("set CAPTURE_DIR to run the offline G2 replay diff")
	}
	corpusPath := os.Getenv("CORPUS")
	if corpusPath == "" {
		corpusPath = "testdata/parity/torznab/corpus.jsonl"
	}
	corpus, err := LoadTorznabCorpus(corpusPath)
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	attrOut := os.Getenv("ATTR_DIFF_OUT")
	if attrOut != "" {
		if err := os.MkdirAll(attrOut, 0o755); err != nil {
			t.Fatalf("mkdir attr out: %v", err)
		}
	}

	diffs := make([]QueryDiff, 0, len(corpus))
	for _, q := range corpus {
		goXML, gErr := os.ReadFile(filepath.Join(dir, "go", q.ID+".xml"))
		rsXML, rErr := os.ReadFile(filepath.Join(dir, "rust", q.ID+".xml"))
		if gErr != nil || rErr != nil {
			t.Errorf("missing capture for %q: go=%v rust=%v", q.ID, gErr, rErr)
			continue
		}
		// Attribute-level canonical diff (all kinds), for drift triage.
		if attrOut != "" {
			gn, e1 := NormalizeTorznabXML(goXML)
			rn, e2 := NormalizeTorznabXML(rsXML)
			if e1 == nil && e2 == nil && string(gn) != string(rn) {
				_ = os.WriteFile(filepath.Join(attrOut, q.ID+".diff"),
					[]byte("--- go\n"+string(gn)+"\n--- rust\n"+string(rn)+"\n"), 0o644)
			}
		}
		if q.Kind != torznabQueryKindSearch {
			continue
		}
		d, err := DiffXMLPair(q.ID, goXML, rsXML)
		if err != nil {
			t.Fatalf("diff %q: %v", q.ID, err)
		}
		if d.SetMatch < 1 || !d.OrderMatch {
			t.Logf("DRIFT %s set=%.4f count=%.4f order=%v goOnly=%v rustOnly=%v",
				d.ID, d.SetMatch, d.CountParity, d.OrderMatch, d.GoOnly, d.RustOnly)
		}
		diffs = append(diffs, d)
	}

	report := aggregateReplayReport(diffs)
	t.Log(report.String())
	if ok, failures := report.Gates(
		DefaultReplayThresholds.SetMatch, DefaultReplayThresholds.Count,
	); !ok {
		t.Fatalf("G2 gate FAILED: %v", failures)
	}
}
