package parity

import (
	"strings"
	"testing"
)

func TestTorznabReplayDiffAndGates(t *testing.T) {
	identicalGo := cannedTorznabXML(
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
	)
	identicalRust := cannedTorznabXML(
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
	)
	identical, err := DiffXMLPair("identical", identicalGo, identicalRust)
	if err != nil {
		t.Fatalf("DiffXMLPair identical: %v", err)
	}
	if identical.SetMatch != 1 || identical.CountParity != 1 || !identical.OrderMatch {
		t.Fatalf("identical diff = %+v, want perfect parity", identical)
	}

	passing := aggregateReplayReport([]QueryDiff{identical})
	if ok, failures := passing.Gates(
		DefaultReplayThresholds.SetMatch,
		DefaultReplayThresholds.Count,
	); !ok {
		t.Fatalf("perfect replay failed gates: %v", failures)
	}

	divergentGo := cannedTorznabXML(
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
	)
	divergentRust := cannedTorznabXML(
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
	)
	divergent, err := DiffXMLPair("divergent", divergentGo, divergentRust)
	if err != nil {
		t.Fatalf("DiffXMLPair divergent: %v", err)
	}
	if divergent.SetMatch >= DefaultReplayThresholds.SetMatch &&
		divergent.CountParity >= DefaultReplayThresholds.Count {
		t.Fatalf("divergent diff unexpectedly met both gates: %+v", divergent)
	}
	if divergent.OrderMatch {
		t.Fatalf("divergent diff unexpectedly preserved order: %+v", divergent)
	}

	report := aggregateReplayReport([]QueryDiff{identical, divergent})
	ok, failures := report.Gates(
		DefaultReplayThresholds.SetMatch,
		DefaultReplayThresholds.Count,
	)
	if ok {
		t.Fatalf("aggregate divergent replay unexpectedly passed: %s", report.String())
	}
	if len(failures) == 0 {
		t.Fatal("aggregate divergent replay failed without a failure message")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "below gate") {
		t.Fatalf("gate failures do not explain threshold miss: %v", failures)
	}
	if summary := report.String(); !strings.Contains(summary, "queries=2") ||
		!strings.Contains(summary, "mean_set_match=") ||
		!strings.Contains(summary, "mean_count_parity=") {
		t.Fatalf("replay summary is incomplete: %q", summary)
	}
}

func cannedTorznabXML(hashes ...string) []byte {
	var xml strings.Builder
	xml.WriteString(`<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>`)
	for _, hash := range hashes {
		xml.WriteString(`<item><guid>`)
		xml.WriteString(hash)
		xml.WriteString(`</guid><torznab:attr name="infohash" value="`)
		xml.WriteString(hash)
		xml.WriteString(`"/></item>`)
	}
	xml.WriteString(`</channel></rss>`)

	return []byte(xml.String())
}
