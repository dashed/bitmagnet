package parity

import (
	"bytes"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/torznab"
)

func TestNormalizeTorznabXML(t *testing.T) {
	raw := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed" version="2.0">
    <channel>
		<empty></empty>
		<also-empty />
		<title data="A &amp; B &lt; C &gt; D &quot;quoted&quot;">Rock &amp; Roll &lt;live&gt; "set"</title>
		<torznab:attr value="ABCDEF" name="infohash"></torznab:attr>
    </channel>
</rss>`)
	want := []byte(`<rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    <empty/>
    <also-empty/>
    <title data="A &amp; B &lt; C &gt; D &quot;quoted&quot;">Rock &amp; Roll &lt;live&gt; "set"</title>
    <torznab:attr name="infohash" value="ABCDEF"/>
  </channel>
</rss>
`)

	got, err := NormalizeTorznabXML(raw)
	if err != nil {
		t.Fatalf("NormalizeTorznabXML: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("canonical XML mismatch\nwant:\n%s\n got:\n%s", want, got)
	}

	again, err := NormalizeTorznabXML(got)
	if err != nil {
		t.Fatalf("NormalizeTorznabXML(canonical): %v", err)
	}
	if !bytes.Equal(got, again) {
		t.Fatalf("normalization is not idempotent\nfirst:\n%s\nsecond:\n%s", got, again)
	}
}

func TestNormalizeTorznabXMLPreservesRealTorznabPrefix(t *testing.T) {
	raw, err := (torznab.SearchResult{
		Channel: torznab.SearchResultChannel{
			Items: []torznab.SearchResultItem{
				{
					GUID: "ABCDEF0123456789",
					TorznabAttrs: []torznab.SearchResultItemTorznabAttr{
						{AttrName: torznab.AttrInfoHash, AttrValue: "ABCDEF0123456789"},
					},
				},
			},
		},
	}).XML()
	if err != nil {
		t.Fatalf("SearchResult.XML: %v", err)
	}

	normalized, err := NormalizeTorznabXML(raw)
	if err != nil {
		t.Fatalf("NormalizeTorznabXML: %v", err)
	}
	if !strings.Contains(string(normalized), `<torznab:attr name="infohash" value="ABCDEF0123456789"/>`) {
		t.Fatalf("normalized adapter output lost torznab prefix:\n%s", normalized)
	}

	again, err := NormalizeTorznabXML(normalized)
	if err != nil {
		t.Fatalf("NormalizeTorznabXML(canonical adapter output): %v", err)
	}
	if !bytes.Equal(normalized, again) {
		t.Fatalf("adapter output normalization is not idempotent\nfirst:\n%s\nsecond:\n%s", normalized, again)
	}
}

func TestTorznabExtractInfohashes(t *testing.T) {
	raw := []byte(`<rss xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    <item>
      <guid>AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA</guid>
      <torznab:attr name="infohash" value="ABCDEF0123456789ABCDEF0123456789ABCDEF01"/>
    </item>
    <item>
      <guid>FEDCBA9876543210FEDCBA9876543210FEDCBA98</guid>
    </item>
  </channel>
</rss>`)

	got, err := ExtractInfohashes(raw)
	if err != nil {
		t.Fatalf("ExtractInfohashes: %v", err)
	}
	want := []string{
		"abcdef0123456789abcdef0123456789abcdef01",
		"fedcba9876543210fedcba9876543210fedcba98",
	}
	if len(got) != len(want) {
		t.Fatalf("ExtractInfohashes length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ExtractInfohashes[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestTorznabCorpusAndFixtureLoaders(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	dataDir := filepath.Join(root, "testdata", "parity", "torznab")

	corpus, err := LoadTorznabCorpus(filepath.Join(dataDir, "corpus.jsonl"))
	if err != nil {
		t.Fatalf("LoadTorznabCorpus: %v", err)
	}
	if len(corpus) != 65 {
		t.Fatalf("corpus length = %d, want 65", len(corpus))
	}
	if corpus[0].ID != "caps" || corpus[0].GoldenName() != "caps.golden.xml" {
		t.Fatalf("first corpus query = %+v, want caps query", corpus[0])
	}
	if corpus[4].ID != "search-q-none" || !corpus[4].HasExpect || len(corpus[4].ExpectIDs) != 0 {
		t.Fatalf("empty expectIds presence was not preserved: %+v", corpus[4])
	}

	fixtures, err := LoadTorznabFixtures(filepath.Join(dataDir, "fixtures.jsonl"))
	if err != nil {
		t.Fatalf("LoadTorznabFixtures: %v", err)
	}
	if len(fixtures) != 25 {
		t.Fatalf("fixture length = %d, want 25", len(fixtures))
	}
	if fixtures[0].ID != "mov-nebula-1080" || fixtures[len(fixtures)-1].ID != "unk-mystery" {
		t.Fatalf("fixture file order was not preserved: first=%q last=%q", fixtures[0].ID, fixtures[len(fixtures)-1].ID)
	}
	if fixtures[2].ID != "mov-comet-480" || fixtures[2].Seeders != nil || fixtures[2].Leechers != nil {
		t.Fatalf("absent source counters were not preserved: %+v", fixtures[2])
	}
	if fixtures[16].ID != "mus-solaris-lp" || fixtures[16].Leechers == nil || *fixtures[16].Leechers != 0 {
		t.Fatalf("explicit zero leechers was not preserved: %+v", fixtures[16])
	}
}
