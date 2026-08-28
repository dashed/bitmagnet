package parity

import (
	"encoding/json"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/fts"
)

type tokenizerDriver struct{}

func (tokenizerDriver) Subsystem() string {
	return "tokenizer"
}

func (tokenizerDriver) Run(input json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	tokens := fts.TokenizeFlat(in.Text)
	if tokens == nil {
		tokens = []string{}
	}

	return json.Marshal(struct {
		Tokens []string `json:"tokens"`
	}{Tokens: tokens})
}

func TestTokenizerParityHarness(t *testing.T) {
	fixtures, err := LoadFile("../../testdata/parity/tokenizer/corpus.jsonl")
	if err != nil {
		t.Fatalf("load tokenizer parity corpus: %v", err)
	}

	report := Run(fixtures, tokenizerDriver{}, Options{})
	if report.Ran < 1000 {
		t.Fatalf("expected large corpus, ran %d", report.Ran)
	}
	if !report.OK() {
		t.Fatalf("tokenizer parity diverged:\n%s", report.String())
	}
}
