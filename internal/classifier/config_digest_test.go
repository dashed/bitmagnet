package classifier

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestEffectiveConfigDigest(t *testing.T) {
	source, err := (yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}).source()
	if err != nil {
		t.Fatal(err)
	}

	const expected = "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae"
	digest, err := EffectiveConfigDigest(source, "default")
	if err != nil {
		t.Fatal(err)
	}
	if digest != expected {
		t.Fatalf("digest mismatch: got %q, want %q", digest, expected)
	}

	source.Schema = "https://example.invalid/schema.json"
	withSchema, err := EffectiveConfigDigest(source, "default")
	if err != nil {
		t.Fatal(err)
	}
	if withSchema != digest {
		t.Fatalf("schema changed digest: got %q, want %q", withSchema, digest)
	}

	otherWorkflow, err := EffectiveConfigDigest(source, "audio")
	if err != nil {
		t.Fatal(err)
	}
	if otherWorkflow == digest {
		t.Fatal("default workflow change did not change digest")
	}

	source.Flags["tmdb_enabled"] = false
	otherFlags, err := EffectiveConfigDigest(source, "default")
	if err != nil {
		t.Fatal(err)
	}
	if otherFlags == digest {
		t.Fatal("effective flag change did not change digest")
	}
}

func TestEffectiveConfigDigestEdgeVector(t *testing.T) {
	source := Source{
		Workflows: workflowSources{
			"edge": map[string]any{
				"value": "before\u2028between\u2029after",
				"items": []any{nil, true, int64(-7), "<&>"},
			},
		},
		FlagDefinitions: flagDefinitions{},
		Flags:           Flags{},
		Keywords:        keywordGroups{},
		Extensions:      extensionGroups{},
	}

	const expected = "sha256:61562ac973ee6a59d1e49d5dbdc555002f23b3a9c24358de5c423aef7edfb7bf"
	digest, err := EffectiveConfigDigest(source, "edge")
	if err != nil {
		t.Fatal(err)
	}
	if digest != expected {
		t.Fatalf("edge digest mismatch: got %q, want %q", digest, expected)
	}
}

func TestEffectiveConfigDigestRejectsFloats(t *testing.T) {
	source := Source{
		Workflows: workflowSources{"edge": map[string]any{"value": 1.5}},
	}
	if _, err := EffectiveConfigDigest(source, "edge"); err == nil {
		t.Fatal("expected floating-point config value to be rejected")
	}
}

func TestLogEffectiveConfigDigestFields(t *testing.T) {
	source, err := (yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}).source()
	if err != nil {
		t.Fatal(err)
	}
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core).Sugar()
	if err := logEffectiveConfigDigest(nil, source, "default"); err != nil {
		t.Fatalf("nil logger: %v", err)
	}
	if err := logEffectiveConfigDigest(logger, source, "default"); err != nil {
		t.Fatal(err)
	}
	digest, err := EffectiveConfigDigest(source, "default")
	if err != nil {
		t.Fatal(err)
	}

	entries := logs.FilterMessage("classifier runner initialized").All()
	if len(entries) != 1 {
		t.Fatalf("got %d initialization log entries, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["effective_config_digest"] != digest {
		t.Fatalf("digest log field = %v, want %q", fields["effective_config_digest"], digest)
	}
	if fields["default_workflow"] != "default" {
		t.Fatalf("workflow log field = %v, want default", fields["default_workflow"])
	}
}
