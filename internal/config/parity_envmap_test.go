package config_test

// Parity golden: the config env-key → dot-path contract (§01 §2.6, phase0 B2).
//
// This is the ground-truth generator for `testdata/parity/config-env-map.golden`,
// one of the five Phase-0 golden-file contract surfaces (00-overview §4 #3). It
// drives the REAL config walker (config.New over the deployed root specs) so the
// golden is, by construction, exactly the set of env keys the running Go binary
// resolves — not a re-derivation that could silently drift from it.
//
// How the two columns are produced, verbatim from the Go resolver:
//   - dot.path : each leaf ResolvedNode.PathString, i.e.
//     strcase.ToSnake(field) joined by "." from the root spec key down
//     (internal/config/config.go resolveStructNode).
//   - ENV_KEY  : strings.ToUpper(strings.Join(path, "_")) — exactly what
//     envResolver.Resolve keys on (internal/config/configresolver/envresolver.go).
//     Hence ENV_KEY == ToUpper(ReplaceAll(dot.path, ".", "_")).
//
// Format (fixed by phase0-tasks.md §Contracts): sorted unique lines
// "ENV_KEY\tdot.path", LF endings, single trailing newline.
//
// Regenerate after an intentional config-surface change:
//   go test ./internal/config/ -run TestConfigEnvMapGolden -update
//
// The Rust port (Lane A, bitmagnet-common A1) reads the SAME golden and must
// resolve every ENV_KEY to the same dot.path node.

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/config"
	"github.com/bitmagnet-io/bitmagnet/internal/database/cache"
	"github.com/bitmagnet-io/bitmagnet/internal/database/postgres"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/dhtcrawler"
	"github.com/bitmagnet-io/bitmagnet/internal/health"
	"github.com/bitmagnet-io/bitmagnet/internal/httpserver"
	"github.com/bitmagnet-io/bitmagnet/internal/logging"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	queueserver "github.com/bitmagnet-io/bitmagnet/internal/queue/server"
	"github.com/bitmagnet-io/bitmagnet/internal/search/graphqlshadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/searchfx"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab"
	"github.com/bitmagnet-io/bitmagnet/internal/webui"
	"github.com/go-playground/validator/v10"
)

// updateGolden regenerates the golden instead of asserting against it.
var updateGolden = flag.Bool("update", false, "update parity golden files")

const configEnvMapGoldenRel = "testdata/parity/config-env-map.golden"

// configSpecs is the deployed set of config root specs, one per
// configfx.NewConfigModule call across the fx modules the application loads
// (grep: `NewConfigModule` — 17 unique root keys; devfx re-registers "postgres"
// with the same struct, so it adds no keys). Keep this list in lockstep with the
// modules; a missing spec silently drops that section's env keys from the
// contract, and the assert test below pins the known-good anchors as a tripwire.
func configSpecs() []config.Spec {
	return []config.Spec{
		{Key: "postgres", DefaultValue: postgres.NewDefaultConfig()},
		{Key: "gorm_cache", DefaultValue: cache.NewDefaultConfig()},
		{Key: "search_features", DefaultValue: search.NewDefaultFeatureFlagsConfig()},
		{Key: "webui", DefaultValue: webui.NewDefaultConfig()},
		{Key: "health", DefaultValue: health.NewDefaultPeerConfig()},
		{Key: "dht_server", DefaultValue: server.NewDefaultConfig()},
		{Key: "tmdb", DefaultValue: tmdb.NewDefaultConfig()},
		{Key: "metainfo_requester", DefaultValue: metainforequester.NewDefaultConfig()},
		{Key: "search", DefaultValue: searchfx.NewDefaultConfig()},
		{Key: "graphql_shadow", DefaultValue: graphqlshadow.NewDefaultConfig()},
		{Key: "http_server", DefaultValue: httpserver.NewDefaultConfig()},
		{Key: "classifier", DefaultValue: classifier.NewDefaultConfig()},
		{Key: "dht_crawler", DefaultValue: dhtcrawler.NewDefaultConfig()},
		{Key: "blob_migration", DefaultValue: blobmigration.NewDefaultConfig()},
		{Key: "torznab", DefaultValue: torznab.NewDefaultConfig()},
		{Key: "log", DefaultValue: logging.NewDefaultConfig()},
		{Key: "queue_server", DefaultValue: queueserver.NewDefaultConfig()},
	}
}

// generateEnvMap resolves every root spec with the real walker (no resolvers, so
// values stay at their defaults — only the path SHAPE matters) and emits the
// sorted unique "ENV_KEY\tdot.path" lines.
func generateEnvMap(t *testing.T) []string {
	t.Helper()

	resolved, err := config.New(config.Params{
		Specs:    configSpecs(),
		Validate: validator.New(),
	})
	if err != nil {
		t.Fatalf("config.New over root specs failed: %v", err)
	}

	var lines []string
	for _, node := range resolved.Resolved.Nodes() {
		collectLeafEnvKeys(node, &lines)
	}

	sort.Strings(lines)

	// Dedupe (paths are unique, but the contract mandates "sorted unique").
	out := lines[:0:0]

	for i, ln := range lines {
		if i == 0 || ln != lines[i-1] {
			out = append(out, ln)
		}
	}

	return out
}

// collectLeafEnvKeys walks a resolved node tree, emitting one line per leaf
// (non-struct) node — the exact nodes envResolver can bind an env var to.
func collectLeafEnvKeys(node config.ResolvedNode, lines *[]string) {
	if node.IsStruct {
		for _, child := range node.Children() {
			collectLeafEnvKeys(child, lines)
		}

		return
	}

	dotPath := node.PathString
	envKey := strings.ToUpper(strings.ReplaceAll(dotPath, ".", "_"))
	*lines = append(*lines, envKey+"\t"+dotPath)
}

func goldenBytes(lines []string) []byte {
	return []byte(strings.Join(lines, "\n") + "\n")
}

// TestConfigEnvMapGolden regenerates (with -update) or asserts the env-map golden.
func TestConfigEnvMapGolden(t *testing.T) {
	t.Parallel()

	lines := generateEnvMap(t)
	got := goldenBytes(lines)
	path := filepath.Join(repoRoot(t), configEnvMapGoldenRel)

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}

		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}

		t.Logf("wrote %d env-key lines to %s", len(lines), path)

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create): %v", path, err)
	}

	if string(got) != string(want) {
		t.Errorf(
			"config env-map golden is stale (%d lines generated). "+
				"Re-run: go test ./internal/config/ -run TestConfigEnvMapGolden -update",
			len(lines),
		)
	}
}

// TestConfigEnvMapKnownGood pins load-bearing anchors so the generator can never
// go green while silently dropping a deployed env key. SEARCH_DUAL_WRITE_ENABLED
// is the walker's known-good check from the phase0 brief; the rest sample the
// documented ~25-var deployed surface (§01 §2.6) across several root sections.
func TestConfigEnvMapKnownGood(t *testing.T) {
	t.Parallel()

	byEnv := make(map[string]string)

	lines := generateEnvMap(t)
	for _, ln := range lines {
		parts := strings.SplitN(ln, "\t", 2)
		if len(parts) != 2 {
			t.Fatalf("malformed golden line %q", ln)
		}

		byEnv[parts[0]] = parts[1]
	}

	anchors := map[string]string{
		"SEARCH_DUAL_WRITE_ENABLED":             "search.dual_write_enabled",
		"SEARCH_ENABLED":                        "search.enabled",
		"SEARCH_FILE_SEARCH_ENABLED":            "search.file_search_enabled",
		"SEARCH_PATHSEARCH_ENABLED":             "search.pathsearch_enabled",
		"SEARCH_FEATURES_DROP_COMPATIBLE_READS": "search_features.drop_compatible_reads",
		"GRAPHQL_SHADOW_ENABLED":                "graphql_shadow.enabled",
		"GRAPHQL_SHADOW_ENDPOINT":               "graphql_shadow.endpoint",
		"GRAPHQL_SHADOW_SAMPLE_RATE":            "graphql_shadow.sample_rate",
		"GRAPHQL_SHADOW_TIMEOUT":                "graphql_shadow.timeout",
		"GRAPHQL_SHADOW_MAX_CONCURRENT":         "graphql_shadow.max_concurrent",
		"GRAPHQL_SHADOW_LOG_DISCREPANCIES":      "graphql_shadow.log_discrepancies",
		"POSTGRES_HOST":                         "postgres.host",
		"QUEUE_SERVER_DISABLED_QUEUES":          "queue_server.disabled_queues",
		"WEBUI_DEFAULT_FRONTEND":                "webui.default_frontend",
	}

	for env, wantPath := range anchors {
		got, ok := byEnv[env]
		if !ok {
			t.Errorf("env key %s missing from env-map (deployed contract regression)", env)
			continue
		}

		if got != wantPath {
			t.Errorf("env key %s resolves to %q, want %q", env, got, wantPath)
		}
	}
}

// repoRoot walks up from this test file to the module root (the dir with go.mod),
// so the golden lives at the shared repo-root testdata/parity/, not a
// package-local testdata dir.
func repoRoot(t *testing.T) string {
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
