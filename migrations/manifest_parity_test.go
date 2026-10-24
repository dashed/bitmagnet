package migrationssql_test

// Parity golden: the goose migration history (§01 §2.1, phase0 B4).
//
// One of the five Phase-0 golden-file contract surfaces (00-overview §4 #5). The
// on-disk 500Gi Postgres tracks applied migrations in goose_db_version by the
// NNNNN_ filename prefix; a rewrite must NEVER renumber, reorder, or edit an
// already-applied migration. This golden freezes the ordered (name, sha256) of
// every embedded migration so any renumber/edit/removal fails CI loudly.
//
// The hash is over the EXACT bytes goose runs: migrationssql.FS is the same
// `//go:embed *.sql` filesystem the migrator embeds (migrations/migrations.go →
// internal/database/migrations), so this protects content, not just names.
//
// Format: sorted-by-name lines "NNNNN_name.sql\t<sha256-hex>", LF, single
// trailing newline. Filenames are zero-padded, so lexical sort == goose's
// numeric apply order.
//
// Regenerate ONLY when legitimately ADDING a new migration (never to “fix” a
// diff on an existing one — that is the exact drift this guards):
//   go test ./migrations/ -run TestMigrationsManifestGolden -update

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
)

var updateMigrationsGolden = flag.Bool("update", false, "update parity golden files")

const migrationsGoldenRel = "testdata/parity/migrations.golden"

// migrationManifest lists the embedded *.sql files with their sha256, in goose
// apply order (== lexical order of the zero-padded names).
func migrationManifest(t *testing.T) []string {
	t.Helper()

	entries, err := migrationssql.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("read embedded migrations FS: %v", err)
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}

	sort.Strings(names)

	if len(names) == 0 {
		t.Fatal("no embedded *.sql migrations found")
	}

	lines := make([]string, 0, len(names))
	for _, name := range names {
		data, readErr := migrationssql.FS.ReadFile(name)
		if readErr != nil {
			t.Fatalf("read embedded %s: %v", name, readErr)
		}

		sum := sha256.Sum256(data)
		lines = append(lines, name+"\t"+hex.EncodeToString(sum[:]))
	}

	return lines
}

func migrationsGoldenBytes(lines []string) []byte {
	return []byte(strings.Join(lines, "\n") + "\n")
}

// TestMigrationsManifestGolden regenerates (with -update) or asserts the manifest.
func TestMigrationsManifestGolden(t *testing.T) {
	lines := migrationManifest(t)
	got := migrationsGoldenBytes(lines)
	path := filepath.Join(repoRootMigrations(t), migrationsGoldenRel)

	if *updateMigrationsGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}

		t.Logf("wrote %d migration manifest lines to %s", len(lines), path)

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create): %v", path, err)
	}

	if string(got) != string(want) {
		t.Errorf(
			"migration manifest golden changed (%d migrations). A renumber or edit "+
				"of an APPLIED migration corrupts goose_db_version on the 500Gi PG. "+
				"If you genuinely added a NEW migration, re-run: "+
				"go test ./migrations/ -run TestMigrationsManifestGolden -update",
			len(lines),
		)
	}
}

// TestMigrationsContiguous asserts the NNNNN_ prefixes are a gap-free 1..N run,
// so a missing/duplicated sequence number can't slip in unnoticed.
func TestMigrationsContiguous(t *testing.T) {
	lines := migrationManifest(t)
	for i, ln := range lines {
		name := strings.SplitN(ln, "\t", 2)[0]

		var seq int
		if _, err := fmt.Sscanf(name[:5], "%05d", &seq); err != nil {
			t.Fatalf("migration %q has no NNNNN_ prefix: %v", name, err)
		}

		if seq != i+1 {
			t.Errorf("migration ordering gap: %q is at position %d, want sequence %05d", name, i+1, i+1)
		}
	}
}

func repoRootMigrations(t *testing.T) string {
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
