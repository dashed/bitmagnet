package main

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBinaryDependencyGraphExcludesApplicationAndWorkers(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/bitmagnet-schema-migrator")
	cmd.Dir = repoRoot
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list migrator dependencies: %v", err)
	}

	blocked := []string{
		"github.com/bitmagnet-io/bitmagnet/internal/app/appfx",
		"github.com/bitmagnet-io/bitmagnet/internal/dev",
		"github.com/bitmagnet-io/bitmagnet/internal/dht",
		"github.com/bitmagnet-io/bitmagnet/internal/dhtcrawler",
		"github.com/bitmagnet-io/bitmagnet/internal/gql",
		"github.com/bitmagnet-io/bitmagnet/internal/httpserver",
		"github.com/bitmagnet-io/bitmagnet/internal/importer",
		"github.com/bitmagnet-io/bitmagnet/internal/processor",
		"github.com/bitmagnet-io/bitmagnet/internal/queue",
		"github.com/bitmagnet-io/bitmagnet/internal/worker",
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, prefix := range blocked {
			if dependency == prefix || strings.HasPrefix(dependency, prefix+"/") {
				t.Fatalf("migration-only binary unexpectedly depends on worker/application package %q", dependency)
			}
		}
	}
}
