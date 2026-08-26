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

func TestBinarySymbolsExcludeUnboundedAndDecoratorEntrypoints(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	binary := filepath.Join(t.TempDir(), "bitmagnet-schema-migrator")
	build := exec.Command("go", "build", "-trimpath", "-o", binary, "./cmd/bitmagnet-schema-migrator")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build migration-only binary: %v\n%s", err, output)
	}
	nm := exec.Command("go", "tool", "nm", binary)
	symbols, err := nm.Output()
	if err != nil {
		t.Fatalf("inspect migration-only binary symbols: %v", err)
	}
	blocked := []string{
		"github.com/bitmagnet-io/bitmagnet/internal/database/migrations.(*migrator).Up",
		"github.com/bitmagnet-io/bitmagnet/internal/database/migrations.(*migrator).Down",
		"github.com/bitmagnet-io/bitmagnet/internal/database/migrations.NewDecorator",
	}
	for _, line := range strings.Split(string(symbols), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		for _, symbol := range blocked {
			if fields[2] == symbol {
				t.Fatalf("migration-only binary contains blocked entrypoint %q", symbol)
			}
		}
	}
}
