package gql_test

// Parity golden: the GraphQL SDL contract (§01 §2.2, phase0 B1).
//
// One of the five Phase-0 golden-file contract surfaces (00-overview §4 #1). The
// served schema is consumed by the Angular + React webui codegen and by Hermes;
// a rewrite (async-graphql) must serve a wire-identical type system.
//
// SOURCE OF TRUTH: the schema is loaded exactly as gqlgen loads it — the same
// files under graphql/schema/*.graphqls fed to gqlparser (gqlgen's gql.gen.go
// does `parsedSchema = gqlparser.MustLoadSchema(sources...)` over these files).
// We reload them from disk (sorted by basename) so the golden regenerates from
// source alone, independent of generated code.
//
// NORMALIZATION (must be reproducible from any SDL source, Go OR the future Rust
// async-graphql `.sdl()` output — parse it with a GraphQL parser and apply the
// SAME rules):
//  1. Drop built-in definitions: the five built-in scalars (String/Int/Float/
//     Boolean/ID), all introspection types (name prefixed "__"), the
//     introspection root fields gqlparser injects onto Query (__schema/__type,
//     likewise name prefixed "__"), and built-in directives. Only the app's
//     declared type system remains. A code-first server (async-graphql) never
//     prints the introspection root fields in its .sdl(), so dropping them —
//     symmetric with the "__"-type strip — is required for the Rust 0-diff gate.
//  2. Drop all descriptions/comments.
//  3. Canonical (order-independent) ordering: type definitions sorted by name;
//     within each type, fields / input fields / enum values / arguments /
//     implemented interfaces / union members each sorted by name. Ordering is
//     therefore NOT part of the contract — presence, kind, and type reference are.
//  4. Deterministic layout: two-space indent, one member per line, a single
//     blank line between definitions, LF endings, single trailing newline. Type
//     references use gqlparser's canonical form (e.g. `[Hash20!]!`).
//
// The current schema uses only scalars, enums, objects, inputs, field arguments,
// and list/non-null wrappers (no directives/interfaces/unions/defaults/
// deprecations); the printer still handles interfaces/unions/directives/defaults
// defensively so a future schema addition surfaces in the golden instead of
// being silently dropped.
//
// Regenerate after an intentional schema change (and re-run gqlgen):
//   go test ./internal/gql/ -run TestGraphQLSDLGolden -update

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

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

var updateSDLGolden = flag.Bool("update", false, "update parity golden files")

const (
	schemaGlobDir     = "graphql/schema"
	sdlGoldenRel      = "testdata/parity/schema.graphql"
	builtinScalarsSet = "String Int Float Boolean ID"
)

// loadServedSchema reloads graphql/schema/*.graphqls exactly as gqlgen does.
func loadServedSchema(t *testing.T) *ast.Schema {
	t.Helper()

	dir := filepath.Join(repoRootGQL(t), schemaGlobDir)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read schema dir %s: %v", dir, err)
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".graphqls") {
			names = append(names, e.Name())
		}
	}

	sort.Strings(names)

	if len(names) == 0 {
		t.Fatalf("no .graphqls files in %s", dir)
	}

	sources := make([]*ast.Source, 0, len(names))
	for _, name := range names {
		data, readErr := os.ReadFile(filepath.Join(dir, name))
		if readErr != nil {
			t.Fatalf("read %s: %v", name, readErr)
		}

		sources = append(sources, &ast.Source{Name: name, Input: string(data)})
	}

	schema, loadErr := gqlparser.LoadSchema(sources...)
	if loadErr != nil {
		t.Fatalf("gqlparser.LoadSchema: %v", loadErr)
	}

	return schema
}

func isBuiltinType(def *ast.Definition) bool {
	if def.BuiltIn || strings.HasPrefix(def.Name, "__") {
		return true
	}

	return strings.Contains(" "+builtinScalarsSet+" ", " "+def.Name+" ")
}

// normalizeSchemaSDL renders the canonical, order-independent SDL described in
// the file header.
func normalizeSchemaSDL(schema *ast.Schema) string {
	defs := make([]*ast.Definition, 0, len(schema.Types))
	for _, def := range schema.Types {
		if !isBuiltinType(def) {
			defs = append(defs, def)
		}
	}

	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })

	var b strings.Builder

	for _, dir := range sortedDirectives(schema) {
		writeDirective(&b, dir)
		b.WriteString("\n")
	}

	for _, def := range defs {
		writeDefinition(&b, def)
		b.WriteString("\n")
	}

	return strings.TrimRight(b.String(), "\n") + "\n"
}

func sortedDirectives(schema *ast.Schema) []*ast.DirectiveDefinition {
	var dirs []*ast.DirectiveDefinition
	for _, d := range schema.Directives {
		if d.Position != nil && d.Position.Src != nil && d.Position.Src.BuiltIn {
			continue
		}

		dirs = append(dirs, d)
	}

	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name < dirs[j].Name })

	return dirs
}

func writeDirective(b *strings.Builder, d *ast.DirectiveDefinition) {
	fmt.Fprintf(b, "directive @%s%s", d.Name, formatArgs(d.Arguments))

	locs := make([]string, len(d.Locations))
	for i, l := range d.Locations {
		locs[i] = string(l)
	}

	sort.Strings(locs)
	fmt.Fprintf(b, " on %s\n", strings.Join(locs, " | "))
}

func writeDefinition(b *strings.Builder, def *ast.Definition) {
	switch def.Kind {
	case ast.Scalar:
		fmt.Fprintf(b, "scalar %s\n", def.Name)
	case ast.Enum:
		values := append(ast.EnumValueList{}, def.EnumValues...)
		sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
		fmt.Fprintf(b, "enum %s {\n", def.Name)

		for _, v := range values {
			fmt.Fprintf(b, "  %s\n", v.Name)
		}

		b.WriteString("}\n")
	case ast.Union:
		members := append([]string{}, def.Types...)
		sort.Strings(members)
		fmt.Fprintf(b, "union %s = %s\n", def.Name, strings.Join(members, " | "))
	case ast.Object, ast.Interface, ast.InputObject:
		keyword := map[ast.DefinitionKind]string{
			ast.Object:      "type",
			ast.Interface:   "interface",
			ast.InputObject: "input",
		}[def.Kind]

		fmt.Fprintf(b, "%s %s", keyword, def.Name)

		if len(def.Interfaces) > 0 {
			ifaces := append([]string{}, def.Interfaces...)
			sort.Strings(ifaces)
			fmt.Fprintf(b, " implements %s", strings.Join(ifaces, " & "))
		}

		b.WriteString(" {\n")
		writeFields(b, def.Fields)
		b.WriteString("}\n")
	default:
		fmt.Fprintf(b, "# unhandled kind %q for %s\n", def.Kind, def.Name)
	}
}

func writeFields(b *strings.Builder, fields ast.FieldList) {
	sorted := append(ast.FieldList{}, fields...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	for _, f := range sorted {
		// Drop the introspection root fields gqlparser injects onto Query
		// (__schema/__type). Names prefixed "__" are reserved by the GraphQL spec
		// for introspection, so no app field uses them; this mirrors the
		// "__"-type strip in isBuiltinType. A code-first async-graphql server does
		// not emit these in .sdl(), so they must not appear in the golden.
		if strings.HasPrefix(f.Name, "__") {
			continue
		}

		fmt.Fprintf(b, "  %s%s: %s\n", f.Name, formatArgs(f.Arguments), f.Type.String())
	}
}

func formatArgs(args ast.ArgumentDefinitionList) string {
	if len(args) == 0 {
		return ""
	}

	sorted := append(ast.ArgumentDefinitionList{}, args...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	parts := make([]string, len(sorted))
	for i, a := range sorted {
		parts[i] = a.Name + ": " + a.Type.String()
		if a.DefaultValue != nil {
			parts[i] += " = " + a.DefaultValue.String()
		}
	}

	return "(" + strings.Join(parts, ", ") + ")"
}

// TestGraphQLSDLGolden regenerates (with -update) or asserts the SDL golden.
func TestGraphQLSDLGolden(t *testing.T) {
	got := normalizeSchemaSDL(loadServedSchema(t))
	path := filepath.Join(repoRootGQL(t), sdlGoldenRel)

	if *updateSDLGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}

		t.Logf("wrote SDL golden (%d bytes, sha256 %s) to %s",
			len(got), shortSHA(got), path)

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create): %v", path, err)
	}

	if got != string(want) {
		t.Errorf("GraphQL SDL golden is stale. Re-run: " +
			"go test ./internal/gql/ -run TestGraphQLSDLGolden -update")
	}
}

// TestGraphQLSDLKnownGood pins load-bearing schema anchors so the generator can
// never go green while silently dropping a consumer-visible type or scalar.
func TestGraphQLSDLKnownGood(t *testing.T) {
	sdl := normalizeSchemaSDL(loadServedSchema(t))

	mustContain := []string{
		"scalar Hash20",
		"scalar Hash32",
		"scalar DateTime",
		"scalar Duration",
		"scalar Void",
		"type Query {",
		"type Mutation {",
		"type TorrentContent {",
		"input TorrentContentSearchQueryInput {",
	}

	for _, s := range mustContain {
		if !strings.Contains(sdl, s) {
			t.Errorf("SDL golden missing required fragment %q (consumer contract regression)", s)
		}
	}

	// Built-ins must NOT leak into the golden.
	for _, s := range []string{"scalar String", "scalar Boolean", "type __Schema", "scalar Int"} {
		if strings.Contains(sdl, s+"\n") {
			t.Errorf("SDL golden leaked built-in %q", s)
		}
	}
}

// TestGraphQLSDLGoldenIdempotent proves the normalizer is a fixed point on its
// own output: parsing the golden back as an SDL source and re-normalizing it
// must reproduce the golden byte-for-byte. This is the property the Rust 0-diff
// gate relies on — both sides canonicalize to the same fixed form — and it
// guards the "__"-field strip specifically: gqlparser re-injects __schema/__type
// onto Query when the golden is reloaded, and the normalizer must strip them
// again to land back on the golden.
func TestGraphQLSDLGoldenIdempotent(t *testing.T) {
	path := filepath.Join(repoRootGQL(t), sdlGoldenRel)

	golden, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create): %v", path, err)
	}

	schema, loadErr := gqlparser.LoadSchema(&ast.Source{Name: sdlGoldenRel, Input: string(golden)})
	if loadErr != nil {
		t.Fatalf("reload golden as schema: %v", loadErr)
	}

	if got := normalizeSchemaSDL(schema); got != string(golden) {
		t.Errorf("normalizer is not idempotent on the golden: normalize(golden) != golden.\n"+
			"golden sha256 %s, renormalized sha256 %s", shortSHA(string(golden)), shortSHA(got))
	}
}

func shortSHA(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}

func repoRootGQL(t *testing.T) string {
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
