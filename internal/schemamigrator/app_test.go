package schemamigrator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

type recordingMigrator struct {
	upTargets   []int64
	downTargets []int64
	err         error
}

func (m *recordingMigrator) UpTo(_ context.Context, version int64) error {
	m.upTargets = append(m.upTargets, version)
	return m.err
}

func (m *recordingMigrator) DownTo(_ context.Context, version int64) error {
	m.downTargets = append(m.downTargets, version)
	return m.err
}

type harness struct {
	stdout     bytes.Buffer
	stderr     bytes.Buffer
	migrator   recordingMigrator
	dsn        string
	openCount  int
	closeCount int
}

func (h *harness) params() Params {
	return Params{
		BuildInfo: BuildInfo{
			Version:      "v-test",
			SourceCommit: strings.Repeat("a", 40),
			SourceTree:   strings.Repeat("b", 40),
		},
		Getenv: func(key string) string {
			if key != postgresDSN {
				h.dsn = "unexpected-key:" + key
				return ""
			}
			return "postgres://migration-role:secret@example.invalid/bitmagnet"
		},
		Open: func(_ context.Context, dsn string) (Session, error) {
			h.openCount++
			h.dsn = dsn
			return Session{
				Migrator: &h.migrator,
				Close: func() error {
					h.closeCount++
					return nil
				},
			}, nil
		},
		Writer:    &h.stdout,
		ErrWriter: &h.stderr,
	}
}

func (h *harness) run(args ...string) error {
	return NewApp(h.params()).RunContext(context.Background(), args)
}

func TestExactUpTargetUsesOnlyUpTo(t *testing.T) {
	h := &harness{}
	if err := h.run("bitmagnet-schema-migrator", "migrate", "up", "--version", "34"); err != nil {
		t.Fatal(err)
	}
	if got, want := h.migrator.upTargets, []int64{UpVersion}; !equalTargets(got, want) {
		t.Fatalf("UpTo targets = %v, want %v", got, want)
	}
	if len(h.migrator.downTargets) != 0 {
		t.Fatalf("DownTo unexpectedly called with %v", h.migrator.downTargets)
	}
	if h.openCount != 1 || h.closeCount != 1 {
		t.Fatalf("session counts open=%d close=%d, want 1/1", h.openCount, h.closeCount)
	}
}

func TestExactDownTargetUsesOnlyDownTo(t *testing.T) {
	h := &harness{}
	if err := h.run("bitmagnet-schema-migrator", "migrate", "down", "--version", "29"); err != nil {
		t.Fatal(err)
	}
	if got, want := h.migrator.downTargets, []int64{DownVersion}; !equalTargets(got, want) {
		t.Fatalf("DownTo targets = %v, want %v", got, want)
	}
	if len(h.migrator.upTargets) != 0 {
		t.Fatalf("UpTo unexpectedly called with %v", h.migrator.upTargets)
	}
}

func TestRejectsUnboundedAndWrongTargetsBeforeOpeningDatabase(t *testing.T) {
	tests := [][]string{
		{"bitmagnet-schema-migrator", "migrate", "up"},
		{"bitmagnet-schema-migrator", "migrate", "down"},
		{"bitmagnet-schema-migrator", "migrate", "up", "--version", "33"},
		{"bitmagnet-schema-migrator", "migrate", "down", "--version", "28"},
		{"bitmagnet-schema-migrator", "migrate", "up", "--version", "034"},
		{"bitmagnet-schema-migrator", "migrate", "up", "--version", "+34"},
		{"bitmagnet-schema-migrator", "migrate", "up", "--version", "latest"},
		{"bitmagnet-schema-migrator", "migrate", "up", "--version", "34", "extra"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args[1:], "_"), func(t *testing.T) {
			h := &harness{}
			if err := h.run(args...); err == nil {
				t.Fatal("expected command to fail")
			}
			if h.openCount != 0 {
				t.Fatalf("database opened %d times for rejected arguments", h.openCount)
			}
		})
	}
}

func TestMigrationErrorStillClosesSession(t *testing.T) {
	h := &harness{}
	h.migrator.err = errors.New("migration failed")
	err := h.run("bitmagnet-schema-migrator", "migrate", "up", "--version", "34")
	if !errors.Is(err, h.migrator.err) {
		t.Fatalf("error = %v, want migration failure", err)
	}
	if h.closeCount != 1 {
		t.Fatalf("close count = %d, want 1", h.closeCount)
	}
}

func TestVersionReportsExactBuildIdentityWithoutDatabase(t *testing.T) {
	h := &harness{}
	if err := h.run("bitmagnet-schema-migrator", "version"); err != nil {
		t.Fatal(err)
	}
	want := `{"schema":"bitmagnet.schema-migrator-version/v1","version":"v-test","sourceCommit":"` +
		strings.Repeat("a", 40) + `","sourceTree":"` + strings.Repeat("b", 40) + `"}` + "\n"
	if got := h.stdout.String(); got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
	if h.openCount != 0 {
		t.Fatalf("version opened database %d times", h.openCount)
	}
}

func equalTargets(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
