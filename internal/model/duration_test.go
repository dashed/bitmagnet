package model

import (
	"strings"
	"testing"
	"time"
)

func TestDurationScanRequiresPostgresTimeComponent(t *testing.T) {
	var safe Duration
	if err := safe.Scan("168:00:00"); err != nil {
		t.Fatalf("scan seven-day time component: %v", err)
	}
	if got, want := time.Duration(safe), 7*24*time.Hour; got != want {
		t.Fatalf("safe duration = %s, want %s", got, want)
	}

	var unsafe Duration
	err := unsafe.Scan("7 days")
	if err == nil {
		t.Fatal("day-component interval unexpectedly scanned")
	}
	const knownFailure = `time: unknown unit " dayss" in duration "7 dayss"`
	if !strings.Contains(err.Error(), knownFailure) {
		t.Fatalf("unsafe scan error = %q, want substring %q", err, knownFailure)
	}
}
