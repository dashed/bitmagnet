package prometheus_test

// Parity golden: the Prometheus metric-name contract (§01 §2.5, phase0 B3).
//
// One of the five Phase-0 golden-file contract surfaces (00-overview §4 #4).
// Grafana dashboards and Loki alert rules key on these `name{labels}` series; a
// rewrite that renames a metric or drops/renames a label silently breaks them.
//
// WHAT THIS DUMPS: one line per collector, `fqName{sorted,variable,label,keys}`,
// extracted from each collector's Describe() Desc — NOT Gather(). Describe is
// side-effect-free (it never runs Collect), so this needs no database, sidecar,
// or network, and a HistogramVec contributes its single logical name (e.g.
// bitmagnet_dht_server_query_duration_seconds) rather than its _bucket/_sum/
// _count expansion. Const labels are unused across these collectors.
//
// COVERAGE (the leaner path sanctioned by phase0-tasks.md B3 — "instantiate the
// metric structs directly … document what's covered vs not"):
//
//   COVERED (constructed via dependency-free constructors, then Describe'd):
//     - search_shadow_* + search_tantivy_doc_count   (shadow.NewMetrics)
//     - search_pathsearch_*                            (pathsearch.NewMetrics)
//     - search_serve_*                                 (router.NewServeMetrics)
//     - blob_consistency_*                             (consistency.NewMetrics)
//     - meta_info_requester_*                          (metainforequester.New — Config+nop logger)
//     - dht_server_*                                   (server.New — Config+nil responder+nop logger)
//
//   NOT YET COVERED (owned by fx factories that need injected runtime deps or
//   have unexported collector constructors; enumerated + guarded by
//   TestMetricNamesUncoveredTracked so the gap is explicit, not forgotten):
//     - dht_responder_*        (responder.New needs a ktable.Table + discovered-node channel)
//     - dht_ktable_*           (generic unexported patchPrometheusCollector, per keyspace)
//     - dht_crawler_*          (built inline in dhtcrawlerfx.New behind heavy deps)
//     - bitmagnet_queue_jobs_total (unexported DB-backed custom collector)
//   Extending coverage means stubbing those deps or driving fx.Populate over the
//   collector group with a live Postgres — deferred past Phase 0.
//
// Regenerate after an intentional metric change:
//   go test ./internal/telemetry/prometheus/ -run TestMetricNamesGolden -update

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/consistency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
)

var updateMetricGolden = flag.Bool("update", false, "update parity golden files")

const metricNamesGoldenRel = "testdata/parity/metric-names.golden"

var (
	reFqName    = regexp.MustCompile(`fqName: "((?:[^"\\]|\\.)*)"`)
	reVarLabels = regexp.MustCompile(`variableLabels: \{([^}]*)\}\}$`)
)

// coveredCollectors instantiates every metric family reachable without injected
// runtime deps (see the COVERAGE note in the file header).
func coveredCollectors(t *testing.T) []prometheus.Collector {
	t.Helper()

	nop := zap.NewNop().Sugar()

	var cs []prometheus.Collector
	cs = append(cs, shadow.NewMetrics().Collectors()...)
	cs = append(cs, pathsearch.NewMetrics().Collectors()...)
	cs = append(cs, router.NewServeMetrics().Collectors()...)
	cs = append(cs, consistency.NewMetrics().Collectors()...)

	// dht meta-info requester: collectors are built eagerly; only Config+Logger
	// are touched at construction (the requester/dialer are never dialed here).
	mr := metainforequester.New(metainforequester.Params{
		Config: metainforequester.NewDefaultConfig(),
		Logger: nop,
	})
	cs = append(cs,
		mr.RequestDuration, mr.RequestSuccessTotal,
		mr.RequestErrorTotal, mr.RequestConcurrency,
	)

	// dht server: the collectors are built eagerly; the Responder + socket live
	// only inside the (uninvoked) lazy, so nil Responder + nop Logger are safe.
	sv := server.New(server.Params{
		Config: server.NewDefaultConfig(),
		Logger: nop,
	})
	cs = append(cs,
		sv.QueryDuration, sv.QuerySuccessTotal, sv.QueryErrorTotal,
		sv.QueryConcurrency, sv.ResponseDroppedTotal,
	)

	return cs
}

// metricLines Describe's every collector and renders sorted-unique
// `fqName{sorted,labels}`. Registering into a fresh registry first validates the
// collectors are well-formed and non-conflicting (as the real app requires).
func metricLines(t *testing.T) []string {
	t.Helper()

	collectors := coveredCollectors(t)
	registry := prometheus.NewRegistry()

	var lines []string
	for _, c := range collectors {
		if err := registry.Register(c); err != nil {
			t.Fatalf("register collector: %v", err)
		}

		ch := make(chan *prometheus.Desc, 8)
		go func(coll prometheus.Collector) {
			coll.Describe(ch)
			close(ch)
		}(c)

		for desc := range ch {
			lines = append(lines, metricLine(t, desc))
		}
	}

	sort.Strings(lines)

	out := lines[:0:0]
	for i, ln := range lines {
		if i == 0 || ln != lines[i-1] {
			out = append(out, ln)
		}
	}

	return out
}

func metricLine(t *testing.T, desc *prometheus.Desc) string {
	t.Helper()

	s := desc.String()

	fq := reFqName.FindStringSubmatch(s)
	if fq == nil {
		t.Fatalf("could not parse fqName from Desc: %s", s)
	}

	var keys []string
	if vl := reVarLabels.FindStringSubmatch(s); vl != nil && strings.TrimSpace(vl[1]) != "" {
		for _, k := range strings.Split(vl[1], ",") {
			if k = strings.TrimSpace(k); k != "" {
				keys = append(keys, k)
			}
		}
	}

	sort.Strings(keys)

	return fq[1] + "{" + strings.Join(keys, ",") + "}"
}

func metricGoldenBytes(lines []string) []byte {
	return []byte(strings.Join(lines, "\n") + "\n")
}

// TestMetricNamesGolden regenerates (with -update) or asserts the metric golden.
func TestMetricNamesGolden(t *testing.T) {
	lines := metricLines(t)
	got := metricGoldenBytes(lines)
	path := filepath.Join(repoRootMetrics(t), metricNamesGoldenRel)

	if *updateMetricGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}

		t.Logf("wrote %d metric-name lines to %s", len(lines), path)

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create): %v", path, err)
	}

	if string(got) != string(want) {
		t.Errorf("metric-name golden is stale (%d lines). Re-run: "+
			"go test ./internal/telemetry/prometheus/ -run TestMetricNamesGolden -update", len(lines))
	}
}

// TestMetricNamesKnownGood pins the fork-critical series (§01 §2.5, the highest
// drift risk) with their exact label sets.
func TestMetricNamesKnownGood(t *testing.T) {
	have := make(map[string]bool)
	for _, ln := range metricLines(t) {
		have[ln] = true
	}

	required := []string{
		"bitmagnet_search_shadow_jaccard{k}",
		"bitmagnet_search_shadow_rbo{}",
		"bitmagnet_search_shadow_top1_match_total{matched}",
		"bitmagnet_search_shadow_comparisons_total{}",
		"bitmagnet_search_tantivy_doc_count{}",
		"bitmagnet_search_pathsearch_doc_count{}",
		"bitmagnet_blob_consistency_checks_total{}",
		"bitmagnet_dht_server_query_duration_seconds{query}",
	}

	for _, r := range required {
		if !have[r] {
			t.Errorf("metric golden missing required series %q (dashboard/alert contract regression)", r)
		}
	}
}

// TestMetricNamesUncoveredTracked keeps the documented coverage gap honest: if a
// future engineer wires one of these families into this test, this list must be
// updated too. It asserts the golden does NOT yet contain them (so accidental
// partial coverage is caught and the header note stays truthful).
func TestMetricNamesUncoveredTracked(t *testing.T) {
	have := make(map[string]bool)
	for _, ln := range metricLines(t) {
		name := ln[:strings.IndexByte(ln, '{')]
		have[name] = true
	}

	uncoveredPrefixes := []string{
		"bitmagnet_dht_responder_",
		"bitmagnet_dht_ktable_",
		"bitmagnet_dht_crawler_",
		"bitmagnet_queue_jobs_total",
	}

	for name := range have {
		for _, p := range uncoveredPrefixes {
			if strings.HasPrefix(name, p) {
				t.Errorf("series %q is now covered — update the COVERAGE note and remove %q "+
					"from the uncovered list", name, p)
			}
		}
	}
}

func repoRootMetrics(t *testing.T) string {
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
