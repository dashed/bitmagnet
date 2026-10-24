package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func testHash(n int) protocol.ID { return fixtureHash("test", fmt.Sprint(n)) }

func TestCanonicalHashesKeepsTheMissingPrefix(t *testing.T) {
	t.Parallel()

	m1, m2, a, b, c := testHash(1), testHash(2), testHash(3), testHash(4), testHash(5)
	missing := func(h protocol.ID) bool { return h == m1 || h == m2 }

	sorted := []protocol.ID{a, b, c}
	slices.SortFunc(sorted, func(x, y protocol.ID) int { return strings.Compare(x.String(), y.String()) })

	got := canonicalHashes([]protocol.ID{m2, m1, sorted[2], sorted[0], sorted[1]}, missing)
	if want := []protocol.ID{m2, m1, sorted[0], sorted[1], sorted[2]}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// A missing hash after a present one is not part of the prefix: it sorts
	// with the rest, so a different placement shows in the dump.
	got = canonicalHashes([]protocol.ID{sorted[1], m1}, missing)
	if slices.Index(got, m1) == 0 && m1.String() > sorted[1].String() {
		t.Fatalf("missing hash kept its place after a present one: %v", got)
	}
}

func TestCanonicalJobError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in, want         string
		reordered, ports bool
	}{
		{in: "job failed to process: b\na", want: "job failed to process: a\nb", reordered: true},
		{
			in:        "job failed to process: missing 2 info hashes\nz\ny",
			want:      "job failed to process: missing 2 info hashes\ny\nz",
			reordered: true,
		},
		// "missing" only leads; anywhere else it sorts with the rest.
		{in: "job failed to process: z\nmissing 1 info hashes", want: "job failed to process: missing 1 info hashes\nz", reordered: true},
		{in: "no prefix", want: "no prefix"},
		{
			in:    `job failed to process: Get "u": read tcp 127.0.0.1:50422->127.0.0.1:38573: read: connection reset by peer`,
			want:  `job failed to process: Get "u": read tcp 127.0.0.1:<port>->127.0.0.1:38573: read: connection reset by peer`,
			ports: true,
		},
	} {
		got, reordered, ports := canonicalJobError(tc.in)
		if got != tc.want || reordered != tc.reordered || ports != tc.ports {
			t.Errorf("canonicalJobError(%q) = %q, %t, %t; want %q, %t, %t",
				tc.in, got, reordered, ports, tc.want, tc.reordered, tc.ports)
		}
	}
}

func TestCanonicalPayloadVerifiesTheFingerprint(t *testing.T) {
	t.Parallel()

	m, a, b := testHash(10), testHash(11), testHash(12)
	if a.String() > b.String() {
		a, b = b, a
	}

	job, err := processor.NewQueueJob(processor.MessageParams{
		ClassifierWorkflow: "default",
		ClassifierFlags:    flagsOnFlags(),
		InfoHashes:         []protocol.ID{m, b, a},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The stored payload went through jsonb, which reorders keys and spaces.
	var v map[string]any
	if err := json.Unmarshal([]byte(job.Payload), &v); err != nil {
		t.Fatal(err)
	}

	stored, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		t.Fatal(err)
	}

	missing := func(h protocol.ID) bool { return h == m }

	fp, payload, verified, err := canonicalPayload(job.Fingerprint, string(stored), missing)
	if err != nil || !verified {
		t.Fatalf("verified %t, err %v", verified, err)
	}

	want, err := processor.NewQueueJob(processor.MessageParams{
		ClassifierWorkflow: "default",
		ClassifierFlags:    flagsOnFlags(),
		InfoHashes:         []protocol.ID{m, a, b},
	})
	if err != nil {
		t.Fatal(err)
	}

	if fp != want.Fingerprint || payload != want.Payload {
		t.Fatalf("got %s %s, want %s %s", fp, payload, want.Fingerprint, want.Payload)
	}

	fp, payload, verified, err = canonicalPayload("0000", string(stored), missing)
	if err != nil || verified || fp != "unverified:0000" || payload != string(stored) {
		t.Fatalf("a wrong fingerprint: got %s %t %v", fp, verified, err)
	}
}

func newTestStub(t *testing.T, faults []stubFault, authFault string) (*httptest.Server, *bytes.Buffer) {
	t.Helper()

	var log bytes.Buffer

	steps, err := stubFaultSteps(faults, authFault)
	if err != nil {
		t.Fatal(err)
	}

	stub := &tmdbStub{
		basePath: "/3", apiKey: "k", timeoutDelay: time.Minute,
		responses: map[string]stubResponse{},
		faults:    steps,
		served:    map[string]int{},
		started:   time.Now(),
		log:       &log,
	}

	srv := httptest.NewUnstartedServer(stub)
	srv.Config.SetKeepAlivesEnabled(false)
	srv.Start()
	t.Cleanup(srv.Close)

	return srv, &log
}

func TestStubFaultSequences(t *testing.T) {
	t.Parallel()

	seqKey := searchKey(endpointSearchMovie, "Zorq", 0)
	resetKey := searchKey(endpointSearchMovie, "Vexq", 0)
	srv, log := newTestStub(t, []stubFault{
		{Key: seqKey, Sequence: []string{faultInternal, faultRateLimited, stepOK}},
		{Key: resetKey, Fault: faultReset},
	}, "")

	get := func(query string) (int, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			srv.URL+"/3/search/movie?api_key=k&include_adult=true&query="+query, nil)
		if err != nil {
			return 0, err
		}

		res, err := srv.Client().Do(req)
		if err != nil {
			return 0, err
		}

		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()

		return res.StatusCode, nil
	}

	statuses := make([]int, 0, 4)

	for range 4 {
		status, err := get("Zorq")
		if err != nil {
			t.Fatal(err)
		}

		statuses = append(statuses, status)
	}

	if want := []int{500, 429, 200, 200}; !slices.Equal(statuses, want) {
		t.Fatalf("sequence answered %v, want %v", statuses, want)
	}

	if _, err := get("Vexq"); err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("a reset fault answered err %v", err)
	}

	lines := strings.Split(strings.TrimSpace(log.String()), "\n")
	entries := make([]stubLogEntry, 0, len(lines))

	for _, line := range lines {
		var e stubLogEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}

		entries = append(entries, e)
	}

	steps := make([]string, 0, len(entries))
	sources := make([]string, 0, len(entries))

	for _, e := range entries {
		steps = append(steps, fmt.Sprint(e.Step))
		sources = append(sources, e.Source+"/"+e.FaultKind)
	}

	if want := []string{"1", "2", "3", "4", "1"}; !slices.Equal(steps, want) {
		t.Errorf("steps %v, want %v", steps, want)
	}

	if want := []string{"fault/500", "fault/429", "default/", "default/", "fault/reset"}; !slices.Equal(sources, want) {
		t.Errorf("sources %v, want %v", sources, want)
	}

	if _, err := (stubFault{Key: "k", Fault: "500", Sequence: []string{"ok"}}).steps(); err == nil {
		t.Error("a fault with both fault and sequence was accepted")
	}
}

func TestStubAuthenticationFault(t *testing.T) {
	t.Parallel()

	srv, log := newTestStub(t, nil, faultInternal)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/3/authentication?api_key=k", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}

	_ = res.Body.Close()

	var e stubLogEntry
	if err := json.Unmarshal(bytes.TrimSpace(log.Bytes()), &e); err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusInternalServerError || e.Endpoint != endpointAuthentication ||
		e.Source != sourceFault || e.FaultKind != faultInternal {
		t.Errorf("answered %d, logged %+v", res.StatusCode, e)
	}

	if _, err := stubFaultSteps([]stubFault{{Key: "GET /authentication", Fault: faultUnauthorized}}, faultInternal); err == nil {
		t.Error("an /authentication fixture and -authentication-fault were both accepted")
	}
}

// simulateClass runs the fault model over one job of a class, with roles and
// steps from the class and keys named after the slot.
func simulateClass(t *testing.T, name string) ([]string, map[string]int) {
	t.Helper()

	var class faultClass

	for _, c := range slices.Concat(faultCorpusClasses, abstainCorpusClasses) {
		if c.name == name {
			class = c
		}
	}

	if class.name == "" {
		t.Fatalf("no class %s", name)
	}

	job := faultJobPlan{Class: name}
	m := &faultModel{
		byHash: map[string]faultTorrent{}, served: map[string]int{}, requests: map[string]int{},
		deleted: map[string]bool{},
	}

	for i, sl := range class.slots {
		ft := faultTorrent{Hash: fmt.Sprintf("h%02d", i), Role: sl.role, Steps: sl.steps}
		if sl.role == roleFault {
			ft.Key = fmt.Sprintf("k%02d", i)
		}

		job.Torrents = append(job.Torrents, ft)
		m.byHash[ft.Hash] = ft
	}

	simulated := m.simulate(job)
	rows := make([]string, 0, len(simulated)+len(m.deleted))

	for _, r := range simulated {
		if len(r.hashes) > 3 {
			r.hashes = []string{fmt.Sprintf("%d hashes", len(r.hashes))}
		}

		rows = append(rows, r.String())
	}

	for h := range m.deleted {
		rows = append(rows, "deleted "+h)
	}

	return rows, m.requests
}

func TestFaultModel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		class    string
		rows     []string
		requests map[string]int
	}{
		{
			class: "P1-500",
			rows: []string{
				"9 hashes status=processed retries=0 errorLines=0",
				"h00 status=failed retries=2 errorLines=1",
			},
			requests: map[string]int{"k00": 4},
		},
		{
			class: "P2-429-ok",
			rows: []string{
				"9 hashes status=processed retries=0 errorLines=0",
				"h00 status=processed retries=0 errorLines=0",
			},
			requests: map[string]int{"k00": 2},
		},
		{
			class: "P3-multi",
			rows: []string{
				"13 hashes status=processed retries=0 errorLines=0",
				"5 hashes status=processed retries=0 errorLines=0",
				"h00,h01,h02 status=failed retries=2 errorLines=3",
			},
			requests: map[string]int{"k00": 5, "k01": 5, "k02": 5, "k03": 2, "k04": 2},
		},
		{
			class:    "P4-all-failed-3-deletion",
			rows:     []string{"h00,h01,h02 status=failed retries=2 errorLines=2"},
			requests: map[string]int{"k00": 3, "k01": 3},
		},
		{
			class:    "P5-last-retry-recovers",
			rows:     []string{"h00 status=processed retries=2 errorLines=1"},
			requests: map[string]int{"k00": 3},
		},
		{
			class: "P5-partial-recovery",
			rows: []string{
				"h00,h01 status=processed retries=1 errorLines=2",
				"h01 status=processed retries=0 errorLines=0",
			},
			requests: map[string]int{"k00": 2, "k01": 3},
		},
		{
			class: "P6-missing-recovers",
			rows: []string{
				"11 hashes status=processed retries=0 errorLines=0",
				"h00,h01 status=failed retries=2 errorLines=1",
				"h00,h01,h02 status=processed retries=0 errorLines=0",
			},
			requests: map[string]int{"k02": 2},
		},
		{
			class: "A1-401",
			rows: []string{
				"6 hashes status=processed retries=0 errorLines=0",
				"h00 status=failed retries=2 errorLines=1",
			},
			requests: map[string]int{"k00": 1},
		},
		{
			class:    "P7-timeout-ok",
			rows:     []string{"9 hashes status=processed retries=0 errorLines=0"},
			requests: map[string]int{"k00": 2},
		},
		{
			class: "A5-reset-500-missing",
			rows: []string{
				"10 hashes status=processed retries=0 errorLines=0",
				"h00,h01,h02 status=failed retries=2 errorLines=3",
			},
			requests: map[string]int{"k00": 4, "k02": 16},
		},
		{
			class:    "A6-auth-500",
			rows:     []string{"8 hashes status=processed retries=0 errorLines=0"},
			requests: map[string]int{},
		},
	} {
		rows, requests := simulateClass(t, tc.class)
		slices.Sort(rows)

		if !slices.Equal(rows, tc.rows) {
			t.Errorf("%s: rows\n%s\nwant\n%s", tc.class, strings.Join(rows, "\n"), strings.Join(tc.rows, "\n"))
		}

		if fmt.Sprint(requests) != fmt.Sprint(tc.requests) {
			t.Errorf("%s: requests %v, want %v", tc.class, requests, tc.requests)
		}
	}
}

func TestAttribution(t *testing.T) {
	t.Parallel()

	plan := []faultJobPlan{
		{Index: 7, Class: "A3-reset", Torrents: []faultTorrent{
			{Hash: "h0", Keys: []string{"abs"}}, {Hash: "h1", Keys: []string{"abs-companion"}},
		}},
		{Index: 9, Class: "A6-auth-500", Hold: holdAuth, Torrents: []faultTorrent{{Hash: "h2", Keys: []string{"auth-io"}}}},
	}
	e := func(key, source, kind string) stubLogEntry {
		endpoint := "other"
		if key == endpointAuthentication {
			endpoint = endpointAuthentication
		}

		return stubLogEntry{Key: key, Status: 200, Source: source, FaultKind: kind, Endpoint: endpoint}
	}
	started := func(h string) guardEvent { return guardEvent{eventCallStarted, h} }
	recorded := func(h string) guardEvent { return guardEvent{eventFailureRecorded, h} }
	refused := func(h string) guardEvent { return guardEvent{eventCallRefused, h} }

	type scenario struct {
		goAlone, rtg []stubLogEntry
		phases       []rustPhase
	}

	// Rust resolves the kept job, abstains from h0's job after one reset and
	// refuses h1's call; then, under a failed key validation, from h2's job
	// without a request. Go makes the abstained jobs' requests again.
	clean := func() scenario {
		kept, abs, authIO := e("kept", "fixture", ""), e("abs", sourceFault, faultReset), e("auth-io", "fixture", "")

		return scenario{
			goAlone: []stubLogEntry{kept, abs, authIO},
			rtg:     []stubLogEntry{kept, abs, abs, authIO},
			phases: []rustPhase{
				{
					name: "rust", abstention: []string{"h0"},
					events:   []guardEvent{started("h0"), recorded("h0"), refused("h1")},
					requests: []stubLogEntry{kept, abs},
				},
				{
					name: "rust-auth", abstention: []string{"h2"},
					events:   []guardEvent{started("h2"), recorded("h2")},
					requests: []stubLogEntry{e(endpointAuthentication, sourceFault, faultInternal)},
				},
			},
		}
	}
	run := func(s scenario) attributionReport {
		return attribute(plan, s.phases, withoutEndpoint(s.goAlone, endpointAuthentication),
			withoutEndpoint(s.rtg, endpointAuthentication), endpointAuthentication)
	}

	if rep := run(clean()); len(rep.violations) != 0 {
		t.Fatalf("clean run: %v", rep.violations)
	}

	for _, tc := range []struct {
		name   string
		mutate func(s *scenario)
		want   string
	}{
		{"Go made fewer", func(s *scenario) { s.rtg = s.rtg[:1] }, "1:"},
		{"an extra not Rust's", func(s *scenario) { s.rtg = append(s.rtg, s.rtg[1]) }, "2:"},
		{"an extra off the abstained keys", func(s *scenario) { s.goAlone = s.goAlone[1:] }, "3:"},
		{"Rust differs off the abstained keys", func(s *scenario) {
			s.phases[0].requests = append(s.phases[0].requests, s.goAlone[0])
		}, "4:"},
		{"Rust kept a job it must leave", func(s *scenario) { s.phases[0].abstention = nil }, "5:"},
		{"an abstention outside the fault jobs", func(s *scenario) {
			s.phases[0].abstention = append(s.phases[0].abstention, "zz")
		}, "5:"},
		{"a call started after the failure", func(s *scenario) {
			s.phases[0].events = append(s.phases[0].events, started("h1"))
		}, "6:"},
		{"no recorded failure", func(s *scenario) { s.phases[0].events = []guardEvent{started("h0")} }, "6:"},
		{"a request without a started call", func(s *scenario) { s.phases[0].events = []guardEvent{recorded("h0")} }, "6:"},
		{"more transport attempts than Go's", func(s *scenario) {
			abs := s.phases[0].requests[1]
			s.phases[0].events = append([]guardEvent{started("h0")}, s.phases[0].events...)
			s.phases[0].requests = append(s.phases[0].requests, abs, abs, abs, abs)
		}, "6:"},
		{"a request under a failed key validation", func(s *scenario) {
			s.phases[1].requests = append(s.phases[1].requests, e("auth-io", "fixture", ""))
		}, "7:"},
		{"no failed key validation", func(s *scenario) {
			s.phases[1].requests = []stubLogEntry{e(endpointAuthentication, "default", "")}
		}, "7:"},
	} {
		s := clean()
		tc.mutate(&s)

		rep := run(s)
		if !slices.ContainsFunc(rep.violations, func(v string) bool { return strings.HasPrefix(v, tc.want) }) {
			t.Errorf("%s: violations %v, want one starting %q", tc.name, rep.violations, tc.want)
		}
	}
}
