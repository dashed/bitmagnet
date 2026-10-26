package main

// flagson-attribution: G3's request attribution. With A = Go alone's stub
// requests, B = Rust then Go's (every phase), R = the requests made while Rust
// ran, and the abstained jobs = the fault jobs (faultplan.jsonl) a Rust
// abstention line names a torrent of, it checks that
//
//  1. A ⊆ B: Go, finishing what Rust left, made every request Go alone made;
//  2. B − A ⊆ R: every extra request is one Rust made;
//  3. every extra request is for a key of an abstained job;
//  4. R and A agree, as multisets, on every other key;
//  5. Rust abstained from every fault job of the corpus, each one a failure
//     Rust leaves to Go, and from nothing else;
//  6. by the worker's guard events (bitmagnet_processor::live, logged at
//     debug): no resolver call of a job started after the job's first
//     recorded failure, every abstained job had one recorded, and within a
//     phase a fault job's keys got at most 4 requests per call started for
//     it (resty's 4 transport attempts). No key got more 401 or transport
//     answers than Go's client makes in one call: 1 and 4. The stub's
//     arrival order would be racy here: calls that started before the
//     failure was recorded may still be in flight;
//  7. a phase whose /authentication the stub failed (-authentication-fault)
//     asked /authentication once and made no other request, and there is
//     one such phase exactly when the plan has holdAuth jobs.
//
// Each phase is one Rust process: its log (-rust-log, repeated), and beside it
// rust-requests-<log name without .log>.jsonl, the stub requests it made, as
// flags_on.sh writes them. Requests compare by request-diff's identity (key,
// status, source, api key validity); -ignore-endpoint leaves /authentication
// out of checks 1 to 6, as compare does.

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The resolver guard's log events, each with info_hash and call.
const (
	eventCallStarted     = "content resolver call started"
	eventFailureRecorded = "content resolver failure recorded; later calls are refused"
	eventCallRefused     = "content resolver call refused"
)

type guardEvent struct {
	kind, hash string
}

// rustPhase is one Rust process of a Rust then Go run.
type rustPhase struct {
	name       string
	abstention []string // the info hash each abstention line names, "" if none
	events     []guardEvent
	requests   []stubLogEntry // every request, /authentication included
}

func runFlagsOnAttribution(args []string) error {
	fs := flag.NewFlagSet("flagson-attribution", flag.ExitOnError)
	goLog := fs.String("go", "", "Go alone's stub request log")
	rtgLog := fs.String("rtg", "", "Rust then Go's stub request log, every phase")
	seedDir := fs.String("seed", "", "the corpus's seed dir (faultplan.jsonl)")
	ignore := fs.String("ignore-endpoint", endpointAuthentication, "endpoint family left out of checks 1 to 6")

	var rustLogs stringList

	fs.Var(&rustLogs, "rust-log", "a Rust phase's log (repeat for every phase)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if len(rustLogs) == 0 {
		return errors.New("usage: flagson-attribution -go A -rtg B -seed DIR -rust-log LOG [-rust-log LOG]")
	}

	plan, err := readJSONL[faultJobPlan](filepath.Join(*seedDir, "faultplan.jsonl"), 0)
	if err != nil {
		return err
	}

	a, err := attributionEntries(*goLog, *ignore)
	if err != nil {
		return err
	}

	b, err := attributionEntries(*rtgLog, *ignore)
	if err != nil {
		return err
	}

	phases := make([]rustPhase, 0, len(rustLogs))

	for _, path := range rustLogs {
		ph, err := readRustPhase(path)
		if err != nil {
			return err
		}

		phases = append(phases, ph)
	}

	report := attribute(plan, phases, a, b, *ignore)

	for _, line := range report.lines {
		_, _ = fmt.Fprintln(os.Stdout, line)
	}

	for _, v := range report.violations {
		_, _ = fmt.Fprintln(os.Stderr, "VIOLATION: "+v)
	}

	if len(report.violations) > 0 {
		return errors.New("flagson-attribution: violations")
	}

	return nil
}

func readRustPhase(logPath string) (rustPhase, error) {
	name := strings.TrimSuffix(filepath.Base(logPath), ".log")
	ph := rustPhase{name: name}

	raw, err := os.ReadFile(logPath)
	if err != nil {
		return ph, err
	}

	for _, line := range strings.Split(ansiEscape.ReplaceAllString(string(raw), ""), "\n") {
		hash := ""
		if m := logInfoHash.FindStringSubmatch(line); m != nil {
			hash = m[1]
		}

		switch {
		case strings.Contains(line, "abstaining from"):
			ph.abstention = append(ph.abstention, hash)
		case strings.Contains(line, eventCallStarted):
			ph.events = append(ph.events, guardEvent{eventCallStarted, hash})
		case strings.Contains(line, eventFailureRecorded):
			ph.events = append(ph.events, guardEvent{eventFailureRecorded, hash})
		case strings.Contains(line, eventCallRefused):
			ph.events = append(ph.events, guardEvent{eventCallRefused, hash})
		}
	}

	ph.requests, err = readJSONL[stubLogEntry](filepath.Join(filepath.Dir(logPath), "rust-requests-"+name+".jsonl"), 0)

	return ph, err
}

func attributionEntries(path, ignore string) ([]stubLogEntry, error) {
	entries, err := readJSONL[stubLogEntry](path, 0)
	if err != nil {
		return nil, err
	}

	return withoutEndpoint(entries, ignore), nil
}

func withoutEndpoint(entries []stubLogEntry, ignore string) []stubLogEntry {
	out := make([]stubLogEntry, 0, len(entries))

	for _, e := range entries {
		if e.Endpoint != ignore {
			out = append(out, e)
		}
	}

	return out
}

func requestIdentity(e stubLogEntry) string {
	return fmt.Sprintf("%s | status=%d | source=%s | apiKeyOk=%t", e.Key, e.Status, e.Source, e.APIKeyOK)
}

func identityCounts(entries []stubLogEntry, keep func(stubLogEntry) bool) map[string]int {
	m := map[string]int{}

	for _, e := range entries {
		if keep == nil || keep(e) {
			m[requestIdentity(e)]++
		}
	}

	return m
}

type attributionReport struct {
	lines, violations []string
}

func (rep *attributionReport) violate(format string, args ...any) {
	rep.violations = append(rep.violations, fmt.Sprintf(format, args...))
}

func attribute(plan []faultJobPlan, phases []rustPhase, a, b []stubLogEntry, ignore string) attributionReport {
	var rep attributionReport

	jobOfHash, jobOfKey := map[string]int{}, map[string]int{}

	for i, job := range plan {
		for _, t := range job.Torrents {
			jobOfHash[t.Hash] = i

			for _, k := range t.Keys {
				jobOfKey[k] = i
			}
		}
	}

	var r []stubLogEntry

	abstained := map[int]bool{}

	for _, ph := range phases {
		r = append(r, withoutEndpoint(ph.requests, ignore)...)

		for _, hash := range ph.abstention {
			i, ok := jobOfHash[hash]
			if !ok {
				rep.violate("5: %s: an abstention outside the fault jobs (info_hash %q)", ph.name, hash)
				continue
			}

			abstained[i] = true
		}
	}

	attributeRequests(&rep, plan, jobOfKey, abstained, a, b, r)
	attributeGuard(&rep, plan, jobOfHash, jobOfKey, abstained, phases, ignore)
	attributeKeyValidation(&rep, plan, phases)

	return rep
}

// attributeRequests: checks 1 to 5.
func attributeRequests(rep *attributionReport, plan []faultJobPlan, jobOfKey map[string]int,
	abstained map[int]bool, a, b, r []stubLogEntry,
) {
	abstainedClasses, keptClasses := make([]string, 0, len(plan)), make([]string, 0, len(plan))

	for i, job := range plan {
		if abstained[i] {
			abstainedClasses = append(abstainedClasses, job.Class)
			continue
		}

		keptClasses = append(keptClasses, job.Class)
		rep.violate("5: job %d (%s): Rust kept a job it must leave to Go", job.Index, job.Class)
	}

	rep.lines = append(rep.lines,
		fmt.Sprintf("attribution: Rust abstained from %d of %d fault jobs %v", len(abstainedClasses), len(plan), abstainedClasses),
		fmt.Sprintf("attribution: Rust kept %d fault jobs %v", len(keptClasses), keptClasses))

	onAbstainedKey := func(key string) bool {
		i, ok := jobOfKey[key]
		return ok && abstained[i]
	}

	ac, bc, rc := identityCounts(a, nil), identityCounts(b, nil), identityCounts(r, nil)
	extra := 0

	for _, id := range sortedIdentities(ac, bc) {
		switch d := bc[id] - ac[id]; {
		case d < 0:
			rep.violate("1: Go alone made %d more of %s", -d, id)
		case d > 0:
			extra += d

			if rc[id] < d {
				rep.violate("2: %d extra of %s, Rust made %d", d, id, rc[id])
			}

			if key, _, _ := strings.Cut(id, " | status="); !onAbstainedKey(key) {
				rep.violate("3: %d extra of %s, not a key of an abstained job", d, id)
			}
		}
	}

	rep.lines = append(rep.lines, fmt.Sprintf("attribution: Go alone %d, Rust then Go %d, Rust %d requests; "+
		"B-A = %d extra, all made by Rust on abstained jobs' keys unless listed", len(a), len(b), len(r), extra))

	kept := func(e stubLogEntry) bool { return !onAbstainedKey(e.Key) }
	rk, ak := identityCounts(r, kept), identityCounts(a, kept)
	keptDiffs := 0

	for _, id := range sortedIdentities(rk, ak) {
		if rk[id] != ak[id] {
			keptDiffs++

			rep.violate("4: Rust made %d of %s, Go alone %d", rk[id], id, ak[id])
		}
	}

	rep.lines = append(rep.lines, fmt.Sprintf("attribution: off the abstained jobs' keys, Rust %d vs Go alone %d requests, %d differ",
		sum(rk), sum(ak), keptDiffs))
}

// attributeGuard: check 6.
func attributeGuard(rep *attributionReport, plan []faultJobPlan, jobOfHash, jobOfKey map[string]int,
	abstained map[int]bool, phases []rustPhase, ignore string,
) {
	recorded := map[int]bool{}
	started, refused, late := 0, 0, 0

	for _, ph := range phases {
		stopped, calls := map[int]bool{}, map[int]int{}

		for _, ev := range ph.events {
			i, ok := jobOfHash[ev.hash]
			if !ok {
				continue
			}

			switch ev.kind {
			case eventFailureRecorded:
				stopped[i], recorded[i] = true, true
			case eventCallStarted:
				started++
				calls[i]++

				if stopped[i] {
					late++

					rep.violate("6: %s: job %d (%s): a call for %s started after the job's failure was recorded",
						ph.name, plan[i].Index, plan[i].Class, ev.hash)
				}
			case eventCallRefused:
				refused++
			}
		}

		requests, answers := map[int]int{}, map[string]int{}

		for _, e := range withoutEndpoint(ph.requests, ignore) {
			i, ok := jobOfKey[e.Key]
			if !ok {
				continue
			}

			requests[i]++

			if e.Source != sourceFault || (e.FaultKind != faultUnauthorized && !isTransportFault(e.FaultKind)) {
				continue
			}

			answers[e.Key]++

			if limit := abstainAnswerLimit(e.FaultKind); answers[e.Key] == limit+1 {
				rep.violate("6: %s: job %d (%s): more than %d %s answers for %s, Go's client makes at most %d",
					ph.name, plan[i].Index, plan[i].Class, limit, e.FaultKind, e.Key, limit)
			}
		}

		for _, i := range sortedJobIndices(requests) {
			if requests[i] > 4*calls[i] {
				rep.violate("6: %s: job %d (%s): %d requests on its keys, but %d calls started (at most 4 requests each)",
					ph.name, plan[i].Index, plan[i].Class, requests[i], calls[i])
			}
		}
	}

	for i := range plan {
		if abstained[i] && !recorded[i] {
			rep.violate("6: job %d (%s): abstained without a recorded resolver failure", plan[i].Index, plan[i].Class)
		}
	}

	rep.lines = append(rep.lines, fmt.Sprintf("attribution: guard events on fault jobs: %d calls started, %d jobs with "+
		"a recorded failure, %d calls refused, %d calls started after their job's failure", started, len(recorded), refused, late))
}

// attributeKeyValidation: check 7.
func attributeKeyValidation(rep *attributionReport, plan []faultJobPlan, phases []rustPhase) {
	want := 0

	for _, job := range plan {
		if job.Hold == holdAuth {
			want = 1
			break
		}
	}

	got := 0

	for _, ph := range phases {
		auth, faulted, other := 0, false, 0

		for _, e := range ph.requests {
			if e.Endpoint != endpointAuthentication {
				other++
				continue
			}

			auth++
			faulted = faulted || e.Source == sourceFault
		}

		if !faulted {
			continue
		}

		got++

		rep.lines = append(rep.lines, fmt.Sprintf("attribution: %s, key validation failed: %d /authentication, "+
			"%d other requests", ph.name, auth, other))

		if auth != 1 || other != 0 {
			rep.violate("7: %s: %d /authentication and %d other requests under a failed key validation, want 1 and 0",
				ph.name, auth, other)
		}
	}

	if got != want {
		rep.violate("7: %d phases with a failed key validation, want %d", got, want)
	}
}

// abstainAnswerLimit is how many requests Go's client makes in one call that
// keeps meeting this fault: resty retries transport errors 3 times, nothing
// else.
func abstainAnswerLimit(kind string) int {
	if isTransportFault(kind) {
		return 4
	}

	return 1
}

func sortedIdentities(ms ...map[string]int) []string {
	seen := map[string]bool{}

	var out []string

	for _, m := range ms {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}

	sort.Strings(out)

	return out
}

func sortedJobIndices(m map[int]int) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	sort.Ints(out)

	return out
}

func sum(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}

	return n
}
