package main

// The fault corpora (flagson-seed -corpus fault|abstain): jobs whose TMDB
// requests fail, by class, among ordinary replay jobs.
//
//   - fault: failures the Go processor handles per torrent, and transport
//     faults its client's retry recovers from, which the Rust worker must
//     reproduce. G2-fault runs Rust alone against Go alone.
//   - abstain: failures Rust leaves to Go, checked Rust then Go. A 401 latches
//     Go's client fail-fast for the process, and a failed key validation
//     latches both clients. A transport fault's Go error text quotes the
//     request URL, api_key included.
//
// What Go does with a failure (internal/tmdb, internal/processor,
// internal/queue/server):
//
//   - resty retries only transport errors: 4 attempts, waits from 2s to 20s,
//     10s client timeout. Any HTTP status is final on the first response, 429
//     included. A details 404 is an unmatched torrent, not a failure; a search
//     404 is a failure.
//   - A job with failed torrents and at least one classified torrent persists
//     and republishes the failed hashes as a new job: missing hashes first,
//     then failures in goroutine completion order. The job itself ends
//     processed.
//   - A job with failed torrents and none classified returns
//     errors.Join(errs), "missing N info hashes" first, and persists nothing,
//     deletions included. The queue retries it twice (max_retries 2), then
//     fails it. A later attempt that succeeds keeps the error text.
//
// Every faulted or companion I/O torrent of a fault job is pseudo-titled, so no
// other job can see its keys. Every key has one owner, so a fault sequence's
// per-key count is deterministic. faultplan.jsonl records each fault job's
// torrents, roles, fault steps and request keys for flagson-faultcheck and
// flagson-attribution.

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// Roles of a fault job's torrents.
const (
	roleFault      = "fault"       // its search, or details, request has a fault fixture
	roleIO         = "io"          // a companion whose TMDB search is answered normally
	roleDetails404 = "details-404" // a companion whose match's details are a 404: unmatched
	roleNoIO       = "no-io"       // classified without any search
	roleMissing    = "missing"     // a hash with no torrent row
	roleDeleted    = "deleted"     // deleted by the classifier's rules
)

// faultSlot is one torrent of a fault class's job.
type faultSlot struct {
	role    string
	steps   []string // roleFault: the stub's answers to its key, in turn
	details bool     // roleFault: fault the details request after a matching search
}

// Hold kinds. A held fault job is parked (heldRunAfter) until flags_on.sh,
// after the unheld jobs, releases it into a fresh worker process.
const (
	// holdAlone: released one job at a time. A 401 latches both TMDB clients
	// fail-fast for the life of the process.
	holdAlone = "alone"
	// holdAuth: released together into one process. In Rust then Go, that
	// Rust process's stub answers /authentication with a 500. Both clients
	// validate the key lazily and latch the outcome, so Rust abstains from
	// every one of these jobs; Go, given a healthy stub, settles them.
	holdAuth = "auth"
)

type faultClass struct {
	name  string
	count int
	hold  string // "", holdAlone or holdAuth
	slots []faultSlot
}

func searchFault(steps ...string) faultSlot { return faultSlot{role: roleFault, steps: steps} }

func detailsFault(steps ...string) faultSlot {
	return faultSlot{role: roleFault, steps: steps, details: true}
}

func repeatSlot(role string, n int) []faultSlot {
	out := make([]faultSlot, n)
	for i := range out {
		out[i] = faultSlot{role: role}
	}

	return out
}

func joinSlots(groups ...[]faultSlot) []faultSlot {
	var out []faultSlot
	for _, g := range groups {
		out = append(out, g...)
	}

	return out
}

// mixed is a job where something is classified, so Go republishes its
// failures: the given slots, two I/O companions, a details-404 companion and
// five torrents without I/O.
func mixed(slots ...faultSlot) []faultSlot {
	return joinSlots(slots, repeatSlot(roleIO, 2), repeatSlot(roleDetails404, 1), repeatSlot(roleNoIO, 5))
}

// faultCorpusClasses: failures Go handles per torrent. The comments give Go's
// expected chain (J = the seeded job, J' its republish), which
// flagson-faultcheck checks against Go alone.
var faultCorpusClasses = []faultClass{
	// J processed; J'{T} fails 3 attempts -> failed. 4 requests for T's key.
	{name: "P1-500", count: 3, slots: mixed(searchFault(faultInternal))},
	{name: "P1-503", count: 3, slots: mixed(searchFault(faultUnavailable))},
	{name: "P1-429", count: 3, slots: mixed(searchFault(faultRateLimited))},
	{name: "P1-search-404", count: 3, slots: mixed(searchFault(faultNotFound))},
	{name: "P1-details-500", count: 3, slots: mixed(detailsFault(faultInternal))},
	// J processed; J'{T} processed on its first attempt. 2 requests.
	{name: "P2-500-ok", count: 2, slots: mixed(searchFault(faultInternal, stepOK))},
	{name: "P2-503-ok", count: 2, slots: mixed(searchFault(faultUnavailable, stepOK))},
	{name: "P2-429-ok", count: 2, slots: mixed(searchFault(faultRateLimited, stepOK))},
	// Several failures in one job: J'{5} (completion order) recovers two,
	// J''{3} (completion order) fails with three joined lines.
	{name: "P3-multi", count: 4, slots: mixed(
		searchFault(faultInternal), searchFault(faultUnavailable), searchFault(faultRateLimited),
		searchFault(faultInternal, stepOK), detailsFault(faultUnavailable, stepOK),
	)},
	// Nothing classified: retry, retry, failed; 3 requests per key; the
	// deletion is never persisted.
	{name: "P4-all-failed", count: 1, slots: []faultSlot{searchFault(faultInternal)}},
	{name: "P4-all-failed-3", count: 1, slots: []faultSlot{
		searchFault(faultInternal), searchFault(faultUnavailable), searchFault(faultRateLimited),
	}},
	{name: "P4-all-failed-deletion", count: 1, slots: []faultSlot{searchFault(faultInternal), {role: roleDeleted}}},
	{name: "P4-all-failed-3-deletion", count: 1, slots: []faultSlot{
		searchFault(faultInternal), searchFault(faultNotFound), {role: roleDeleted},
	}},
	// Nothing classified, then a retry recovers: processed with retries 1 or
	// 2, keeping the failed attempt's error; or failed before the "ok".
	{name: "P5-retry-recovers", count: 2, slots: []faultSlot{searchFault(faultInternal, stepOK)}},
	{name: "P5-last-retry-recovers", count: 2, slots: []faultSlot{searchFault(faultInternal, faultInternal, stepOK)}},
	{name: "P5-never-recovers", count: 1, slots: []faultSlot{
		searchFault(faultInternal, faultInternal, faultInternal, stepOK),
	}},
	// Attempt 1 fails both (2 joined lines); attempt 2 classifies one and
	// republishes the other, keeping attempt 1's error; J' recovers.
	{name: "P5-partial-recovery", count: 1, slots: []faultSlot{
		searchFault(faultInternal, stepOK), searchFault(faultUnavailable, faultUnavailable, stepOK),
	}},
	// Missing hashes ride along: J'{m1,m2,T} fails with "missing 2 info
	// hashes" first; when T recovers, J''{m1,m2} fails on the missing alone.
	{name: "P6-missing-mixed", count: 2, slots: joinSlots(
		[]faultSlot{{role: roleMissing}}, mixed(searchFault(faultInternal), faultSlot{role: roleMissing}),
	)},
	{name: "P6-missing-recovers", count: 2, slots: joinSlots(
		repeatSlot(roleMissing, 2), mixed(searchFault(faultInternal, stepOK)),
	)},
	{name: "P6-missing-all-failed", count: 2, slots: []faultSlot{{role: roleMissing}, searchFault(faultInternal)}},
	// resty's retry recovers inside the call: T classified, 2 requests. The
	// Rust client retries transport errors as resty does.
	{name: "P7-reset-ok", count: 2, slots: mixed(searchFault(faultReset, stepOK))},
	{name: "P7-timeout-ok", count: 2, slots: mixed(searchFault(faultTimeout, stepOK))},
}

// abstainCorpusClasses: failures Rust leaves to Go.
var abstainCorpusClasses = []faultClass{
	// T fails and latches the client: J'{T} fails 3 attempts without a request.
	// No other torrent of the job does I/O, since the latch's timing would
	// decide theirs.
	{name: "A1-401", count: 2, hold: holdAlone, slots: joinSlots(
		[]faultSlot{searchFault(faultUnauthorized)}, repeatSlot(roleNoIO, 5),
	)},
	// 4 attempts per call: J, then J'{T} 3 times -> failed. 16 requests.
	{name: "A2-timeout", count: 2, slots: mixed(searchFault(faultTimeout))},
	{name: "A3-reset", count: 2, slots: mixed(searchFault(faultReset))},
	// A per-torrent failure and a missing hash before the transport fault:
	// Rust must drop what it saw and leave the whole job.
	{name: "A5-reset-500-missing", count: 2, slots: joinSlots(
		[]faultSlot{searchFault(faultInternal), {role: roleMissing}, searchFault(faultReset)},
		repeatSlot(roleIO, 1), repeatSlot(roleDetails404, 1), repeatSlot(roleNoIO, 5),
	)},
	// No fault of its own: the key validation fails (holdAuth). Rust latches
	// the 500 at its first TMDB call and abstains from both jobs, the second
	// without a request; Go, never shown the fault, processes them as usual.
	{name: "A6-auth-500", count: 2, hold: holdAuth, slots: mixed()},
}

// faultJobPlan is one fault job (faultplan.jsonl).
type faultJobPlan struct {
	Index       int            `json:"index"` // in jobs.jsonl
	Class       string         `json:"class"`
	Fingerprint string         `json:"fingerprint"`
	Hold        string         `json:"hold,omitempty"` // see faultClass
	Torrents    []faultTorrent `json:"torrents"`
}

// faultTorrent is one torrent of a fault job, in the job's info hash order.
type faultTorrent struct {
	Hash  string   `json:"hash"`
	Role  string   `json:"role"`
	Key   string   `json:"key,omitempty"`   // roleFault: the faulted request
	Steps []string `json:"steps,omitempty"` // roleFault: its fault steps
	// Keys is every TMDB request the torrent can make: its search, and the
	// details of every result its search answers.
	Keys []string `json:"keys,omitempty"`
}

func faultClassSlots(classes []faultClass, role string) int {
	n := 0

	for _, c := range classes {
		for _, sl := range c.slots {
			if sl.role == role {
				n += c.count
			}
		}
	}

	return n
}

func (s *seedRun) seedFaultCorpus(classes []faultClass) error {
	o := s.opts

	replay, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier-replay/inputs.jsonl"), o.faultReplay)
	if err != nil {
		return err
	}

	for _, in := range replay {
		if _, err := s.addTorrent(o.corpus, sliceMain, in); err != nil {
			return err
		}
	}

	if err := s.addFaultDeletions(faultClassSlots(classes, roleDeleted)); err != nil {
		return err
	}

	// Fault jobs hold only torrents whose titles are pseudo-words no other
	// title contains. Distinct exact keys would not be enough: local search is
	// a phrase query, so content titled "A B C" is a candidate for base title
	// "A B", and moving a job could then change another torrent's result.
	need := faultClassSlots(classes, roleFault) + faultClassSlots(classes, roleIO) +
		faultClassSlots(classes, roleDetails404)
	pseudo := map[string]bool{}

	for i := range need + need/2 + 10 {
		h, err := s.addTorrent(o.corpus+"-pseudo", sliceMain, pseudoInput(i))
		if err != nil {
			return err
		}

		pseudo[h.String()] = true
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	pools, err := faultPools(planned, pseudo)
	if err != nil {
		return err
	}

	built, err := s.buildFaultJobs(classes, pools)
	if err != nil {
		return err
	}

	sliceOfHash := map[string]string{}

	for _, j := range built {
		for _, t := range j.Torrents {
			sliceOfHash[t.Hash] = sliceFault
		}
	}

	for i := range planned {
		if sl, ok := sliceOfHash[planned[i].Hash]; ok {
			planned[i].Slice = sl
		}
	}

	// Everything outside the fault jobs gets ordinary fixtures, then the
	// fault jobs' I/O companions.
	if err := s.searchFixtures(planned, searchWeights{local: 30, tmdbNew: 40, tmdbExisting: 15, details404: 10, decoys: 10}); err != nil {
		return err
	}

	if err := s.companionFixtures(planned, sliceOfHash); err != nil {
		return err
	}

	var other []protocol.ID

	for _, h := range s.hashes {
		if _, inFault := sliceOfHash[h.String()]; !inFault {
			other = append(other, h)
		}
	}

	sortHashes(other)

	specs, faultSpec, held := interleaveFaultJobs(other, built, o.jobSize-1)

	jobs, err := s.finish(labelPlanned(planned, specs), digest, specs, held)
	if err != nil {
		return err
	}

	for i := range built {
		built[i].Index = faultSpec[i]
		built[i].Fingerprint = jobs[faultSpec[i]].Fingerprint

		for k := range built[i].Torrents {
			built[i].Torrents[k].Keys = s.torrentKeys(built[i].Torrents[k], planned)
		}
	}

	return writeJSONLFile(filepath.Join(o.out, "faultplan.jsonl"), built)
}

// addFaultDeletions queues copies of the first n synthetic inputs the rules
// delete.
func (s *seedRun) addFaultDeletions(n int) error {
	if n == 0 {
		return nil
	}

	synthetic, err := readJSONL[input](filepath.Join(s.opts.root, "testdata/parity/classifier/inputs.jsonl"), 0)
	if err != nil {
		return err
	}

	goldens, err := readJSONL[golden](filepath.Join(s.opts.root, "testdata/parity/classifier/corpus.golden.jsonl"), 0)
	if err != nil {
		return err
	}

	deleted := make(map[string]bool, len(goldens))
	for _, g := range goldens {
		deleted[g.ID] = g.Expected.Outcome == outcomeDeleted
	}

	for _, in := range synthetic {
		if n == 0 {
			break
		}

		if !deleted[in.ID] || (in.Hint != nil && in.Hint.ContentSource != "") {
			continue
		}

		if _, err := s.addTorrent(s.opts.corpus+"-deletion", sliceMain, in); err != nil {
			return err
		}

		n--
	}

	if n > 0 {
		return errors.New("fault corpus: too few synthetic deletion inputs")
	}

	return nil
}

// faultTorrentPools: pseudo-titled torrents with a local and a TMDB search of
// keys no other torrent makes, non-pseudo torrents without I/O, and deleted
// torrents.
type faultTorrentPools struct {
	unique, noIO, deleted []plannedTorrent
}

func faultPools(planned []plannedTorrent, pseudo map[string]bool) (*faultTorrentPools, error) {
	pools := &faultTorrentPools{}
	seenKeys := map[string]bool{}

	for _, p := range planned {
		k, hasLocal := localKeyOf(p)
		c, hasSearch := tmdbSearchOf(p)

		switch {
		case p.Outcome == outcomeDeleted && !pseudo[p.Hash] && len(p.Calls) == 0:
			pools.deleted = append(pools.deleted, p)
		case p.Outcome != outcomeClassified:
		case pseudo[p.Hash] && hasLocal && hasSearch:
			if seenKeys[k] || seenKeys[planSearchKey(c)] {
				return nil, fmt.Errorf("fault corpus: pseudo title %q is not unique", p.BaseTitle)
			}

			seenKeys[k], seenKeys[planSearchKey(c)] = true, true

			pools.unique = append(pools.unique, p)
		case !pseudo[p.Hash] && !hasLocal && !hasSearch:
			pools.noIO = append(pools.noIO, p)
		}
	}

	return pools, nil
}

func takeTorrent(pool *[]plannedTorrent, what string) (plannedTorrent, error) {
	if len(*pool) == 0 {
		return plannedTorrent{}, fmt.Errorf("fault corpus: too few %s torrents", what)
	}

	p := (*pool)[0]
	*pool = (*pool)[1:]

	return p, nil
}

// buildFaultJobs fills every class's jobs from the pools and writes their
// fault and companion fixtures.
func (s *seedRun) buildFaultJobs(classes []faultClass, pools *faultTorrentPools) ([]faultJobPlan, error) {
	var built []faultJobPlan

	for _, class := range classes {
		for k := range class.count {
			job := faultJobPlan{Class: class.name, Hold: class.hold}

			for i, slot := range class.slots {
				t, err := s.fillFaultSlot(slot, pools, fmt.Sprintf("%s/%d/%d", class.name, k, i))
				if err != nil {
					return nil, fmt.Errorf("%s: %w", class.name, err)
				}

				job.Torrents = append(job.Torrents, t)
			}

			built = append(built, job)
		}
	}

	return built, nil
}

func (s *seedRun) fillFaultSlot(slot faultSlot, pools *faultTorrentPools, id string) (faultTorrent, error) {
	switch slot.role {
	case roleMissing:
		return faultTorrent{Hash: fixtureHash(s.opts.corpus+"-missing", id).String(), Role: roleMissing}, nil
	case roleNoIO:
		p, err := takeTorrent(&pools.noIO, "no-I/O")
		return faultTorrent{Hash: p.Hash, Role: roleNoIO}, err
	case roleDeleted:
		p, err := takeTorrent(&pools.deleted, "deleted")
		return faultTorrent{Hash: p.Hash, Role: roleDeleted}, err
	}

	p, err := takeTorrent(&pools.unique, "pseudo-titled")
	if err != nil {
		return faultTorrent{}, err
	}

	t := faultTorrent{Hash: p.Hash, Role: slot.role}
	c, _ := tmdbSearchOf(p)
	group := c.Endpoint + "\x00" + c.Query

	switch slot.role {
	case roleIO:
		return t, nil // companionFixtures
	case roleDetails404:
		return t, s.tmdbGroup(group, []plannedCall{c}, "details-404", false)
	}

	t.Key, t.Steps = planSearchKey(c), slot.steps
	recovers := slices.Contains(slot.steps, stepOK)

	switch {
	case slot.details:
		tv := c.Endpoint == endpointSearchTV
		matchID := s.allocTMDBID()

		if err := s.respond(t.Key, 200, buildSearchBody(s.tpl, fixtureRand("fault/"+t.Key), tv,
			[]searchItem{{id: matchID, title: c.Query, year: c.Year}})); err != nil {
			return t, err
		}

		t.Key = detailsKey(tv, matchID)

		if recovers {
			if err := s.respond(t.Key, 200, buildDetailsBody(s.tpl, fixtureRand("fault-details/"+t.Key),
				detailsSpec{tv: tv, id: matchID, title: c.Query, year: c.Year})); err != nil {
				return t, err
			}
		}
	case recovers:
		// A recovered search finds new content.
		if err := s.tmdbGroup(group, []plannedCall{c}, categoryNew, false); err != nil {
			return t, err
		}
	}

	f := stubFault{Key: t.Key, Sequence: slot.steps}
	if len(slot.steps) == 1 {
		f = stubFault{Key: t.Key, Fault: slot.steps[0]}
	}

	s.faults = append(s.faults, f)
	s.note(fixtureNote{Kind: "fault", Key: t.Key, Category: strings.Join(slot.steps, ",")})

	return t, nil
}

// interleaveFaultJobs chunks the other torrents into jobs of step hashes and
// spreads the fault jobs evenly among them. It returns the specs, the spec
// index of every fault job, and the hold of every held spec.
func interleaveFaultJobs(other []protocol.ID, built []faultJobPlan, step int) ([]jobSpec, []int, map[int]string) {
	var specs []jobSpec

	faultSpec := make([]int, len(built))
	held := map[int]string{}
	every := max(1, (len(other)/step)/max(1, len(built)))
	f := 0

	for i := 0; i < len(other) || f < len(built); i += step {
		if i < len(other) {
			specs = append(specs, jobSpec{
				slice: sliceMain, flagsOn: true,
				hashes: append([]protocol.ID(nil), other[i:min(i+step, len(other))]...),
			})
		}

		if f < len(built) && ((i/step)%every == every-1 || i+step >= len(other)) {
			hashes := make([]protocol.ID, len(built[f].Torrents))
			for k, t := range built[f].Torrents {
				hashes[k] = mustHash(t.Hash)
			}

			if built[f].Hold != "" {
				held[len(specs)] = built[f].Hold
			}

			faultSpec[f] = len(specs)
			specs = append(specs, jobSpec{slice: sliceFault, flagsOn: true, hashes: hashes, fault: built[f].Class})
			f++
		}
	}

	return specs, faultSpec, held
}

// torrentKeys lists every TMDB request a fault job's torrent can make.
func (s *seedRun) torrentKeys(t faultTorrent, planned []plannedTorrent) []string {
	i, found := slices.BinarySearchFunc(planned, t.Hash, func(p plannedTorrent, h string) int {
		return strings.Compare(p.Hash, h)
	})
	if !found {
		return nil
	}

	c, ok := tmdbSearchOf(planned[i])
	if !ok {
		return nil
	}

	keys := []string{planSearchKey(c)}
	if t.Key != "" && t.Key != keys[0] {
		keys = append(keys, t.Key)
	}

	if r, ok := s.responses[planSearchKey(c)]; ok {
		var body searchResultIDs

		if json.Unmarshal(r.Body, &body) == nil {
			for _, item := range body.Results {
				if k := detailsKey(c.Endpoint == endpointSearchTV, item.ID); !slices.Contains(keys, k) {
					keys = append(keys, k)
				}
			}
		}
	}

	return keys
}

// searchResultIDs is the part of a TMDB search body torrentKeys reads.
type searchResultIDs struct {
	Results []searchResultID `json:"results"`
}

type searchResultID struct {
	ID int64 `json:"id"`
}

var pseudoSyllables = []string{"zor", "vex", "qui", "lam", "drav", "oth", "kem", "bru", "sil", "fen", "gax", "tor", "mib", "yul"}

// pseudoInput is a single-file video torrent whose title is two pseudo-words
// built from i, so no other title in the corpus contains either word.
func pseudoInput(i int) input {
	word := func(n int) string {
		w := ""
		for range 3 {
			w += pseudoSyllables[n%len(pseudoSyllables)]
			n /= len(pseudoSyllables)
		}

		return strings.ToUpper(w[:1]) + w[1:] + "q"
	}

	title := word(2*i+7) + " " + word(2*i+8+len(pseudoSyllables)*len(pseudoSyllables))

	name := fmt.Sprintf("%s %d 1080p BluRay x264-QZ.mkv", title, 1975+i%45)
	if i%2 == 1 {
		name = fmt.Sprintf("%s S%02dE%02d 720p HDTV x264-QZ.mkv", title, 1+i%5, 1+i%12)
	}

	return input{ID: fmt.Sprintf("p%d", i), Name: name, Size: 1_500_000_000 + uint64(i)*1_000_003, FilesStatus: "single"}
}

// companionFixtures answers the I/O companions riding in fault jobs: half
// find their content on TMDB, half find nothing.
func (s *seedRun) companionFixtures(planned []plannedTorrent, sliceOfHash map[string]string) error {
	faulted := map[string]bool{}
	for _, f := range s.faults {
		faulted[f.Key] = true
	}

	for _, p := range planned {
		if sliceOfHash[p.Hash] != sliceFault {
			continue
		}

		c, ok := tmdbSearchOf(p)
		if !ok || faulted[planSearchKey(c)] {
			continue
		}

		if _, answered := s.responses[planSearchKey(c)]; answered {
			continue // a details fault's search, a recovered search, a details-404 companion
		}

		category := categoryUnmatched
		if fixtureRand("companion/"+p.Hash).IntN(2) == 0 {
			category = categoryNew
		}

		if err := s.tmdbGroup(c.Endpoint+"\x00"+c.Query, []plannedCall{c}, category, false); err != nil {
			return err
		}
	}

	return nil
}
