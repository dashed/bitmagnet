package main

// The organic corpora (flags_on.sh PHASES="organic organic2"): process_torrent
// jobs as Go's own producers write them, which the Rust worker claims with
// --admit-organic up to --organic-max-priority (M4b), in disjoint slices of
// hint-free torrents, one producer shape per slice:
//
//   - dht: processor.NewQueueJob({InfoHashes}) as dhtcrawler/persist.go
//     queues it, priority 0. Its one-minute run_after delay is not
//     reproduced: every job here has a run_after the harness sets, so both
//     workers claim in one order.
//   - importer: the same payload at priority 20 (importer.go).
//   - retry: a processor republish, workflow "default" (processor.go).
//   - empty-workflow: the literal payload {"ClassifierWorkflow":"",...}.
//   - graphql-*: manager.EnqueueReprocessTorrentsBatch's parent job, and cli-*:
//     the real `bitmagnet reprocess` binary's, both expanded into their
//     process_torrent children by Go's own batch handler (a Go worker with
//     process_torrent disabled), in a copy of the seed holding only the
//     slice's torrents (flagson-organic-produce).
//
// Jobs record their payload bytes and priority, and flagson-organic-enqueue
// queues them verbatim. Three slices of pseudo-titled torrents ride along,
// whose keys no other torrent shares, so moving their jobs in the claim order
// changes nobody else's result:
//
//   - hinted (G5): DHT-shaped jobs with one sourced hint each, which Rust
//     claims and abstains from once, leaving them to Go;
//   - fault-* (G7): a per-torrent 500 in a DHT-shaped, a rematch and a
//     local-search-off batch child, whose republish must be Go's
//     {"ClassifierWorkflow":"default",...} row at priority 0, mode and flags
//     kept (flagson-organic-check);
//   - negative-*: payloads Rust's organic claim must never lock (G3), each
//     within any bound.
//
// organic2 is round 2 over Go's drained organic round 1: flagson-requeue
// -organic queues the round-1 torrents again with organic payloads, in
// ClassifyMode rematch and in the default mode (G6).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/manager"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"github.com/jackc/pgx/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	corpusOrganic  = "organic"
	corpusOrganic2 = "organic2"

	// Producers of an organic slice's jobs.
	producerDHT      = "dht"
	producerImporter = "importer"
	producerRetry    = "retry"
	producerPlanner  = "planner" // batch.Planner in process: the fault slices' batch children
	producerRaw      = "raw"
	producerGraphQL  = "graphql"
	producerCLI      = "cli"

	// Roles of the pseudo-titled slices.
	organicRoleHinted   = "hinted"
	organicRoleFault    = "fault"
	organicRoleNegative = "negative"

	// organicJobSize is dhtcrawler's classifyBatchSize and the batch
	// handler's default page.
	organicJobSize = 100

	batchQueue = batch.MessageName
)

// attachFlags are the classifier flags an organic payload may carry, in the
// plan's order.
var attachFlags = []string{"local_search_enabled", "apis_enabled", "tmdb_enabled"}

// organicSlice is one producer shape.
type organicSlice struct {
	name     string
	producer string
	role     string // "", organicRoleHinted, organicRoleFault or organicRoleNegative

	request manager.EnqueueReprocessTorrentsBatchRequest // producerGraphQL
	args    []string                                     // producerCLI: `bitmagnet reprocess` flags
	retry   processor.MessageParams                      // producerRetry: the republish, without hashes
	planner batch.MessageParams                          // producerPlanner: the parent
	raw     func(hashes []string) string                 // producerRaw: the payload
	mode    processor.ClassifyMode                       // producerDHT in round 2 (a default-mode reuse)

	jobs, hashes int // pseudo slices other than fault-*: jobs, and torrents per job
}

// batchProduced reports whether Go's batch handler makes the slice's jobs.
func (s organicSlice) batchProduced() bool {
	return s.producer == producerGraphQL || s.producer == producerCLI
}

// rawPayload is a producerRaw payload: format with the hashes' JSON array.
func rawPayload(format string, hash func(string) string) func([]string) string {
	return func(hashes []string) string {
		out := make([]string, len(hashes))
		for i, h := range hashes {
			out[i] = hash(h)
		}

		array, _ := json.Marshal(out)

		return fmt.Sprintf(format, array)
	}
}

func sameHash(h string) string { return h }

func negative(name, format string, hashes int) organicSlice {
	return organicSlice{
		name: "negative-" + name, producer: producerRaw, role: organicRoleNegative,
		raw: rawPayload(format, sameHash), jobs: 1, hashes: hashes,
	}
}

// organicRound1 is round 1's slices, in queue interleaving order.
var organicRound1 = []organicSlice{
	{name: "dht", producer: producerDHT},
	{name: "importer", producer: producerImporter},
	{name: "retry", producer: producerRetry, retry: processor.MessageParams{ClassifierWorkflow: "default"}},
	{name: "empty-workflow", producer: producerRaw, raw: rawPayload(`{"ClassifierWorkflow":"","InfoHashes":%s}`, sameHash)},
	{name: "graphql", producer: producerGraphQL},
	{
		name: "graphql-workflow", producer: producerGraphQL,
		request: manager.EnqueueReprocessTorrentsBatchRequest{ClassifierWorkflow: "default"},
	},
	{
		name: "graphql-apis-off", producer: producerGraphQL,
		request: manager.EnqueueReprocessTorrentsBatchRequest{ApisDisabled: true},
	},
	{
		name: "graphql-local-off", producer: producerGraphQL,
		request: manager.EnqueueReprocessTorrentsBatchRequest{LocalSearchDisabled: true},
	},
	{
		name: "graphql-both-off", producer: producerGraphQL,
		request: manager.EnqueueReprocessTorrentsBatchRequest{ApisDisabled: true, LocalSearchDisabled: true},
	},
	{name: "cli-tmdb-off", producer: producerCLI, args: []string{"--classifierFlags", `{"tmdb_enabled":false}`}},
	{name: "cli-all-true", producer: producerCLI, args: []string{
		"--classifierFlags", `{"local_search_enabled":true,"apis_enabled":true,"tmdb_enabled":true}`,
	}},
	{name: "cli-all-false", producer: producerCLI, args: []string{
		"--classifierFlags", `{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false}`,
	}},

	{name: "hinted", producer: producerDHT, role: organicRoleHinted, jobs: 3, hashes: 5},

	{name: "fault-dht", producer: producerDHT, role: organicRoleFault},
	{
		name: "fault-rematch", producer: producerPlanner, role: organicRoleFault,
		planner: batch.MessageParams{ClassifyMode: processor.ClassifyModeRematch},
	},
	{
		name: "fault-local-off", producer: producerPlanner, role: organicRoleFault,
		planner: batch.MessageParams{ClassifierFlags: classifier.Flags{"local_search_enabled": false}},
	},

	negative("workflow-custom", `{"ClassifierWorkflow":"custom","InfoHashes":%s}`, 5),
	negative("workflow-null", `{"ClassifierWorkflow":null,"InfoHashes":%s}`, 5),
	negative("workflow-key-case", `{"classifierWorkflow":"default","InfoHashes":%s}`, 5),
	negative("hashes-key-case", `{"infohashes":%s}`, 5),
	negative("extra-key", `{"InfoHashes":%s,"Priority":0}`, 5),
	negative("flags-null", `{"ClassifierFlags":null,"InfoHashes":%s}`, 5),
	negative("flags-array", `{"ClassifierFlags":[],"InfoHashes":%s}`, 5),
	negative("flag-null", `{"ClassifierFlags":{"tmdb_enabled":null},"InfoHashes":%s}`, 5),
	negative("flag-string", `{"ClassifierFlags":{"tmdb_enabled":"true"},"InfoHashes":%s}`, 5),
	negative("flag-number", `{"ClassifierFlags":{"apis_enabled":1},"InfoHashes":%s}`, 5),
	negative("flag-delete-xxx", `{"ClassifierFlags":{"delete_xxx":true},"InfoHashes":%s}`, 5),
	negative("flag-delete-content-types", `{"ClassifierFlags":{"delete_content_types":["movie"]},"InfoHashes":%s}`, 5),
	negative("mode-2", `{"ClassifyMode":2,"InfoHashes":%s}`, 5),
	negative("mode-float", `{"ClassifyMode":1.0,"InfoHashes":%s}`, 5),
	negative("mode-string", `{"ClassifyMode":"1","InfoHashes":%s}`, 5),
	negative("hashes-150", `{"InfoHashes":%s}`, 150),
	negative("hashes-empty", `{"InfoHashes":[]%.0s}`, 0),
	negative("hashes-null", `{"InfoHashes":null%.0s}`, 0),
	negative("not-object", `[]%.0s`, 0),
	{
		name: "negative-hash-upper", producer: producerRaw, role: organicRoleNegative, jobs: 1, hashes: 5,
		raw: rawPayload(`{"InfoHashes":%s}`, strings.ToUpper),
	},
	{
		name: "negative-hash-0x", producer: producerRaw, role: organicRoleNegative, jobs: 1, hashes: 5,
		raw: rawPayload(`{"InfoHashes":%s}`, func(h string) string { return "0x" + h }),
	},
}

// organicRound2 is round 2's slices: rematch-mode torrents of the requeue
// plan (flagson_rematch.go) in the first three, default-mode ones in the rest.
var organicRound2 = []organicSlice{
	{
		name: "rematch-graphql", producer: producerGraphQL,
		request: manager.EnqueueReprocessTorrentsBatchRequest{ClassifyMode: processor.ClassifyModeRematch},
	},
	{name: "rematch-cli", producer: producerCLI, args: []string{"--classifyMode", "rematch"}},
	{name: "rematch-cli-apis-off", producer: producerCLI, args: []string{"--classifyMode", "rematch", "--apisDisabled"}},
	{name: "reuse-dht", producer: producerDHT},
	{name: "reuse-graphql", producer: producerGraphQL},
}

// organicRound2Rematch is how many of organicRound2's slices take the
// rematch-mode torrents.
const organicRound2Rematch = 3

// organicFaultClasses are the fault slices' jobs (flagson_faults.go): a 500
// on one pseudo-titled torrent's search, with companions.
var organicFaultClasses = map[string]faultClass{
	"fault-dht":       {name: "fault-dht", count: 2, slots: mixed(searchFault(faultInternal))},
	"fault-rematch":   {name: "fault-rematch", count: 2, slots: mixed(searchFault(faultInternal))},
	"fault-local-off": {name: "fault-local-off", count: 2, slots: mixed(searchFault(faultInternal))},
}

func organicSliceByName(name string) (organicSlice, bool) {
	for _, s := range slices.Concat(organicRound1, organicRound2) {
		if s.name == name {
			return s, true
		}
	}

	return organicSlice{}, false
}

// organicSliceHashes is one line of slices.jsonl: a slice's torrents, as its
// jobs hold them (one entry per job for the pseudo slices).
type organicSliceHashes struct {
	Slice string     `json:"slice"`
	Jobs  [][]string `json:"jobs"`
}

func (s organicSliceHashes) all() []string { return slices.Concat(s.Jobs...) }

// organicJob builds one job of an in-process producer.
func organicJob(slice organicSlice, hashes []string) (plannedJob, error) {
	ids := make([]protocol.ID, len(hashes))
	for i, h := range hashes {
		ids[i] = mustHash(h)
	}

	var (
		job model.QueueJob
		err error
	)

	switch slice.producer {
	case producerDHT:
		job, err = processor.NewQueueJob(processor.MessageParams{ClassifyMode: slice.mode, InfoHashes: ids},
			model.QueueJobDelayBy(time.Minute))
	case producerImporter:
		job, err = processor.NewQueueJob(processor.MessageParams{InfoHashes: ids}, model.QueueJobPriority(20))
	case producerRetry:
		params := slice.retry
		params.InfoHashes = ids
		job, err = processor.NewQueueJob(params)
	case producerPlanner:
		// A page is in ascending info hash order.
		parent := slice.planner
		parent.BatchSize = organicJobSize

		var spec *batch.JobSpec

		sortHashes(ids)
		hashes = slices.Sorted(slices.Values(hashes))

		if spec, err = batch.NewPlanner(parent).AddPage(ids); err == nil {
			job, err = spec.QueueJob()
		}
	case producerRaw:
		job, err = model.NewQueueJob(processor.MessageName, json.RawMessage(slice.raw(hashes)),
			model.QueueJobMaxRetries(2))
	default:
		err = fmt.Errorf("slice %s: %s jobs are made by Go's batch handler", slice.name, slice.producer)
	}

	if err != nil {
		return plannedJob{}, fmt.Errorf("slice %s: %w", slice.name, err)
	}

	return rawPlannedJob(slice.name, job.Fingerprint, job.Payload, job.Priority, job.MaxRetries,
		time.Duration(job.ArchivalDuration).String(), hashes), nil
}

func rawPlannedJob(slice, fingerprint, payload string, priority int, maxRetries uint, archival string,
	hashes []string,
) plannedJob {
	var params struct {
		ClassifyMode    processor.ClassifyMode `json:"ClassifyMode"`
		ClassifierFlags map[string]any         `json:"ClassifierFlags"`
	}

	// Only the flags and mode of a payload Go's producers wrote; the negative
	// slice's are never resolved (organicFlagsOn).
	_ = json.Unmarshal([]byte(payload), &params)

	return plannedJob{
		Slice:            slice,
		Fingerprint:      fingerprint,
		InfoHashes:       hashes,
		ClassifyMode:     params.ClassifyMode,
		Payload:          payload,
		Priority:         priority,
		MaxRetries:       maxRetries,
		ArchivalDuration: archival,
		FlagsOn:          organicFlagsOn(params.ClassifierFlags, goFlagDefaults()),
	}
}

// goFlagDefaults are Go's effective classifier flags, as the processor resolves
// a payload's absent flags: the compiled core configuration merged with any
// configuration file and the TMDB setting, never a literal.
var goFlagDefaults = sync.OnceValue(func() classifier.Flags {
	result := classifier.New(classifier.Params{
		Config:     classifier.NewDefaultConfig(),
		TmdbConfig: tmdb.NewDefaultConfig(),
		Lifecycle:  planLifecycle{},
	})

	source, err := result.Source.Get()
	if err != nil {
		panic(fmt.Sprintf("the Go classifier source: %v", err))
	}

	return source.Flags
})

// resolveAttachFlags is each attach flag as Go's runner takes it: the
// payload's value if it has one, else the default. ok is false when a value
// is not a boolean.
func resolveAttachFlags(payload map[string]any, defaults classifier.Flags) (resolved map[string]bool, ok bool) {
	resolved = make(map[string]bool, len(attachFlags))

	for _, k := range attachFlags {
		v, set := payload[k]
		if !set {
			v = defaults[k]
		}

		b, isBool := v.(bool)
		if !isBool {
			return nil, false
		}

		resolved[k] = b
	}

	return resolved, true
}

// organicFlagsOn reports whether every attach flag resolves to true.
func organicFlagsOn(payload map[string]any, defaults classifier.Flags) bool {
	resolved, ok := resolveAttachFlags(payload, defaults)
	if !ok {
		return false
	}

	for _, v := range resolved {
		if !v {
			return false
		}
	}

	return true
}

// ---- round 1 seed (flagson-seed -corpus organic) ----

func (s *seedRun) seedOrganic() error {
	o := s.opts

	synthetic, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier/inputs.jsonl"), 0)
	if err != nil {
		return err
	}

	replay, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier-replay/inputs.jsonl"), o.organicReplay)
	if err != nil {
		return err
	}

	for _, group := range []struct {
		corpus string
		inputs []input
	}{{"organic-synthetic", synthetic}, {"organic-replay", replay}} {
		for _, in := range group.inputs {
			if in.Hint != nil {
				continue
			}

			if _, err := s.addTorrent(group.corpus, corpusOrganic, in); err != nil {
				return err
			}
		}
	}

	// Pseudo-titled torrents: hinted ones first, then plain ones for the
	// fault, hinted and negative jobs.
	var hinted, plain []string

	pseudo, unhinted := map[string]bool{}, map[string]bool{}
	faultClasses := make([]faultClass, 0, len(organicFaultClasses))

	for _, name := range sortedKeys(organicFaultClasses) {
		faultClasses = append(faultClasses, organicFaultClasses[name])
	}

	need := faultClassSlots(faultClasses, roleFault) + faultClassSlots(faultClasses, roleIO) +
		faultClassSlots(faultClasses, roleDetails404)

	for _, sl := range organicRound1 {
		need += sl.jobs * sl.hashes
	}

	for i := range need + need/2 + 10 {
		in := pseudoInput(i)

		if i < organicSliceJobs(organicRoleHinted) {
			in.Hint = &inputHint{ContentType: "movie", ContentSource: "imdb", ContentID: fmt.Sprintf("tt%07d", 9100000+i)}
			if i%2 == 1 {
				in.Hint.ContentType = "tv_show"
			}
		}

		h, err := s.addTorrent(corpusOrganic+"-pseudo", corpusOrganic, in)
		if err != nil {
			return err
		}

		pseudo[h.String()] = true

		if in.Hint != nil {
			hinted = append(hinted, h.String())
		} else {
			unhinted[h.String()] = true
			plain = append(plain, h.String())
		}
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	// Not the hinted ones: they must be in the hinted jobs.
	pools, err := faultPools(slices.DeleteFunc(slices.Clone(planned), func(p plannedTorrent) bool {
		return pseudo[p.Hash] && !unhinted[p.Hash]
	}), unhinted)
	if err != nil {
		return err
	}

	// The fault jobs take pseudo-titled torrents with I/O first, and their
	// no-I/O companions from the replay torrents.
	sliceJobs := map[string][][]string{}
	used := map[string]bool{}
	sliceOfHash := map[string]string{}

	var faultPlans []faultJobPlan

	for _, sl := range organicRound1 {
		if sl.role != organicRoleFault {
			continue
		}

		built, err := s.buildFaultJobs([]faultClass{organicFaultClasses[sl.name]}, pools)
		if err != nil {
			return err
		}

		for _, j := range built {
			var hashes []string

			for _, t := range j.Torrents {
				hashes = append(hashes, t.Hash)
				used[t.Hash] = true
				sliceOfHash[t.Hash] = sliceFault
			}

			sliceJobs[sl.name] = append(sliceJobs[sl.name], hashes)
		}

		faultPlans = append(faultPlans, built...)
	}

	plain = slices.DeleteFunc(plain, func(h string) bool { return used[h] })

	for _, sl := range organicRound1 {
		if sl.role != organicRoleHinted && sl.role != organicRoleNegative {
			continue
		}

		for range sl.jobs {
			var job []string

			if sl.role == organicRoleHinted {
				job, hinted = append(job, hinted[0]), hinted[1:]
			}

			for len(job) < sl.hashes {
				if len(plain) == 0 {
					return errors.New("organic corpus: too few pseudo-titled torrents")
				}

				job, plain = append(job, plain[0]), plain[1:]
			}

			for _, h := range job {
				sliceOfHash[h] = sliceFault
			}

			sliceJobs[sl.name] = append(sliceJobs[sl.name], job)
		}
	}

	// The rest of the replay and synthetic torrents, in info hash order, go
	// round-robin to the replay slices.
	var rest []string

	for _, h := range s.hashes {
		if hs := h.String(); !pseudo[hs] && !used[hs] {
			rest = append(rest, hs)
		}
	}

	sort.Strings(rest)

	var replaySlices []organicSlice

	for _, sl := range organicRound1 {
		if sl.role == "" {
			replaySlices = append(replaySlices, sl)
		}
	}

	byReplaySlice := map[string][]string{}
	for i, h := range rest {
		sl := replaySlices[i%len(replaySlices)].name
		byReplaySlice[sl] = append(byReplaySlice[sl], h)
	}

	for _, sl := range replaySlices {
		sliceJobs[sl.name] = chunkHashes(byReplaySlice[sl.name], organicJobSize)
	}

	for i := range planned {
		if sl, ok := sliceOfHash[planned[i].Hash]; ok {
			planned[i].Slice = sl
		} else {
			planned[i].Slice = corpusOrganic
		}
	}

	if err := s.searchFixtures(planned, searchWeights{local: 25, tmdbNew: 42, tmdbExisting: 20, details404: 12, decoys: 9}); err != nil {
		return err
	}

	if err := s.companionFixtures(planned, sliceOfHash); err != nil {
		return err
	}

	for i := range faultPlans {
		for k := range faultPlans[i].Torrents {
			faultPlans[i].Torrents[k].Keys = s.torrentKeys(faultPlans[i].Torrents[k], planned)
		}
	}

	if err := s.flush(); err != nil {
		return err
	}

	if err := s.insertContents(); err != nil {
		return err
	}

	if err := writeOrganicSlices(s.opts.out, organicRound1, sliceJobs); err != nil {
		return err
	}

	if err := writeJSONLFile(filepath.Join(s.opts.out, "faultplan.jsonl"), faultPlans); err != nil {
		return err
	}

	return s.writeSeedFiles(planned, digest, nil, 0)
}

// organicSliceJobs is the number of jobs of round 1's pseudo slices of a role.
func organicSliceJobs(role string) int {
	n := 0

	for _, sl := range organicRound1 {
		if sl.role == role {
			n += sl.jobs
		}
	}

	return n
}

func chunkHashes(hashes []string, size int) [][]string {
	var out [][]string

	for i := 0; i < len(hashes); i += size {
		out = append(out, hashes[i:min(i+size, len(hashes))])
	}

	return out
}

// writeOrganicSlices writes slices.jsonl, and organic-jobs.jsonl with the
// in-process producers' jobs.
func writeOrganicSlices(dir string, table []organicSlice, sliceJobs map[string][][]string) error {
	lines := make([]organicSliceHashes, 0, len(table))

	var jobs []plannedJob

	for _, sl := range table {
		lines = append(lines, organicSliceHashes{Slice: sl.name, Jobs: sliceJobs[sl.name]})

		if sl.batchProduced() {
			continue
		}

		for _, hashes := range sliceJobs[sl.name] {
			job, err := organicJob(sl, hashes)
			if err != nil {
				return err
			}

			jobs = append(jobs, job)
		}
	}

	if err := writeJSONLFile(filepath.Join(dir, "slices.jsonl"), lines); err != nil {
		return err
	}

	return writeJSONLFile(filepath.Join(dir, "organic-jobs.jsonl"), jobs)
}

// ---- flagson-organic-produce ----

// runFlagsOnOrganicProduce makes one batch slice's jobs with Go's own code: in
// a copy of the seed (-dsn) it deletes every other torrent and every queued
// job, queues the slice's parent (GraphQL: the queue manager in process; CLI:
// `bitmagnet reprocess`), runs a Go worker with process_torrent disabled until
// the batch handler has expanded it, and writes the children it queued.
func runFlagsOnOrganicProduce(args []string) error {
	fs := flag.NewFlagSet("flagson-organic-produce", flag.ExitOnError)
	dsn := fs.String("dsn", "", "a disposable copy of the slice's seed")
	seedDir := fs.String("seed", "", "the phase's seed dir (slices.jsonl)")
	sliceName := fs.String("slice", "", "the batch slice")
	bin := fs.String("bitmagnet", "", "the Go bitmagnet binary")
	runDir := fs.String("run", "", "the directory for the Go processes' logs")
	tmdbURL := fs.String("tmdb-base-url", "", "the TMDB stub (never TMDB itself)")
	tmdbKey := fs.String("tmdb-api-key", "", "the dummy TMDB key")

	if err := fs.Parse(args); err != nil {
		return err
	}

	for name, v := range map[string]string{
		"dsn": *dsn, "seed": *seedDir, "slice": *sliceName, "bitmagnet": *bin, "run": *runDir,
		"tmdb-base-url": *tmdbURL, "tmdb-api-key": *tmdbKey,
	} {
		if err := requireFlag(name, v); err != nil {
			return err
		}
	}

	slice, ok := organicSliceByName(*sliceName)
	if !ok || !slice.batchProduced() {
		return fmt.Errorf("-slice %s: not a batch slice", *sliceName)
	}

	lines, err := readJSONL[organicSliceHashes](filepath.Join(*seedDir, "slices.jsonl"), 0)
	if err != nil {
		return err
	}

	var hashes []string

	for _, l := range lines {
		if l.Slice == slice.name {
			hashes = l.all()
		}
	}

	if len(hashes) == 0 {
		return fmt.Errorf("slice %s has no torrents in %s", slice.name, *seedDir)
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	ids := make([][]byte, len(hashes))
	for i, h := range hashes {
		ids[i] = mustHash(h).Bytes()
	}

	var left, ofSlice int

	if _, err := conn.Exec(ctx, `DELETE FROM queue_jobs`); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, `DELETE FROM torrents WHERE NOT (info_hash = ANY($1))`, ids); err != nil {
		return err
	}

	if err := conn.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE info_hash = ANY($1)) FROM torrents`,
		ids).Scan(&left, &ofSlice); err != nil {
		return err
	}

	if left != len(hashes) || ofSlice != len(hashes) {
		return fmt.Errorf("slice %s: %d torrents left, %d of them the slice's, want its %d", slice.name, left, ofSlice, len(hashes))
	}

	env := append(os.Environ(), "POSTGRES_DSN="+*dsn, "TMDB_BASE_URL="+*tmdbURL, "TMDB_API_KEY="+*tmdbKey,
		"LOG_LEVEL=info")

	if err := produceParent(ctx, slice, *dsn, *bin, *runDir, env); err != nil {
		return fmt.Errorf("slice %s: %w", slice.name, err)
	}

	if err := expandBatches(ctx, conn, *bin, *runDir, env); err != nil {
		return fmt.Errorf("slice %s: %w", slice.name, err)
	}

	jobs, parent, err := capturedChildren(ctx, conn, slice.name)
	if err != nil {
		return err
	}

	var got []string
	for _, j := range jobs {
		got = append(got, j.InfoHashes...)
	}

	want := slices.Clone(hashes)
	sort.Strings(want)

	if !slices.Equal(got, want) {
		return fmt.Errorf("slice %s: the batch handler queued %d hashes, want the slice's %d", slice.name, len(got), len(want))
	}

	if err := os.WriteFile(filepath.Join(*seedDir, "parent-"+slice.name+".json"), []byte(parent+"\n"), 0o644); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stderr, "flagson-organic-produce %s: %d jobs, %d hashes, parent %s\n",
		slice.name, len(jobs), len(got), parent)

	return writeJSONLFile(filepath.Join(*seedDir, "produced-"+slice.name+".jsonl"), jobs)
}

func produceParent(ctx context.Context, slice organicSlice, dsn, bin, runDir string, env []string) error {
	if slice.producer == producerCLI {
		cmd := exec.CommandContext(ctx, bin, append([]string{"reprocess"}, slice.args...)...)
		cmd.Dir, cmd.Env = runDir, env

		out, err := cmd.CombinedOutput()
		if err != nil || !bytes.Contains(out, []byte("Reprocess queued!")) {
			return fmt.Errorf("bitmagnet reprocess %v: %w\n%s", slice.args, err, out)
		}

		return os.WriteFile(filepath.Join(runDir, "reprocess-"+slice.name+".log"), out, 0o644)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return err
	}

	if sqlDB, err := db.DB(); err == nil {
		defer func() { _ = sqlDB.Close() }()
	}

	m, err := manager.New(manager.Params{
		Dao: lazy.New(func() (*dao.Query, error) { return dao.Use(db), nil }),
		DB:  lazy.New(func() (*gorm.DB, error) { return db, nil }),
	}).Manager.Get()
	if err != nil {
		return err
	}

	return m.EnqueueReprocessTorrentsBatch(ctx, slice.request)
}

// expandBatches runs a Go worker with process_torrent disabled until no batch
// job is pending, and fails if one failed.
func expandBatches(ctx context.Context, conn *pgx.Conn, bin, runDir string, env []string) error {
	logFile, err := os.OpenFile(filepath.Join(runDir, "batch-worker.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()

	cmd := exec.CommandContext(ctx, bin, "worker", "run", "--keys=queue_server")
	cmd.Dir, cmd.Env = runDir, slices.Concat(env, []string{"QUEUE_SERVER_DISABLED_QUEUES=" + processTorrentQueue})
	cmd.Stdout, cmd.Stderr = logFile, logFile

	if err := cmd.Start(); err != nil {
		return err
	}

	stop := func() error {
		_ = cmd.Process.Signal(syscall.SIGINT)
		return cmd.Wait()
	}

	deadline := time.Now().Add(5 * time.Minute)

	for {
		var active, failed int

		if err := conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status IN ('pending', 'retry')),
				count(*) FILTER (WHERE status = 'failed' OR error IS NOT NULL)
			FROM queue_jobs WHERE queue = $1`, batchQueue).Scan(&active, &failed); err != nil {
			_ = stop()
			return err
		}

		switch {
		case failed > 0:
			_ = stop()
			return errors.New("a batch job failed; see batch-worker.log")
		case active == 0:
			if err := stop(); err != nil && !isInterrupted(err) {
				return fmt.Errorf("the batch worker: %w", err)
			}

			return nil
		case time.Now().After(deadline):
			_ = stop()
			return errors.New("the batch handler did not finish in 5 minutes")
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func isInterrupted(err error) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && !exit.Success()
}

// capturedChildren reads the batch handler's process_torrent jobs, ordered
// by their first info hash (the handler pages in info hash order), and the
// parent's payload.
func capturedChildren(ctx context.Context, conn *pgx.Conn, slice string) ([]plannedJob, string, error) {
	var parent string

	if err := conn.QueryRow(ctx, `SELECT string_agg(payload::text, ' ') FROM queue_jobs WHERE queue = $1`,
		batchQueue).Scan(&parent); err != nil {
		return nil, "", err
	}

	rows, err := conn.Query(ctx, `SELECT fingerprint, payload::text, priority, max_retries, archival_duration::text
		FROM queue_jobs WHERE queue = $1 AND status = 'pending'`, processTorrentQueue)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var jobs []plannedJob

	for rows.Next() {
		var (
			fingerprint, payload, archival string
			priority, maxRetries           int
		)

		if err := rows.Scan(&fingerprint, &payload, &priority, &maxRetries, &archival); err != nil {
			return nil, "", err
		}

		var params processor.MessageParams
		if err := json.Unmarshal([]byte(payload), &params); err != nil {
			return nil, "", err
		}

		hashes := make([]string, len(params.InfoHashes))
		for i, h := range params.InfoHashes {
			hashes[i] = h.String()
		}

		jobs = append(jobs, rawPlannedJob(slice, fingerprint, payload, priority, uint(maxRetries), archival, hashes))
	}

	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	sort.Slice(jobs, func(i, j int) bool { return jobs[i].InfoHashes[0] < jobs[j].InfoHashes[0] })

	return jobs, parent, nil
}

// ---- flagson-organic-enqueue ----

// runFlagsOnOrganicEnqueue queues every slice's jobs verbatim in the seed, one
// job of each slice in turn, and writes jobs.jsonl.
func runFlagsOnOrganicEnqueue(args []string) error {
	fs := flag.NewFlagSet("flagson-organic-enqueue", flag.ExitOnError)
	dsn := fs.String("dsn", "", "the phase's seed database")
	seedDir := fs.String("seed", "", "the phase's seed dir (slices.jsonl, organic-jobs.jsonl, produced-*.jsonl)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := errors.Join(requireFlag("dsn", *dsn), requireFlag("seed", *seedDir)); err != nil {
		return err
	}

	lines, err := readJSONL[organicSliceHashes](filepath.Join(*seedDir, "slices.jsonl"), 0)
	if err != nil {
		return err
	}

	inProcess, err := readJSONL[plannedJob](filepath.Join(*seedDir, "organic-jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	bySlice := map[string][]plannedJob{}
	for _, j := range inProcess {
		bySlice[j.Slice] = append(bySlice[j.Slice], j)
	}

	order := make([]string, 0, len(lines))

	for _, l := range lines {
		order = append(order, l.Slice)

		slice, ok := organicSliceByName(l.Slice)
		if !ok {
			return fmt.Errorf("unknown organic slice %q", l.Slice)
		}

		if !slice.batchProduced() {
			continue
		}

		produced, err := readJSONL[plannedJob](filepath.Join(*seedDir, "produced-"+l.Slice+".jsonl"), 0)
		if err != nil {
			return err
		}

		bySlice[l.Slice] = produced
	}

	jobs := interleaveSlices(order, bySlice)

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	ts, _ := time.Parse(time.RFC3339Nano, fixtureTime)

	if err := enqueueRawJobs(ctx, conn, jobs, ts.Add(-time.Hour)); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, "ANALYZE"); err != nil {
		return err
	}

	counts := map[string]string{}
	for _, name := range order {
		n := 0
		for _, j := range bySlice[name] {
			n += len(j.InfoHashes)
		}

		counts[name] = fmt.Sprintf("%d/%d", len(bySlice[name]), n)
	}

	_, _ = fmt.Fprintf(os.Stderr, "flagson-organic-enqueue: %d jobs, jobs/hashes by slice %v\n", len(jobs), counts)

	return writeJSONLFile(filepath.Join(*seedDir, "jobs.jsonl"), jobs)
}

// interleaveSlices orders the jobs one of each slice in turn and numbers them.
func interleaveSlices(order []string, bySlice map[string][]plannedJob) []plannedJob {
	var jobs []plannedJob

	for k := 0; ; k++ {
		added := false

		for _, name := range order {
			if k < len(bySlice[name]) {
				j := bySlice[name][k]
				j.Index = len(jobs)
				jobs = append(jobs, j)
				added = true
			}
		}

		if !added {
			return jobs
		}
	}
}

// ---- flagson-organic-check (G7) ----

// organicRepublish is the job Go's processor queues for a failed torrent T of
// an organic job: T alone, the job's mode and flags, the workflow resolved
// to "default", priority 0.
func organicRepublish(seeded plannedJob, hash string) (model.QueueJob, error) {
	var params processor.MessageParams
	if err := json.Unmarshal([]byte(seeded.Payload), &params); err != nil {
		return model.QueueJob{}, err
	}

	return processor.NewQueueJob(processor.MessageParams{
		ClassifyMode:       params.ClassifyMode,
		ClassifierWorkflow: "default",
		ClassifierFlags:    params.ClassifierFlags,
		InfoHashes:         []protocol.ID{mustHash(hash)},
	})
}

// runFlagsOnOrganicCheck checks G7 in a drained run: every fault-* job's
// faulted torrent was republished as organicRepublish says.
func runFlagsOnOrganicCheck(args []string) error {
	fs := flag.NewFlagSet("flagson-organic-check", flag.ExitOnError)
	dsn := fs.String("dsn", "", "a drained organic run's database")
	seedDir := fs.String("seed", "", "the phase's seed dir (jobs.jsonl, faultplan.jsonl)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := errors.Join(requireFlag("dsn", *dsn), requireFlag("seed", *seedDir)); err != nil {
		return err
	}

	jobs, err := readJSONL[plannedJob](filepath.Join(*seedDir, "jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	faults, err := readJSONL[faultJobPlan](filepath.Join(*seedDir, "faultplan.jsonl"), 0)
	if err != nil {
		return err
	}

	jobOf := map[string]plannedJob{}
	for _, j := range jobs {
		for _, h := range j.InfoHashes {
			jobOf[h] = j
		}
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var violations []string

	checked := 0

	for _, f := range faults {
		for _, t := range f.Torrents {
			if t.Role != roleFault {
				continue
			}

			seeded := jobOf[t.Hash]

			want, err := organicRepublish(seeded, t.Hash)
			if err != nil {
				return err
			}

			var (
				priority int
				same     bool
			)

			err = conn.QueryRow(ctx, `SELECT priority, payload = $2::jsonb FROM queue_jobs WHERE fingerprint = $1`,
				want.Fingerprint, want.Payload).Scan(&priority, &same)

			switch {
			case errors.Is(err, pgx.ErrNoRows):
				violations = append(violations, fmt.Sprintf("%s (%s, job %d): no republish %s", t.Hash, seeded.Slice,
					seeded.Index, want.Payload))
			case err != nil:
				return err
			case priority != 0 || !same:
				violations = append(violations, fmt.Sprintf("%s (%s): republish at priority %d, payload as Go's %t",
					t.Hash, seeded.Slice, priority, same))
			default:
				checked++
				_, _ = fmt.Fprintf(os.Stdout, "%-16s job %4d -> %s priority 0\n", seeded.Slice, seeded.Index, want.Payload)
			}
		}
	}

	_, _ = fmt.Fprintf(os.Stdout, "G7: %d republished fault torrents as Go queues them, %d violations\n", checked, len(violations))

	for _, v := range violations {
		_, _ = fmt.Fprintln(os.Stderr, "VIOLATION: "+v)
	}

	if len(violations) > 0 || checked == 0 {
		return errors.New("flagson-organic-check: G7 violations")
	}

	return nil
}

// ---- Rust's share (flagson-abstained -organic-bound, flagson-organic-info) ----

// organicLeft is the reason each job a Rust worker with --organic-max-priority
// bound must leave gives, by fingerprint: "none" for a negative slice's job
// and for one above the bound (never claimed), "hint" for a hinted slice's
// (claimed, then abstained from).
func organicLeft(jobs []plannedJob, bound int) map[string]string {
	left := map[string]string{}

	for _, j := range jobs {
		sl, _ := organicSliceByName(j.Slice)

		switch {
		case sl.role == organicRoleNegative || j.Priority > bound:
			left[j.Fingerprint] = abstainNone
		case sl.role == organicRoleHinted:
			left[j.Fingerprint] = abstainHint
		}
	}

	return left
}

// organicShares prints, per slice, how many of its jobs a Rust-only phase
// took and how many it left (active: their fingerprints), against what
// organicLeft expects.
func organicShares(jobs []plannedJob, want map[string]string, active map[string]bool) {
	type share struct{ jobs, left, wantLeft int }

	var order []string

	shares := map[string]*share{}

	for _, j := range jobs {
		s := shares[j.Slice]
		if s == nil {
			s = &share{}
			shares[j.Slice] = s
			order = append(order, j.Slice)
		}

		s.jobs++

		if active[j.Fingerprint] {
			s.left++
		}

		if _, ok := want[j.Fingerprint]; ok {
			s.wantLeft++
		}
	}

	for _, name := range order {
		s := shares[name]
		_, _ = fmt.Fprintf(os.Stdout, "  %-36s %4d jobs: Rust took %4d, left %4d (expected %d)\n",
			name, s.jobs, s.jobs-s.left, s.left, s.wantLeft)
	}
}

// runFlagsOnOrganicInfo prints, for flags_on.sh, a phase's batch slices
// (-batch) or the jobs a Rust worker at a bound must leave and how many of
// them it abstains from (-left N).
func runFlagsOnOrganicInfo(args []string) error {
	fs := flag.NewFlagSet("flagson-organic-info", flag.ExitOnError)
	seedDir := fs.String("seed", "", "the phase's seed dir (slices.jsonl, jobs.jsonl)")
	batchSlices := fs.Bool("batch", false, "print the batch slices, one per line")
	bound := fs.Int("left", -1, "print \"<jobs left> <abstentions>\" for this --organic-max-priority")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := requireFlag("seed", *seedDir); err != nil {
		return err
	}

	if *batchSlices {
		lines, err := readJSONL[organicSliceHashes](filepath.Join(*seedDir, "slices.jsonl"), 0)
		if err != nil {
			return err
		}

		for _, l := range lines {
			if sl, ok := organicSliceByName(l.Slice); ok && sl.batchProduced() {
				_, _ = fmt.Fprintln(os.Stdout, l.Slice)
			}
		}

		return nil
	}

	if *bound < 0 {
		return errors.New("one of -batch or -left is required")
	}

	jobs, err := readJSONL[plannedJob](filepath.Join(*seedDir, "jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	left := organicLeft(jobs, *bound)
	hinted := 0

	for _, reason := range left {
		if reason == abstainHint {
			hinted++
		}
	}

	_, _ = fmt.Fprintf(os.Stdout, "%d %d\n", len(left), hinted)

	return nil
}

// ---- round 2 (flagson-requeue -organic) ----

// organicReplayAttachments keeps the attachments of round 1's replay slices'
// torrents: the pseudo-titled ones are not queued again.
func organicReplayAttachments(roundOne string, before map[string]attachment) (map[string]attachment, error) {
	lines, err := readJSONL[organicSliceHashes](filepath.Join(roundOne, "slices.jsonl"), 0)
	if err != nil {
		return nil, err
	}

	kept := map[string]attachment{}

	for _, l := range lines {
		sl, ok := organicSliceByName(l.Slice)
		if !ok {
			return nil, fmt.Errorf("unknown organic slice %q in %s", l.Slice, roundOne)
		}

		if sl.role != "" {
			continue
		}

		for _, h := range l.all() {
			if a, ok := before[h]; ok {
				kept[h] = a
			}
		}
	}

	return kept, nil
}

// organicRound2Slices puts the plan's rematch-mode torrents in turn into
// organicRound2's first organicRound2Rematch slices and its default-mode ones
// into the others, in jobs of organicJobSize.
func organicRound2Slices(plan []rematchTorrent) map[string][][]string {
	groups := map[string][]organicSlice{
		sliceRematch: organicRound2[:organicRound2Rematch],
		sliceReuse:   organicRound2[organicRound2Rematch:],
	}
	taken := map[string]int{}
	hashes := map[string][]string{}

	for _, t := range plan {
		group := groups[t.Mode]
		sl := group[taken[t.Mode]%len(group)]
		taken[t.Mode]++
		hashes[sl.name] = append(hashes[sl.name], t.Hash)
	}

	out := make(map[string][][]string, len(hashes))
	for name, hs := range hashes {
		out[name] = chunkHashes(hs, organicJobSize)
	}

	return out
}

// requeueOrganic writes round 2's slices (slices.jsonl, organic-jobs.jsonl)
// and the rematch plan; flagson-organic-produce and -enqueue queue them.
func requeueOrganic(ctx context.Context, conn *pgx.Conn, outDir string, plan []rematchTorrent, better int) error {
	if err := writeOrganicSlices(outDir, organicRound2, organicRound2Slices(plan)); err != nil {
		return err
	}

	if err := writeJSONLFile(filepath.Join(outDir, "rematchplan.jsonl"), plan); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, "ANALYZE"); err != nil {
		return err
	}

	counts := map[string]int{}
	for _, t := range plan {
		counts[t.Mode+" "+t.Kind]++
	}

	_, _ = fmt.Fprintf(os.Stderr, "flagson-requeue -organic: %d torrents %v, %d better candidates\n",
		len(plan), counts, better)

	return nil
}
