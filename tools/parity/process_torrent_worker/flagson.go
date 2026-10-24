package main

// Flags-on differential (see flags_on.sh): the Go oracle for the Rust worker's
// flags-on mode. The seed is the flags-off seed's torrents plus
//
//   - content rows with attributes and collections, and a TMDB stub fixture,
//     both generated from what Go's own classifier asks for each seeded torrent
//     (flagson_plan.go), so local and TMDB searches really match;
//   - torrents with explicit content-ID hints, which are the only way Go ever
//     reaches the by-ID attach actions and TMDB's /find;
//   - copies of the corpus's deletion inputs;
//
// queued as flags-ON jobs with a flags-OFF slice interleaved. Go persists
// content found through TMDB and later jobs can then find it locally, so the
// result depends on job order: every job has a strictly increasing run_after
// and both workers drain the queue one job at a time.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
)

// Slices of the flags-on corpus. A job holds torrents of one slice only.
const (
	sliceMain     = "main"      // replay and synthetic torrents, flags-on payload
	sliceFlagsOff = "flags-off" // replay and synthetic torrents, explicit flags-off payload
	sliceHint     = "hint"      // explicit content-ID hints: the by-ID actions
	sliceMissing  = "missing"   // hashes with no torrent row
	sliceDeletion = "deletion"  // a deletion-only job
	sliceFault    = "fault"     // a fault job of the fault or abstain corpus
)

func flagsOnFlags() classifier.Flags {
	return classifier.Flags{
		"local_search_enabled": true,
		"apis_enabled":         true,
		"tmdb_enabled":         true,
	}
}

func flagsOffFlags() classifier.Flags {
	return classifier.Flags{
		"local_search_enabled": false,
		"apis_enabled":         false,
		"tmdb_enabled":         false,
	}
}

// plannedJob is one queued job, in queue order. It is written to jobs.jsonl so
// the coverage report and the G3 checks can map hashes and fingerprints back to
// their slice.
type plannedJob struct {
	Index       int      `json:"index"`
	Slice       string   `json:"slice"`
	FlagsOn     bool     `json:"flagsOn"`
	Fingerprint string   `json:"fingerprint"`
	InfoHashes  []string `json:"infoHashes"`
	Fault       string   `json:"fault,omitempty"` // the fault class (flagson_faults.go)
	Hold        string   `json:"hold,omitempty"`  // parked until flags_on.sh releases it (holdAlone, holdAuth)
	// ClassifyMode is the payload's mode (the rematch phase's rematch jobs).
	ClassifyMode processor.ClassifyMode `json:"classifyMode,omitempty"`
	// Expect is what each worker generation does with a hint corpus's job
	// (flagson_hints.go sliceExpect), keyed by flags_on.sh HINT_EXPECT.
	Expect map[string]string `json:"expect,omitempty"`
	// An organic job (flagson_organic.go) as its producer wrote it, queued
	// byte for byte by enqueueRawJobs: never decoded and marshalled again,
	// which would normalize exactly what the negative slice is made of.
	Payload          string `json:"payload,omitempty"`
	Priority         int    `json:"priority,omitempty"`
	MaxRetries       uint   `json:"maxRetries,omitempty"`
	ArchivalDuration string `json:"archivalDuration,omitempty"`
}

type jobSpec struct {
	slice        string
	flagsOn      bool
	classifyMode processor.ClassifyMode
	hashes       []protocol.ID
	fault        string
	expect       map[string]string
}

// enqueueFlagsOnJobs inserts the jobs with Go's own processor.NewQueueJob, in a
// strictly increasing run_after order, and returns what it queued.
func enqueueFlagsOnJobs(ctx context.Context, conn *pgx.Conn, specs []jobSpec, base time.Time) ([]plannedJob, error) {
	batch := &pgx.Batch{}
	planned := make([]plannedJob, 0, len(specs))

	for i, spec := range specs {
		flags := flagsOffFlags()
		if spec.flagsOn {
			flags = flagsOnFlags()
		}

		job, err := processor.NewQueueJob(processor.MessageParams{
			ClassifyMode:       spec.classifyMode,
			ClassifierWorkflow: "default",
			ClassifierFlags:    flags,
			InfoHashes:         spec.hashes,
		})
		if err != nil {
			return nil, err
		}

		batch.Queue(`INSERT INTO queue_jobs
			(fingerprint, queue, status, payload, retries, max_retries, run_after,
			 archival_duration, created_at, priority)
			VALUES ($1, $2, 'pending', $3::jsonb, 0, $4, $5, $6::interval, $5, 0)`,
			job.Fingerprint, job.Queue, job.Payload, job.MaxRetries,
			base.Add(time.Duration(i)*time.Millisecond),
			time.Duration(job.ArchivalDuration).String())

		hashes := make([]string, len(spec.hashes))
		for j, h := range spec.hashes {
			hashes[j] = h.String()
		}

		planned = append(planned, plannedJob{
			Index:        i,
			Slice:        spec.slice,
			FlagsOn:      spec.flagsOn,
			Fingerprint:  job.Fingerprint,
			InfoHashes:   hashes,
			Fault:        spec.fault,
			ClassifyMode: spec.classifyMode,
			Expect:       spec.expect,
		})

		if batch.Len() >= 1500 {
			if err := conn.SendBatch(ctx, batch).Close(); err != nil {
				return nil, err
			}

			batch = &pgx.Batch{}
		}
	}

	if batch.Len() > 0 {
		if err := conn.SendBatch(ctx, batch).Close(); err != nil {
			return nil, err
		}
	}

	return planned, nil
}

// rawJobArgs are the queue_jobs values of an organic job: its fingerprint,
// payload, priority, retries and archival duration as its producer wrote them.
func rawJobArgs(job plannedJob, runAfter time.Time) []any {
	return []any{
		job.Fingerprint, processTorrentQueue, job.Payload, job.MaxRetries, runAfter,
		job.ArchivalDuration, job.Priority,
	}
}

// enqueueRawJobs inserts organic jobs verbatim, in a strictly increasing
// run_after order.
func enqueueRawJobs(ctx context.Context, conn *pgx.Conn, jobs []plannedJob, base time.Time) error {
	batch := &pgx.Batch{}

	for i, job := range jobs {
		batch.Queue(`INSERT INTO queue_jobs
			(fingerprint, queue, status, payload, retries, max_retries, run_after,
			 archival_duration, created_at, priority)
			VALUES ($1, $2, 'pending', $3::jsonb, 0, $4, $5, $6::interval, $5, $7)`,
			rawJobArgs(job, base.Add(time.Duration(i)*time.Millisecond))...)

		if batch.Len() >= 1500 {
			if err := conn.SendBatch(ctx, batch).Close(); err != nil {
				return err
			}

			batch = &pgx.Batch{}
		}
	}

	if batch.Len() == 0 {
		return nil
	}

	return conn.SendBatch(ctx, batch).Close()
}

// planFlagsOnJobs groups the seeded torrents:
//
//   - main and flags-off torrents, in info-hash order (DHT discovery order is
//     effectively random, so every job mixes I/O and non-I/O torrents), in
//     chunks of jobSize-1; every flagsOffEvery-th chunk carries a flags-off
//     payload, and every tenth chunk gains a missing hash (partial failure:
//     the failed hash is republished before persistence);
//   - one deletion-only job (Go's second early return) and three jobs of only
//     missing hashes (all-failed: the retry lifecycle to `failed`).
//
// The hint corpora plan their jobs with planHintedJobs (flagson_hints.go).
func planFlagsOnJobs(main, deletionOnly []protocol.ID, jobSize, flagsOffEvery int, withMissing bool) []jobSpec {
	sortHashes(main)

	step := jobSize - 1

	var mainJobs []jobSpec

	for i := 0; i < len(main); i += step {
		chunkIndex := i / step
		chunk := append([]protocol.ID(nil), main[i:min(i+step, len(main))]...)

		if withMissing && chunkIndex%10 == 0 {
			chunk = append(chunk, fixtureHash("flagson-missing", fmt.Sprint(i)))
		}

		spec := jobSpec{slice: sliceMain, flagsOn: true, hashes: chunk}
		if flagsOffEvery > 0 && chunkIndex%flagsOffEvery == flagsOffEvery-1 {
			spec.slice, spec.flagsOn = sliceFlagsOff, false
		}

		mainJobs = append(mainJobs, spec)
	}

	specs := make([]jobSpec, 0, len(mainJobs)+4)

	if len(deletionOnly) > 0 {
		specs = append(specs, jobSpec{slice: sliceDeletion, flagsOn: true, hashes: deletionOnly})
	}

	specs = append(specs, mainJobs...)

	for i := range 3 {
		if !withMissing {
			break
		}

		specs = append(specs, jobSpec{
			slice: sliceMissing, flagsOn: true,
			hashes: []protocol.ID{fixtureHash("flagson-all-missing", fmt.Sprint(i))},
		})
	}

	return specs
}

func sortHashes(hashes []protocol.ID) {
	sort.Slice(hashes, func(i, j int) bool { return hashes[i].String() < hashes[j].String() })
}

func writeJSONLFile[T any](path string, rows []T) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)

	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			_ = f.Close()
			return err
		}
	}

	return f.Close()
}

func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func requireFlag(name, value string) error {
	if value == "" {
		return errors.New("-" + name + " is required")
	}

	return nil
}
