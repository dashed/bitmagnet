package tape

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Replay answers observations from a recorded tape.
//
// Every session it opens is a replay session, so a seam that finds one on the
// context serves the recording instead of its live dependency. Nothing in the
// production wiring constructs a Replay; it is reachable only from tests and
// offline tooling.
type Replay struct {
	manifest Manifest
	records  map[recordKey]Record
}

// Load reads the tape in dir and pins it to effectiveConfigDigest.
//
// It fails closed on drift: a tape recorded under a different classifier
// configuration answers questions about a classifier that no longer exists.
// Pass an empty digest only when the caller has separately established that the
// configuration is irrelevant.
func Load(dir string, effectiveConfigDigest string) (*Replay, error) {
	manifestBytes, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if err != nil {
		return nil, err
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode tape manifest: %w", err)
	}

	if manifest.Schema != Schema {
		return nil, fmt.Errorf("tape schema is %q, want %q", manifest.Schema, Schema)
	}

	if effectiveConfigDigest != "" && manifest.EffectiveConfigDigest != effectiveConfigDigest {
		return nil, fmt.Errorf(
			"tape was recorded under effective classifier config digest %s, but the current digest is %s",
			manifest.EffectiveConfigDigest,
			effectiveConfigDigest,
		)
	}

	tapeFile, err := os.Open(filepath.Join(dir, TapeFileName))
	if err != nil {
		return nil, err
	}

	defer func() { _ = tapeFile.Close() }()

	records, err := DecodeRecords(tapeFile)
	if err != nil {
		return nil, err
	}

	indexed := make(map[recordKey]Record, len(records))
	seen := make(map[recordKey]struct{}, len(records))

	for _, record := range records {
		key := recordKey{record.Subject, record.Attempt}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("tape has duplicate record for subject %q attempt %d", record.Subject, record.Attempt)
		}

		seen[key] = struct{}{}

		// An incomplete record holds a prefix of what that classification
		// observed. Serving it would answer the first questions and then run
		// out, which reads like the classification legitimately stopped asking.
		// Excluding it turns that into a miss naming the subject.
		if record.Incomplete {
			continue
		}

		indexed[key] = record
	}

	if manifest.RecordCount != len(records) {
		return nil, fmt.Errorf(
			"tape manifest declares %d records but the tape holds %d",
			manifest.RecordCount,
			len(records),
		)
	}

	return &Replay{manifest: manifest, records: indexed}, nil
}

// DecodeRecords reads newline-delimited tape records, validating each one.
func DecodeRecords(reader io.Reader) ([]Record, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	var records []Record

	line := 0

	for scanner.Scan() {
		line++

		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			return nil, fmt.Errorf("tape line %d is empty", line)
		}

		var record Record

		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("decode tape line %d: %w", line, err)
		}

		if err := record.validate(); err != nil {
			return nil, fmt.Errorf("tape line %d: %w", line, err)
		}

		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// Manifest returns the loaded tape's header.
func (r *Replay) Manifest() Manifest { return r.manifest }

// Begin opens a replay session for one classification and returns a context
// carrying it.
//
// A subject the tape has no record for still gets a session, holding no
// observations, so the first question asked of it reports a [*MissError] naming
// the subject. Returning no session instead would silently drop the replay back
// onto the live dependency, which is precisely the failure this package exists
// to prevent.
func (r *Replay) Begin(ctx context.Context, subject string, attempt int) context.Context {
	record := r.records[recordKey{subject, attempt}]

	return context.WithValue(ctx, contextKey{}, &Session{
		subject:  subject,
		attempt:  attempt,
		recorded: record.Observations,
	})
}

// Subjects returns the recorded (subject, attempt) pairs.
func (r *Replay) Subjects() []Record {
	records := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		records = append(records, record)
	}

	return records
}
