package tape

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// File names within a tape directory.
const (
	TapeFileName        = "tape.jsonl"
	ManifestFileName    = "manifest.json"
	ProvenanceFileName  = "PROVENANCE.md"
	tapeDirPermissions  = 0o755
	tapeFilePermissions = 0o644
)

// Write serialises the recording into dir as tape.jsonl, manifest.json and
// PROVENANCE.md, creating dir if needed.
//
// generatedAt is passed in rather than read from the clock so callers that need
// byte-reproducible output (tests) can pin it.
func (r *Recorder) Write(dir string, generatedAt time.Time) (writeErr error) {
	// A cap callback and lifecycle stop can race. Serialize whole generations so
	// their three files cannot interleave in the shared directory.
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	var (
		summary   recordSummary
		truncated bool
		revision  uint64
	)
	defer func() { r.finishWrite(generatedAt, summary, truncated, revision, writeErr) }()

	records, truncated, revision, err := r.snapshotRecords()
	if err != nil {
		return err
	}
	summary = summarizeRecords(records)
	actionEntryCount := summary.actionEntryCount

	manifest := Manifest{
		Schema:                   Schema,
		EffectiveConfigDigest:    r.digest,
		AcquisitionPlanDigest:    r.provenance.AcquisitionPlanDigest,
		GeneratedAt:              generatedAt.UTC().Format(time.RFC3339),
		Recorder:                 r.provenance.Command,
		RecordCount:              summary.recordCount,
		ObservationCount:         summary.observationCount,
		IncompleteRecordCount:    summary.incompleteRecordCount,
		AuthoritativeRecordCount: summary.authoritativeRecordCount,
		RecordOutcomeCounts:      copyCountMap(summary.recordOutcomeCounts),
		ActionEntryCount:         &actionEntryCount,
		ActionEntryCounts:        copyCountMap(summary.actionEntryCounts),
		Truncated:                truncated,
	}

	if err := os.MkdirAll(dir, tapeDirPermissions); err != nil {
		return err
	}

	tapeBytes, err := EncodeRecords(records)
	if err != nil {
		return err
	}

	manifestBytes, err := Marshal(manifest)
	if err != nil {
		return err
	}

	files := map[string][]byte{
		TapeFileName:       tapeBytes,
		ManifestFileName:   append(manifestBytes, '\n'),
		ProvenanceFileName: []byte(r.provenanceDocument(manifest, records)),
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), files[name], tapeFilePermissions); err != nil {
			return err
		}
	}

	return nil
}

// EncodeRecords renders records as newline-delimited JSON, one record per line.
func EncodeRecords(records []Record) ([]byte, error) {
	var buf bytes.Buffer

	for _, record := range records {
		line, err := Marshal(record)
		if err != nil {
			return nil, err
		}

		buf.Write(line)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

func (r *Recorder) provenanceDocument(manifest Manifest, records []Record) string {
	var doc strings.Builder

	doc.WriteString("# Classifier attach tape\n\n")
	doc.WriteString("Recording of the observations the Go classifier made against its impure\n")
	doc.WriteString("dependencies (local content search, TMDB) while classifying the subjects below.\n")
	doc.WriteString("It exists because the Go classifier is not a pure function of\n")
	doc.WriteString("(torrent, database snapshot): the local content search orders candidates by a\n")
	doc.WriteString("ts_rank_cd that ties, so the candidate window and its order are decided by the\n")
	doc.WriteString("query plan, and the levenshtein selection that follows is first-wins. Only the\n")
	doc.WriteString("ordered candidate list that was actually observed is replayable.\n\n")

	doc.WriteString("## Run\n\n")
	fmt.Fprintf(&doc, "- Command: %s\n", orUnknown(r.provenance.Command))
	fmt.Fprintf(&doc, "- Host: %s\n", orUnknown(r.provenance.Host))
	fmt.Fprintf(&doc, "- Generated at: %s\n", manifest.GeneratedAt)
	fmt.Fprintf(&doc, "- Content database: %s\n", orUnknown(r.provenance.Database))
	fmt.Fprintf(&doc, "- Effective classifier config digest: %s\n", orUnknown(manifest.EffectiveConfigDigest))
	if manifest.AcquisitionPlanDigest != "" {
		fmt.Fprintf(&doc, "- Acquisition plan digest: %s\n", manifest.AcquisitionPlanDigest)
	}
	fmt.Fprintf(&doc, "- Records: %d\n", manifest.RecordCount)
	fmt.Fprintf(&doc, "- Observations: %d\n", manifest.ObservationCount)
	fmt.Fprintf(&doc, "- Incomplete records: %d\n", manifest.IncompleteRecordCount)
	fmt.Fprintf(&doc, "- Authoritative records: %d\n", manifest.AuthoritativeRecordCount)
	if manifest.ActionEntryCount != nil {
		fmt.Fprintf(&doc, "- Action entries: %d\n", *manifest.ActionEntryCount)
	}
	fmt.Fprintf(&doc, "- Truncated: %t\n", manifest.Truncated)

	for _, kind := range sortedKeys(manifest.RecordOutcomeCounts) {
		fmt.Fprintf(&doc, "  - ended %s: %d\n", kind, manifest.RecordOutcomeCounts[kind])
	}

	if manifest.AuthoritativeRecordCount < manifest.RecordCount {
		doc.WriteString("\n  Only the authoritative records are a COMPLETE account of what their\n")
		doc.WriteString("  classification asked. The rest ended early -- cancelled, or stopped by an\n")
		doc.WriteString("  error -- so their observation lists are prefixes, and a replay asking MORE\n")
		doc.WriteString("  than they hold is not evidence of a divergence.\n")
	}

	if manifest.IncompleteRecordCount > 0 {
		doc.WriteString("\n  Those classifications were still running when the tape was written, so\n")
		doc.WriteString("  their observation lists are prefixes. A replay excludes them and reports\n")
		doc.WriteString("  a miss for those subjects rather than serving a short answer.\n")
	}

	if manifest.Truncated {
		doc.WriteString("\n  The record cap was reached and recording stopped. This tape is not a\n")
		doc.WriteString("  complete oracle for the population it was drawn from; replaying that whole\n")
		doc.WriteString("  population against it will report misses.\n")
	}

	doc.WriteString("\n## Flag state\n\n")

	for _, line := range flagSummary(records) {
		fmt.Fprintf(&doc, "- %s\n", line)
	}

	doc.WriteString("\n## Observation kinds\n\n")

	for _, line := range kindSummary(records) {
		fmt.Fprintf(&doc, "- %s\n", line)
	}

	doc.WriteString("\n## Action entries\n\n")

	for _, line := range actionEntrySummary(records) {
		fmt.Fprintf(&doc, "- %s\n", line)
	}

	if r.provenance.ScopeLimits != "" {
		doc.WriteString("\n## What a green replay against this tape does NOT prove\n\n")
		doc.WriteString(r.provenance.ScopeLimits)

		if !strings.HasSuffix(r.provenance.ScopeLimits, "\n") {
			doc.WriteByte('\n')
		}
	}

	if r.provenance.Notes != "" {
		doc.WriteString("\n## Notes\n\n")
		doc.WriteString(r.provenance.Notes)

		if !strings.HasSuffix(r.provenance.Notes, "\n") {
			doc.WriteByte('\n')
		}
	}

	return doc.String()
}

func orUnknown(value string) string {
	if value == "" {
		return "(unrecorded)"
	}

	return value
}

// flagSummary lists the distinct flag states across the recording. Flags gate
// the enrichment actions, so a tape recorded under mixed flag states is
// answering more than one question and the reader deserves to see that.
func flagSummary(records []Record) []string {
	counts := make(map[string]int)

	for _, record := range records {
		encoded, err := Marshal(record.Flags)
		if err != nil {
			continue
		}

		counts[record.Workflow+" "+string(encoded)]++
	}

	return summaryLines(counts, "no records")
}

func kindSummary(records []Record) []string {
	counts := make(map[string]int)

	for _, record := range records {
		for _, observation := range record.Observations {
			counts[observation.Kind+" ("+observation.Outcome+")"]++
		}
	}

	return summaryLines(counts, "no observations")
}

func actionEntrySummary(records []Record) []string {
	counts := make(map[string]int)

	for _, record := range records {
		for _, action := range record.ActionEntries {
			counts[action.Name]++
		}
	}

	return summaryLines(counts, "no action entries")
}

func summaryLines(counts map[string]int, empty string) []string {
	if len(counts) == 0 {
		return []string{empty}
	}

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("`%s`: %d", key, counts[key]))
	}

	return lines
}

// sortedKeys orders a count map so the provenance document is byte-stable
// between runs with the same content; Go map iteration is deliberately random.
func sortedKeys(counts map[string]int) []string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
