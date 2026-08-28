package tape

import (
	"fmt"
	"maps"
)

// recordSummary is the one definition of the aggregate evidence carried by a
// tape. The writer uses it to build the manifest and the reader recomputes it
// from tape.jsonl, so a stale or hand-edited manifest cannot bless different
// records.
type recordSummary struct {
	recordCount              int
	observationCount         int
	incompleteRecordCount    int
	authoritativeRecordCount int
	recordOutcomeCounts      map[string]int
	actionEntryCount         int
	actionEntryCounts        map[string]int
}

func summarizeRecords(records []Record) recordSummary {
	summary := recordSummary{
		recordCount:         len(records),
		recordOutcomeCounts: make(map[string]int),
		actionEntryCounts:   make(map[string]int),
	}

	for _, record := range records {
		summary.observationCount += len(record.Observations)

		if record.Incomplete {
			summary.incompleteRecordCount++
		}

		if record.Authoritative() {
			summary.authoritativeRecordCount++
		}

		outcome := "unknown"
		if record.Outcome != nil {
			outcome = string(record.Outcome.Kind)
		}
		summary.recordOutcomeCounts[outcome]++

		for _, action := range record.ActionEntries {
			summary.actionEntryCount++
			summary.actionEntryCounts[action.Name]++
		}
	}

	return summary
}

// validateManifestRecords checks every manifest count whose value can be
// recomputed from tape.jsonl. Action-entry fields and outcome counts are
// optional for backward compatibility with older v1 writers; whenever they are
// present, however, they are just as load-bearing as the original counts.
func validateManifestRecords(manifest Manifest, records []Record) error {
	summary := summarizeRecords(records)

	checks := []struct {
		name string
		got  int
		want int
	}{
		{"recordCount", manifest.RecordCount, summary.recordCount},
		{"observationCount", manifest.ObservationCount, summary.observationCount},
		{"incompleteRecordCount", manifest.IncompleteRecordCount, summary.incompleteRecordCount},
	}
	if manifest.authoritativeRecordCountPresent {
		checks = append(checks, struct {
			name string
			got  int
			want int
		}{"authoritativeRecordCount", manifest.AuthoritativeRecordCount, summary.authoritativeRecordCount})
	}

	for _, check := range checks {
		if check.got != check.want {
			return fmt.Errorf(
				"tape manifest declares %s %d but the tape holds %d",
				check.name,
				check.got,
				check.want,
			)
		}
	}

	if manifest.RecordOutcomeCounts != nil &&
		!maps.Equal(manifest.RecordOutcomeCounts, summary.recordOutcomeCounts) {
		return fmt.Errorf(
			"tape manifest recordOutcomeCounts %v do not match the tape %v",
			manifest.RecordOutcomeCounts,
			summary.recordOutcomeCounts,
		)
	}

	if manifest.ActionEntryCount == nil {
		if manifest.ActionEntryCounts != nil {
			return fmt.Errorf("tape manifest has actionEntryCounts without actionEntryCount")
		}
		for _, record := range records {
			if record.actionEntriesPresent || len(record.ActionEntries) > 0 {
				return fmt.Errorf(
					"tape record %q has actionEntries but the manifest does not declare actionEntryCount",
					record.Subject,
				)
			}
		}
		return nil
	}

	if *manifest.ActionEntryCount != summary.actionEntryCount {
		return fmt.Errorf(
			"tape manifest declares actionEntryCount %d but the tape holds %d",
			*manifest.ActionEntryCount,
			summary.actionEntryCount,
		)
	}

	// A known total of zero legitimately omits the empty map. Treat nil and an
	// empty map as the same value only in that case.
	if !maps.Equal(manifest.ActionEntryCounts, summary.actionEntryCounts) {
		return fmt.Errorf(
			"tape manifest actionEntryCounts %v do not match the tape %v",
			manifest.ActionEntryCounts,
			summary.actionEntryCounts,
		)
	}

	return nil
}

func copyCountMap(source map[string]int) map[string]int {
	if source == nil {
		return nil
	}

	copied := make(map[string]int, len(source))
	maps.Copy(copied, source)

	return copied
}
