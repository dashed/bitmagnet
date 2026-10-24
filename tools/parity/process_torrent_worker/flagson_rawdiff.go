package main

// flagson-rawdiff: which canonical field explains each queue_jobs difference
// between two raw dumps (flagson-dump -raw-out). It is the evidence for the
// canonicalization: rows are paired by (sorted InfoHashes, status, retries) and
// each pair's difference is named -- the InfoHashes order (with the
// fingerprint that follows it), the error's line order, the error's client
// port -- or reported as unexplained, as is every row without a partner and
// every difference outside queue_jobs.

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
)

// rawQueueRow is one queue_jobs line of the shared dump: concat_ws(' | ',
// fingerprint, queue, status, payload, retries, max_retries, error, priority,
// ran_at set, deadline, archival_duration).
type rawQueueRow struct {
	fingerprint string
	hashes      []string
	jobError    string
	rest        string // every other field
}

func readRawQueueRows(path string) (map[string][]rawQueueRow, map[string]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	rows := map[string][]rawQueueRow{}
	other := map[string]int{} // lines outside queue_jobs

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<26)

	for scanner.Scan() {
		line := scanner.Text()

		body, ok := strings.CutPrefix(line, "queue_jobs\t")
		if !ok {
			if !strings.HasPrefix(line, "raw:queue_jobs") {
				other[line]++
			}

			continue
		}

		fields := strings.Split(body, " | ")
		if len(fields) < 11 {
			return nil, nil, fmt.Errorf("%s: short queue_jobs line %q", path, line)
		}

		var payload struct {
			InfoHashes []string `json:"InfoHashes"`
		}

		if err := json.Unmarshal([]byte(fields[3]), &payload); err != nil {
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}

		n := len(fields)
		row := rawQueueRow{
			fingerprint: fields[0],
			hashes:      payload.InfoHashes,
			jobError:    strings.Join(fields[6:n-4], " | "),
			rest:        strings.Join(slices.Concat(fields[1:3], fields[4:6], fields[n-4:]), " | "),
		}

		sorted := slices.Sorted(slices.Values(row.hashes))
		key := strings.Join(sorted, ",") + " | " + fields[2] + " | retries=" + fields[4]
		rows[key] = append(rows[key], row)
	}

	return rows, other, scanner.Err()
}

func errorLines(text string) []string {
	body, _ := strings.CutPrefix(text, jobErrorPrefix)
	return slices.Sorted(slices.Values(strings.Split(body, `\n`)))
}

func runFlagsOnRawDiff(args []string) error {
	fs := flag.NewFlagSet("flagson-rawdiff", flag.ExitOnError)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() != 2 {
		return errors.New("usage: flagson-rawdiff A/dump.raw B/dump.raw")
	}

	a, aOther, err := readRawQueueRows(fs.Arg(0))
	if err != nil {
		return err
	}

	b, bOther, err := readRawQueueRows(fs.Arg(1))
	if err != nil {
		return err
	}

	kinds := map[string]int{}

	var unexplained []string

	for _, line := range sortedIdentities(aOther, bOther) {
		if aOther[line] != bOther[line] {
			unexplained = append(unexplained, fmt.Sprintf("outside queue_jobs: %d vs %d of %.200s", aOther[line], bOther[line], line))
		}
	}

	keys := make([]string, 0, len(a)+len(b))
	for k := range a {
		keys = append(keys, k)
	}

	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		ra, rb := a[k], b[k]
		if len(ra) != 1 || len(rb) != 1 {
			if fmt.Sprint(ra) != fmt.Sprint(rb) {
				unexplained = append(unexplained, fmt.Sprintf("%d vs %d rows of %.200s", len(ra), len(rb), k))
			}

			continue
		}

		x, y := ra[0], rb[0]

		if x.rest != y.rest {
			unexplained = append(unexplained, fmt.Sprintf("other fields of %.120s: %s vs %s", k, x.rest, y.rest))
		}

		switch {
		case !slices.Equal(x.hashes, y.hashes):
			kinds["InfoHashes order (and fingerprint)"]++
		case x.fingerprint != y.fingerprint:
			unexplained = append(unexplained, fmt.Sprintf("fingerprint of %.120s with the same InfoHashes order", k))
		}

		if x.jobError == y.jobError {
			continue
		}

		xm, ym := ephemeralPort.ReplaceAllString(x.jobError, "$1 tcp 127.0.0.1:<port>->"),
			ephemeralPort.ReplaceAllString(y.jobError, "$1 tcp 127.0.0.1:<port>->")

		switch {
		case xm == ym:
			kinds["error client port"]++
		case slices.Equal(errorLines(xm), errorLines(ym)):
			kinds["error line order"]++
		default:
			unexplained = append(unexplained, fmt.Sprintf("error of %.120s: %.300q vs %.300q", k, x.jobError, y.jobError))
		}
	}

	parts := make([]string, 0, len(kinds))
	for _, kind := range sortedIdentities(kinds) {
		parts = append(parts, fmt.Sprintf("%d rows differ in %s", kinds[kind], kind))
	}

	if len(parts) == 0 {
		parts = append(parts, "no row differs in a canonical field")
	}

	_, _ = fmt.Fprintf(os.Stdout, "%s; %d unexplained\n", strings.Join(parts, ", "), len(unexplained))

	for _, u := range unexplained {
		_, _ = fmt.Fprintln(os.Stdout, "UNEXPLAINED: "+u)
	}

	if len(unexplained) > 0 {
		return errors.New("flagson-rawdiff: unexplained differences")
	}

	return nil
}
