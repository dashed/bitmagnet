package main

// flagson-hint-coverage: the hint corpora's floors (flagson_hints.go), from a
// Go-alone run's drained database and tape, over the jobs a worker generation
// processes (-mode, flags_on.sh HINT_EXPECT). In process and offline mode
// those are the Rust-vs-Go counts once the run's G2 dumps are identical, and
// that generation's floors are enforced; in abstain mode a worker before M3b
// leaves these jobs to Go, so process's floors are reported only. Every count
// over all of the corpus's jobs (the Go oracle's) is reported beside it.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/jackc/pgx/v5"
)

type hintFloor struct {
	name  string
	floor int
}

// Metrics of the hint corpora.
const (
	metricImdbFind     = "imdb /find then details"
	metricTvdbFind     = "tvdb /find"
	metricTmdbByID     = "tmdb by-ID details"
	metricPreAttached  = "association pre-attached, no I/O"
	metricReAttached   = "association re-attached by imdb id, no TMDB request"
	metricStaleGone    = "stale association replaced"
	metricIDNoSource   = "id without source classified"
	metricTvdbMovie    = "tvdb hint on a movie, no /find"
	metricSourcedRich  = "sourced + enriched applied"
	metricFolded       = "video_* folded spelling applied"
	metricEpisodesNone = "episodes '{}' classified"
	metricGroup        = "release_group applied"
	metricGroupEmpty   = "release_group '' applied"
	metricLanguages    = "languages applied"
	metricReuse        = "reuse kept the hint's attributes"
	metricBare         = "bare type-only hint classified"

	metricOffAttached    = "flags-off: sourced hint attached locally, no request"
	metricOffKept        = "flags-off: association kept by ID, no request"
	metricOffStale       = "flags-off: stale association replaced, no request"
	metricOffUnmatched   = "flags-off: sourced hint unmatched, no request"
	metricFind404Failed  = "/find 404: torrent failed, nothing written"
	metricFind500Failed  = "/find 500: torrent failed, nothing written"
	metricFindFaultOK    = "/find fault job: companion attached by /find"
	metricFindDetails404 = "/find fault job: details 404 unmatched, searched"
	metricFindEmpty      = "/find fault job: empty /find, searched"
	metricOffVideo       = "flags-off: video_* applied"
	metricOffReuse       = "flags-off: reuse kept the hint's video_*"
	metricOffOtherType   = "flags-off: other-type association replaced"
	metricRematchVideo   = "rematch: video_* applied, stale association replaced"
)

// hintFloors are each generation's floors by corpus; abstain reports
// process's.
var hintFloors = map[string]map[string][]hintFloor{
	hintExpectProcess: processFloors,
	hintExpectOffline: {
		corpusEnriched: {
			{metricOffVideo, 200},
			{metricOffReuse, 50},
			{metricOffOtherType, 25},
		},
	},
}

var processFloors = map[string][]hintFloor{
	corpusHint: {
		{metricImdbFind, 2000},
		{metricTvdbFind, 200},
		{metricTmdbByID, 200},
		{metricPreAttached, 100},
		{metricReAttached, 200},
		{metricStaleGone, 100},
		{metricIDNoSource, 100},
		{metricTvdbMovie, 100},
		{metricSourcedRich, 200},
		{metricOffAttached, 150},
		{metricOffKept, 50},
		{metricOffStale, 40},
		{metricOffUnmatched, 40},
		{metricFind404Failed, 10},
		{metricFind500Failed, 10},
		{metricFindFaultOK, 15},
		{metricFindDetails404, 15},
		{metricFindEmpty, 15},
	},
	corpusEnriched: {
		{"video_resolution applied", 500},
		{"video_source applied", 500},
		{"video_codec applied", 500},
		{"video_3d applied", 500},
		{"video_modifier applied", 500},
		{metricFolded, 100},
		{"episodes-season applied", 100},
		{"episodes-multi-season applied", 100},
		{"episodes-range applied", 100},
		{"episodes-mixed applied", 100},
		{metricEpisodesNone, 100},
		{metricGroup, 100},
		{metricGroupEmpty, 100},
		{metricLanguages, 100},
		{metricReuse, 100},
		{metricBare, 100},
		{metricOffVideo, 200},
		{metricOffReuse, 50},
		{metricOffOtherType, 25},
		{metricRematchVideo, 100},
	},
}

// hintTape is what Go's tape says one hinted torrent's classification asked.
type hintTape struct {
	finds        []string // each /find's external_source
	findDetails  bool     // a details request after a /find
	byIDDetails  bool     // a details request before any /find or search
	searched     bool     // a TMDB search
	tmdbRequests int
	observations int
}

// hintOutcome is a hinted torrent's hint and its associations after the run.
type hintOutcome struct {
	hint      map[string]*string // column -> stored text
	tcs       []hintTC
	equalLang bool
	equalEps  bool
}

type hintTC struct {
	id      string
	touched bool
	values  map[string]*string
}

var hintOutcomeColumns = append(append([]string{}, videoColumns...), "release_group")

func runFlagsOnHintCoverage(args []string) error {
	fs := flag.NewFlagSet("flagson-hint-coverage", flag.ExitOnError)
	dir := fs.String("dir", "", "a Go-alone run of a hint corpus: seed/ (jobs.jsonl, notes.jsonl, summary.json), tape/tape.jsonl")
	dsn := fs.String("dsn", "", "that run's drained database")
	mode := fs.String("mode", hintExpectProcess, "abstain | process | offline: the worker generation whose processed jobs count")

	if err := fs.Parse(args); err != nil {
		return err
	}

	var summary struct {
		Corpus string `json:"corpus"`
	}

	raw, err := os.ReadFile(filepath.Join(*dir, "seed", "summary.json"))
	if err != nil {
		return err
	}

	if err := json.Unmarshal(raw, &summary); err != nil {
		return err
	}

	jobs, err := readJSONL[plannedJob](filepath.Join(*dir, "seed", "jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	left, err := hintExpectations(jobs, *mode)
	if err != nil {
		return err
	}

	processed := map[string]bool{}
	jobsBySlice := map[string][2]int{}

	for _, j := range jobs {
		n := jobsBySlice[j.Slice]
		n[0]++

		if _, ok := left[j.Fingerprint]; !ok {
			n[1]++

			for _, h := range j.InfoHashes {
				processed[h] = true
			}
		}

		jobsBySlice[j.Slice] = n
	}

	notes, err := readJSONL[fixtureNote](filepath.Join(*dir, "seed", "notes.jsonl"), 0)
	if err != nil {
		return err
	}

	categoryOf := map[string]string{}

	for _, n := range notes {
		if n.Kind == noteHint {
			categoryOf[n.Key] = n.Category
		}
	}

	seededIDs, err := readSeededAssociations(filepath.Join(*dir, "seed"))
	if err != nil {
		return err
	}

	seeded := map[string]bool{}
	for _, id := range seededIDs {
		seeded[id] = true
	}

	tapes, err := readHintTapes(filepath.Join(*dir, "tape", "tape.jsonl"))
	if err != nil {
		return err
	}

	outcomes, err := readHintOutcomes(*dsn)
	if err != nil {
		return err
	}

	oracle, counted := coverageCounts{}, coverageCounts{}

	for hash, category := range categoryOf {
		for _, m := range hintMetrics(category, tapes[hash], outcomes[hash], seeded) {
			oracle[m]++

			if processed[hash] {
				counted[m]++
			}
		}

		oracle["category "+category]++

		if processed[hash] {
			counted["category "+category]++
		}
	}

	return reportHintCoverage(summary.Corpus, *mode, oracle, counted, jobsBySlice)
}

func readHintTapes(path string) (map[string]hintTape, error) {
	tapes := map[string]hintTape{}

	err := forEachTapeRecord(path, func(r tapeRecord) error {
		t := tapes[r.Subject]
		sawFind, sawSearch := false, false

		for _, o := range r.Observations {
			t.observations++

			if o.Kind != observationTMDBRequest {
				continue
			}

			t.tmdbRequests++

			var req tapeTMDBRequest
			if err := json.Unmarshal(o.Request, &req); err != nil {
				return err
			}

			switch {
			case strings.HasPrefix(req.Path, "/find/"):
				sawFind = true
				t.finds = append(t.finds, req.QueryParams["external_source"])
			case strings.HasPrefix(req.Path, "/search/"):
				sawSearch, t.searched = true, true
			case sawFind:
				t.findDetails = true
			case !sawSearch:
				t.byIDDetails = true
			}
		}

		tapes[r.Subject] = t

		return nil
	})

	return tapes, err
}

func readHintOutcomes(dsn string) (map[string]*hintOutcome, error) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	fixtureTS, err := time.Parse(time.RFC3339Nano, fixtureTime)
	if err != nil {
		return nil, err
	}

	cols := make([]string, 0, 2*len(hintOutcomeColumns))
	for _, c := range hintOutcomeColumns {
		cols = append(cols, "h."+c, "tc."+c)
	}

	rows, err := conn.Query(ctx, `SELECT encode(h.info_hash, 'hex'), h.episodes::text, tc.id,
			coalesce(tc.updated_at <> $1, false),
			coalesce(h.languages = tc.languages, false), coalesce(h.episodes = tc.episodes, false), `+
		strings.Join(cols, ", ")+`
		FROM torrent_hints h LEFT JOIN torrent_contents tc USING (info_hash)`, fixtureTS)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	outcomes := map[string]*hintOutcome{}

	for rows.Next() {
		var (
			hash      string
			episodes  *string
			tcID      *string
			touched   bool
			equalLang bool
			equalEps  bool
		)

		values := make([]*string, len(cols))
		dest := []any{&hash, &episodes, &tcID, &touched, &equalLang, &equalEps}

		for i := range values {
			dest = append(dest, &values[i])
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		o := outcomes[hash]
		if o == nil {
			o = &hintOutcome{hint: map[string]*string{"episodes": episodes}}

			for i, c := range hintOutcomeColumns {
				o.hint[c] = values[2*i]
			}

			outcomes[hash] = o
		}

		if tcID == nil {
			continue
		}

		tc := hintTC{id: *tcID, touched: touched, values: map[string]*string{}}
		for i, c := range hintOutcomeColumns {
			tc.values[c] = values[2*i+1]
		}

		o.tcs = append(o.tcs, tc)
		o.equalLang = o.equalLang || equalLang
		o.equalEps = o.equalEps || equalEps
	}

	return outcomes, rows.Err()
}

// canonicalVideo is a stored video value as Go's go-enum scan reads it.
func canonicalVideo(column, value string) (string, error) {
	switch column {
	case "video_resolution":
		v, err := model.ParseVideoResolution(value)
		return v.String(), err
	case "video_source":
		v, err := model.ParseVideoSource(value)
		return v.String(), err
	case "video_codec":
		v, err := model.ParseVideoCodec(value)
		return v.String(), err
	case "video_3d":
		v, err := model.ParseVideo3D(value)
		return v.String(), err
	case "video_modifier":
		v, err := model.ParseVideoModifier(value)
		return v.String(), err
	}

	return "", fmt.Errorf("video column %q", column)
}

// hintMetrics are the metrics one hinted torrent meets.
func hintMetrics(category string, t hintTape, o *hintOutcome, seeded map[string]bool) []string {
	metrics := make([]string, 0, 8)

	if slices.Contains(t.finds, "imdb_id") && t.findDetails {
		metrics = append(metrics, metricImdbFind)
	}

	if slices.Contains(t.finds, "tvdb_id") {
		metrics = append(metrics, metricTvdbFind)
	}

	if t.byIDDetails {
		metrics = append(metrics, metricTmdbByID)
	}

	if o == nil || len(o.tcs) == 0 {
		switch {
		case !slices.Contains(t.finds, "imdb_id"):
		case category == catFindFault404:
			metrics = append(metrics, metricFind404Failed)
		case category == catFindFault500:
			metrics = append(metrics, metricFind500Failed)
		}

		return metrics
	}

	tc := o.tcs[0]
	single := len(o.tcs) == 1 && tc.touched
	attached := !strings.HasSuffix(tc.id, ":?:?")
	videoApplied, folded, anyVideo := true, 0, false

	for _, c := range videoColumns {
		stored := o.hint[c]
		if stored == nil {
			continue
		}

		anyVideo = true

		canonical, err := canonicalVideo(c, *stored)
		if err != nil || tc.values[c] == nil || *tc.values[c] != canonical {
			videoApplied = false
			continue
		}

		metrics = append(metrics, c+" applied")

		if *stored != canonical {
			folded++
		}
	}

	for range folded {
		metrics = append(metrics, metricFolded)
	}

	if group := o.hint["release_group"]; group != nil && tc.values["release_group"] != nil && *group == *tc.values["release_group"] {
		if *group == "" {
			metrics = append(metrics, metricGroupEmpty)
		} else {
			metrics = append(metrics, metricGroup)
		}
	}

	if eps := o.hint["episodes"]; eps != nil && *eps == "{}" && single {
		metrics = append(metrics, metricEpisodesNone)
	}

	switch category {
	case catAssociationCanonical:
		if single && seeded[tc.id] && t.observations == 0 {
			metrics = append(metrics, metricPreAttached)
		}
	case catAssociationImdb:
		if single && seeded[tc.id] && t.tmdbRequests == 0 {
			metrics = append(metrics, metricReAttached)
		}
	case catAssociationOther:
		if !seeded[tc.id] && len(o.tcs) == 1 {
			metrics = append(metrics, metricStaleGone)
		}
	case catIDWithoutSource:
		if single {
			metrics = append(metrics, metricIDNoSource)
		}
	case catTvdbMovie:
		if single && len(t.finds) == 0 {
			metrics = append(metrics, metricTvdbMovie)
		}
	case catSourcedEnriched:
		if single && anyVideo && videoApplied {
			metrics = append(metrics, metricSourcedRich)
		}
	case catEpisodesSeason, catEpisodesMultiSeason, catEpisodesRange, catEpisodesMixed:
		if single && o.equalEps {
			metrics = append(metrics, category+" applied")
		}
	case catLanguages, catAll:
		if single && o.equalLang {
			metrics = append(metrics, metricLanguages)
		}
	case catReuse:
		if single && seeded[tc.id] && t.tmdbRequests == 0 && anyVideo && videoApplied {
			metrics = append(metrics, metricReuse)
		}
	case catBare, catBareEpisodesEmpty, catBareReuse:
		if single {
			metrics = append(metrics, metricBare)
		}
	default:
		metrics = append(metrics, newSliceMetrics(category, t, o, seeded, attached, anyVideo && videoApplied)...)
	}

	return metrics
}

// newSliceMetrics are the metrics of the pseudo-titled slices' categories.
func newSliceMetrics(category string, t hintTape, o *hintOutcome, seeded map[string]bool, attached, videoApplied bool) []string {
	tc := o.tcs[0]
	single := len(o.tcs) == 1 && tc.touched
	replaced := len(o.tcs) == 1 && !seeded[tc.id]
	noRequest := t.tmdbRequests == 0

	var metric string

	switch category {
	case catOffLocalImdb, catOffLocalMulti, catOffLocalTmdb:
		if single && attached && !seeded[tc.id] && noRequest {
			metric = metricOffAttached
		}
	case catOffPreAttached, catOffReAttached:
		if single && seeded[tc.id] && noRequest {
			metric = metricOffKept
		}
	case catOffStale:
		if replaced && attached && noRequest {
			metric = metricOffStale
		}
	case catOffUnmatched:
		if single && !attached && noRequest {
			metric = metricOffUnmatched
		}
	case catFindFaultOK:
		if single && attached && t.findDetails {
			metric = metricFindFaultOK
		}
	case catFindFaultDetails:
		if single && !attached && t.findDetails && t.searched {
			metric = metricFindDetails404
		}
	case catFindFaultEmpty:
		if single && !attached && len(t.finds) > 0 && !t.findDetails && t.searched {
			metric = metricFindEmpty
		}
	case catOffVideo:
		if single && videoApplied {
			metric = metricOffVideo
		}
	case catOffReuse:
		if single && seeded[tc.id] && noRequest && videoApplied {
			metric = metricOffReuse
		}
	case catOffReuseOtherType:
		if replaced {
			metric = metricOffOtherType
		}
	case catRematchVideo:
		if single && replaced && videoApplied {
			metric = metricRematchVideo
		}
	}

	if metric == "" {
		return nil
	}

	return []string{metric}
}

func reportHintCoverage(corpus, mode string, oracle, counted coverageCounts, jobsBySlice map[string][2]int) error {
	_, _ = fmt.Fprintf(os.Stdout, "%s coverage, HINT_EXPECT=%s (counted: torrents of the jobs this generation processes)\n", corpus, mode)
	_, _ = fmt.Fprintf(os.Stdout, "%-56s %10s %10s %7s %s\n", "metric", "all jobs", "counted", "floor", "")

	failed := 0
	isFloor := map[string]bool{}
	floors, enforced := hintFloors[mode][corpus], mode != hintExpectAbstain

	if !enforced {
		floors = hintFloors[hintExpectProcess][corpus]
	}

	for _, f := range floors {
		isFloor[f.name] = true
		status := verdictMet

		switch {
		case counted[f.name] >= f.floor:
		case enforced:
			status = verdictNotMet
			failed++
		default:
			status = "not met (info: this generation leaves these jobs to Go)"
		}

		_, _ = fmt.Fprintf(os.Stdout, "%-56s %10d %10d %7d %s\n", f.name, oracle[f.name], counted[f.name], f.floor, status)
	}

	_, _ = fmt.Fprintln(os.Stdout, "--")

	keys := make([]string, 0, len(oracle))
	for k := range oracle {
		if !isFloor[k] {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		_, _ = fmt.Fprintf(os.Stdout, "%-56s %10d %10d\n", k, oracle[k], counted[k])
	}

	sliceNames := make([]string, 0, len(jobsBySlice))
	for name := range jobsBySlice {
		sliceNames = append(sliceNames, name)
	}

	sort.Strings(sliceNames)

	for _, name := range sliceNames {
		_, _ = fmt.Fprintf(os.Stdout, "%-56s %10d %10d\n", "jobs of slice "+name, jobsBySlice[name][0], jobsBySlice[name][1])
	}

	if failed > 0 {
		return fmt.Errorf("hint coverage: %d floors not met", failed)
	}

	return nil
}
