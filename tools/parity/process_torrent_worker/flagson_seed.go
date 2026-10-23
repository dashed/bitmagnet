package main

// flagson-seed: one flags-on corpus into a migrated, empty database, plus its
// TMDB stub fixtures. Three corpora, each in its own database:
//
//   - main: the flags-off seed's torrents (synthetic inputs without a sourced
//     hint, and the real-name replay corpus) plus copies of the deletion
//     inputs. Content and stub fixtures are generated from what Go's own
//     classifier asks for each torrent (flagson_plan.go), so local and TMDB
//     searches really match. This is the G-1 and G2 corpus.
//   - hint: torrents with explicit content-ID hints, the only way into the
//     by-ID attach actions and /find. The Rust worker abstains from them by
//     design (load.rs is_supported_type_only), so this corpus is the Go
//     oracle's by-ID coverage and an abstention check, not part of G2.
//   - fault: G3. A keyed TMDB fault per fault job, every torrent of a fault
//     job unique in the corpus, so a hand-off that runs the fault jobs last
//     cannot change any other job's result.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	corpusMain  = "main"
	corpusHint  = "hint"
	corpusFault = "fault"

	// heldRunAfter parks the 401 fault jobs until flags_on.sh releases them,
	// one at a time, into fresh worker processes: a 401 latches both TMDB
	// clients fail-fast for the life of the process.
	heldRunAfter = "2100-01-01T00:00:00Z"

	// sourceLocal is the metadata source of seeded content no TMDB request
	// can reach (seedRun inserts it beside TVmaze and Ömdb).
	sourceLocal = "local"

	// TMDB search group categories (searchFixtures).
	categoryNew       = "new"
	categoryExisting  = "existing"
	categoryUnmatched = "unmatched"
)

type seedOptions struct {
	dsn, root, out, corpus string
	replayLimit            int
	jobSize                int
	flagsOffEvery          int
	deletionCopies         int
	hintCount              int
	faultReplay            int
}

// seedRun accumulates one corpus: queued torrent rows, local content, stub
// fixtures and what every torrent was planned to exercise.
type seedRun struct {
	opts seedOptions
	ctx  context.Context
	conn *pgx.Conn
	db   *gorm.DB
	tpl  *bodyTemplates
	ts   time.Time

	batch   *pgx.Batch
	sliceOf map[protocol.ID]string
	hashes  []protocol.ID

	responses map[string]stubResponse
	faults    []stubFault
	contents  []seededContent
	notes     []fixtureNote

	nextTMDBID  int64
	nextLocalID int64
}

// fixtureNote records the generator's intent for one key, for the coverage
// report and for reading diffs.
type fixtureNote struct {
	Kind     string `json:"kind"` // local | tmdb | tmdb-layout | hint | fault | deletion-copy
	Key      string `json:"key"`
	Category string `json:"category"`
	Variant  string `json:"variant,omitempty"`
	ID       string `json:"id,omitempty"`
}

func runFlagsOnSeed(args []string) error {
	fs := flag.NewFlagSet("flagson-seed", flag.ExitOnError)
	o := seedOptions{}
	fs.StringVar(&o.dsn, "dsn", "", "disposable PostgreSQL DSN, migrated and empty")
	fs.StringVar(&o.root, "root", ".", "repository root")
	fs.StringVar(&o.out, "out", "", "directory for responses.jsonl, faults.jsonl, jobs.jsonl, plan.jsonl, ...")
	fs.StringVar(&o.corpus, "corpus", corpusMain, "main | hint | fault")
	fs.IntVar(&o.replayLimit, "replay-limit", 120000, "real-name replay inputs in the main corpus")
	fs.IntVar(&o.jobSize, "job-size", 100, "info hashes per job")
	fs.IntVar(&o.flagsOffEvery, "flags-off-every", 8, "every Nth main chunk carries a flags-off payload (0 = none)")
	fs.IntVar(&o.deletionCopies, "deletion-copies", 32, "copies of each synthetic deletion input in the main corpus")
	fs.IntVar(&o.hintCount, "hints", 4000, "hinted torrents in the hint corpus")
	fs.IntVar(&o.faultReplay, "fault-replay", 8000, "replay inputs in the fault corpus")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := requireFlag("dsn", o.dsn); err != nil {
		return err
	}

	if err := requireFlag("out", o.out); err != nil {
		return err
	}

	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, o.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	db, err := gorm.Open(postgres.Open(o.dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return err
	}

	tpl, err := loadBodyTemplates(o.root)
	if err != nil {
		return err
	}

	ts, _ := time.Parse(time.RFC3339Nano, fixtureTime)

	s := &seedRun{
		opts: o, ctx: ctx, conn: conn, db: db, tpl: tpl, ts: ts,
		batch:       &pgx.Batch{},
		sliceOf:     map[protocol.ID]string{},
		responses:   map[string]stubResponse{},
		nextTMDBID:  9001,
		nextLocalID: 2000000,
	}

	// Sources the generated local content and attributes use, beyond the
	// tmdb/imdb/tvdb the migrations insert: "TVmaze" sorts after "tmdb" under
	// en_US but before it bytewise; "Ömdb" carries an accent.
	if _, err := conn.Exec(ctx, `INSERT INTO metadata_sources (key, name, created_at, updated_at)
		VALUES ('local', 'Local', now(), now()), ('TVmaze', 'TVmaze', now(), now()), ('Ömdb', 'Ömdb', now(), now())
		ON CONFLICT DO NOTHING`); err != nil {
		return err
	}

	switch o.corpus {
	case corpusMain:
		err = s.seedMain()
	case corpusHint:
		err = s.seedHint()
	case corpusFault:
		err = s.seedFault()
	default:
		err = fmt.Errorf("unknown corpus %q", o.corpus)
	}

	if err != nil {
		return err
	}

	sqlDB, err := db.DB()
	if err == nil {
		_ = sqlDB.Close()
	}

	return nil
}

func (s *seedRun) flush() error {
	if s.batch.Len() == 0 {
		return nil
	}

	err := s.conn.SendBatch(s.ctx, s.batch).Close()
	s.batch = &pgx.Batch{}

	return err
}

func (s *seedRun) addTorrent(corpus, slice string, in input) (protocol.ID, error) {
	hash := fixtureHash(corpus, in.ID)
	if _, dup := s.sliceOf[hash]; dup {
		return hash, fmt.Errorf("duplicate torrent %s/%s", corpus, in.ID)
	}

	if _, err := queueTorrent(s.batch, hash, in, s.ts); err != nil {
		return hash, err
	}

	s.sliceOf[hash] = slice
	s.hashes = append(s.hashes, hash)

	if s.batch.Len() >= 1500 {
		return hash, s.flush()
	}

	return hash, nil
}

func (s *seedRun) respond(key string, status int, body []byte) error {
	if prev, ok := s.responses[key]; ok {
		if prev.Status != status || string(prev.Body) != string(body) {
			return fmt.Errorf("conflicting fixtures for %q", key)
		}

		return nil
	}

	s.responses[key] = stubResponse{Key: key, Status: status, Body: body}

	return nil
}

func (s *seedRun) note(n fixtureNote) { s.notes = append(s.notes, n) }

func (s *seedRun) allocTMDBID() int64 {
	// Steps of 37 from 9001 cross from four to five to six digits, so text
	// order and numeric order of the ids disagree.
	id := s.nextTMDBID
	s.nextTMDBID += 37

	return id
}

func (s *seedRun) allocLocalID() int64 {
	s.nextLocalID += 13
	return s.nextLocalID
}

func searchKey(endpoint, query string, year int) string {
	v := url.Values{"include_adult": {"true"}, "query": {query}}

	if year > 0 {
		if endpoint == endpointSearchTV {
			v.Set("first_air_date_year", strconv.Itoa(year))
		} else {
			v.Set("year", strconv.Itoa(year))
		}
	}

	return requestKey("GET", endpoint, v)
}

func detailsKey(tv bool, id int64) string {
	if tv {
		return requestKey("GET", "/tv/"+strconv.FormatInt(id, 10), url.Values{"append_to_response": {"external_ids"}})
	}

	return requestKey("GET", "/movie/"+strconv.FormatInt(id, 10), nil)
}

func findKey(externalID, externalSource string) string {
	return requestKey("GET", "/find/"+externalID, url.Values{"external_source": {externalSource}})
}

func planSearchKey(c plannedCall) string { return searchKey(c.Endpoint, c.Query, c.Year) }

// plan runs Go's classifier over every seeded torrent (flagson_plan.go) and
// checks it ran under the same effective configuration as the Go binary.
func (s *seedRun) plan() ([]plannedTorrent, string, error) {
	if err := s.flush(); err != nil {
		return nil, "", err
	}

	runner, digest, err := newPlanRunner(s.db)
	if err != nil {
		return nil, "", err
	}

	hashes := append([]protocol.ID(nil), s.hashes...)
	sortHashes(hashes)

	planned, err := planTorrents(s.ctx, s.db, runner, hashes, s.sliceOf)
	if err != nil {
		return nil, "", err
	}

	sort.Slice(planned, func(i, j int) bool { return planned[i].Hash < planned[j].Hash })

	return planned, digest, nil
}

// finish inserts the local content, queues the jobs and writes the fixtures.
func (s *seedRun) finish(planned []plannedTorrent, digest string, specs []jobSpec, held map[int]bool) error {
	if err := s.flush(); err != nil {
		return err
	}

	if err := s.insertContents(); err != nil {
		return err
	}

	jobs, err := enqueueFlagsOnJobs(s.ctx, s.conn, specs, s.ts.Add(-time.Hour))
	if err != nil {
		return err
	}

	if len(held) > 0 {
		heldAt, _ := time.Parse(time.RFC3339, heldRunAfter)

		for i := range jobs {
			if !held[i] {
				continue
			}

			if _, err := s.conn.Exec(s.ctx, `UPDATE queue_jobs SET run_after = $1 WHERE fingerprint = $2`,
				heldAt.Add(time.Duration(i)*time.Millisecond), jobs[i].Fingerprint); err != nil {
				return err
			}
		}
	}

	if _, err := s.conn.Exec(s.ctx, "ANALYZE"); err != nil {
		return err
	}

	responses := make([]stubResponse, 0, len(s.responses))
	for _, r := range s.responses {
		responses = append(responses, r)
	}

	sort.Slice(responses, func(i, j int) bool { return responses[i].Key < responses[j].Key })
	sort.Slice(s.faults, func(i, j int) bool { return s.faults[i].Key < s.faults[j].Key })

	contentRefs := make([]string, len(s.contents))
	for i, c := range s.contents {
		contentRefs[i] = string(c.content.Type) + "/" + c.content.Source + "/" + c.content.ID
	}

	sort.Strings(contentRefs)

	out := func(name string) string { return filepath.Join(s.opts.out, name) }

	for _, w := range []struct {
		name string
		fn   func(string) error
	}{
		{"responses.jsonl", func(p string) error { return writeJSONLFile(p, responses) }},
		{"faults.jsonl", func(p string) error { return writeJSONLFile(p, s.faults) }},
		{"jobs.jsonl", func(p string) error { return writeJSONLFile(p, jobs) }},
		{"plan.jsonl", func(p string) error { return writeJSONLFile(p, planned) }},
		{"notes.jsonl", func(p string) error { return writeJSONLFile(p, s.notes) }},
		{"seeded-content.txt", func(p string) error {
			return os.WriteFile(p, []byte(strings.Join(contentRefs, "\n")+"\n"), 0o644)
		}},
	} {
		if err := w.fn(out(w.name)); err != nil {
			return err
		}
	}

	slices := map[string]int{}
	for _, j := range jobs {
		slices[j.Slice]++
	}

	outcomes := map[string]int{}
	for _, p := range planned {
		outcomes[p.Outcome]++
	}

	categories := map[string]int{}
	for _, n := range s.notes {
		categories[n.Kind+"/"+n.Category]++
	}

	summary := map[string]any{
		"corpus":                s.opts.corpus,
		"torrents":              len(s.hashes),
		"jobs":                  len(jobs),
		"jobsBySlice":           slices,
		"heldJobs":              len(held),
		"plannedOutcomes":       outcomes,
		"responses":             len(responses),
		"faults":                len(s.faults),
		"seededContent":         len(s.contents),
		"fixtureCategories":     categories,
		"effectiveConfigDigest": digest,
	}

	_, _ = fmt.Fprintf(os.Stderr, "flagson-seed %s: %d torrents, %d jobs %v, %d responses, %d faults, %d content, digest %s\n",
		s.opts.corpus, len(s.hashes), len(jobs), slices, len(responses), len(s.faults), len(s.contents), digest)

	return writeJSONFile(out("summary.json"), summary)
}

// ---- main corpus ----

func (s *seedRun) seedMain() error {
	o := s.opts

	synthetic, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier/inputs.jsonl"), 0)
	if err != nil {
		return err
	}

	goldens, err := readJSONL[golden](filepath.Join(o.root, "testdata/parity/classifier/corpus.golden.jsonl"), 0)
	if err != nil {
		return err
	}

	deleted := make(map[string]bool, len(goldens))
	for _, g := range goldens {
		deleted[g.ID] = g.Expected.Outcome == outcomeDeleted
	}

	replay, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier-replay/inputs.jsonl"), o.replayLimit)
	if err != nil {
		return err
	}

	var deletionCopies []protocol.ID

	for _, in := range synthetic {
		if in.Hint != nil && in.Hint.ContentSource != "" {
			continue // the hint corpus
		}

		if _, err := s.addTorrent("synthetic", sliceMain, in); err != nil {
			return err
		}

		if !deleted[in.ID] {
			continue
		}

		for k := range o.deletionCopies {
			c := in
			c.ID = fmt.Sprintf("%s/%d", in.ID, k)

			hash, err := s.addTorrent("deletion", sliceMain, c)
			if err != nil {
				return err
			}

			deletionCopies = append(deletionCopies, hash)
			s.note(fixtureNote{Kind: "deletion-copy", Key: hash.String(), Category: in.ID})
		}
	}

	for _, in := range replay {
		if _, err := s.addTorrent("replay", sliceMain, in); err != nil {
			return err
		}
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	if err := s.searchFixtures(planned, searchWeights{local: 25, tmdbNew: 42, tmdbExisting: 20, details404: 12, decoys: 9}); err != nil {
		return err
	}

	// Ten deletion copies form a deletion-only job (Go's second early return);
	// everything else is chunked.
	sortHashes(deletionCopies)

	deletionOnly := deletionCopies[:min(10, len(deletionCopies))]
	inDeletionJob := map[protocol.ID]bool{}

	for _, h := range deletionOnly {
		inDeletionJob[h] = true
		s.sliceOf[h] = sliceDeletion
	}

	var main []protocol.ID

	for _, h := range s.hashes {
		if !inDeletionJob[h] {
			main = append(main, h)
		}
	}

	specs := planFlagsOnJobs(main, nil, deletionOnly, o.jobSize, o.flagsOffEvery, true)

	return s.finish(labelPlanned(planned, specs), digest, specs, nil)
}

// labelPlanned stamps each planned torrent with the slice of the job it was
// queued in.
func labelPlanned(planned []plannedTorrent, specs []jobSpec) []plannedTorrent {
	slice := map[string]string{}

	for _, spec := range specs {
		for _, h := range spec.hashes {
			slice[h.String()] = spec.slice
		}
	}

	for i := range planned {
		planned[i].Slice = slice[planned[i].Hash]
	}

	return planned
}

// ---- search fixtures (main and fault corpora) ----

type searchWeights struct {
	local, tmdbNew, tmdbExisting, details404, decoys int // the rest of 100: unmatched (no fixture)
}

// localKeyOf is the local search a planned torrent makes: Go's
// attach_local_content_by_search runs whenever the result has a content type
// and a base title, so a planned local search ("local" call) carries them.
func localKeyOf(p plannedTorrent) (string, bool) {
	for _, c := range p.Calls {
		if c.Endpoint == planLocalSearch && p.Type != "" && p.BaseTitle != "" {
			return p.Type + "\x00" + p.BaseTitle + "\x00" + strconv.Itoa(p.Year), true
		}
	}

	return "", false
}

func tmdbSearchOf(p plannedTorrent) (plannedCall, bool) {
	for _, c := range p.Calls {
		if c.Endpoint == endpointSearchMovie || c.Endpoint == endpointSearchTV {
			return c, true
		}
	}

	return plannedCall{}, false
}

type localIdentity struct {
	ctype model.ContentType
	title string
	year  int
}

// searchFixtures decides, per local search and per TMDB search group, what the
// corpus answers, and generates the content and bodies for it. Decisions are
// keyed hashes of the request, so they do not depend on corpus size or order.
func (s *seedRun) searchFixtures(planned []plannedTorrent, w searchWeights) error {
	locals := map[string]localIdentity{}
	groups := map[string][]plannedCall{} // endpoint + query -> its year variants
	groupAdult := map[string]bool{}
	groupNotAdult := map[string]bool{}
	seenSearch := map[string]bool{}

	for _, p := range planned {
		if p.Outcome != outcomeClassified || p.Slice == sliceFault {
			continue
		}

		if key, ok := localKeyOf(p); ok {
			ct, err := model.ParseContentType(p.Type)
			if err != nil {
				return err
			}

			locals[key] = localIdentity{ctype: ct, title: p.BaseTitle, year: p.Year}
		}

		if c, ok := tmdbSearchOf(p); ok {
			g := c.Endpoint + "\x00" + c.Query
			if !seenSearch[planSearchKey(c)] {
				seenSearch[planSearchKey(c)] = true
				groups[g] = append(groups[g], c)
			}

			if p.Type == string(model.ContentTypeXxx) {
				groupAdult[g] = true
			} else {
				groupNotAdult[g] = true
			}
		}
	}

	localKeys := sortedKeys(locals)
	for _, key := range localKeys {
		if fixtureRand("local-cat/"+key).IntN(100) >= w.local {
			continue
		}

		if err := s.localContent(key, locals[key]); err != nil {
			return err
		}
	}

	for _, g := range sortedKeys(groups) {
		calls := groups[g]
		sort.Slice(calls, func(i, j int) bool { return calls[i].Year < calls[j].Year })

		r := fixtureRand("tmdb-cat/" + g).IntN(100)

		var category string

		switch {
		case r < w.tmdbNew:
			category = categoryNew
		case r < w.tmdbNew+w.tmdbExisting:
			category = categoryExisting
		case r < w.tmdbNew+w.tmdbExisting+w.details404:
			category = "details-404"
		case r < w.tmdbNew+w.tmdbExisting+w.details404+w.decoys:
			category = "decoys"
		default:
			category = categoryUnmatched
		}

		adult := groupAdult[g] && !groupNotAdult[g] && fixtureRand("adult/"+g).IntN(10) < 7
		if err := s.tmdbGroup(g, calls, category, adult); err != nil {
			return err
		}
	}

	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// tmdbGroup answers every year variant of one TMDB search query. The variants
// share the matched TMDB id, so a later job's variant finds the content an
// earlier job created: TMDB details for content that already exists.
func (s *seedRun) tmdbGroup(group string, calls []plannedCall, category string, adult bool) error {
	tv := calls[0].Endpoint == endpointSearchTV
	query := calls[0].Query

	for _, c := range calls {
		s.note(fixtureNote{Kind: "tmdb", Key: planSearchKey(c), Category: category})
	}

	if category == categoryUnmatched {
		return nil
	}

	r := fixtureRand("tmdb-group/" + group)

	if category == "decoys" {
		for _, c := range calls {
			items := []searchItem{}
			for i := range 1 + r.IntN(3) {
				items = append(items, searchItem{
					id:    s.allocTMDBID(),
					title: decoyTitle(query, i),
					year:  c.Year,
				})
			}

			if err := s.respond(planSearchKey(c), 200, buildSearchBody(s.tpl, r, tv, items)); err != nil {
				return err
			}
		}

		return nil
	}

	matchID := s.allocTMDBID()
	year := calls[len(calls)-1].Year

	for vi, c := range calls {
		layout := fixtureRand("layout/" + planSearchKey(c)).IntN(10)
		match := searchItem{id: matchID, title: query, year: c.Year}

		var items []searchItem

		var variant string

		switch {
		case layout < 4:
			variant, items = "match", []searchItem{match}
		case layout < 5:
			variant, items = "far-then-match", []searchItem{{id: s.allocTMDBID(), title: decoyTitle(query, 0), year: c.Year}, match}
		case layout < 6:
			variant, items = "near-then-match", []searchItem{{id: s.allocTMDBID(), title: query + " X", year: c.Year}, match}
		case layout < 7:
			match.title, match.originalTitle = "Ⓢ "+query+" ⓩ", query
			variant, items = "original-title", []searchItem{match}
		case layout < 8:
			match.title = query + " X"
			variant, items = "near-only", []searchItem{match}
		default:
			variant, items = "equal-first-wins", []searchItem{match, {id: s.allocTMDBID(), title: query, year: c.Year}}
		}

		if vi > 0 {
			variant += "/shared-id"
		}

		s.note(fixtureNote{
			Kind: "tmdb-layout", Key: planSearchKey(c), Category: category, Variant: variant,
			ID: strconv.FormatInt(matchID, 10),
		})

		if err := s.respond(planSearchKey(c), 200, buildSearchBody(s.tpl, r, tv, items)); err != nil {
			return err
		}
	}

	if category == "details-404" {
		return nil
	}

	spec := detailsSpec{tv: tv, id: matchID, title: query, year: year, adult: adult && category == categoryNew}

	switch r.IntN(4) {
	case 0:
	case 1:
		spec.originalTitle = query + " (original)"
		fallthrough
	default:
		spec.imdbID = fmt.Sprintf("tt%07d", 1000000+matchID%8999999)
		if tv && r.IntN(3) > 0 {
			spec.tvdbID = 70000 + matchID%900000
		}
	}

	if err := s.respond(detailsKey(tv, matchID), 200, buildDetailsBody(s.tpl, r, spec)); err != nil {
		return err
	}

	if category == categoryExisting {
		// The content already exists under the id TMDB returns, but under a
		// title no local search for this query matches; Go's upsert overwrites
		// it (UpdateAll) and adds to its associations.
		ct := model.ContentTypeMovie
		if tv {
			ct = model.ContentTypeTvShow
		}

		c := s.newLocalContent(ct, model.SourceTmdb, strconv.FormatInt(matchID, 10),
			fmt.Sprintf("Qz %x Placeholder", matchID), 0, r)
		s.contents = append(s.contents, c)
	}

	return nil
}

var decoyWords = []string{"Vortel", "Mandrake", "Expedition", "Quasar", "Lighthouse", "Ferro", "Calliope", "Brine"}

func decoyTitle(query string, i int) string {
	r := fixtureRand(fmt.Sprintf("decoy/%s/%d", query, i))

	return fmt.Sprintf("%s %s %s of %s", decoyWords[r.IntN(len(decoyWords))], decoyWords[r.IntN(len(decoyWords))],
		decoyWords[r.IntN(len(decoyWords))], decoyWords[r.IntN(len(decoyWords))])
}

// ---- local content ----

type seededContent struct {
	content model.Content
	// physical insertion orders, deliberately not the required ones
	attributeOrder  []int
	collectionOrder []int
}

func (*seedRun) newLocalContent(ct model.ContentType, source, id, title string, year int, r interface{ IntN(int) int }) seededContent {
	c := model.Content{Type: ct, Source: source, ID: id, Title: title}

	if year > 0 {
		c.ReleaseDate = model.NewDateFromParts(model.Year(year), time.Month(1+r.IntN(12)), uint8(1+r.IntN(28)))
		c.ReleaseYear = model.Year(year)
	} else if r.IntN(2) == 0 {
		y := 1950 + r.IntN(75)
		c.ReleaseDate = model.NewDateFromParts(model.Year(y), time.Month(1+r.IntN(12)), uint8(1+r.IntN(28)))
		c.ReleaseYear = model.Year(y)
	}

	switch r.IntN(4) {
	case 0:
		c.OriginalTitle = model.NewNullString(title)
	case 1:
		c.OriginalTitle = model.NewNullString(title + " – Originaltitel")
	}

	if r.IntN(3) > 0 {
		c.Overview = model.NewNullString("Seeded local content " + id + ".")
	}

	c.OriginalLanguage = model.ParseLanguage(fixtureLanguages[r.IntN(len(fixtureLanguages))])
	c.Popularity = model.NewNullFloat32(float32(r.IntN(100000)) / 997)
	c.VoteAverage = model.NewNullFloat32(float32(r.IntN(100)) / 10)
	c.VoteCount = model.NewNullUint(uint(r.IntN(9000)))

	if ct == model.ContentTypeXxx {
		c.Adult = model.NewNullBool(true)
	}

	addAttr := func(source, key, value string) {
		c.Attributes = append(c.Attributes, model.ContentAttribute{
			ContentType: ct, ContentSource: c.Source, ContentID: c.ID, Source: source, Key: key, Value: value,
		})
	}

	n := r.IntN(1 << 6)
	if n&1 != 0 || ct != model.ContentTypeTvShow {
		addAttr("imdb", "id", fmt.Sprintf("tt%07d", 3000000+len(id)*7919+n))
	}

	if ct == model.ContentTypeTvShow && n&2 != 0 {
		addAttr("tvdb", "id", strconv.Itoa(300000+n*131))
	}

	if n&4 != 0 {
		addAttr("TVmaze", "id", "tvm-"+id)
	}

	if n&8 != 0 {
		addAttr("Ömdb", "id", "ö-"+id)
	}

	if n&16 != 0 {
		addAttr("tmdb", "poster_path", "/p"+id+".jpg")
	}

	if n&32 != 0 {
		addAttr("tmdb", "backdrop_path", "/b"+id+".jpg")
	}

	used := map[int]bool{}
	for range r.IntN(5) {
		g := fixtureGenres[r.IntN(len(fixtureGenres))]
		if used[g.id] {
			continue
		}

		used[g.id] = true

		c.Collections = append(c.Collections, model.ContentCollection{
			Type: "genre", Source: model.SourceTmdb, ID: strconv.Itoa(g.id), Name: g.name,
		})
	}

	if r.IntN(6) == 0 {
		fid := 950000 + r.IntN(50)
		c.Collections = append(c.Collections, model.ContentCollection{
			Type: "franchise", Source: model.SourceTmdb, ID: strconv.Itoa(fid), Name: fmt.Sprintf("Franchise %d", fid),
		})
	}

	c.UpdateTsv()

	// Insert associations in reverse of the order they were built in, which
	// is not their (source, key) / (type, source, id) order either.
	sc := seededContent{content: c}
	for i := len(c.Attributes) - 1; i >= 0; i-- {
		sc.attributeOrder = append(sc.attributeOrder, i)
	}

	for i := len(c.Collections) - 1; i >= 0; i-- {
		sc.collectionOrder = append(sc.collectionOrder, i)
	}

	return sc
}

// tiePatterns are pairs of identities in the order the database returns them
// under en_US (first wins the levenshtein tie), each disagreeing with a
// bytewise or numeric order.
var tiePatterns = []struct {
	name             string
	first, second    func(n int64) (string, string)
	firstIsLexically bool
}{
	{
		name: "ten-before-nine", first: func(n int64) (string, string) { return model.SourceTmdb, "10" + strconv.FormatInt(n, 10) },
		second: func(n int64) (string, string) { return model.SourceTmdb, "9" + strconv.FormatInt(n, 10) },
	},
	{
		name: "tmdb-before-TVmaze", first: func(n int64) (string, string) { return model.SourceTmdb, strconv.FormatInt(n, 10) },
		second: func(n int64) (string, string) { return "TVmaze", strconv.FormatInt(n, 10) },
	},
	{
		name: "a-before-B", first: func(n int64) (string, string) { return sourceLocal, "a" + strconv.FormatInt(n, 10) },
		second: func(n int64) (string, string) { return sourceLocal, "B" + strconv.FormatInt(n, 10) },
	},
	{
		name: "é-before-f", first: func(n int64) (string, string) { return sourceLocal, "é" + strconv.FormatInt(n, 10) },
		second: func(n int64) (string, string) { return sourceLocal, "f" + strconv.FormatInt(n, 10) },
	},
}

// localContent seeds what one local search finds.
func (s *seedRun) localContent(key string, li localIdentity) error {
	r := fixtureRand("local/" + key)
	v := r.IntN(10)

	add := func(source, id, title string) {
		s.contents = append(s.contents, s.newLocalContent(li.ctype, source, id, title, li.year, r))
	}

	var variant string

	switch {
	case v < 4:
		variant = "single"
		add(model.SourceTmdb, strconv.FormatInt(s.allocLocalID(), 10), li.title)
	case v < 7:
		p := tiePatterns[r.IntN(len(tiePatterns))]
		variant = "tie/" + p.name
		n := s.allocLocalID()
		// The loser first, physically.
		src2, id2 := p.second(n)
		add(src2, id2, li.title)

		src1, id1 := p.first(n)
		add(src1, id1, li.title)
	case v < 8:
		variant = "original-title"
		sc := s.newLocalContent(li.ctype, model.SourceTmdb, strconv.FormatInt(s.allocLocalID(), 10), "Ⓛ "+li.title+" ⓛ", li.year, r)
		sc.content.OriginalTitle = model.NewNullString(li.title)
		sc.content.UpdateTsv()
		s.contents = append(s.contents, sc)
	default:
		variant = "near"
		add(model.SourceTmdb, strconv.FormatInt(s.allocLocalID(), 10), li.title+" II")
	}

	if r.IntN(3) == 0 {
		variant += "+decoy"

		add(sourceLocal, "d"+strconv.FormatInt(s.allocLocalID(), 10), li.title+" "+decoyTitle(li.title, 9))
	}

	s.note(fixtureNote{Kind: planLocalSearch, Key: key, Category: planLocalSearch, Variant: variant})

	return nil
}

// withoutAttribute drops an attribute and renumbers the insertion order.
func (sc *seededContent) withoutAttribute(source, key string) {
	var kept []model.ContentAttribute

	for _, a := range sc.content.Attributes {
		if a.Source != source || a.Key != key {
			kept = append(kept, a)
		}
	}

	sc.content.Attributes = kept
	sc.attributeOrder = nil

	for i := len(kept) - 1; i >= 0; i-- {
		sc.attributeOrder = append(sc.attributeOrder, i)
	}
}

func (s *seedRun) insertContents() error {
	batch := &pgx.Batch{}

	for _, sc := range s.contents {
		c := sc.content

		for _, i := range sc.collectionOrder {
			col := c.Collections[i]
			batch.Queue(`INSERT INTO content_collections (type, source, id, name, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $5) ON CONFLICT DO NOTHING`, col.Type, col.Source, col.ID, col.Name, s.ts)
		}

		batch.Queue(`INSERT INTO content (type, source, id, title, release_date, release_year, adult,
				original_language, original_title, overview, runtime, popularity, vote_average, vote_count,
				tsv, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::tsvector, $16, $16)`,
			string(c.Type), c.Source, c.ID, c.Title, c.ReleaseDate, c.ReleaseYear, c.Adult,
			c.OriginalLanguage, c.OriginalTitle, c.Overview, c.Runtime, c.Popularity, c.VoteAverage, c.VoteCount,
			c.Tsv.String(), s.ts)

		for _, i := range sc.attributeOrder {
			a := c.Attributes[i]
			batch.Queue(`INSERT INTO content_attributes
				(content_type, content_source, content_id, source, key, value, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`, string(c.Type), c.Source, c.ID, a.Source, a.Key, a.Value, s.ts)
		}

		for _, i := range sc.collectionOrder {
			col := c.Collections[i]
			batch.Queue(`INSERT INTO content_collections_content
				(content_type, content_source, content_id,
				 content_collection_type, content_collection_source, content_collection_id)
				VALUES ($1, $2, $3, $4, $5, $6)`, string(c.Type), c.Source, c.ID, col.Type, col.Source, col.ID)
		}

		if batch.Len() >= 1500 {
			if err := s.conn.SendBatch(s.ctx, batch).Close(); err != nil {
				return err
			}

			batch = &pgx.Batch{}
		}
	}

	if batch.Len() == 0 {
		return nil
	}

	return s.conn.SendBatch(s.ctx, batch).Close()
}

// ---- hint corpus ----

// hintCategories, with weights out of 100. "reuse" hints content an earlier
// hint job fetched from TMDB, so it is found locally by then.
var hintCategories = []struct {
	name   string
	weight int
}{
	{"local-canonical", 9},
	{"local-alternative", 6},
	{"local-alternative-multi", 3},
	{"direct-new", 10},
	{"direct-404", 4},
	{"find-new", 34},
	{"find-existing", 12},
	{"find-empty", 8},
	{"find-details-404", 6},
	{"find-wrong-bucket", 3},
	{"invalid-tmdb-id", 2},
	{"reuse", 3},
}

func (s *seedRun) seedHint() error {
	o := s.opts

	synthetic, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier/inputs.jsonl"), 0)
	if err != nil {
		return err
	}

	replay, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier-replay/inputs.jsonl"), o.hintCount)
	if err != nil {
		return err
	}

	// The synthetic corpus's own sourced hints, as they are.
	for _, in := range synthetic {
		if in.Hint != nil && in.Hint.ContentSource != "" {
			if _, err := s.addTorrent("synthetic", sliceHint, in); err != nil {
				return err
			}

			s.note(fixtureNote{Kind: "hint", Key: in.ID, Category: "synthetic"})
		}
	}

	var fetched []model.ContentRef // TMDB content earlier hints create, for "reuse"

	for i, in := range replay {
		r := fixtureRand("hint/" + in.ID)

		ct := model.ContentTypeMovie

		switch v := r.IntN(10); {
		case v < 4:
			ct = model.ContentTypeTvShow
		case v == 4:
			ct = model.ContentTypeXxx
		}

		tv := ct == model.ContentTypeTvShow

		cat := ""
		w := r.IntN(100)

		for _, c := range hintCategories {
			if w < c.weight {
				cat = c.name
				break
			}

			w -= c.weight
		}

		if cat == "reuse" && len(fetched) == 0 {
			cat = "find-new"
		}

		hint := &inputHint{ContentType: string(ct)}
		tmdbID := s.allocTMDBID()
		imdbID := fmt.Sprintf("tt%07d", 5000000+i*3)
		external, externalSource := imdbID, "imdb_id"

		if tv && r.IntN(3) == 0 {
			external, externalSource = strconv.Itoa(400000+i*7), "tvdb_id"
		}

		hintExternal := func() {
			if externalSource == "tvdb_id" {
				hint.ContentSource, hint.ContentID = model.SourceTvdb, external
			} else {
				hint.ContentSource, hint.ContentID = model.SourceImdb, external
			}
		}

		details := func(id int64) error {
			spec := detailsSpec{
				tv: tv, id: id, title: fmt.Sprintf("Hinted %s %d", ct, i), year: 1970 + i%55,
				adult: ct == model.ContentTypeXxx,
			}
			if externalSource == "imdb_id" {
				spec.imdbID = external
			} else {
				spec.tvdbID, _ = strconv.ParseInt(external, 10, 64)
			}

			return s.respond(detailsKey(tv, id), 200, buildDetailsBody(s.tpl, r, spec))
		}

		find := func(items []searchItem, asTV bool) error {
			return s.respond(findKey(external, externalSource), 200, buildFindBody(s.tpl, r, asTV, items))
		}

		var err error

		switch cat {
		case "local-canonical":
			hint.ContentSource, hint.ContentID = model.SourceTmdb, strconv.FormatInt(tmdbID, 10)
			s.contents = append(s.contents, s.newLocalContent(ct, model.SourceTmdb, hint.ContentID,
				fmt.Sprintf("Local canonical %d", i), 0, r))
		case "local-alternative", "local-alternative-multi":
			hintExternal()

			copies := 1
			if cat == "local-alternative-multi" {
				copies = 2 + r.IntN(2)
			}

			p := tiePatterns[r.IntN(len(tiePatterns))]
			n := s.allocLocalID()

			for k := copies - 1; k >= 0; k-- {
				var source, id string

				switch k {
				case 0:
					source, id = p.first(n)
				case 1:
					source, id = p.second(n)
				default:
					source, id = sourceLocal, fmt.Sprintf("z%d-%d", n, k)
				}

				sc := s.newLocalContent(ct, source, id, fmt.Sprintf("Local alternative %d/%d", i, k), 0, r)
				sc.withoutAttribute(hint.ContentSource, "id")
				sc.content.Attributes = append(sc.content.Attributes, model.ContentAttribute{
					ContentType: ct, ContentSource: source, ContentID: id,
					Source: hint.ContentSource, Key: "id", Value: hint.ContentID,
				})
				sc.attributeOrder = append([]int{len(sc.content.Attributes) - 1}, sc.attributeOrder...)
				sc.content.UpdateTsv()
				s.contents = append(s.contents, sc)
			}
		case "direct-new":
			hint.ContentSource, hint.ContentID = model.SourceTmdb, strconv.FormatInt(tmdbID, 10)
			err = details(tmdbID)
			fetched = append(fetched, model.ContentRef{Type: ct, Source: model.SourceTmdb, ID: hint.ContentID})
		case "direct-404":
			hint.ContentSource, hint.ContentID = model.SourceTmdb, strconv.FormatInt(tmdbID, 10)
		case "find-new", "find-existing":
			hintExternal()

			if err = find([]searchItem{{id: tmdbID, title: "Found " + external, year: 2001}}, tv); err == nil {
				err = details(tmdbID)
			}

			if cat == "find-existing" {
				s.contents = append(s.contents, s.newLocalContent(ct, model.SourceTmdb,
					strconv.FormatInt(tmdbID, 10), fmt.Sprintf("Qz %x Placeholder", tmdbID), 0, r))
			} else {
				fetched = append(fetched, model.ContentRef{
					Type: ct, Source: model.SourceTmdb,
					ID: strconv.FormatInt(tmdbID, 10),
				})
			}
		case "find-empty":
			hintExternal()
		case "find-details-404":
			hintExternal()
			err = find([]searchItem{{id: tmdbID, title: "Found " + external, year: 2002}}, tv)
		case "find-wrong-bucket":
			hintExternal()
			err = find([]searchItem{{id: tmdbID, title: "Wrong bucket " + external}}, !tv)
		case "invalid-tmdb-id":
			hint.ContentSource, hint.ContentID = model.SourceTmdb, fmt.Sprintf("x%d", tmdbID)
		case "reuse":
			ref := fetched[r.IntN(len(fetched))]
			hint.ContentType, hint.ContentSource, hint.ContentID = string(ref.Type), ref.Source, ref.ID
		}

		if err != nil {
			return err
		}

		in.Hint = hint

		if _, err := s.addTorrent("hint", sliceHint, in); err != nil {
			return err
		}

		s.note(fixtureNote{Kind: "hint", Key: in.ID, Category: cat, ID: hint.ContentSource + ":" + hint.ContentID})
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	specs := planFlagsOnJobs(nil, append([]protocol.ID(nil), s.hashes...), nil, s.opts.jobSize, 0, false)

	return s.finish(labelPlanned(planned, specs), digest, specs, nil)
}

// ---- fault corpus ----

var faultPlan = []struct {
	fault   string
	details bool // fault the details request after a matching search
	count   int
}{
	{faultRateLimited, false, 3},
	{faultInternal, false, 3},
	{faultUnavailable, false, 3},
	{faultNotFound, false, 3},
	{faultTimeout, false, 2},
	{faultInternal, true, 3},
	{faultUnauthorized, false, 2},
}

func (s *seedRun) seedFault() error {
	o := s.opts

	replay, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier-replay/inputs.jsonl"), o.faultReplay)
	if err != nil {
		return err
	}

	for _, in := range replay {
		if _, err := s.addTorrent("fault", sliceMain, in); err != nil {
			return err
		}
	}

	// Fault jobs hold only torrents whose titles are pseudo-words no other
	// title contains. Distinct exact keys would not be enough: local search is
	// a phrase query, so content titled "A B C" is a candidate for base title
	// "A B", and moving a job could then change another torrent's result.
	pseudo := map[string]bool{}

	for i := range 6 * faultJobCount() {
		h, err := s.addTorrent("fault-pseudo", sliceMain, pseudoInput(i))
		if err != nil {
			return err
		}

		pseudo[h.String()] = true
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	var unique, noIO []plannedTorrent

	seenKeys := map[string]bool{}

	for _, p := range planned {
		k, hasLocal := localKeyOf(p)
		c, hasSearch := tmdbSearchOf(p)

		switch {
		case p.Outcome != outcomeClassified:
		case pseudo[p.Hash] && hasLocal && hasSearch:
			if seenKeys[k] || seenKeys[planSearchKey(c)] {
				return fmt.Errorf("fault corpus: pseudo title %q is not unique", p.BaseTitle)
			}

			seenKeys[k], seenKeys[planSearchKey(c)] = true, true

			unique = append(unique, p)
		case !pseudo[p.Hash] && !hasLocal && !hasSearch:
			noIO = append(noIO, p)
		}
	}

	sliceOfHash := map[string]string{}

	var faultJobs [][]protocol.ID

	var faultKinds []string

	take := func(list *[]plannedTorrent) plannedTorrent {
		p := (*list)[0]
		*list = (*list)[1:]

		return p
	}

	for _, f := range faultPlan {
		for range f.count {
			if len(unique) < 4 || len(noIO) < 5 {
				return errors.New("fault corpus: too few pseudo-titled or no-I/O torrents")
			}

			ft := take(&unique)
			c, _ := tmdbSearchOf(ft)
			key := planSearchKey(c)
			kind := f.fault

			if f.details {
				tv := c.Endpoint == endpointSearchTV
				id := s.allocTMDBID()

				if err := s.respond(key, 200, buildSearchBody(s.tpl, fixtureRand("fault/"+key), tv,
					[]searchItem{{id: id, title: c.Query, year: c.Year}})); err != nil {
					return err
				}

				key = detailsKey(tv, id)
				kind = "details-" + f.fault
			}

			s.faults = append(s.faults, stubFault{Key: key, Fault: f.fault})
			s.note(fixtureNote{Kind: "fault", Key: key, Category: kind})

			job := []protocol.ID{mustHash(ft.Hash)}
			sliceOfHash[ft.Hash] = sliceFault

			// A 401 latches the Go client for the process, so a 401 job holds
			// no other torrent that does I/O; other fault jobs carry three
			// unique I/O torrents, whose writes Rust must withhold too.
			if f.fault != faultUnauthorized {
				for range 3 {
					p := take(&unique)
					job = append(job, mustHash(p.Hash))
					sliceOfHash[p.Hash] = sliceFault
				}
			}

			for range 5 {
				p := take(&noIO)
				job = append(job, mustHash(p.Hash))
				sliceOfHash[p.Hash] = sliceFault
			}

			faultJobs = append(faultJobs, job)
			faultKinds = append(faultKinds, kind)
		}
	}

	for i := range planned {
		if sl, ok := sliceOfHash[planned[i].Hash]; ok {
			planned[i].Slice = sl
		}
	}

	// Unique torrents that are left over, and everything else, get ordinary
	// fixtures; the fault jobs' companions too.
	if err := s.searchFixtures(planned, searchWeights{local: 30, tmdbNew: 40, tmdbExisting: 15, details404: 10, decoys: 10}); err != nil {
		return err
	}

	// Their companions in fault jobs, which searchFixtures skipped.
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

	step := o.jobSize - 1

	var specs []jobSpec

	held := map[int]bool{}
	every := max(1, (len(other)/step)/max(1, len(faultJobs)))
	f := 0

	for i := 0; i < len(other) || f < len(faultJobs); i += step {
		if i < len(other) {
			specs = append(specs, jobSpec{
				slice: sliceMain, flagsOn: true,
				hashes: append([]protocol.ID(nil), other[i:min(i+step, len(other))]...),
			})
		}

		if f < len(faultJobs) && ((i/step)%every == every-1 || i+step >= len(other)) {
			if faultKinds[f] == faultUnauthorized {
				held[len(specs)] = true
			}

			specs = append(specs, jobSpec{slice: sliceFault, flagsOn: true, hashes: faultJobs[f], fault: faultKinds[f]})
			f++
		}
	}

	return s.finish(labelPlanned(planned, specs), digest, specs, held)
}

func faultJobCount() int {
	n := 0
	for _, f := range faultPlan {
		n += f.count
	}

	return n
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

// companionFixtures answers the unique I/O torrents riding in fault jobs:
// half find their content on TMDB, half find nothing.
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
			continue // the details-fault torrent's search
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

func mustHash(hex string) protocol.ID {
	id, err := protocol.ParseID(hex)
	if err != nil {
		panic(err)
	}

	return id
}
