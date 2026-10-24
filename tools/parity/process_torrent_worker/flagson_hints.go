package main

// The hint corpora (M3): explicit torrent_hints rows as prod has them, written
// through Go's own model.TorrentHint, so an unenriched hint has languages '[]'
// and episodes SQL NULL, as the importer wrote every prod row. Prod
// (2026-10-23, read-only): 3,874,615 hints; the 825,878 sourced ones are all
// imdb and the 1,288,124 enriched ones carry video_* only. So imdb /find then
// details is the by-ID path and video_* the enrichment that carry the weight;
// tmdb, tvdb, episodes, release groups and languages are correctness slices.
//
//   - hint: sourced hints (by-ID attach, /find, pre-attach to an existing
//     association, a stale association replaced), plus an id without a source
//     and a sourced hint with enrichment;
//   - enriched: type-only hints with video_*, episodes, release groups and
//     languages, some reusing an existing association;
//   - conflict: a sourced hint that fetches content from TMDB in the job of a
//     torrent whose local search attaches the same content as seeded, so one
//     job holds two images of it (CONTRACT.md case E);
//   - ambiguous: torrents whose associations Go picks among by row order
//     (several reusable ones, or source-less ones of several types), and the
//     same shapes with one candidate.
//
// Every job holds torrents of one slice, and sliceExpect says what each worker
// generation does with a slice's jobs (plannedJob.Expect, keyed by flags_on.sh
// HINT_EXPECT):
//
//   - abstain: a worker before M3b, which processes a bare type-only hint only
//     (languages '[]' or NULL, episodes '{}' or NULL, nothing else) and
//     abstains from any other hinted job with reason hint;
//   - process: M3b, which processes sourced and enriched hints as Go does and
//     abstains only from values it cannot reproduce (hint), from row-order
//     ambiguities (ambiguous) and from conflicting images (conflict).
//
// Rust runs first and Go settles what it left, so a slice one generation
// processes and the other leaves must share no search or by-ID key with a
// slice treated the other way round: the replay-named slices (hint, enriched)
// are all left or all processed together, and every other slice is
// pseudo-titled, with keys no other torrent makes.

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
	"gorm.io/gorm/clause"
)

const (
	corpusEnriched  = "enriched"
	corpusConflict  = "conflict"
	corpusAmbiguous = "ambiguous"

	// HINT_EXPECT: the worker generation a Rust phase runs.
	hintExpectAbstain = "abstain"
	hintExpectProcess = "process"

	// Abstention reasons M3b adds (worker.rs AbstainReason).
	abstainAmbiguous = "ambiguous"
	abstainConflict  = "conflict"

	// The note kind of every torrent of the hint corpora.
	noteHint = "hint"
)

// Categories of the hint corpora's torrents (fixtureNote.Category).
const (
	catFindNew               = "find-new"
	catFindExisting          = "find-existing"
	catFindDetails404        = "find-details-404"
	catFindEmpty             = "find-empty"
	catFindWrongBucket       = "find-wrong-bucket"
	catLocalAlternative      = "local-alternative"
	catLocalAlternativeMulti = "local-alternative-multi"
	catAssociationImdb       = "association-imdb"
	catAssociationOther      = "association-other"
	catAssociationCanonical  = "association-canonical"
	catSourcedEnriched       = "sourced-enriched"
	catDirectNew             = "direct-new"
	catDirect404             = "direct-404"
	catLocalCanonical        = "local-canonical"
	catInvalidTMDBID         = "invalid-tmdb-id"
	catReuse                 = "reuse"
	catTvdbMovie             = "tvdb-movie"
	catIDWithoutSource       = "id-without-source"

	catVideo               = "video"
	catEpisodesSeason      = "episodes-season"
	catEpisodesMultiSeason = "episodes-multi-season"
	catEpisodesRange       = "episodes-range"
	catEpisodesMixed       = "episodes-mixed"
	catEpisodesEmpty       = "episodes-empty"
	catEpisodesJSONNull    = "episodes-json-null"
	catReleaseGroup        = "release-group"
	catReleaseGroupEmpty   = "release-group-empty"
	catLanguages           = "languages"
	catLanguagesJSONNull   = "languages-json-null"
	catLanguagesUnordered  = "languages-unordered"
	catAll                 = "all"
	catReuseOtherType      = "reuse-other-type"

	catBare              = "bare"
	catBareEpisodesEmpty = "bare-episodes-empty"
	catBareReuse         = "bare-reuse"
)

// Slices of the hint corpora; sliceHint (flagson.go) is the sourced one.
const (
	sliceEnriched         = "enriched"          // replay-named type-only enriched hints
	sliceHintBare         = "hint-bare"         // pseudo-titled bare type-only hints
	sliceHintRefused      = "hint-refused"      // pseudo-titled: values Go reads and M3b refuses
	sliceHintGoFail       = "hint-gofail"       // pseudo-titled: values Go's scan rejects, failing the job
	sliceConflict         = "conflict"          // two images of one content in a job
	sliceAmbiguous        = "ambiguous"         // associations Go picks among by row order
	sliceAmbiguityControl = "ambiguity-control" // the same shapes with one candidate
)

// sliceExpect is what each worker generation does with a slice's jobs: the
// abstention reason, or abstainNone for a job it processes.
var sliceExpect = map[string]map[string]string{
	sliceHint:             {hintExpectAbstain: abstainHint, hintExpectProcess: abstainNone},
	sliceEnriched:         {hintExpectAbstain: abstainHint, hintExpectProcess: abstainNone},
	sliceHintBare:         {hintExpectAbstain: abstainNone, hintExpectProcess: abstainNone},
	sliceHintRefused:      {hintExpectAbstain: abstainHint, hintExpectProcess: abstainHint},
	sliceHintGoFail:       {hintExpectAbstain: abstainHint, hintExpectProcess: abstainHint},
	sliceConflict:         {hintExpectAbstain: abstainHint, hintExpectProcess: abstainConflict},
	sliceAmbiguous:        {hintExpectAbstain: abstainNone, hintExpectProcess: abstainAmbiguous},
	sliceAmbiguityControl: {hintExpectAbstain: abstainNone, hintExpectProcess: abstainNone},
}

// hintRewrite stores a value model.TorrentHint never writes: a spelling Go's
// scan folds, or a shape its JSON decoder takes or rejects. A late one is a
// value Go's scan rejects; it goes in after the plan, which loads hints as Go
// does.
type hintRewrite struct {
	hash   protocol.ID
	column string
	value  string
	late   bool
}

// hintRewriteCast is the cast of every column a rewrite may set.
var hintRewriteCast = map[string]string{
	"content_type": "text", "content_id": "text", "languages": "jsonb", "episodes": "jsonb",
	"video_resolution": "text", "video_source": "text", "video_codec": "text", "video_3d": "text",
	"video_modifier": "text", "release_group": "text",
}

// addHinted queues a torrent and its explicit hint, which writeHints writes
// through GORM before the plan.
func (s *seedRun) addHinted(corpus, slice string, in input, h model.TorrentHint) (protocol.ID, error) {
	in.Hint = nil

	hash, err := s.addTorrent(corpus, slice, in)
	if err != nil {
		return hash, err
	}

	h.InfoHash, h.CreatedAt, h.UpdatedAt = hash, s.ts, s.ts
	s.hints = append(s.hints, h)

	return hash, nil
}

func (s *seedRun) rewrite(hash protocol.ID, column, value string, late bool) {
	s.rewrites = append(s.rewrites, hintRewrite{hash: hash, column: column, value: value, late: late})
}

func (s *seedRun) writeHints() error {
	if len(s.hints) > 0 {
		if err := s.db.WithContext(s.ctx).CreateInBatches(s.hints, 500).Error; err != nil {
			return err
		}

		s.hints = nil
	}

	return s.applyRewrites(false)
}

func (s *seedRun) applyRewrites(late bool) error {
	batch := &pgx.Batch{}

	for _, rw := range s.rewrites {
		if rw.late != late {
			continue
		}

		cast, ok := hintRewriteCast[rw.column]
		if !ok {
			return fmt.Errorf("hint rewrite of column %q", rw.column)
		}

		batch.Queue(fmt.Sprintf(`UPDATE torrent_hints SET %s = $2::%s WHERE info_hash = $1`, rw.column, cast),
			rw.hash[:], rw.value)
	}

	if batch.Len() == 0 {
		return nil
	}

	return s.conn.SendBatch(s.ctx, batch).Close()
}

// associate seeds an existing torrent_contents row of a torrent: one Go
// would have written, pointing at content (sourced) or at none (a
// rule-derived type).
func (s *seedRun) associate(hash protocol.ID, in input, ct model.ContentType, content *model.Content) {
	tc := model.TorrentContent{
		InfoHash:    hash,
		ContentType: model.NewNullContentType(ct),
		Size:        uint(in.Size),
		PublishedAt: s.ts,
		CreatedAt:   s.ts,
		UpdatedAt:   s.ts,
		Torrent:     model.Torrent{InfoHash: hash, Name: in.Name},
	}

	if content != nil {
		tc.ContentSource, tc.ContentID = model.NewNullString(content.Source), model.NewNullString(content.ID)
		tc.Content = *content
	}

	tc.UpdateTsv()
	tc.Torrent, tc.Content = model.Torrent{}, model.Content{}
	s.associations = append(s.associations, tc)
}

// insertAssociations writes the seeded associations, after their content.
func (s *seedRun) insertAssociations() error {
	if len(s.associations) == 0 {
		return nil
	}

	return s.db.WithContext(s.ctx).Omit(clause.Associations).CreateInBatches(s.associations, 500).Error
}

func (s *seedRun) associationIDs() []string {
	ids := make([]string, len(s.associations))
	for i, tc := range s.associations {
		ids[i] = tc.InferID()
	}

	sort.Strings(ids)

	return ids
}

// reverseByID sorts associations into descending id order. A torrent's ids
// first differ in a lowercase type name or a digit, so a bytewise and a
// linguistic collation order them alike.
func reverseByID(tcs []model.TorrentContent) {
	sort.Slice(tcs, func(i, j int) bool { return tcs[i].InferID() > tcs[j].InferID() })
}

// hintGroup is one slice's torrents, chunked into jobs of size.
type hintGroup struct {
	slice  string
	hashes []protocol.ID
	size   int
	mode   processor.ClassifyMode
}

// planHintedJobs chunks every group in info-hash order and spreads each
// group's jobs evenly through the queue: job k of a group of n sits at
// (2k+1)/2n, ties in the order the groups are given.
func planHintedJobs(groups []hintGroup) []jobSpec {
	type placed struct {
		num, den, group int
		spec            jobSpec
	}

	var all []placed

	for g, grp := range groups {
		hashes := append([]protocol.ID(nil), grp.hashes...)
		sortHashes(hashes)

		n := (len(hashes) + grp.size - 1) / grp.size
		for k := range n {
			all = append(all, placed{num: 2*k + 1, den: 2 * n, group: g, spec: jobSpec{
				slice: grp.slice, flagsOn: true, classifyMode: grp.mode,
				hashes: hashes[k*grp.size : min((k+1)*grp.size, len(hashes))],
				expect: sliceExpect[grp.slice],
			}})
		}
	}

	sort.SliceStable(all, func(i, j int) bool {
		a, b := all[i].num*all[j].den, all[j].num*all[i].den
		if a != b {
			return a < b
		}

		return all[i].group < all[j].group
	})

	specs := make([]jobSpec, len(all))
	for i, p := range all {
		specs[i] = p.spec
	}

	return specs
}

// pickWeighted picks a category by weight (out of 100).
func pickWeighted(r *rand.Rand, categories []weightedCategory) string {
	w := r.IntN(100)

	for _, c := range categories {
		if w < c.weight {
			return c.name
		}

		w -= c.weight
	}

	return categories[len(categories)-1].name
}

type weightedCategory struct {
	name   string
	weight int
}

// ---- enrichment ----

var (
	hintLanguages     = []string{"en", "fr", "de", "es", "it", "ja", "ko", "zh", "ru", "pt", "hi", "sv", "nl", "pl"}
	hintReleaseGroups = []string{"QZ", "FLUX", "NTb", "EVO", "SPARKS", "x0r", "YIFY", "RARBG"}
	videoColumns      = []string{"video_resolution", "video_source", "video_codec", "video_3d", "video_modifier"}
)

// enrichVideo sets each video field with probability p (at least one), in a
// spelling Go's scan folds for about one value in five (a rewrite).
func (s *seedRun) enrichVideo(hash protocol.ID, h *model.TorrentHint, r *rand.Rand, p int) {
	set := make([]bool, len(videoColumns))
	someSet := false

	for i := range set {
		set[i] = r.IntN(100) < p
		someSet = someSet || set[i]
	}

	if !someSet {
		set[r.IntN(len(set))] = true
	}

	for i, column := range videoColumns {
		if !set[i] {
			continue
		}

		var canonical string

		switch column {
		case "video_resolution":
			v := pick(r, model.VideoResolutionValues())
			h.VideoResolution, canonical = model.NullVideoResolution{VideoResolution: v, Valid: true}, v.String()
		case "video_source":
			v := pick(r, model.VideoSourceValues())
			h.VideoSource, canonical = model.NullVideoSource{VideoSource: v, Valid: true}, v.String()
		case "video_codec":
			v := pick(r, model.VideoCodecValues())
			h.VideoCodec, canonical = model.NullVideoCodec{VideoCodec: v, Valid: true}, v.String()
		case "video_3d":
			v := pick(r, model.Video3DValues())
			h.Video3D, canonical = model.NullVideo3D{Video3D: v, Valid: true}, v.String()
		default:
			v := pick(r, model.VideoModifierValues())
			h.VideoModifier, canonical = model.NullVideoModifier{VideoModifier: v, Valid: true}, v.String()
		}

		if spelling := foldedSpelling(canonical, r.IntN(20)); spelling != "" {
			s.rewrite(hash, column, spelling, false)
		}
	}
}

// foldedSpelling is a spelling of canonical that go-enum's case-insensitive
// parse reads back as canonical: lower case for v < 3, upper case for v == 3,
// "" (none) otherwise or when the case is canonical already.
func foldedSpelling(canonical string, v int) string {
	var spelling string

	switch {
	case v < 3:
		spelling = strings.ToLower(canonical)
	case v == 3:
		spelling = strings.ToUpper(canonical)
	}

	if spelling == canonical {
		return ""
	}

	return spelling
}

func hintLanguageSet(r *rand.Rand, n int) model.Languages {
	langs := model.Languages{}

	for len(langs) < n {
		lang := model.ParseLanguage(pick(r, hintLanguages))
		if lang.Valid {
			langs[lang.Language] = struct{}{}
		}
	}

	return langs
}

func hintReleaseGroup(r *rand.Rand) model.NullString {
	return model.NewNullString(pick(r, hintReleaseGroups))
}

// hintEpisodes builds the episodes of one shape (Go's encoder writes them
// with canonical keys and {} values).
func hintEpisodes(r *rand.Rand, shape string) model.Episodes {
	e := model.Episodes{}
	season := 1 + r.IntN(12)

	switch shape {
	case catEpisodesSeason:
		e.AddSeason(season)
	case catEpisodesMultiSeason:
		for k := range 2 + r.IntN(2) {
			e.AddSeason(season + k)
		}
	case catEpisodesRange:
		first := 1 + r.IntN(20)
		for k := range 2 + r.IntN(5) {
			e.AddEpisode(season, first+k)
		}
	case catEpisodesMixed:
		first := 1 + r.IntN(20)
		e.AddEpisode(season, first)
		e.AddEpisode(season, first+3)
		e.AddSeason(season + 1)
	}

	return e
}

// ---- hint corpus ----

// hintCategories, with weights out of 100, of the replay-named sourced hints.
// catReuse hints content an earlier hint job fetched from TMDB, so it is found
// locally by then; the catAssociation* ones seed an existing association.
var hintCategories = []weightedCategory{
	{catFindNew, 34},
	{catFindExisting, 9},
	{catFindDetails404, 5},
	{catFindEmpty, 4},
	{catFindWrongBucket, 2},
	{catLocalAlternative, 5},
	{catLocalAlternativeMulti, 2},
	{catAssociationImdb, 8},  // the association's content carries the hinted imdb id: re-attached locally by ID
	{catAssociationOther, 5}, // the association is other content: replaced, the stale row deleted
	{catSourcedEnriched, 6},
	{catDirectNew, 4},
	{catDirect404, 2},
	{catLocalCanonical, 3},
	{catAssociationCanonical, 3}, // the association is the hinted content: pre-attached, no I/O
	{catInvalidTMDBID, 1},
	{catReuse, 2},
	{catTvdbMovie, 2},       // unmatched by ID without a request
	{catIDWithoutSource, 3}, // type-only for Go
}

// refusedSourced are sourced hints Go reads and M3b refuses: ids a TMDB path
// would not carry as they stand, an empty id, and a content type in a
// spelling Go's scan folds (with a video field, so a worker before M3b leaves
// it too).
var refusedSourced = []struct {
	variant, source, id, contentType string
}{
	{"imdb-empty-id", model.SourceImdb, "", ""},
	{"tmdb-empty-id", model.SourceTmdb, "", ""},
	{"imdb-slash", model.SourceImdb, "tt%d/1", ""},
	{"imdb-dot-dot", model.SourceImdb, "..", ""},
	{"imdb-leading-dot", model.SourceImdb, ".tt%d", ""},
	{"imdb-escape", model.SourceImdb, "tt%d%%2F1", ""},
	{"imdb-space", model.SourceImdb, "tt%d 1", ""},
	{"imdb-query", model.SourceImdb, "tt%d?x=1", ""},
	{"imdb-fragment", model.SourceImdb, "tt%d#a", ""},
	{"imdb-non-ascii", model.SourceImdb, "tt%dé", ""},
	{"content-type-case", model.SourceImdb, "tt%d", "MOVIE"},
	{"content-type-title-case", model.SourceImdb, "tt%d", "Movie"},
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

	sourced := make([]protocol.ID, 0, len(replay)+4)

	// The synthetic corpus's own sourced hints, as they are.
	for _, in := range synthetic {
		if in.Hint == nil || in.Hint.ContentSource == "" {
			continue
		}

		ct, err := model.ParseContentType(in.Hint.ContentType)
		if err != nil {
			return err
		}

		hash, err := s.addHinted("synthetic", sliceHint, in, model.TorrentHint{
			ContentType:   ct,
			ContentSource: model.NewNullString(in.Hint.ContentSource),
			ContentID:     model.NewNullString(in.Hint.ContentID),
		})
		if err != nil {
			return err
		}

		sourced = append(sourced, hash)
		s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: "synthetic"})
	}

	var fetched []model.ContentRef // TMDB content earlier hints create, for catReuse

	for i, in := range replay {
		hash, err := s.replayHint(i, in, &fetched)
		if err != nil {
			return err
		}

		sourced = append(sourced, hash)
	}

	refused := make([]protocol.ID, 0, 5*len(refusedSourced))

	for i := range 5 * len(refusedSourced) {
		v := refusedSourced[i%len(refusedSourced)]
		in := pseudoInput(2 * i) // a movie name
		h := model.TorrentHint{
			ContentType:   model.ContentTypeMovie,
			ContentSource: model.NewNullString(v.source),
			ContentID:     model.NewNullString(v.id),
		}

		if strings.Contains(v.id, "%") {
			h.ContentID = model.NewNullString(fmt.Sprintf(v.id, 7000000+i))
		}

		if v.contentType != "" {
			h.VideoResolution = model.NullVideoResolution{VideoResolution: model.VideoResolutionV1080p, Valid: true}
		}

		hash, err := s.addHinted(corpusHint+"-refused", sliceHintRefused, in, h)
		if err != nil {
			return err
		}

		if v.contentType != "" {
			s.rewrite(hash, "content_type", v.contentType, false)
		}

		refused = append(refused, hash)
		s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: "refused", Variant: v.variant, ID: v.source + ":" + h.ContentID.String})
	}

	// A content type Go's scan rejects: Go fails the job. Its error text is
	// stable only because no hint in this corpus has languages or episodes
	// Go fails to decode (see goFailEnriched).
	gofail := make([]protocol.ID, 0, 3)

	for i := range 3 {
		in := pseudoInput(2*(5*len(refusedSourced)+i) + 1000)
		hash, err := s.addHinted(corpusHint+"-gofail", sliceHintGoFail, in, model.TorrentHint{
			ContentType:     model.ContentTypeMovie,
			ContentSource:   model.NewNullString(model.SourceImdb),
			ContentID:       model.NewNullString(fmt.Sprintf("tt%d", 7100000+i)),
			VideoResolution: model.NullVideoResolution{VideoResolution: model.VideoResolutionV720p, Valid: true},
		})
		if err != nil {
			return err
		}

		s.rewrite(hash, "content_type", "film", true)
		gofail = append(gofail, hash)
		s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: "gofail", Variant: "content-type-unknown"})
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	specs := planHintedJobs([]hintGroup{
		{slice: sliceHint, hashes: sourced, size: o.jobSize},
		{slice: sliceHintRefused, hashes: refused, size: 4},
		{slice: sliceHintGoFail, hashes: gofail, size: 1},
	})

	_, err = s.finish(labelPlanned(planned, specs), digest, specs, nil)

	return err
}

// replayHint seeds one replay-named sourced hint of a category, with its TMDB
// fixtures, local content and association.
func (s *seedRun) replayHint(i int, in input, fetched *[]model.ContentRef) (protocol.ID, error) {
	r := fixtureRand("hint/" + in.ID)

	ct := model.ContentTypeMovie

	switch v := r.IntN(10); {
	case v < 4:
		ct = model.ContentTypeTvShow
	case v == 4:
		ct = model.ContentTypeXxx
	}

	cat := pickWeighted(r, hintCategories)

	switch {
	case cat == catReuse && len(*fetched) == 0:
		cat = catFindNew
	case cat == catTvdbMovie:
		ct = model.ContentTypeMovie
	}

	tv := ct == model.ContentTypeTvShow
	hint := model.TorrentHint{ContentType: ct}
	tmdbID := s.allocTMDBID()
	imdbID := fmt.Sprintf("tt%07d", 5000000+i*3)
	external, externalSource := imdbID, "imdb_id"

	if tv && cat != catAssociationImdb && r.IntN(6) == 0 {
		external, externalSource = strconv.Itoa(400000+i*7), "tvdb_id"
	}

	setHint := func(source, id string) {
		hint.ContentSource, hint.ContentID = model.NewNullString(source), model.NewNullString(id)
	}

	hintExternal := func() {
		if externalSource == "tvdb_id" {
			setHint(model.SourceTvdb, external)
		} else {
			setHint(model.SourceImdb, external)
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

	findNew := func() error {
		hintExternal()

		if err := find([]searchItem{{id: tmdbID, title: "Found " + external, year: 2001}}, tv); err != nil {
			return err
		}

		*fetched = append(*fetched, model.ContentRef{Type: ct, Source: model.SourceTmdb, ID: strconv.FormatInt(tmdbID, 10)})

		return details(tmdbID)
	}

	var (
		err        error
		assoc      *model.Content
		assocType  = ct
		enrichment bool
	)

	switch cat {
	case catLocalCanonical, catAssociationCanonical:
		setHint(model.SourceTmdb, strconv.FormatInt(tmdbID, 10))

		title := fmt.Sprintf("Local canonical %d", i)
		if cat == catAssociationCanonical {
			title = fmt.Sprintf("Qz %x Association", tmdbID)
		}

		sc := s.newLocalContent(ct, model.SourceTmdb, hint.ContentID.String, title, 0, r)
		s.contents = append(s.contents, sc)

		if cat == catAssociationCanonical {
			assoc = &sc.content
		}
	case catLocalAlternative, catLocalAlternativeMulti:
		hintExternal()

		copies := 1
		if cat == catLocalAlternativeMulti {
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
			sc.withExternalID(hint.ContentSource.String, hint.ContentID.String)
			s.contents = append(s.contents, sc)
		}
	case catAssociationImdb:
		setHint(model.SourceImdb, imdbID)

		sc := s.newLocalContent(ct, model.SourceTmdb, strconv.FormatInt(tmdbID, 10), fmt.Sprintf("Qz %x Association", tmdbID), 0, r)
		sc.withExternalID(model.SourceImdb, imdbID)
		s.contents = append(s.contents, sc)
		assoc = &sc.content
	case catAssociationOther:
		if r.IntN(3) == 0 {
			setHint(model.SourceTmdb, strconv.FormatInt(tmdbID, 10))
			err = details(tmdbID)
		} else {
			err = findNew()
		}

		if r.IntN(3) == 0 {
			assocType = model.ContentTypeMovie
			if ct == model.ContentTypeMovie {
				assocType = model.ContentTypeTvShow
			}
		}

		n := s.allocLocalID()
		sc := s.newLocalContent(assocType, model.SourceTmdb, strconv.FormatInt(n, 10), fmt.Sprintf("Qz %x Stale", n), 0, r)
		s.contents = append(s.contents, sc)
		assoc = &sc.content
	case catSourcedEnriched:
		err = findNew()
		enrichment = true
	case catDirectNew:
		setHint(model.SourceTmdb, strconv.FormatInt(tmdbID, 10))
		err = details(tmdbID)
		*fetched = append(*fetched, model.ContentRef{Type: ct, Source: model.SourceTmdb, ID: hint.ContentID.String})
	case catDirect404:
		setHint(model.SourceTmdb, strconv.FormatInt(tmdbID, 10))
	case catFindNew:
		err = findNew()
	case catFindExisting:
		hintExternal()

		if err = find([]searchItem{{id: tmdbID, title: "Found " + external, year: 2001}}, tv); err == nil {
			err = details(tmdbID)
		}

		s.contents = append(s.contents, s.newLocalContent(ct, model.SourceTmdb,
			strconv.FormatInt(tmdbID, 10), fmt.Sprintf("Qz %x Placeholder", tmdbID), 0, r))
	case catFindEmpty:
		hintExternal()
	case catFindDetails404:
		hintExternal()
		err = find([]searchItem{{id: tmdbID, title: "Found " + external, year: 2002}}, tv)
	case catFindWrongBucket:
		hintExternal()
		err = find([]searchItem{{id: tmdbID, title: "Wrong bucket " + external}}, !tv)
	case catInvalidTMDBID:
		setHint(model.SourceTmdb, fmt.Sprintf("x%d", tmdbID))
	case catReuse:
		ref := (*fetched)[r.IntN(len(*fetched))]
		hint.ContentType = ref.Type
		setHint(ref.Source, ref.ID)
	case catTvdbMovie:
		setHint(model.SourceTvdb, strconv.Itoa(400000+i*7))
	case catIDWithoutSource:
		hint.ContentID = model.NewNullString(imdbID)
	default:
		err = fmt.Errorf("hint category %q", cat)
	}

	if err != nil {
		return protocol.ID{}, err
	}

	hash, err := s.addHinted(corpusHint, sliceHint, in, hint)
	if err != nil {
		return hash, err
	}

	if enrichment {
		h := &s.hints[len(s.hints)-1]
		s.enrichVideo(hash, h, r, 40)

		if r.IntN(4) == 0 {
			h.ReleaseGroup = hintReleaseGroup(r)
		}
	}

	if assoc != nil {
		s.associate(hash, in, assocType, assoc)
	}

	s.note(fixtureNote{
		Kind: noteHint, Key: hash.String(), Category: cat,
		ID: hint.ContentSource.String + ":" + hint.ContentID.String,
	})

	return hash, nil
}

// withExternalID gives the content the external id a hint names, first in
// insertion order, instead of any it had from that source.
func (sc *seededContent) withExternalID(source, id string) {
	sc.withoutAttribute(source, "id")
	sc.content.Attributes = append(sc.content.Attributes, model.ContentAttribute{
		ContentType: sc.content.Type, ContentSource: sc.content.Source, ContentID: sc.content.ID,
		Source: source, Key: "id", Value: id,
	})
	sc.attributeOrder = append([]int{len(sc.content.Attributes) - 1}, sc.attributeOrder...)
	sc.content.UpdateTsv()
}

// ---- enriched corpus ----

// enrichedCategories, with weights out of 100, of the replay-named type-only
// enriched hints. Prod's carry video_* only, so those carry the weight.
var enrichedCategories = []weightedCategory{
	{catVideo, 58},
	{catEpisodesSeason, 4},
	{catEpisodesMultiSeason, 4},
	{catEpisodesRange, 4},
	{catEpisodesMixed, 3},
	{catEpisodesEmpty, 3},    // '{}' with a video field: not applied
	{catEpisodesJSONNull, 1}, // jsonb null with a video field
	{catReleaseGroup, 3},
	{catReleaseGroupEmpty, 3},
	{catLanguages, 4},
	{catLanguagesJSONNull, 1},  // jsonb null with a video field
	{catLanguagesUnordered, 1}, // Go-read alpha-2 arrays GORM never writes: out of order, repeated
	{catAll, 5},                // video, episodes, release group and languages
	{catReuse, 4},              // a sourced association of the hint's type: reused, the hint's attributes kept
	{catReuseOtherType, 2},     // an association of another type: not reused, deleted
}

// refusedEnriched are type-only hints Go reads and M3b refuses (rewrites).
var refusedEnriched = []struct {
	variant, column, value string
}{
	{"languages-alpha-3", "languages", `["eng"]`},
	{"languages-name", "languages", `["English"]`},
	{"languages-upper-case", "languages", `["EN"]`},
	{"episodes-zero-padded-season", "episodes", `{"01": {}}`},
	{"episodes-zero-padded-episode", "episodes", `{"1": {"02": {}}}`},
	{"episodes-null-season", "episodes", `{"1": null}`},
	{"episodes-null-episode", "episodes", `{"1": {"2": null}}`},
	{"episodes-above-u16", "episodes", `{"70000": {}}`},
	{"episodes-negative", "episodes", `{"-1": {}}`},
	{"episodes-plus-sign", "episodes", `{"+1": {}}`},
	{"video-source-kelvin-sign", "video_source", "WORKPRINT"},
}

// goFailEnriched are values Go's scan rejects: Go fails the job (late
// rewrites, each over a video field so a worker before M3b leaves it too).
//
// No unknown content_type here (the hint corpus has it): a scan that fails
// at content_type never assigns languages and episodes, and GORM then
// decodes whatever its pooled serializer last held (gorm scan.go
// scanIntoStruct; serializer.Scan keeps s.value across Put). After the
// languages and episodes failures below, that adds one of their errors to
// Go's job error text, or not, by sync.Pool reuse.
var goFailEnriched = []struct {
	variant, column, value string
}{
	{"video-resolution-unknown", "video_resolution", "V999p"},
	{"video-source-unknown", "video_source", "HDCAM"},
	{"video-codec-unknown", "video_codec", "h.264"},
	{"video-3d-unknown", "video_3d", "3d-sbs"},
	{"video-modifier-unknown", "video_modifier", "DIRCUT"},
	{"languages-unknown", "languages", `["zz"]`},
	{"languages-object", "languages", `{}`},
	{"languages-string", "languages", `"en"`},
	{"episodes-non-numeric", "episodes", `{"a": {}}`},
	{"episodes-number-value", "episodes", `{"1": {"2": 1}}`},
	{"episodes-array-value", "episodes", `{"1": []}`},
	{"episodes-array", "episodes", `[]`},
}

func (s *seedRun) seedEnriched() error {
	o := s.opts

	replay, err := readJSONL[input](filepath.Join(o.root, "testdata/parity/classifier-replay/inputs.jsonl"), o.enrichedCount)
	if err != nil {
		return err
	}

	enriched := make([]protocol.ID, 0, len(replay))

	for _, in := range replay {
		hash, err := s.replayEnriched(in)
		if err != nil {
			return err
		}

		enriched = append(enriched, hash)
	}

	// Bare type-only hints, as prod's most common hint: every generation
	// processes them.
	bare := make([]protocol.ID, 0, 250)

	for i := range 250 {
		in := pseudoInput(i)
		ct := model.ContentTypeMovie

		if i%2 == 1 {
			ct = model.ContentTypeTvShow
		}

		variant := catBare
		h := model.TorrentHint{ContentType: ct}

		switch {
		case i >= 200:
			variant = catBareReuse
		case i >= 150:
			variant, h.Episodes = catBareEpisodesEmpty, model.Episodes{}
		}

		hash, err := s.addHinted(corpusEnriched+"-bare", sliceHintBare, in, h)
		if err != nil {
			return err
		}

		if variant == catBareReuse {
			r := fixtureRand("bare/" + in.ID)
			id := s.allocTMDBID()
			sc := s.newLocalContent(ct, model.SourceTmdb, strconv.FormatInt(id, 10), fmt.Sprintf("Qz %x Association", id), 0, r)
			s.contents = append(s.contents, sc)
			s.associate(hash, in, ct, &sc.content)
		}

		bare = append(bare, hash)
		s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: variant})
	}

	refused := make([]protocol.ID, 0, 3*len(refusedEnriched))

	for i := range 3 * len(refusedEnriched) {
		v := refusedEnriched[i%len(refusedEnriched)]
		in := pseudoInput(400 + i)

		ct := model.ContentTypeMovie
		if i%2 == 1 {
			ct = model.ContentTypeTvShow
		}

		hash, err := s.addHinted(corpusEnriched+"-refused", sliceHintRefused, in, model.TorrentHint{ContentType: ct})
		if err != nil {
			return err
		}

		s.rewrite(hash, v.column, v.value, false)
		refused = append(refused, hash)
		s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: "refused", Variant: v.variant})
	}

	gofail := make([]protocol.ID, 0, len(goFailEnriched))

	for i, v := range goFailEnriched {
		in := pseudoInput(500 + i)
		hash, err := s.addHinted(corpusEnriched+"-gofail", sliceHintGoFail, in, model.TorrentHint{
			ContentType:     model.ContentTypeMovie,
			VideoResolution: model.NullVideoResolution{VideoResolution: model.VideoResolutionV720p, Valid: true},
		})
		if err != nil {
			return err
		}

		s.rewrite(hash, v.column, v.value, true)
		gofail = append(gofail, hash)
		s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: "gofail", Variant: v.variant})
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	if err := s.searchFixtures(planned, searchWeights{local: 25, tmdbNew: 42, tmdbExisting: 20, details404: 12, decoys: 9}); err != nil {
		return err
	}

	specs := planHintedJobs([]hintGroup{
		{slice: sliceEnriched, hashes: enriched, size: o.jobSize},
		{slice: sliceHintBare, hashes: bare, size: 10},
		{slice: sliceHintRefused, hashes: refused, size: 3},
		{slice: sliceHintGoFail, hashes: gofail, size: 1},
	})

	_, err = s.finish(labelPlanned(planned, specs), digest, specs, nil)

	return err
}

// replayEnriched seeds one replay-named type-only enriched hint of a category.
func (s *seedRun) replayEnriched(in input) (protocol.ID, error) {
	r := fixtureRand("enriched/" + in.ID)
	cat := pickWeighted(r, enrichedCategories)

	ct := model.ContentTypeMovie

	switch v := r.IntN(20); {
	case strings.HasPrefix(cat, "episodes-") && v < 17:
		ct = model.ContentTypeTvShow
	case strings.HasPrefix(cat, "episodes-"):
	case strings.HasPrefix(cat, catReuse):
		if v < 10 {
			ct = model.ContentTypeTvShow
		}
	case v < 8:
		ct = model.ContentTypeTvShow
	case v < 10:
		ct = model.ContentTypeXxx
	}

	hash, err := s.addHinted(corpusEnriched, sliceEnriched, in, model.TorrentHint{ContentType: ct})
	if err != nil {
		return hash, err
	}

	h := &s.hints[len(s.hints)-1]
	variant := ""

	switch cat {
	case catVideo:
		s.enrichVideo(hash, h, r, 55)
	case catEpisodesSeason, catEpisodesMultiSeason, catEpisodesRange, catEpisodesMixed:
		h.Episodes = hintEpisodes(r, cat)
	case catEpisodesEmpty:
		h.Episodes = model.Episodes{}
		s.enrichVideo(hash, h, r, 0)
	case catEpisodesJSONNull:
		s.enrichVideo(hash, h, r, 0)
		s.rewrite(hash, "episodes", "null", false)
	case catReleaseGroup:
		h.ReleaseGroup = hintReleaseGroup(r)
	case catReleaseGroupEmpty:
		h.ReleaseGroup = model.NullString{Valid: true}
	case catLanguages:
		h.Languages = hintLanguageSet(r, 1+r.IntN(3))
	case catLanguagesJSONNull:
		s.enrichVideo(hash, h, r, 0)
		s.rewrite(hash, "languages", "null", false)
	case catLanguagesUnordered:
		langs := hintLanguageSet(r, 2+r.IntN(2)).Slice()
		codes := make([]string, 0, len(langs)+1)

		for k := len(langs) - 1; k >= 0; k-- {
			codes = append(codes, `"`+langs[k].String()+`"`)
		}

		codes = append(codes, codes[0])
		variant = "[" + strings.Join(codes, ",") + "]"
		s.rewrite(hash, "languages", variant, false)
	case catAll:
		s.enrichVideo(hash, h, r, 50)
		h.Episodes = hintEpisodes(r, pick(r, []string{catEpisodesSeason, catEpisodesRange}))
		h.ReleaseGroup = hintReleaseGroup(r)
		h.Languages = hintLanguageSet(r, 1+r.IntN(2))
	case catReuse, catReuseOtherType:
		s.enrichVideo(hash, h, r, 50)

		assocType := ct
		if cat == catReuseOtherType {
			assocType = model.ContentTypeMovie
			if ct == model.ContentTypeMovie {
				assocType = model.ContentTypeTvShow
			}
		}

		id := s.allocTMDBID()
		sc := s.newLocalContent(assocType, model.SourceTmdb, strconv.FormatInt(id, 10), fmt.Sprintf("Qz %x Association", id), 0, r)
		s.contents = append(s.contents, sc)
		s.associate(hash, in, assocType, &sc.content)
	default:
		return hash, fmt.Errorf("enriched category %q", cat)
	}

	s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: cat, Variant: variant})

	return hash, nil
}

// ---- conflict corpus ----

// seedConflict: jobs of four pseudo-titled torrents. A movie with an imdb
// hint that no local content carries, which Go fetches from TMDB (/find,
// then details: a new image of content X), and a movie whose local search
// finds X as seeded (the seeded image), plus two torrents with I/O of their
// own. Go upserts the fetched image and gives each association the tsv of its
// own copy; Rust refuses the job (ConflictingAttachedContent).
func (s *seedRun) seedConflict() error {
	type pair struct {
		hinted, searcher protocol.ID
		tmdbID           int64
		r                *rand.Rand
	}

	pairs := make([]pair, 0, s.opts.conflictCount)
	jobs := make([][]protocol.ID, 0, s.opts.conflictCount)

	for i := range s.opts.conflictCount {
		r := fixtureRand(fmt.Sprintf("conflict/%d", i))
		tmdbID := s.allocTMDBID()
		imdbID := fmt.Sprintf("tt%07d", 6000000+i)

		hinted, err := s.addHinted(corpusConflict, sliceConflict, pseudoInput(4*i), model.TorrentHint{
			ContentType:   model.ContentTypeMovie,
			ContentSource: model.NewNullString(model.SourceImdb),
			ContentID:     model.NewNullString(imdbID),
		})
		if err != nil {
			return err
		}

		if err := s.respond(findKey(imdbID, "imdb_id"), 200, buildFindBody(s.tpl, r, false,
			[]searchItem{{id: tmdbID, title: "Conflict " + imdbID, year: 2003}})); err != nil {
			return err
		}

		if err := s.respond(detailsKey(false, tmdbID), 200, buildDetailsBody(s.tpl, r, detailsSpec{
			id: tmdbID, title: fmt.Sprintf("Conflict TMDB %d", tmdbID), year: 2003, imdbID: imdbID,
		})); err != nil {
			return err
		}

		job := []protocol.ID{hinted}

		// The searcher (an even index: a movie name), then the other two.
		for _, n := range []int{4*i + 2, 4*i + 1, 4*i + 3} {
			h, err := s.addTorrent(corpusConflict, sliceConflict, pseudoInput(n))
			if err != nil {
				return err
			}

			job = append(job, h)
		}

		pairs = append(pairs, pair{hinted: hinted, searcher: job[1], tmdbID: tmdbID, r: r})
		jobs = append(jobs, job)
		s.note(fixtureNote{Kind: noteHint, Key: hinted.String(), Category: "conflict-hinted", ID: "imdb:" + imdbID})
		s.note(fixtureNote{Kind: noteHint, Key: job[1].String(), Category: "conflict-searcher", ID: "tmdb:" + strconv.FormatInt(tmdbID, 10)})
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	plannedBy := map[string]plannedTorrent{}
	for _, p := range planned {
		plannedBy[p.Hash] = p
	}

	searchers := map[string]bool{}

	for _, p := range pairs {
		sp := plannedBy[p.searcher.String()]
		if _, ok := localKeyOf(sp); !ok || sp.Type != string(model.ContentTypeMovie) {
			return fmt.Errorf("conflict: searcher %s makes no movie local search (%+v)", p.searcher, sp)
		}

		searchers[sp.Hash] = true
		s.contents = append(s.contents, s.newLocalContent(model.ContentTypeMovie, model.SourceTmdb,
			strconv.FormatInt(p.tmdbID, 10), sp.BaseTitle, sp.Year, p.r))
	}

	// The other two torrents' searches are answered as the main corpus's.
	var others []plannedTorrent

	for _, p := range planned {
		if !searchers[p.Hash] {
			others = append(others, p)
		}
	}

	if err := s.searchFixtures(others, searchWeights{local: 25, tmdbNew: 42, tmdbExisting: 20, details404: 12, decoys: 9}); err != nil {
		return err
	}

	specs := make([]jobSpec, len(jobs))
	for i, job := range jobs {
		specs[i] = jobSpec{slice: sliceConflict, flagsOn: true, hashes: job, expect: sliceExpect[sliceConflict]}
	}

	_, err = s.finish(labelPlanned(planned, specs), digest, specs, nil)

	return err
}

// ---- ambiguous corpus ----

// assocShape is one seeded association: a type, sourced (pointing at seeded
// tmdb content) or not (a rule-derived type).
type assocShape struct {
	ct      model.ContentType
	sourced bool
}

// ambiguityShape is one slice of the ambiguous corpus: pseudo-titled torrents
// with these associations, a bare movie hint or none, and a file list or none.
type ambiguityShape struct {
	slice, name string
	hint        bool
	noFiles     bool
	rematch     bool
	assocs      []assocShape
}

var (
	movieTMDB = assocShape{model.ContentTypeMovie, true}
	tvTMDB    = assocShape{model.ContentTypeTvShow, true}
	xxxRule   = assocShape{model.ContentTypeXxx, false}
	movieRule = assocShape{model.ContentTypeMovie, false}
	musicRule = assocShape{model.ContentTypeMusic, false}
)

var ambiguityShapes = []ambiguityShape{
	{slice: sliceAmbiguous, name: "reuse-two", assocs: []assocShape{movieTMDB, movieTMDB}},
	{slice: sliceAmbiguous, name: "reuse-two-types", assocs: []assocShape{movieTMDB, tvTMDB}},
	{slice: sliceAmbiguous, name: "reuse-two-hinted", hint: true, assocs: []assocShape{movieTMDB, movieTMDB, tvTMDB}},
	{slice: sliceAmbiguous, name: "preserved-two-types", noFiles: true, assocs: []assocShape{xxxRule, movieRule}},
	{slice: sliceAmbiguityControl, name: "reuse-one-eligible", hint: true, assocs: []assocShape{movieTMDB, tvTMDB}},
	{slice: sliceAmbiguityControl, name: "reuse-wins", noFiles: true, assocs: []assocShape{movieTMDB, xxxRule, musicRule}},
	{slice: sliceAmbiguityControl, name: "preserved-with-files", assocs: []assocShape{xxxRule, movieRule}},
	{slice: sliceAmbiguityControl, name: "preserved-one", noFiles: true, assocs: []assocShape{xxxRule}},
	{slice: sliceAmbiguityControl, name: "rematch-two", rematch: true, assocs: []assocShape{movieTMDB, movieTMDB}},
}

func (s *seedRun) seedAmbiguous() error {
	groups := make([]hintGroup, 0, len(ambiguityShapes))
	next := 0

	for _, shape := range ambiguityShapes {
		g := hintGroup{slice: shape.slice, size: 10}
		if shape.rematch {
			g.mode = processor.ClassifyModeRematch
		}

		for range s.opts.ambiguousCount {
			in := pseudoInput(next)
			r := fixtureRand(fmt.Sprintf("ambiguous/%d", next))
			next++

			if shape.noFiles {
				in.FilesStatus, in.FilesCount, in.Files = "no_info", nil, nil
			}

			var (
				hash protocol.ID
				err  error
			)

			if shape.hint {
				hash, err = s.addHinted(corpusAmbiguous, shape.slice, in, model.TorrentHint{ContentType: model.ContentTypeMovie})
			} else {
				hash, err = s.addTorrent(corpusAmbiguous, shape.slice, in)
			}

			if err != nil {
				return err
			}

			first := len(s.associations)

			for _, a := range shape.assocs {
				if !a.sourced {
					s.associate(hash, in, a.ct, nil)
					continue
				}

				id := s.allocTMDBID()
				sc := s.newLocalContent(a.ct, model.SourceTmdb, strconv.FormatInt(id, 10), fmt.Sprintf("Qz %x Association", id), 0, r)
				s.contents = append(s.contents, sc)
				s.associate(hash, in, a.ct, &sc.content)
			}

			// Physically, the associations go in the reverse of their id
			// order, so Go's first (heap order) is never the first by id.
			reverseByID(s.associations[first:])

			g.hashes = append(g.hashes, hash)
			s.note(fixtureNote{Kind: noteHint, Key: hash.String(), Category: shape.name})
		}

		groups = append(groups, g)
	}

	planned, digest, err := s.plan()
	if err != nil {
		return err
	}

	if err := s.searchFixtures(planned, searchWeights{local: 25, tmdbNew: 42, tmdbExisting: 20, details404: 12, decoys: 9}); err != nil {
		return err
	}

	specs := planHintedJobs(groups)

	_, err = s.finish(labelPlanned(planned, specs), digest, specs, nil)

	return err
}

// errNoHintExpect: a job without an expectation for a HINT_EXPECT mode.
var errNoHintExpect = errors.New("job has no expectation for this HINT_EXPECT")

// hintExpectations maps each job that a worker generation leaves to its
// abstention reason (abstainNone jobs are processed and left out).
func hintExpectations(jobs []plannedJob, mode string) (map[string]string, error) {
	if mode != hintExpectAbstain && mode != hintExpectProcess {
		return nil, fmt.Errorf("HINT_EXPECT %q: want %s or %s", mode, hintExpectAbstain, hintExpectProcess)
	}

	left := map[string]string{}

	for _, j := range jobs {
		reason, ok := j.Expect[mode]
		if !ok {
			return nil, fmt.Errorf("job %d (%s) of slice %s: %w", j.Index, j.Fingerprint, j.Slice, errNoHintExpect)
		}

		if reason != abstainNone {
			left[j.Fingerprint] = reason
		}
	}

	return left, nil
}
