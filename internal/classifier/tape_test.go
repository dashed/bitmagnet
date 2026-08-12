package classifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	tmdb_mocks "github.com/bitmagnet-io/bitmagnet/internal/tmdb/mocks"
	"github.com/go-resty/resty/v2"
)

const testDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

var testFlagsOn = Flags{
	"local_search_enabled": true,
	"apis_enabled":         true,
	"tmdb_enabled":         true,
}

var testFlagsOff = Flags{
	"local_search_enabled": false,
	"apis_enabled":         false,
	"tmdb_enabled":         false,
}

// stubContentSearch stands in for the database below the tape seam, so a
// recording exercises the real localSearch code path -- option construction,
// levenshtein selection, ErrUnmatched mapping -- and only the rows are canned.
type stubContentSearch struct {
	results []search.ContentResult
	err     error

	mu    sync.Mutex
	calls int
}

func (s *stubContentSearch) Content(
	_ context.Context,
	_ ...query.Option,
) (search.ContentResult, error) {
	if s.err != nil {
		return search.ContentResult{}, s.err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	call := s.calls
	s.calls++

	if call >= len(s.results) {
		return search.ContentResult{}, nil
	}

	return s.results[call], nil
}

// forbiddenContentSearch fails the test if the database is consulted. A replay
// that reaches it has fallen off the tape.
type forbiddenContentSearch struct{ t *testing.T }

func (f forbiddenContentSearch) Content(
	_ context.Context,
	_ ...query.Option,
) (search.ContentResult, error) {
	f.t.Helper()
	f.t.Fatal("replay consulted the live content search")

	return search.ContentResult{}, nil
}

// stubRequester stands in for the TMDB HTTP API below the tape seam.
type stubRequester struct {
	bodies map[string][]byte
	err    error
}

func (s stubRequester) Request(
	_ context.Context,
	path string,
	_ map[string]string,
	result any,
) (*resty.Response, error) {
	if s.err != nil {
		return nil, s.err
	}

	body, ok := s.bodies[path]
	if !ok {
		return nil, tmdb.ErrNotFound
	}

	if result != nil {
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
	}

	return (&resty.Response{
		RawResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
	}).SetBody(body), nil
}

// forbiddenRequester fails the test if TMDB is called.
type forbiddenRequester struct{ t *testing.T }

func (f forbiddenRequester) Request(
	_ context.Context,
	path string,
	_ map[string]string,
	_ any,
) (*resty.Response, error) {
	f.t.Helper()
	f.t.Fatalf("replay called the live TMDB API at %s", path)

	return nil, nil
}

func newTapeRunner(t *testing.T, deps dependencies, recorder *tape.Recorder) Runner {
	t.Helper()

	source, err := yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}.source()
	if err != nil {
		t.Fatalf("load core classifier source: %v", err)
	}

	runner, err := compiler{
		options: []compilerOption{
			compilerFeatures(defaultFeatures),
			celEnvOption,
		},
		dependencies: deps,
		recorder:     recorder,
	}.Compile(source)
	if err != nil {
		t.Fatalf("compile core classifier source: %v", err)
	}

	return runner
}

// localSearchDeps wires a content search through the production localSearch and
// its serialising semaphore, exactly as the factory does.
func localSearchDeps(cs contentSearch, requester tmdb.Requester) dependencies {
	return dependencies{
		search: localSearchSemaphore{
			search:    localSearch{cs},
			semaphore: make(chan struct{}, 1),
		},
		tmdbClient: tmdb.NewClient(requester),
	}
}

func testContent(id, title string, year model.Year) model.Content {
	return model.Content{
		Type:        model.ContentTypeMovie,
		Source:      "tmdb",
		ID:          id,
		Title:       title,
		ReleaseYear: year,
		ReleaseDate: model.NewDateFromParts(year, time.January, 1),
	}
}

// tiedResult builds the pathological shape the tape exists for: several
// candidates, every one of them ranked identically, so the window's order is
// the only thing that decides the winner.
func tiedResult(titles ...string) search.ContentResult {
	items := make([]search.ContentResultItem, 0, len(titles))
	for i, title := range titles {
		items = append(items, search.ContentResultItem{
			ResultItem: query.ResultItem{QueryStringRank: 1},
			Content:    testContent(fmt.Sprintf("%d", 1000+i), title, 1950),
		})
	}

	return search.ContentResult{Items: items}
}

func movieTorrent(name string) model.Torrent {
	return model.Torrent{
		Name:        name,
		Size:        4 * 1024 * 1024 * 1024,
		FilesStatus: model.FilesStatusSingle,
		Extension:   model.NewNullString("mkv"),
	}
}

func writeTapeTo(t *testing.T, recorder *tape.Recorder) string {
	t.Helper()

	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatalf("write tape: %v", err)
	}

	return dir
}

func loadTape(t *testing.T, dir string) *tape.Replay {
	t.Helper()

	replay, err := tape.Load(dir, testDigest)
	if err != nil {
		t.Fatalf("load tape: %v", err)
	}

	return replay
}

// TestProductionRecorderIsOffByDefaultAndCarriesItsLimits pins the two
// properties of the production wiring that are easiest to lose in a refactor:
// recording stays off unless a tape directory is configured, and every tape it
// does produce states what a green replay against it does not prove.
func TestProductionRecorderIsOffByDefaultAndCarriesItsLimits(t *testing.T) {
	if recorder := newTapeRecorder(Params{}, testDigest); recorder != nil {
		t.Fatal("recording is on with no tape directory configured")
	}

	recorder := newTapeRecorder(Params{Config: Config{TapeDir: "unused"}}, testDigest)
	if recorder == nil {
		t.Fatal("a configured tape directory did not enable recording")
	}

	document, err := os.ReadFile(filepath.Join(writeTapeTo(t, recorder), tape.ProvenanceFileName))
	if err != nil {
		t.Fatal(err)
	}

	// The tsquery boundary is the one class of divergence this tape cannot
	// catch, so it has to travel with the tape rather than live in a report.
	for _, want := range []string{
		"does NOT prove",
		"stops at the `searchString` boundary",
		"out of scope for this tape",
		"char::is_alphanumeric",
	} {
		if !strings.Contains(string(document), want) {
			t.Errorf("PROVENANCE.md does not state %q:\n%s", want, document)
		}
	}
}

// TestTapeRecordsTiedCandidateWindow is the core claim: the tape holds the
// ordered candidate list as the query returned it, before the levenshtein
// selection collapses it. Wrapping LocalSearch instead would have recorded only
// the winner and thrown away the evidence that the choice was arbitrary.
func TestTapeRecordsTiedCandidateWindow(t *testing.T) {
	// Three candidates, all ranked 1, all equidistant from the base title.
	// Levenshtein takes the first; nothing about that is a fact about the data.
	contentSearch := &stubContentSearch{
		results: []search.ContentResult{tiedResult("Cinderella", "Cinderella", "Cinderella")},
	}
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})

	runner := newTapeRunner(t, localSearchDeps(contentSearch, forbiddenRequester{t}), recorder)

	ctx := tape.WithSubject(context.Background(), "cinderella")
	if _, err := runner.Run(ctx, "default", testFlagsOn, movieTorrent("Cinderella.1950.1080p.BluRay.x264")); err != nil {
		t.Fatalf("classify: %v", err)
	}

	records, err := recorder.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	record := records[0]
	if record.Subject != "cinderella" {
		t.Errorf("subject is %q, want %q", record.Subject, "cinderella")
	}

	if len(record.Observations) != 1 {
		t.Fatalf("got %d observations, want 1: %+v", len(record.Observations), record.Observations)
	}

	observation := record.Observations[0]
	if observation.Kind != tapeKindLocalContentBySearch {
		t.Fatalf("observation kind is %q, want %q", observation.Kind, tapeKindLocalContentBySearch)
	}

	var request localContentBySearchRequest
	if err := json.Unmarshal(observation.Request, &request); err != nil {
		t.Fatalf("decode request: %v", err)
	}

	if request.SearchString != `"Cinderella"` {
		t.Errorf("recorded search string is %q, want %q", request.SearchString, `"Cinderella"`)
	}

	if request.Year == nil || *request.Year != 1950 {
		t.Errorf("recorded year is %v, want 1950", request.Year)
	}

	if request.ReleaseDateRange == nil {
		t.Error("recorded request has no release date range for a year-filtered search")
	}

	if request.Limit != contentBySearchLimit || request.OrderBy != contentBySearchOrderBy {
		t.Errorf("recorded window is limit=%d order=%q, want %d / %q",
			request.Limit, request.OrderBy, contentBySearchLimit, contentBySearchOrderBy)
	}

	var response localContentResponse
	if err := json.Unmarshal(observation.Response, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(response.Items) != 3 {
		t.Fatalf("recorded %d candidates, want the whole window of 3", len(response.Items))
	}

	for i, item := range response.Items {
		if item.QueryStringRank != "1" {
			t.Errorf("candidate %d rank is %q, want the recorded tie %q", i, item.QueryStringRank, "1")
		}

		if item.Content.ID != fmt.Sprintf("%d", 1000+i) {
			t.Errorf("candidate %d is %q, want the query's order preserved", i, item.Content.ID)
		}
	}
}

// TestTapeCapturesClassifierTimeInput pins the boundary the production corpus
// needs: the Record owns the exact Torrent handed to runner.Run, including the
// effective hint and any content that was already attached, before the workflow
// can make the database state diverge from that input.
func TestTapeCapturesClassifierTimeInput(t *testing.T) {
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
	runner := newTapeRunner(
		t,
		localSearchDeps(forbiddenContentSearch{t}, forbiddenRequester{t}),
		recorder,
	)

	content := testContent("42", "Cinderella", 1950)
	content.OriginalTitle = model.NewNullString("Cendrillon")
	content.CreatedAt = time.Unix(11, 0).UTC()
	content.UpdatedAt = time.Unix(12, 0).UTC()
	content.Collections = []model.ContentCollection{
		{Type: "genre", Source: "tmdb", ID: "1", Name: "Fantasy"},
		{Type: "genre", Source: "tmdb", ID: "2", Name: "Family"},
	}
	content.Attributes = []model.ContentAttribute{
		{ContentType: model.ContentTypeMovie, ContentSource: "tmdb", ContentID: "42", Source: "imdb", Key: "id", Value: "tt0042332"},
	}

	torrent := movieTorrent("Cinderella.1950.2160p.UHD.BluRay.x265")
	torrent.FilesCount = model.NewNullUint(2)
	torrent.Files = []model.TorrentFile{
		{Index: 1, Path: "extras/trailer.mp4", Extension: model.NewNullString("mp4"), Size: 20},
		{Index: 0, Path: "Cinderella.mkv", Extension: model.NewNullString("mkv"), Size: 100},
	}
	torrent.Hint = model.TorrentHint{
		ContentType:   model.ContentTypeMovie,
		ContentSource: model.NewNullString("tmdb"),
		ContentID:     model.NewNullString("42"),
		Languages:     model.Languages{model.Language("fr"): {}, model.Language("en"): {}},
		Episodes:      model.Episodes{1: {2: {}}},
		ReleaseGroup:  model.NewNullString("GROUP"),
	}
	torrent.Contents = []model.TorrentContent{
		{
			ID:            "first",
			ContentType:   model.NewNullContentType(model.ContentTypeMovie),
			ContentSource: model.NewNullString("tmdb"),
			ContentID:     model.NewNullString("42"),
			Content:       content,
		},
		{
			ID:            "second",
			ContentType:   model.NewNullContentType(model.ContentTypeMovie),
			ContentSource: model.NewNullString("tmdb"),
			ContentID:     model.NewNullString("99"),
		},
	}

	ctx := tape.WithSubject(context.Background(), "classifier-time")
	if _, err := runner.Run(ctx, "default", testFlagsOn, torrent); err != nil {
		t.Fatalf("classify: %v", err)
	}

	records, err := recorder.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 || len(records[0].Input) == 0 {
		t.Fatalf("record has no captured input: %+v", records)
	}

	var got tapeClassifierInput
	if err := json.Unmarshal(records[0].Input, &got); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	if !reflect.DeepEqual(got, newTapeClassifierInput("classifier-time", torrent)) {
		t.Fatalf("captured input differs\ngot:  %+v\nwant: %+v", got, newTapeClassifierInput("classifier-time", torrent))
	}
	if got.Hint == nil || got.Hint.Episodes != "S01E02" || fmt.Sprint(got.Hint.Languages) != "[en fr]" || got.Hint.ReleaseGroup == nil || *got.Hint.ReleaseGroup != "GROUP" {
		t.Fatalf("effective hint was not captured completely: %+v", got.Hint)
	}
	if len(got.Files) != 2 || got.Files[0].Path != "extras/trailer.mp4" || got.Files[1].Path != "Cinderella.mkv" {
		t.Fatalf("file order changed: %+v", got.Files)
	}
	if len(got.Contents) != 2 || got.Contents[0].Content == nil || got.Contents[1].Content != nil {
		t.Fatalf("content hydration or order changed: %+v", got.Contents)
	}
	if got.Contents[0].Content.CreatedAt == nil || *got.Contents[0].Content.CreatedAt != 11 || len(got.Contents[0].Content.Collections) != 2 || len(got.Contents[0].Content.Attributes) != 1 {
		t.Fatalf("hydrated content was not captured completely: %+v", got.Contents[0].Content)
	}
}

// TestTapeDistinguishesEmptyFromMissing is the guarantee the Rust side mirrors:
// a recorded empty answer is an answer, and a gap is not.
func TestTapeDistinguishesEmptyFromMissing(t *testing.T) {
	contentSearch := &stubContentSearch{results: []search.ContentResult{{}}}
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
	runner := newTapeRunner(t, localSearchDeps(contentSearch, stubRequester{}), recorder)

	ctx := tape.WithSubject(context.Background(), "empty")

	result, err := runner.Run(ctx, "default", Flags{
		"local_search_enabled": true,
		"apis_enabled":         false,
		"tmdb_enabled":         false,
	}, movieTorrent("Nonexistent.Film.1999.1080p.BluRay.x264"))
	if err != nil {
		t.Fatalf("classify: %v", err)
	}

	if result.Content != nil {
		t.Fatal("an empty candidate list attached content")
	}

	records, recordsErr := recorder.Records()
	if recordsErr != nil {
		t.Fatalf("records: %v", recordsErr)
	}

	if len(records) != 1 || len(records[0].Observations) != 1 {
		t.Fatalf("want one record with one observation, got %+v", records)
	}

	observation := records[0].Observations[0]
	if observation.Outcome != tape.OutcomeOK {
		t.Fatalf("outcome is %q, want %q: an empty result set is a successful observation",
			observation.Outcome, tape.OutcomeOK)
	}

	if !bytes.Equal(observation.Response, []byte(`{"items":[]}`)) {
		t.Errorf("recorded response is %s, want an explicit empty list", observation.Response)
	}

	// The same tape, asked about a subject it never saw, must report a miss --
	// not the empty answer above.
	replay := loadTape(t, writeTapeTo(t, recorder))
	session := tape.SessionFrom(replay.Begin(context.Background(), "never-recorded", 0))

	_, _, err = session.Next(tapeKindLocalContentBySearch, localContentBySearchRequest{})
	if !errors.Is(err, tape.ErrMiss) {
		t.Fatalf("unrecorded subject gave %v, want a miss", err)
	}

	// And asked one question too many about a subject it did see.
	session = tape.SessionFrom(replay.Begin(context.Background(), "empty", 0))

	var request localContentBySearchRequest
	if err := json.Unmarshal(observation.Request, &request); err != nil {
		t.Fatalf("decode request: %v", err)
	}

	if _, _, err := session.Next(tapeKindLocalContentBySearch, request); err != nil {
		t.Fatalf("first question: %v", err)
	}

	if _, _, err := session.Next(tapeKindLocalContentBySearch, request); !errors.Is(err, tape.ErrMiss) {
		t.Fatalf("second question gave %v, want a miss", err)
	}
}

// TestTapeRoundTrip records a classification that consults both seams, then
// replays it with both live dependencies rigged to fail the test, and requires
// the same classification out the other side.
func TestTapeRoundTrip(t *testing.T) {
	searchBody, detailsBody := tmdbBodies(t, 42, "Cinderella")
	requester := stubRequester{bodies: map[string][]byte{
		"/search/movie": searchBody,
		"/movie/42":     detailsBody,
	}}
	// The local search finds nothing, so find_match falls through to TMDB and
	// the classification makes three observations in a fixed order.
	contentSearch := &stubContentSearch{results: []search.ContentResult{{}}}

	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
	torrent := movieTorrent("Cinderella.1950.1080p.BluRay.x264")

	recordRunner := newTapeRunner(t, localSearchDeps(contentSearch, requester), recorder)
	ctx := tape.WithSubject(context.Background(), "cinderella")

	recorded, recordErr := recordRunner.Run(ctx, "default", testFlagsOn, torrent)
	if recordErr != nil {
		t.Fatalf("record run: %v", recordErr)
	}

	if recorded.Content == nil {
		t.Fatal("record run attached no content; the round trip would prove nothing")
	}

	records, err := recorder.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	wantKinds := []string{
		tapeKindLocalContentBySearch,
		tmdb.TapeKindRequest,
		tmdb.TapeKindRequest,
	}
	gotKinds := make([]string, 0, len(records[0].Observations))

	for _, observation := range records[0].Observations {
		gotKinds = append(gotKinds, observation.Kind)
	}

	if fmt.Sprint(gotKinds) != fmt.Sprint(wantKinds) {
		t.Fatalf("observation order is %v, want %v", gotKinds, wantKinds)
	}

	replay := loadTape(t, writeTapeTo(t, recorder))
	replayRunner := newTapeRunner(
		t,
		localSearchDeps(forbiddenContentSearch{t}, forbiddenRequester{t}),
		nil,
	)

	replayed, replayErr := replayRunner.Run(
		replay.Begin(context.Background(), "cinderella", 0),
		"default",
		testFlagsOn,
		torrent,
	)
	if replayErr != nil {
		t.Fatalf("replay run: %v", replayErr)
	}

	want := encodeClassifierCorpus(t, []classifierCorpusRecord{{
		ID: "cinderella", Expected: normalizeClassifierResult(recorded, recordErr),
	}})
	got := encodeClassifierCorpus(t, []classifierCorpusRecord{{
		ID: "cinderella", Expected: normalizeClassifierResult(replayed, replayErr),
	}})

	if !bytes.Equal(want, got) {
		t.Fatalf("replay diverged\nrecorded: %s\nreplayed: %s", want, got)
	}

	if replayed.Content == nil || replayed.Content.ID != recorded.Content.ID {
		t.Fatalf("replay attached %v, recorded attached %v", replayed.Content, recorded.Content)
	}
}

// TestTapeRoundTripAttachedContent replays a classification whose winner came
// from the local search, and requires the attached content back field for
// field. The candidate rows are the part of the tape a port has to reconstruct
// exactly -- they become the write set -- so a round trip that only compared
// the classification's scalar attributes would not be evidence of much.
func TestTapeRoundTripAttachedContent(t *testing.T) {
	contentSearch := &stubContentSearch{
		results: []search.ContentResult{tiedResult("Cinderella", "Cinderella", "Cinderella")},
	}
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
	torrent := movieTorrent("Cinderella.1950.1080p.BluRay.x264")

	runner := newTapeRunner(t, localSearchDeps(contentSearch, forbiddenRequester{t}), recorder)

	recorded, err := runner.Run(
		tape.WithSubject(context.Background(), "cinderella"),
		"default", testFlagsOn, torrent,
	)
	if err != nil {
		t.Fatalf("record run: %v", err)
	}

	if recorded.Content == nil {
		t.Fatal("record run attached no content")
	}

	replay := loadTape(t, writeTapeTo(t, recorder))
	replayRunner := newTapeRunner(
		t,
		localSearchDeps(forbiddenContentSearch{t}, forbiddenRequester{t}),
		nil,
	)

	replayed, err := replayRunner.Run(
		replay.Begin(context.Background(), "cinderella", 0),
		"default", testFlagsOn, torrent,
	)
	if err != nil {
		t.Fatalf("replay run: %v", err)
	}

	if replayed.Content == nil {
		t.Fatal("replay attached no content")
	}

	if !reflect.DeepEqual(*recorded.Content, *replayed.Content) {
		t.Fatalf("attached content did not survive the round trip\nrecorded: %+v\nreplayed: %+v",
			*recorded.Content, *replayed.Content)
	}

	// The tie is what makes the order load-bearing: the winner is the first
	// candidate, so a replay that reordered the window would attach a different
	// row while every other assertion still passed.
	if replayed.Content.ID != "1000" {
		t.Fatalf("replay attached candidate %q, want the first of the tied window", replayed.Content.ID)
	}
}

// TestTapeDesyncOnDifferentQuestion is the property the request half of the
// tape buys: a port that asks a different question fails even when an answer
// exists and would have looked plausible.
func TestTapeDesyncOnDifferentQuestion(t *testing.T) {
	contentSearch := &stubContentSearch{
		results: []search.ContentResult{tiedResult("Cinderella")},
	}
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
	runner := newTapeRunner(t, localSearchDeps(contentSearch, forbiddenRequester{t}), recorder)

	ctx := tape.WithSubject(context.Background(), "cinderella")
	if _, err := runner.Run(ctx, "default", testFlagsOn, movieTorrent("Cinderella.1950.1080p.BluRay.x264")); err != nil {
		t.Fatalf("record run: %v", err)
	}

	replay := loadTape(t, writeTapeTo(t, recorder))
	replayRunner := newTapeRunner(
		t,
		localSearchDeps(forbiddenContentSearch{t}, forbiddenRequester{t}),
		nil,
	)

	for _, testCase := range []struct {
		name    string
		torrent model.Torrent
	}{{
		// A different base title: the search string differs.
		name:    "different search string",
		torrent: movieTorrent("Cinderella.Story.1950.1080p.BluRay.x264"),
	}, {
		// The same title with a different year: the search string is identical
		// and the recorded answer would have been perfectly usable, so
		// comparing answers alone would pass this. Only the question differs --
		// the year filter and the release-date range it expands into.
		name:    "different year filter",
		torrent: movieTorrent("Cinderella.1951.1080p.BluRay.x264"),
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := replayRunner.Run(
				replay.Begin(context.Background(), "cinderella", 0),
				"default",
				testFlagsOn,
				testCase.torrent,
			)
			if !errors.Is(err, tape.ErrDesync) {
				t.Fatalf("got %v, want a desync", err)
			}

			var desync *tape.DesyncError
			if !errors.As(err, &desync) {
				t.Fatalf("error %v does not carry the desync detail", err)
			}

			if desync.WantRequestJSON == desync.GotRequestJSON {
				t.Fatal("desync reported identical requests")
			}
		})
	}

	// A wrong-kind desync: the tape's first observation is a local search, so
	// asking TMDB first is a different question even before the parameters are
	// compared.
	session := tape.SessionFrom(replay.Begin(context.Background(), "cinderella", 0))

	_, _, err := session.Next(tmdb.TapeKindRequest, struct{}{})
	if !errors.Is(err, tape.ErrDesync) {
		t.Fatalf("wrong observation kind gave %v, want a desync", err)
	}
}

// TestTapeRecordsTmdbFailureKinds pins the error half of the round trip. The
// classifier's control flow turns on which failure it got, so a replay that
// flattened them would silently change behaviour.
func TestTapeRecordsTmdbFailureKinds(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		err      error
		wantKind string
		wantErr  error
	}{
		{"not found", tmdb.ErrNotFound, tmdb.TapeErrorKindNotFound, tmdb.ErrNotFound},
		{"unauthorized", tmdb.ErrUnauthorized, tmdb.TapeErrorKindUnauthorized, tmdb.ErrUnauthorized},
		{"transport", errors.New("dial tcp: connection refused"), tmdb.TapeErrorKindTransport, nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
			client := tmdb.NewClient(stubRequester{err: testCase.err})

			ctx := recorder.Begin(context.Background(), "subject", "default", nil, nil)
			if _, err := client.SearchMovie(ctx, tmdb.SearchMovieRequest{Query: "x"}); !errors.Is(err, testCase.err) {
				t.Fatalf("record: got %v, want %v", err, testCase.err)
			}

			tape.EndSession(ctx, tape.RecordOutcome{Kind: tape.RecordCompleted})

			records, err := recorder.Records()
			if err != nil {
				t.Fatalf("records: %v", err)
			}

			observation := records[0].Observations[0]
			if observation.Outcome != tape.OutcomeError {
				t.Fatalf("outcome is %q, want %q", observation.Outcome, tape.OutcomeError)
			}

			if observation.Error.Kind != testCase.wantKind {
				t.Fatalf("error kind is %q, want %q", observation.Error.Kind, testCase.wantKind)
			}

			replay := loadTape(t, writeTapeTo(t, recorder))
			replayClient := tmdb.NewClient(forbiddenRequester{t})

			_, replayErr := replayClient.SearchMovie(
				replay.Begin(context.Background(), "subject", 0),
				tmdb.SearchMovieRequest{Query: "x"},
			)

			if testCase.wantErr != nil {
				if !errors.Is(replayErr, testCase.wantErr) {
					t.Fatalf("replay: got %v, want the %v sentinel", replayErr, testCase.wantErr)
				}
			} else if replayErr == nil || replayErr.Error() != testCase.err.Error() {
				t.Fatalf("replay: got %v, want %v", replayErr, testCase.err)
			}
		})
	}
}

// TestTapeIsDeterministicUnderConcurrency runs the same population sequentially
// and concurrently and requires byte-identical tapes. Classifications interleave
// arbitrarily, so append order is a race; the tape is keyed and sorted by
// subject, and sequenced within a subject by the single classification that
// produced it.
func TestTapeIsDeterministicUnderConcurrency(t *testing.T) {
	const subjects = 64

	sequential := recordPopulation(t, subjects, false)
	concurrent := recordPopulation(t, subjects, true)

	if !bytes.Equal(sequential, concurrent) {
		t.Fatalf("concurrent tape differs from the sequential one\nsequential:\n%s\nconcurrent:\n%s",
			sequential, concurrent)
	}

	records, err := tape.DecodeRecords(bytes.NewReader(concurrent))
	if err != nil {
		t.Fatalf("decode concurrent tape: %v", err)
	}

	if len(records) != subjects {
		t.Fatalf("got %d records, want %d", len(records), subjects)
	}

	for i, record := range records {
		// Each subject's own observations are three, in a fixed order, no
		// matter what else was running at the time.
		if len(record.Observations) != 3 {
			t.Fatalf("record %d (%s) has %d observations, want 3",
				i, record.Subject, len(record.Observations))
		}

		if record.Observations[0].Kind != tapeKindLocalContentBySearch {
			t.Fatalf("record %d starts with %q, want the local search",
				i, record.Observations[0].Kind)
		}

		if i > 0 && records[i-1].Subject >= record.Subject {
			t.Fatalf("records are not sorted: %q then %q", records[i-1].Subject, record.Subject)
		}
	}
}

func recordPopulation(t *testing.T, subjects int, concurrent bool) []byte {
	t.Helper()

	searchBody, detailsBody := tmdbBodies(t, 42, "Cinderella")
	requester := stubRequester{bodies: map[string][]byte{
		"/search/movie": searchBody,
		"/movie/42":     detailsBody,
	}}
	contentSearch := &stubContentSearch{}
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
	runner := runnerSemaphore{
		runner:    newTapeRunner(t, localSearchDeps(contentSearch, requester), recorder),
		semaphore: make(chan struct{}, 10),
	}

	run := func(i int) {
		ctx := tape.WithSubject(context.Background(), fmt.Sprintf("subject-%03d", i))
		if _, err := runner.Run(
			ctx,
			"default",
			testFlagsOn,
			movieTorrent("Cinderella.1950.1080p.BluRay.x264"),
		); err != nil {
			t.Errorf("classify subject %d: %v", i, err)
		}
	}

	if concurrent {
		var wg sync.WaitGroup

		for i := range subjects {
			wg.Add(1)

			go func() {
				defer wg.Done()
				run(i)
			}()
		}

		wg.Wait()
	} else {
		for i := range subjects {
			run(i)
		}
	}

	records, err := recorder.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	encoded, err := tape.EncodeRecords(records)
	if err != nil {
		t.Fatalf("encode records: %v", err)
	}

	return encoded
}

// TestClassifierCorpusWithRecorderEnabled is the no-behaviour-change gate: the
// 330-case corpus is run through the real localSearch with a recorder attached
// and must reproduce the checked-in golden byte for byte, while producing a
// well-formed tape.
//
// The tmdb Client mock is left expectation-free, so any flags-off impurity fails
// the test -- the purity assertion the base corpus makes, carried over.
func TestClassifierCorpusWithRecorderEnabled(t *testing.T) {
	inputs := loadClassifierInputs(t, filepath.Join(classifierCorpusDir, "inputs.jsonl"))
	goldenPath := filepath.Join(classifierCorpusDir, "corpus.golden.jsonl")

	// The hinted-content-id fixtures reach attach_local_content_by_id, which is
	// not flag-gated. An empty result set is how the database says "no such
	// content", and localSearch maps it to ErrUnmatched -- the same outcome the
	// base corpus gets from its LocalSearch mock, but reached through the code
	// the tape seam actually sits in.
	contentSearch := &stubContentSearch{}
	recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "corpus test"})

	runner := newTapeRunner(t, dependencies{
		search: localSearchSemaphore{
			search:    localSearch{contentSearch},
			semaphore: make(chan struct{}, 1),
		},
		tmdbClient: tmdb_mocks.NewClient(t),
	}, recorder)

	records := make([]classifierCorpusRecord, 0, len(inputs))

	for _, input := range inputs {
		ctx := tape.WithSubject(context.Background(), input.ID)
		result, runErr := runner.Run(ctx, "default", testFlagsOff, toTorrent(input))
		records = append(records, classifierCorpusRecord{
			ID:        input.ID,
			Subsystem: "classifier",
			Input:     input,
			Expected:  normalizeClassifierResult(result, runErr),
		})
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read classifier corpus golden: %v", err)
	}

	actual := encodeClassifierCorpus(t, records)
	if !bytes.Equal(expected, actual) {
		line, want, got := firstClassifierCorpusDifference(expected, actual)
		t.Fatalf("recording changed classifier output at line %d\nwant: %s\n got: %s", line, want, got)
	}

	dir := writeTapeTo(t, recorder)
	replay := loadTape(t, dir)

	if replay.Manifest().RecordCount != len(inputs) {
		t.Fatalf("tape holds %d records, want one per input (%d)",
			replay.Manifest().RecordCount, len(inputs))
	}

	if contentSearch.calls == 0 {
		t.Fatal("the corpus never reached the local search seam; the tape proves nothing")
	}

	if replay.Manifest().ObservationCount != contentSearch.calls {
		t.Fatalf("tape holds %d observations but the search was called %d times",
			replay.Manifest().ObservationCount, contentSearch.calls)
	}

	for _, name := range []string{tape.TapeFileName, tape.ManifestFileName, tape.ProvenanceFileName} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("tape directory is missing %s: %v", name, err)
		}
	}
}

// tmdbBodies renders response bodies for a movie search and its details lookup.
func tmdbBodies(t *testing.T, id int64, title string) ([]byte, []byte) {
	t.Helper()

	searchBody, err := json.Marshal(tmdb.SearchMovieResponse{
		Page:         1,
		TotalResults: 1,
		TotalPages:   1,
		Results: []tmdb.SearchMovieResult{{
			ID:               id,
			Title:            title,
			OriginalTitle:    title,
			OriginalLanguage: "en",
			ReleaseDate:      "1950-02-15",
		}},
	})
	if err != nil {
		t.Fatalf("encode search body: %v", err)
	}

	var details tmdb.MovieDetailsResponse

	details.ID = id
	details.Title = title
	details.OriginalTitle = title
	details.OriginalLanguage = "en"
	details.ReleaseDate = "1950-02-15"
	details.IMDbID = "tt0042332"

	detailsBody, err := json.Marshal(details)
	if err != nil {
		t.Fatalf("encode details body: %v", err)
	}

	return searchBody, detailsBody
}

// TestTapeRecordsHowTheClassificationEnded is the runner half of the outcome
// fix. The recorder can only stamp what the runner tells it, and the whole
// point is that a classification which ended EARLY is distinguishable from one
// that ran to the end and consulted nothing.
func TestTapeRecordsHowTheClassificationEnded(t *testing.T) {
	// "Cinderella (1950)" with no hint and no files reaches no enrichment
	// action, so it completes having observed nothing -- the case that used to
	// be indistinguishable from an early exit.
	torrent := model.Torrent{
		Name:        "Cinderella (1950)",
		Size:        1,
		FilesStatus: model.FilesStatusNoInfo,
	}

	t.Run("a completed classification says so", func(t *testing.T) {
		recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
		runner := newTapeRunner(t, localSearchDeps(
			&stubContentSearch{}, forbiddenRequester{t: t},
		), recorder)

		if _, err := runner.Run(context.Background(), "default", Flags{
			"local_search_enabled": false,
			"apis_enabled":         false,
			"tmdb_enabled":         false,
		}, torrent); err != nil {
			t.Fatalf("run: %v", err)
		}

		record := onlyRecord(t, recorder)
		if record.Outcome == nil || record.Outcome.Kind != tape.RecordCompleted {
			t.Fatalf("want a completed outcome, got %+v", record.Outcome)
		}

		if !record.Authoritative() {
			t.Error("a completed classification's observation list is an answer")
		}
	})

	t.Run("a cancelled classification is not an oracle", func(t *testing.T) {
		recorder := tape.NewRecorder(testDigest, 0, tape.Provenance{Command: "test"})
		runner := newTapeRunner(t, localSearchDeps(
			&stubContentSearch{}, forbiddenRequester{t: t},
		), recorder)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _ = runner.Run(ctx, "default", Flags{
			"local_search_enabled": true,
			"apis_enabled":         false,
			"tmdb_enabled":         false,
		}, torrent)

		record := onlyRecord(t, recorder)

		// 🚨 This classification returned a clean nil: the workflow never
		// touched the cancelled context. The record is therefore COMPLETE --
		// the session closed on the way out -- and holds zero observations,
		// which is indistinguishable from the subtest above by every field
		// except the outcome. That is the whole bug in one comparison.
		if record.Incomplete {
			t.Fatal("an early exit closes its session, so Incomplete cannot catch it")
		}

		if record.Authoritative() {
			t.Errorf("a cancelled classification is a prefix, not an answer: %+v", record.Outcome)
		}
	})
}

func onlyRecord(t *testing.T, recorder *tape.Recorder) tape.Record {
	t.Helper()

	records, err := recorder.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("want exactly one record, got %d", len(records))
	}

	return records[0]
}
