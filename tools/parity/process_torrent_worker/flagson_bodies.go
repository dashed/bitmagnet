package main

// TMDB response bodies for the stub fixture. Every body is built from a real
// 200 body recorded in the classifier-attach corpora, decoded with UseNumber so
// the real number literals ("0.0", "19.7133") survive, with only the fields the
// fixture needs overridden. Fields neither client reads (softcore, seasons,
// networks, ...) stay in, as TMDB sends them.

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type bodyTemplates struct {
	movieDetails [][]byte
	tvDetails    [][]byte
	movieItems   [][]byte
	tvItems      [][]byte
}

type tapeLine struct {
	Observations []tapeLineObservation `json:"observations"`
}

type tapeLineObservation struct {
	Kind     string                `json:"kind"`
	Request  tapeLineRequest       `json:"request"`
	Response *tapeLineHTTPResponse `json:"response"`
}

type tapeLineRequest struct {
	Path string `json:"path"`
}

type tapeLineHTTPResponse struct {
	StatusCode int    `json:"statusCode"`
	BodyBase64 string `json:"bodyBase64"`
}

// loadBodyTemplates collects the distinct real 200 bodies, in a fixed order:
// corpora by name, records and observations in file order.
func loadBodyTemplates(root string) (*bodyTemplates, error) {
	tapes, err := filepath.Glob(filepath.Join(root, "testdata/parity/classifier-attach/*/tape.jsonl"))
	if err != nil {
		return nil, err
	}

	sort.Strings(tapes)

	t := &bodyTemplates{}
	seen := map[[32]byte]bool{}

	add := func(list *[][]byte, raw []byte) {
		sum := sha256.Sum256(raw)
		if !seen[sum] {
			seen[sum] = true
			*list = append(*list, raw)
		}
	}

	for _, path := range tapes {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<26)

		for scanner.Scan() {
			var line tapeLine
			if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("%s: %w", path, err)
			}

			for _, o := range line.Observations {
				if o.Kind != observationTMDBRequest || o.Response == nil || o.Response.StatusCode != http.StatusOK {
					continue
				}

				body, err := base64.StdEncoding.DecodeString(o.Response.BodyBase64)
				if err != nil {
					_ = f.Close()
					return nil, err
				}

				switch p := o.Request.Path; {
				case p == endpointSearchMovie, p == endpointSearchTV:
					var page struct {
						Results []json.RawMessage `json:"results"`
					}
					if err := json.Unmarshal(body, &page); err != nil {
						_ = f.Close()
						return nil, err
					}

					for _, item := range page.Results {
						if p == endpointSearchMovie {
							add(&t.movieItems, item)
						} else {
							add(&t.tvItems, item)
						}
					}
				case strings.HasPrefix(p, "/movie/"):
					add(&t.movieDetails, body)
				case strings.HasPrefix(p, "/tv/"):
					add(&t.tvDetails, body)
				}
			}
		}

		_ = f.Close()

		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}

	if len(t.movieDetails) == 0 || len(t.tvDetails) == 0 || len(t.movieItems) == 0 || len(t.tvItems) == 0 {
		return nil, fmt.Errorf("body templates: too few real bodies (%d movie, %d tv, %d movie items, %d tv items)",
			len(t.movieDetails), len(t.tvDetails), len(t.movieItems), len(t.tvItems))
	}

	return t, nil
}

// fixtureRand is a deterministic generator keyed by a string.
func fixtureRand(key string) *rand.Rand {
	sum := sha256.Sum256([]byte("flagson/" + key))

	return rand.New(rand.NewPCG(binary.LittleEndian.Uint64(sum[:8]), binary.LittleEndian.Uint64(sum[8:16])))
}

func decodeTemplate(raw []byte) map[string]any {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		panic(err)
	}

	return m
}

func encodeBody(v any) json.RawMessage {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(v); err != nil {
		panic(err)
	}

	return bytes.TrimRight(buf.Bytes(), "\n")
}

func pick[T any](r *rand.Rand, list []T) T { return list[r.IntN(len(list))] }

// Values that exercise the clients' decoding: float literals float32 rounds,
// zero, and literals with trailing zeros.
var fixtureFloats = []string{"0.0", "0", "5.3", "7.125", "19.7133", "123.456789", "0.6", "8.75", "1e1", "3.3333333333"}

var fixtureLanguages = []string{"en", "en", "en", "fr", "ja", "ko", "zh", "cn", "xx", "pt", "de", "es", "hi", ""}

// Genre names TMDB uses, with ids whose text order differs from their numeric
// order (10749 < 878 as text).
var fixtureGenres = []struct {
	id   int
	name string
}{
	{28, "Action"},
	{12, "Adventure"},
	{16, "Animation"},
	{35, "Comedy"},
	{80, "Crime"},
	{99, "Documentary"},
	{18, "Drama"},
	{10751, "Family"},
	{14, "Fantasy"},
	{36, "History"},
	{27, "Horror"},
	{10402, "Music"},
	{9648, "Mystery"},
	{10749, "Romance"},
	{878, "Science Fiction"},
	{53, "Thriller"},
	{10752, "War"},
	{37, "Western"},
	{10759, "Action & Adventure"},
	{10764, "Reality"},
}

func fixtureDate(r *rand.Rand, year int) string {
	if year <= 0 {
		return ""
	}

	return fmt.Sprintf("%04d-%02d-%02d", year, 1+r.IntN(12), 1+r.IntN(28))
}

func randomGenres(r *rand.Rand) []any {
	n := r.IntN(4)
	if r.IntN(10) == 0 {
		n = 0
	}

	out := make([]any, 0, n)
	used := map[int]bool{}

	for range n {
		g := pick(r, fixtureGenres)
		if used[g.id] {
			continue
		}

		used[g.id] = true

		out = append(out, map[string]any{"id": json.Number(fmt.Sprint(g.id)), "name": g.name})
	}

	return out
}

// detailsSpec is what a details body says about the content it describes.
type detailsSpec struct {
	tv            bool
	id            int64
	title         string
	originalTitle string // "" = same as title
	year          int    // 0 = no date
	adult         bool
	imdbID        string // "" = JSON null
	tvdbID        int64  // 0 = JSON null
}

func buildDetailsBody(t *bodyTemplates, r *rand.Rand, s detailsSpec) json.RawMessage {
	var m map[string]any
	if s.tv {
		m = decodeTemplate(pick(r, t.tvDetails))
	} else {
		m = decodeTemplate(pick(r, t.movieDetails))
	}

	m["id"] = json.Number(fmt.Sprint(s.id))

	original := s.originalTitle
	if original == "" {
		original = s.title
	}

	if r.IntN(3) == 0 {
		m["genres"] = randomGenres(r)
	}

	if r.IntN(2) == 0 {
		m["popularity"] = json.Number(pick(r, fixtureFloats))
		m["vote_average"] = json.Number(pick(r, fixtureFloats))
		m["vote_count"] = json.Number(fmt.Sprint(r.IntN(5000)))
	}

	if r.IntN(3) == 0 {
		m["original_language"] = pick(r, fixtureLanguages)
	}

	switch r.IntN(8) {
	case 0:
		m["overview"] = ""
	case 1:
		m["poster_path"] = nil
		m["backdrop_path"] = nil
	}

	if s.tv {
		m["name"], m["original_name"] = s.title, original
		m["first_air_date"] = fixtureDate(r, s.year)

		ext := map[string]any{
			"imdb_id": nil, "freebase_mid": nil, "freebase_id": nil, "tvdb_id": nil, "tvrage_id": nil,
			"wikidata_id": nil, "facebook_id": nil, "instagram_id": nil, "twitter_id": nil,
		}
		if s.imdbID != "" {
			ext["imdb_id"] = s.imdbID
		}

		if s.tvdbID != 0 {
			ext["tvdb_id"] = json.Number(fmt.Sprint(s.tvdbID))
		}

		m["external_ids"] = ext
	} else {
		m["title"], m["original_title"] = s.title, original
		m["release_date"] = fixtureDate(r, s.year)
		m["adult"] = s.adult

		if s.imdbID != "" {
			m["imdb_id"] = s.imdbID
		} else {
			m["imdb_id"] = nil
		}

		switch r.IntN(4) {
		case 0:
			// One name per collection id, as TMDB has: Go inserts collections
			// ON CONFLICT DO NOTHING in a batch whose order is the order the
			// job's classifications finished, so two names for one id would
			// make the stored name a race.
			fid := 900000 + r.IntN(200)
			m["belongs_to_collection"] = map[string]any{
				"id":            json.Number(fmt.Sprint(fid)),
				"name":          fmt.Sprintf("Collection %d", fid),
				"poster_path":   nil,
				"backdrop_path": "/c" + fmt.Sprint(r.IntN(1000)) + ".jpg",
			}
		case 1:
			m["belongs_to_collection"] = nil
		}

		if r.IntN(5) == 0 {
			m["runtime"] = json.Number("0")
		}
	}

	return encodeBody(m)
}

// searchItem is one entry of a search or find results array.
type searchItem struct {
	id            int64
	title         string
	originalTitle string
	year          int
}

func buildSearchItem(t *bodyTemplates, r *rand.Rand, tv bool, it searchItem) map[string]any {
	var m map[string]any
	if tv {
		m = decodeTemplate(pick(r, t.tvItems))
	} else {
		m = decodeTemplate(pick(r, t.movieItems))
	}

	original := it.originalTitle
	if original == "" {
		original = it.title
	}

	m["id"] = json.Number(fmt.Sprint(it.id))

	if tv {
		m["name"], m["original_name"] = it.title, original
		m["first_air_date"] = fixtureDate(r, it.year)
	} else {
		m["title"], m["original_title"] = it.title, original
		m["release_date"] = fixtureDate(r, it.year)
	}

	return m
}

func buildSearchBody(t *bodyTemplates, r *rand.Rand, tv bool, items []searchItem) json.RawMessage {
	results := make([]any, len(items))
	for i, it := range items {
		results[i] = buildSearchItem(t, r, tv, it)
	}

	return encodeBody(map[string]any{
		"page":          json.Number("1"),
		"results":       results,
		"total_pages":   json.Number("1"),
		"total_results": json.Number(fmt.Sprint(len(items))),
	})
}

// buildFindBody answers /find with the given items in movie_results or
// tv_results.
func buildFindBody(t *bodyTemplates, r *rand.Rand, tv bool, items []searchItem) json.RawMessage {
	movies, shows := []any{}, []any{}

	for _, it := range items {
		m := buildSearchItem(t, r, tv, it)
		if tv {
			m["media_type"] = "tv"
			shows = append(shows, m)
		} else {
			m["media_type"] = "movie"
			movies = append(movies, m)
		}
	}

	return encodeBody(map[string]any{
		"movie_results":      movies,
		"person_results":     []any{},
		"tv_results":         shows,
		"tv_episode_results": []any{},
		"tv_season_results":  []any{},
	})
}
