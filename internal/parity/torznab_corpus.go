package parity

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

const (
	torznabQueryKindCaps   = "caps"
	torznabQueryKindSearch = "search"
	torznabQueryKindError  = "error"
)

// CorpusQuery is one ordered query from the shared Torznab parity corpus.
type CorpusQuery struct {
	ID        string
	Kind      string
	Path      string
	Desc      string
	Dims      []string
	ExpectIDs []string
	HasExpect bool
}

// GoldenName returns the committed golden filename for the query.
func (query CorpusQuery) GoldenName() string {
	if query.ID == torznabQueryKindCaps {
		return "caps.golden.xml"
	}

	return "q-" + query.ID + ".golden.xml"
}

// TorznabFixture is one deterministic row in the shared Torznab fixture set.
type TorznabFixture struct {
	ID              string
	Pub             int
	ContentType     string
	TMDB            string
	IMDB            string
	ContentTitle    string
	Year            int
	Title           string
	Size            uint64
	VideoResolution string
	Video3D         string
	VideoCodec      string
	ReleaseGroup    string
	Seeders         *uint
	Leechers        *uint
	Episodes        map[int][]int
	Files           []string
	// FilesCount seeds torrent_contents.files_count independently of the Files
	// slice, so a fixture can exercise the divergence between the denormalized
	// count column and the hydrated file list (len(files_data)). When nil the
	// column is left at its default; the Torznab `files` attr never derives from
	// it (live Go and the Rust adapter both use len(files_data)).
	FilesCount *uint
}

type corpusQueryJSON struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	Path      string          `json:"path"`
	Desc      string          `json:"desc"`
	Dims      []string        `json:"dims"`
	ExpectIDs json.RawMessage `json:"expectIds"`
}

type torznabFixtureJSON struct {
	ID              string           `json:"id"`
	Pub             int              `json:"pub"`
	ContentType     string           `json:"contentType"`
	TMDB            string           `json:"tmdb"`
	IMDB            string           `json:"imdb"`
	ContentTitle    string           `json:"contentTitle"`
	Year            int              `json:"year"`
	Title           string           `json:"title"`
	Size            uint64           `json:"size"`
	VideoResolution string           `json:"videoResolution"`
	Video3D         string           `json:"video3d"`
	VideoCodec      string           `json:"videoCodec"`
	ReleaseGroup    string           `json:"releaseGroup"`
	Seeders         *uint            `json:"seeders"`
	Leechers        *uint            `json:"leechers"`
	Episodes        map[string][]int `json:"episodes"`
	Files           []string         `json:"files"`
	FilesCount      *uint            `json:"filesCount"`
}

// LoadTorznabCorpus loads a JSONL corpus in file order.
func LoadTorznabCorpus(path string) ([]CorpusQuery, error) {
	var queries []CorpusQuery
	seen := make(map[string]int)

	err := scanTorznabJSONL(path, func(line int, raw []byte) error {
		var wire corpusQueryJSON
		if err := decodeTorznabJSONLine(raw, &wire); err != nil {
			return err
		}
		if wire.ID == "" {
			return fmt.Errorf("empty id")
		}
		if firstLine, exists := seen[wire.ID]; exists {
			return fmt.Errorf("duplicate id %q (first seen on line %d)", wire.ID, firstLine)
		}
		switch wire.Kind {
		case torznabQueryKindCaps, torznabQueryKindSearch, torznabQueryKindError:
		default:
			return fmt.Errorf("query %q has invalid kind %q", wire.ID, wire.Kind)
		}

		query := CorpusQuery{
			ID:   wire.ID,
			Kind: wire.Kind,
			Path: wire.Path,
			Desc: wire.Desc,
			Dims: wire.Dims,
		}
		if wire.ExpectIDs != nil {
			if wire.Kind != torznabQueryKindSearch {
				return fmt.Errorf("query %q has expectIds but kind is %q", wire.ID, wire.Kind)
			}
			if err := json.Unmarshal(wire.ExpectIDs, &query.ExpectIDs); err != nil {
				return fmt.Errorf("decode expectIds for query %q: %w", wire.ID, err)
			}
			query.HasExpect = true
		}

		seen[wire.ID] = line
		queries = append(queries, query)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return queries, nil
}

// LoadTorznabFixtures loads JSONL fixtures in file order.
func LoadTorznabFixtures(path string) ([]TorznabFixture, error) {
	var fixtures []TorznabFixture
	seen := make(map[string]int)

	err := scanTorznabJSONL(path, func(line int, raw []byte) error {
		var wire torznabFixtureJSON
		if err := decodeTorznabJSONLine(raw, &wire); err != nil {
			return err
		}
		if wire.ID == "" {
			return fmt.Errorf("empty id")
		}
		if firstLine, exists := seen[wire.ID]; exists {
			return fmt.Errorf("duplicate id %q (first seen on line %d)", wire.ID, firstLine)
		}

		fixture := TorznabFixture{
			ID:              wire.ID,
			Pub:             wire.Pub,
			ContentType:     wire.ContentType,
			TMDB:            wire.TMDB,
			IMDB:            wire.IMDB,
			ContentTitle:    wire.ContentTitle,
			Year:            wire.Year,
			Title:           wire.Title,
			Size:            wire.Size,
			VideoResolution: wire.VideoResolution,
			Video3D:         wire.Video3D,
			VideoCodec:      wire.VideoCodec,
			ReleaseGroup:    wire.ReleaseGroup,
			Seeders:         wire.Seeders,
			Leechers:        wire.Leechers,
			Files:           wire.Files,
			FilesCount:      wire.FilesCount,
		}
		if wire.Episodes != nil {
			fixture.Episodes = make(map[int][]int, len(wire.Episodes))
			for seasonText, episodes := range wire.Episodes {
				season, parseErr := strconv.Atoi(seasonText)
				if parseErr != nil {
					return fmt.Errorf("fixture %q has invalid episode season %q: %w", wire.ID, seasonText, parseErr)
				}
				if _, exists := fixture.Episodes[season]; exists {
					return fmt.Errorf("fixture %q has duplicate numeric episode season %d", wire.ID, season)
				}
				fixture.Episodes[season] = episodes
			}
		}

		seen[wire.ID] = line
		fixtures = append(fixtures, fixture)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return fixtures, nil
}

func scanTorznabJSONL(path string, consume func(line int, raw []byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		if err := consume(line, raw); err != nil {
			return fmt.Errorf("%s line %d: %w", path, line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}

	return nil
}

func decodeTorznabJSONLine(raw []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}

	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err == nil {
		return fmt.Errorf("multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode trailing JSON: %w", err)
	}

	return nil
}
