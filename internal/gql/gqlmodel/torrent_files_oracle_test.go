package gqlmodel

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
)

var updateTorrentFilesParity = flag.Bool(
	"update-torrent-files-parity",
	false,
	"regenerate the torrent.files Go oracle corpus",
)

type torrentFilesFixture struct {
	InfoHash string              `json:"infoHash"`
	Files    []torrentFilesInput `json:"files"`
}

type torrentFilesInput struct {
	Index     uint   `json:"index"`
	Path      string `json:"path"`
	Extension string `json:"extension"`
	Size      uint   `json:"size"`
}

type torrentFilesOracleInput struct {
	InfoHashes  []string                       `json:"infoHashes"`
	Limit       *uint                          `json:"limit,omitempty"`
	Page        *uint                          `json:"page,omitempty"`
	Offset      *uint                          `json:"offset,omitempty"`
	TotalCount  *bool                          `json:"totalCount,omitempty"`
	HasNextPage *bool                          `json:"hasNextPage,omitempty"`
	Cached      *bool                          `json:"cached,omitempty"`
	OrderBy     []torrentFilesOracleOrderInput `json:"orderBy,omitempty"`
}

type torrentFilesOracleOrderInput struct {
	Field      gen.TorrentFilesOrderByField `json:"field"`
	Descending *bool                        `json:"descending,omitempty"`
}

type torrentFilesOracleCase struct {
	ID       string                   `json:"id"`
	Input    torrentFilesOracleInput  `json:"input"`
	Expected torrentFilesOracleResult `json:"expected"`
}

type torrentFilesOracleResult struct {
	TotalCount  uint                     `json:"totalCount"`
	HasNextPage bool                     `json:"hasNextPage"`
	Items       []torrentFilesOracleItem `json:"items"`
}

type torrentFilesOracleItem struct {
	InfoHash  string  `json:"infoHash"`
	Index     uint    `json:"index"`
	Path      string  `json:"path"`
	Extension *string `json:"extension"`
	FileType  *string `json:"fileType"`
	Size      uint    `json:"size"`
	CreatedAt string  `json:"createdAt"`
	UpdatedAt string  `json:"updatedAt"`
}

func TestTorrentFilesGoOracle(t *testing.T) {
	fixtures := loadTorrentFilesFixtures(t)
	allFiles := make([]model.TorrentFile, 0)
	for _, fixture := range fixtures {
		hash, err := protocol.ParseID(fixture.InfoHash)
		require.NoError(t, err)
		for _, file := range fixture.Files {
			// The stored extension is deliberately ignored, matching filesFromBlob.
			extension := model.FileExtensionFromPath(file.Path)
			allFiles = append(allFiles, model.TorrentFile{
				InfoHash:  hash,
				Index:     file.Index,
				Path:      file.Path,
				Extension: extension,
				Size:      file.Size,
			})
		}
	}

	trueValue := true
	zero, one, two := uint(0), uint(1), uint(2)
	maxInt := uint(2_147_483_647)
	ascending, descending := false, true
	cases := []torrentFilesOracleCase{
		{
			ID: "default-path",
			Input: torrentFilesOracleInput{
				InfoHashes: []string{fixtures[0].InfoHash},
			},
		},
		{
			ID: "paged-size-desc",
			Input: torrentFilesOracleInput{
				InfoHashes:  []string{fixtures[0].InfoHash},
				Limit:       &one,
				Page:        &two,
				Offset:      &zero,
				TotalCount:  &trueValue,
				HasNextPage: &trueValue,
				Cached:      &trueValue,
				OrderBy: []torrentFilesOracleOrderInput{{
					Field:      gen.TorrentFilesOrderByFieldSize,
					Descending: &descending,
				}},
			},
		},
		{
			// Equal size keys prove stable multi-order behavior. The fixture/hash
			// order is ascending, matching the Rust adapter's explicit hash order;
			// Go's DAO has no cross-hash tie-order contract beyond that seed order.
			ID: "multi-hash-equal-keys",
			Input: torrentFilesOracleInput{
				InfoHashes:  []string{fixtures[0].InfoHash, fixtures[1].InfoHash},
				TotalCount:  &trueValue,
				HasNextPage: &trueValue,
				OrderBy: []torrentFilesOracleOrderInput{
					{Field: gen.TorrentFilesOrderByFieldSize, Descending: &descending},
					{Field: gen.TorrentFilesOrderByFieldPath, Descending: &ascending},
				},
			},
		},
		{
			ID: "zero-limit",
			Input: torrentFilesOracleInput{
				InfoHashes:  []string{fixtures[0].InfoHash, fixtures[0].InfoHash},
				Limit:       &zero,
				Page:        &two,
				TotalCount:  &trueValue,
				HasNextPage: &trueValue,
			},
		},
		{
			ID: "large-page-offset",
			Input: torrentFilesOracleInput{
				InfoHashes:  []string{fixtures[0].InfoHash},
				Limit:       &maxInt,
				Page:        &maxInt,
				Offset:      &maxInt,
				TotalCount:  &trueValue,
				HasNextPage: &trueValue,
			},
		},
	}

	for i := range cases {
		files := filterTorrentFiles(allFiles, cases[i].Input.InfoHashes)
		cases[i].Expected = oracleTorrentFilesResult(torrentFilesResult(
			files,
			oracleQueryInput(cases[i].Input),
		))
	}

	reconcileTorrentFilesCorpus(t, cases)
}

func filterTorrentFiles(files []model.TorrentFile, hashes []string) []model.TorrentFile {
	wanted := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		wanted[hash] = struct{}{}
	}
	filtered := make([]model.TorrentFile, 0, len(files))
	for _, file := range files {
		if _, ok := wanted[file.InfoHash.String()]; ok {
			filtered = append(filtered, file)
		}
	}
	return filtered
}

func oracleQueryInput(input torrentFilesOracleInput) TorrentFilesQueryInput {
	query := TorrentFilesQueryInput{}
	for _, raw := range input.InfoHashes {
		hash, err := protocol.ParseID(raw)
		if err != nil {
			panic(err)
		}
		query.InfoHashes = append(query.InfoHashes, hash)
	}
	if input.Limit != nil {
		query.Limit = model.NewNullUint(*input.Limit)
	}
	if input.Page != nil {
		query.Page = model.NewNullUint(*input.Page)
	}
	if input.Offset != nil {
		query.Offset = model.NewNullUint(*input.Offset)
	}
	if input.TotalCount != nil {
		query.TotalCount = model.NewNullBool(*input.TotalCount)
	}
	if input.HasNextPage != nil {
		query.HasNextPage = model.NewNullBool(*input.HasNextPage)
	}
	if input.Cached != nil {
		query.Cached = model.NewNullBool(*input.Cached)
	}
	for _, order := range input.OrderBy {
		mapped := gen.TorrentFilesOrderByInput{Field: order.Field}
		if order.Descending != nil {
			mapped.Descending = graphql.OmittableOf(order.Descending)
		}
		query.OrderBy = append(query.OrderBy, mapped)
	}
	return query
}

func oracleTorrentFilesResult(result search.TorrentFilesResult) torrentFilesOracleResult {
	items := make([]torrentFilesOracleItem, 0, len(result.Items))
	for _, file := range result.Items {
		var extension, fileType *string
		if file.Extension.Valid {
			value := file.Extension.String
			extension = &value
		}
		if value := file.FileType(); value.Valid {
			name := value.FileType.String()
			fileType = &name
		}
		items = append(items, torrentFilesOracleItem{
			InfoHash:  file.InfoHash.String(),
			Index:     file.Index,
			Path:      file.Path,
			Extension: extension,
			FileType:  fileType,
			Size:      file.Size,
			CreatedAt: file.CreatedAt.UTC().Format(time.RFC3339),
			UpdatedAt: file.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}
	return torrentFilesOracleResult{
		TotalCount:  result.TotalCount,
		HasNextPage: result.HasNextPage,
		Items:       items,
	}
}

func loadTorrentFilesFixtures(t *testing.T) []torrentFilesFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(torrentFilesParityDir(t), "fixtures.json"))
	require.NoError(t, err)
	var fixtures []torrentFilesFixture
	require.NoError(t, json.Unmarshal(raw, &fixtures))
	require.NotEmpty(t, fixtures)
	return fixtures
}

func reconcileTorrentFilesCorpus(t *testing.T, cases []torrentFilesOracleCase) {
	t.Helper()
	var actual bytes.Buffer
	for _, fixture := range cases {
		require.NoError(t, json.NewEncoder(&actual).Encode(fixture))
	}
	path := filepath.Join(torrentFilesParityDir(t), "corpus.jsonl")
	if *updateTorrentFilesParity {
		require.NoError(t, os.WriteFile(path, actual.Bytes(), 0o644))
		return
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err, "run with -update-torrent-files-parity to regenerate")
	if bytes.Equal(expected, actual.Bytes()) {
		return
	}
	wantScanner := bufio.NewScanner(bytes.NewReader(expected))
	gotScanner := bufio.NewScanner(bytes.NewReader(actual.Bytes()))
	for line := 1; wantScanner.Scan() || gotScanner.Scan(); line++ {
		if !bytes.Equal(wantScanner.Bytes(), gotScanner.Bytes()) {
			t.Fatalf("torrent.files oracle differs at line %d\nwant: %s\n got: %s", line, wantScanner.Bytes(), gotScanner.Bytes())
		}
	}
	t.Fatal("torrent.files oracle differs")
}

func torrentFilesParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(filepath.Dir(filename), "..", "..", "..", "testdata", "parity", "graphql-torrent-files")
}
