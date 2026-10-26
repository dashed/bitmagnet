package classifier

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	tmdb_mocks "github.com/bitmagnet-io/bitmagnet/internal/tmdb/mocks"
)

// recordingLocalSearch misses every lookup and records what it was asked.
type recordingLocalSearch struct {
	calls []string
}

func (s *recordingLocalSearch) ContentByID(_ context.Context, ref model.ContentRef) (model.Content, error) {
	s.calls = append(s.calls, fmt.Sprintf("ContentByID(%s,%s,%s)", ref.Type, ref.Source, ref.ID))
	return model.Content{}, classification.ErrUnmatched
}

func (s *recordingLocalSearch) ContentBySearch(
	_ context.Context,
	contentType model.ContentType,
	baseTitle string,
	year model.Year,
) (model.Content, error) {
	s.calls = append(s.calls, fmt.Sprintf("ContentBySearch(%s,%q,%d)", contentType, baseTitle, year))
	return model.Content{}, classification.ErrUnmatched
}

// TestHintTitleAndReleaseYearDoNotReachClassification proves, over the whole
// classifier corpus, that `torrent_hints.title` and `release_year` change
// neither the classification nor a single local search argument. The Rust
// processor does not load those columns at all, so this is what entitles it to.
func TestHintTitleAndReleaseYearDoNotReachClassification(t *testing.T) {
	inputs := loadClassifierInputs(t, filepath.Join(classifierCorpusDir, "inputs.jsonl"))

	search := &recordingLocalSearch{}
	hintCompiler := compiler{
		options: []compilerOption{
			compilerFeatures(defaultFeatures),
			celEnvOption,
		},
		dependencies: dependencies{
			search: search,
			// TMDB stays disabled below, so any call fails the test.
			tmdbClient: tmdb_mocks.NewClient(t),
		},
	}

	source, err := yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}.source()
	if err != nil {
		t.Fatalf("load core classifier source: %v", err)
	}

	workflow, err := hintCompiler.Compile(source)
	if err != nil {
		t.Fatalf("compile core classifier source: %v", err)
	}

	hints := map[string]func(model.Torrent) model.TorrentHint{
		"corpus": func(torrent model.Torrent) model.TorrentHint {
			if torrent.Hint.IsNil() {
				return model.TorrentHint{ContentType: model.ContentTypeMovie}
			}
			return torrent.Hint
		},
		"sourced_enriched": func(model.Torrent) model.TorrentHint {
			return model.TorrentHint{
				ContentType:     model.ContentTypeMovie,
				ContentSource:   model.NewNullString("imdb"),
				ContentID:       model.NewNullString("tt0133093"),
				Languages:       model.Languages{model.Language("en"): {}},
				Episodes:        model.Episodes{}.AddEpisode(1, 2),
				VideoResolution: model.NewNullVideoResolution(model.VideoResolutionV1080p),
				VideoCodec:      model.NewNullVideoCodec(model.VideoCodecXviD),
				ReleaseGroup:    model.NewNullString("GRP"),
			}
		},
		"type_only_tv": func(model.Torrent) model.TorrentHint {
			return model.TorrentHint{ContentType: model.ContentTypeTvShow}
		},
	}

	runs := 0
	for _, flags := range []Flags{
		{"local_search_enabled": false, "apis_enabled": false, "tmdb_enabled": false},
		{"local_search_enabled": true, "apis_enabled": false, "tmdb_enabled": false},
	} {
		for _, input := range inputs {
			for name, hint := range hints {
				plain := toTorrent(input)
				plain.Hint = hint(plain)

				titled := plain
				titled.Hint.Title = model.NewNullString("Some Unrelated Title 1901")
				titled.Hint.ReleaseYear = 1901

				search.calls = nil
				plainResult, plainErr := workflow.Run(context.Background(), "default", flags, plain)
				plainCalls := search.calls

				search.calls = nil
				titledResult, titledErr := workflow.Run(context.Background(), "default", flags, titled)
				titledCalls := search.calls

				if got, want := NormalizeResult(titledResult, titledErr), NormalizeResult(plainResult, plainErr); !reflect.DeepEqual(got, want) {
					t.Fatalf("%s/%s/%v: title and release_year changed the result:\n got: %+v\nwant: %+v",
						input.ID, name, flags, got, want)
				}
				if !reflect.DeepEqual(titledCalls, plainCalls) {
					t.Fatalf("%s/%s/%v: title and release_year changed the local searches:\n got: %v\nwant: %v",
						input.ID, name, flags, titledCalls, plainCalls)
				}
				runs++
			}
		}
	}
	if runs != 2*len(inputs)*len(hints) {
		t.Fatalf("ran %d comparisons", runs)
	}
}
