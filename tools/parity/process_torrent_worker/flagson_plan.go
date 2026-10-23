package main

// The plan: run Go's own classifier, in process, over every seeded flags-on
// torrent, loaded the way the processor loads it, against the seed database
// before any content exists and a TMDB client that knows nothing. What each
// classification asks -- its base title, year and content type, and the TMDB
// requests it makes when nothing matches -- is what the content and stub
// fixtures are then generated from.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"go.uber.org/fx"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

// planLocalSearch marks a local content query in plannedTorrent.Calls.
const planLocalSearch = "local"

// plannedTorrent outcomes other than "error".
const (
	outcomeClassified = "classified"
	outcomeDeleted    = "deleted"
)

// plannedCall is one TMDB request, or local content query, a classification
// made.
type plannedCall struct {
	Endpoint string `json:"endpoint"`
	Query    string `json:"query,omitempty"`
	Year     int    `json:"year,omitempty"`
	ID       int64  `json:"id,omitempty"`
	External string `json:"external,omitempty"`
	Source   string `json:"externalSource,omitempty"`
}

// plannedTorrent is what Go's classifier made of one seeded torrent when
// nothing matched.
type plannedTorrent struct {
	Hash      string        `json:"hash"`
	Slice     string        `json:"slice"`
	Outcome   string        `json:"outcome"`
	Error     string        `json:"error,omitempty"`
	Type      string        `json:"type,omitempty"`
	BaseTitle string        `json:"baseTitle,omitempty"`
	Year      int           `json:"year,omitempty"`
	Episodes  bool          `json:"episodes,omitempty"`
	Calls     []plannedCall `json:"calls,omitempty"`
}

type planCallsKey struct{}

type planCalls struct {
	mu    sync.Mutex
	calls []plannedCall
}

func recordPlanCall(ctx context.Context, call plannedCall) {
	if calls, ok := ctx.Value(planCallsKey{}).(*planCalls); ok {
		calls.mu.Lock()
		calls.calls = append(calls.calls, call)
		calls.mu.Unlock()
	}
}

// planTMDB is a TMDB that has nothing: empty searches and finds, and a 404 for
// every details request. It records what it was asked on the request's context.
type planTMDB struct{}

func (planTMDB) ValidateAPIKey(context.Context) error { return nil }

func (planTMDB) SearchMovie(ctx context.Context, r tmdb.SearchMovieRequest) (tmdb.SearchMovieResponse, error) {
	recordPlanCall(ctx, plannedCall{Endpoint: endpointSearchMovie, Query: r.Query, Year: int(r.Year)})
	return tmdb.SearchMovieResponse{Page: 1, TotalPages: 1}, nil
}

func (planTMDB) SearchTv(ctx context.Context, r tmdb.SearchTvRequest) (tmdb.SearchTvResponse, error) {
	recordPlanCall(ctx, plannedCall{Endpoint: endpointSearchTV, Query: r.Query, Year: int(r.FirstAirDateYear)})
	return tmdb.SearchTvResponse{Page: 1, TotalPages: 1}, nil
}

func (planTMDB) MovieDetails(ctx context.Context, r tmdb.MovieDetailsRequest) (tmdb.MovieDetailsResponse, error) {
	recordPlanCall(ctx, plannedCall{Endpoint: "/movie/{id}", ID: r.ID})
	return tmdb.MovieDetailsResponse{}, tmdb.ErrNotFound
}

func (planTMDB) TvDetails(ctx context.Context, r tmdb.TvDetailsRequest) (tmdb.TvDetailsResponse, error) {
	recordPlanCall(ctx, plannedCall{Endpoint: "/tv/{id}", ID: r.SeriesID})
	return tmdb.TvDetailsResponse{}, tmdb.ErrNotFound
}

func (planTMDB) FindByID(ctx context.Context, r tmdb.FindByIDRequest) (tmdb.FindByIDResponse, error) {
	recordPlanCall(ctx, plannedCall{Endpoint: "/find", External: r.ExternalID, Source: r.ExternalSource})
	return tmdb.FindByIDResponse{}, nil
}

// planSearch records each local content query on the request's context.
type planSearch struct {
	search.Search
}

func (p planSearch) Content(ctx context.Context, options ...query.Option) (search.ContentResult, error) {
	recordPlanCall(ctx, plannedCall{Endpoint: planLocalSearch})
	return p.Search.Content(ctx, options...)
}

// planLifecycle satisfies fx.Lifecycle for a classifier built outside fx. The
// hooks only manage tape recording, which the plan never enables.
type planLifecycle struct{}

func (planLifecycle) Append(fx.Hook) {}

// newPlanRunner builds Go's classifier exactly as the processor gets it, with
// the default configuration plus whatever classifier.yml the XDG config dir or
// the working directory supplies, and returns it with its effective config
// digest.
func newPlanRunner(db *gorm.DB) (classifier.Runner, string, error) {
	daoQuery := dao.Use(db)
	searchResource := search.New(search.Params{Query: lazy.New(func() (*dao.Query, error) {
		return daoQuery, nil
	})})

	result := classifier.New(classifier.Params{
		Config:     classifier.NewDefaultConfig(),
		TmdbConfig: tmdb.NewDefaultConfig(),
		Search: lazy.New(func() (search.Search, error) {
			s, err := searchResource.Search.Get()
			return planSearch{s}, err
		}),
		TmdbClient: lazy.New(func() (tmdb.Client, error) { return planTMDB{}, nil }),
		Lifecycle:  planLifecycle{},
	})

	source, err := result.Source.Get()
	if err != nil {
		return nil, "", err
	}

	digest, err := classifier.EffectiveConfigDigest(source, classifier.NewDefaultConfig().Workflow)
	if err != nil {
		return nil, "", err
	}

	runner, err := result.Runner.Get()

	return runner, digest, err
}

// planTorrents classifies every hash with flags on, loading each 100-hash batch
// through the processor's own search and preloads.
func planTorrents(
	ctx context.Context,
	db *gorm.DB,
	runner classifier.Runner,
	hashes []protocol.ID,
	sliceOf map[protocol.ID]string,
) ([]plannedTorrent, error) {
	daoQuery := dao.Use(db)
	searchResource := search.New(search.Params{Query: lazy.New(func() (*dao.Query, error) {
		return daoQuery, nil
	})})

	s, err := searchResource.Search.Get()
	if err != nil {
		return nil, err
	}

	planned := make([]plannedTorrent, 0, len(hashes))

	for i := 0; i < len(hashes); i += 100 {
		batch := hashes[i:min(i+100, len(hashes))]

		result, err := s.TorrentsWithMissingInfoHashes(ctx, batch, query.Preload(func(q *dao.Query) []field.RelationField {
			return []field.RelationField{q.Torrent.Hint.RelationField, q.Torrent.Sources.RelationField}
		}))
		if err != nil {
			return nil, err
		}

		if len(result.MissingInfoHashes) > 0 {
			return nil, fmt.Errorf("plan: %d seeded torrents not found", len(result.MissingInfoHashes))
		}

		out := make([]plannedTorrent, len(result.Torrents))

		var wg sync.WaitGroup

		for j, torrent := range result.Torrents {
			wg.Add(1)

			go func(j int, torrent model.Torrent) {
				defer wg.Done()

				out[j] = planTorrent(ctx, runner, torrent, sliceOf[torrent.InfoHash])
			}(j, torrent)
		}

		wg.Wait()

		planned = append(planned, out...)
	}

	return planned, nil
}

func planTorrent(ctx context.Context, runner classifier.Runner, torrent model.Torrent, slice string) plannedTorrent {
	calls := &planCalls{}
	cl, err := runner.Run(context.WithValue(ctx, planCallsKey{}, calls), "default", flagsOnFlags(), torrent)

	p := plannedTorrent{Hash: torrent.InfoHash.String(), Slice: slice, Calls: calls.calls}

	switch {
	case errors.Is(err, classification.ErrDeleteTorrent):
		p.Outcome, p.Error = outcomeDeleted, err.Error() // names the workflow rule that deleted
		return p
	case err != nil:
		p.Outcome, p.Error = "error", err.Error()
		return p
	}

	p.Outcome = outcomeClassified

	if cl.ContentType.Valid {
		p.Type = cl.ContentType.ContentType.String()
	}

	if cl.BaseTitle.Valid {
		p.BaseTitle = cl.BaseTitle.String
	}

	p.Year = int(cl.Date.Year)
	p.Episodes = len(cl.Episodes) > 0

	return p
}
