package classifier

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/postgres"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const tapeProgressLogInterval = time.Minute

type Params struct {
	fx.In
	Config     Config
	TmdbConfig tmdb.Config
	Search     lazy.Lazy[search.Search]
	TmdbClient lazy.Lazy[tmdb.Client]
	Lifecycle  fx.Lifecycle
	Logger     *zap.SugaredLogger `optional:"true"`
	// PostgresConfig only names the database a tape was recorded against, in
	// that tape's provenance. Optional so the classifier still builds in graphs
	// that do not wire the database config.
	PostgresConfig postgres.Config `optional:"true"`
}

type Result struct {
	fx.Out
	Compiler lazy.Lazy[Compiler]
	Source   lazy.Lazy[Source]
	Runner   lazy.Lazy[Runner]
}

func New(params Params) Result {
	lsrc := lazy.New[Source](func() (Source, error) {
		src, err := newSourceProvider(params.Config, params.TmdbConfig).source()
		if err != nil {
			return Source{}, err
		}

		if _, ok := src.Workflows[params.Config.Workflow]; !ok {
			return Source{}, fmt.Errorf("default workflow '%s' not found", params.Config.Workflow)
		}
		if err := rejectReservedTapeEvidenceWorkflows(src); err != nil {
			return Source{}, err
		}

		return src, nil
	})
	// One Recorder is shared by every classification and by both observation
	// seams, so a classification's local searches and TMDB calls land in the
	// same tape in the order they were made. It resolves to nil unless recording
	// is configured, which leaves every seam on its normal path.
	//
	// It is lazy because the digest it is keyed by comes from the resolved
	// source, and shared through a lazy rather than a captured variable so the
	// compiler and the shutdown hook see the same one without racing for it.
	lrec := lazy.New(func() (*tape.Recorder, error) {
		src, err := lsrc.Get()
		if err != nil {
			return nil, err
		}

		digest, err := logEffectiveConfigDigest(params.Logger, src, params.Config.Workflow)
		if err != nil {
			return nil, err
		}

		return newTapeRecorder(params, digest), nil
	})
	lc := lazy.New(func() (Compiler, error) {
		s, err := params.Search.Get()
		if err != nil {
			return nil, err
		}

		tmdbClient, err := params.TmdbClient.Get()
		if err != nil {
			return nil, err
		}

		recorder, err := lrec.Get()
		if err != nil {
			return nil, err
		}

		return compiler{
			options: []compilerOption{
				compilerFeatures(defaultFeatures),
				celEnvOption,
			},
			dependencies: dependencies{
				search: localSearchSemaphore{
					search:    localSearch{s},
					semaphore: make(chan struct{}, 1),
				},
				tmdbClient: tmdbClient,
			},
			recorder: recorder,
		}, nil
	})

	// Written on a clean shutdown as well as when the record cap is reached, so
	// a run that is stopped early still leaves its evidence behind. A bounded
	// evidence run is intentionally long-lived; while it is active, structured
	// progress logs expose exact registered/open/authoritative/action counts to
	// the no-exec Kubernetes controller.
	var (
		progressCancel context.CancelFunc
		progressDone   chan struct{}
	)
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(startCtx context.Context) error {
			planConfigured, err := tapePlanConfigured(params.Config)
			if err != nil {
				return err
			}
			if params.Config.TapeDir == "" {
				return nil
			}

			recorder, err := lrec.Get()
			if err != nil {
				return err
			}
			if recorder == nil {
				return nil
			}
			if planConfigured {
				source, sourceErr := lsrc.Get()
				if sourceErr != nil {
					return sourceErr
				}
				executor, executorErr := newTapeAcquisitionPlanExecutor(params.Config, source, recorder)
				if executorErr != nil {
					return executorErr
				}
				if runErr := executor.Run(startCtx); runErr != nil {
					return fmt.Errorf("execute classifier tape acquisition plan: %w", runErr)
				}
			}

			progressCtx, cancel := context.WithCancel(context.Background())
			progressCancel = cancel
			progressDone = make(chan struct{})
			logTapeProgress(params.Logger, params.Config.TapeDir, recorder)
			go func() {
				defer close(progressDone)
				logTapeProgressUntilDone(
					progressCtx,
					params.Logger,
					params.Config.TapeDir,
					recorder,
					tapeProgressLogInterval,
				)
			}()

			return nil
		},
		OnStop: func(context.Context) error {
			if progressCancel != nil {
				progressCancel()
			}
			if progressDone != nil {
				<-progressDone
			}
			return lrec.IfInitialized(func(recorder *tape.Recorder) error {
				return writeTape(params.Logger, params.Config.TapeDir, recorder)
			})
		},
	})

	return Result{
		Compiler: lc,
		Source:   lsrc,
		Runner: lazy.New(func() (Runner, error) {
			src, err := lsrc.Get()
			if err != nil {
				return nil, err
			}
			c, err := lc.Get()
			if err != nil {
				return nil, err
			}
			r, err := c.Compile(src)
			if err != nil {
				return nil, err
			}

			return runnerSemaphore{
				runner:    r,
				semaphore: make(chan struct{}, params.Config.Concurrency),
			}, nil
		}),
	}
}

// newTapeRecorder returns nil -- recording off -- unless a tape directory is
// configured.
//
// The recorder is keyed by the effective classifier config digest, so a tape
// carries the configuration it was recorded under and a replay against a
// different configuration fails closed instead of quietly comparing against a
// stale oracle.
func newTapeRecorder(params Params, digest string) *tape.Recorder {
	if params.Config.TapeDir == "" {
		return nil
	}

	host, _ := os.Hostname()

	recorder := tape.NewRecorder(digest, params.Config.TapeMaxRecords, tape.Provenance{
		Command:               "bitmagnet classifier (CLASSIFIER_TAPE_DIR set)",
		Host:                  host,
		AcquisitionPlanDigest: params.Config.TapePlanSHA256,
		// Enough to tell two database snapshots apart, and no credentials.
		Database: fmt.Sprintf(
			"postgres %s:%d/%s",
			params.PostgresConfig.Host,
			params.PostgresConfig.Port,
			params.PostgresConfig.Name,
		),
		ScopeLimits: TapeScopeLimits,
		Notes:       "Recorded live from the running classifier.",
	})

	if params.Logger != nil {
		params.Logger.Warnw(
			"classifier observation recording is ENABLED; this is an offline evidence mode, not a serving mode",
			"tape_dir", params.Config.TapeDir,
			"effective_config_digest", digest,
		)
	}

	// A bounded recording run is finished the moment the cap is reached.
	// Writing then, rather than waiting for a clean shutdown, means an
	// ungracefully killed process still leaves the evidence behind.
	recorder.OnFull(func() {
		if err := writeTape(params.Logger, params.Config.TapeDir, recorder); err != nil && params.Logger != nil {
			params.Logger.Errorw("failed to write classifier tape", "error", err)
		}
	})

	return recorder
}

func writeTape(logger *zap.SugaredLogger, dir string, recorder *tape.Recorder) error {
	if recorder == nil || dir == "" {
		return nil
	}

	if err := recorder.Write(dir, time.Now()); err != nil {
		return err
	}

	if logger != nil {
		progress := recorder.Progress()
		logger.Infow(
			"wrote classifier tape",
			"tape_dir", dir,
			"truncated", progress.Truncated,
			"registered_records", progress.RegisteredRecords,
			"open_sessions", progress.OpenSessions,
			"authoritative_records", progress.AuthoritativeRecords,
			"non_authoritative_records", progress.NonAuthoritativeRecords,
			"observation_count", progress.ObservationCount,
			"action_entry_count", progress.ActionEntryCount,
			"action_entry_counts", progress.ActionEntryCounts,
			"record_outcome_counts", progress.RecordOutcomeCounts,
			"write_attempt", progress.LastWrite.Attempt,
			"write_final", progress.LastWrite.Final,
			"acquisition_plan_digest", progress.AcquisitionPlanDigest,
		)
	}

	return nil
}

func logTapeProgressUntilDone(
	ctx context.Context,
	logger *zap.SugaredLogger,
	dir string,
	recorder *tape.Recorder,
	interval time.Duration,
) {
	if logger == nil || recorder == nil || interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logTapeProgress(logger, dir, recorder)
		}
	}
}

func logTapeProgress(logger *zap.SugaredLogger, dir string, recorder *tape.Recorder) {
	if logger == nil || recorder == nil {
		return
	}

	progress := recorder.Progress()
	logger.Infow(
		"classifier tape progress",
		"tape_dir", dir,
		"registered_records", progress.RegisteredRecords,
		"open_sessions", progress.OpenSessions,
		"authoritative_records", progress.AuthoritativeRecords,
		"non_authoritative_records", progress.NonAuthoritativeRecords,
		"observation_count", progress.ObservationCount,
		"action_entry_count", progress.ActionEntryCount,
		"action_entry_counts", progress.ActionEntryCounts,
		"record_outcome_counts", progress.RecordOutcomeCounts,
		"truncated", progress.Truncated,
		"error", progress.Error,
		"write_attempt", progress.LastWrite.Attempt,
		"write_final", progress.LastWrite.Final,
		"write_error", progress.LastWrite.Error,
		"acquisition_plan_digest", progress.AcquisitionPlanDigest,
	)
}
