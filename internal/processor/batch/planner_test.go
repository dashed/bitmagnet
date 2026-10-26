package batch

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
)

func plannerID(last byte) protocol.ID {
	var id protocol.ID
	id[len(id)-1] = last
	return id
}

func TestPlannerStopsOnShortPageWithoutContinuation(t *testing.T) {
	planner := NewPlanner(MessageParams{
		InfoHashGreaterThan: plannerID(0xaa),
		BatchSize:           3,
		ChunkSize:           10,
	})
	require.Equal(t, plannerID(0xaa), planner.MaxInfoHash())
	_, err := planner.AddPage([]protocol.ID{plannerID(0xab), plannerID(0xac)})
	require.NoError(t, err)
	require.False(t, planner.ShouldQuery())

	plan, err := planner.Finalize()
	require.NoError(t, err)
	require.True(t, plan.Done)
	require.Equal(t, uint(2), plan.ChunkSize)
	require.Equal(t, plannerID(0xac), plan.MaxInfoHash)
	require.Len(t, plan.Jobs, 1)
	require.Equal(t, 10, plan.Jobs[0].Priority)
	job, err := plan.Jobs[0].QueueJob()
	require.NoError(t, err)
	require.Equal(t, processor.MessageName, job.Queue)
}

func TestPlannerStopsOnEmptyPage(t *testing.T) {
	planner := NewPlanner(MessageParams{BatchSize: 2, ChunkSize: 10})
	_, err := planner.AddPage([]protocol.ID{plannerID(1), plannerID(2)})
	require.NoError(t, err)
	require.True(t, planner.ShouldQuery())
	_, err = planner.AddPage(nil)
	require.NoError(t, err)

	plan, err := planner.Finalize()
	require.NoError(t, err)
	require.True(t, plan.Done)
	require.Len(t, plan.Jobs, 1)
}

func TestPlannerOvershootsChunkByOnePageAndContinues(t *testing.T) {
	message := MessageParams{
		ClassifyMode:       processor.ClassifyModeRematch,
		ClassifierWorkflow: "custom",
		ClassifierFlags: classifier.Flags{
			"apis_enabled":         false,
			"local_search_enabled": true,
		},
		ChunkSize:    3,
		BatchSize:    2,
		ContentTypes: []model.NullContentType{model.NewNullContentType(nil)},
		Orphans:      true,
	}
	planner := NewPlanner(message)
	_, err := planner.AddPage([]protocol.ID{plannerID(1), plannerID(2)})
	require.NoError(t, err)
	require.True(t, planner.ShouldQuery())
	_, err = planner.AddPage([]protocol.ID{plannerID(3), plannerID(4)})
	require.NoError(t, err)
	require.False(t, planner.ShouldQuery())

	plan, err := planner.Finalize()
	require.NoError(t, err)
	require.False(t, plan.Done)
	require.Equal(t, uint(4), plan.ChunkSize)
	require.Equal(t, plannerID(4), plan.MaxInfoHash)
	require.Len(t, plan.Jobs, 3)
	require.Equal(t, 4, plan.Jobs[0].Priority)
	require.Equal(t, 4, plan.Jobs[1].Priority)
	continuationJob, err := plan.Jobs[2].QueueJob()
	require.NoError(t, err)
	require.Equal(t, MessageName, continuationJob.Queue)

	var continuation MessageParams
	require.NoError(t, json.Unmarshal([]byte(continuationJob.Payload), &continuation))
	require.Equal(t, plannerID(4), continuation.InfoHashGreaterThan)
	require.Len(t, continuation.ContentTypes, 1)
	require.False(t, continuation.ContentTypes[0].Valid)
	require.True(t, continuation.Orphans)
}

func TestPlannerSelectionFreezesEveryDatabasePredicate(t *testing.T) {
	updatedBefore := time.Date(2026, time.August, 12, 4, 5, 6, 123456789, time.UTC)
	message := MessageParams{
		InfoHashGreaterThan: plannerID(10),
		UpdatedBefore:       updatedBefore,
		BatchSize:           2,
		ChunkSize:           10,
		ContentTypes: []model.NullContentType{
			model.NewNullContentType(model.ContentTypeMovie),
			model.NewNullContentType(nil),
		},
		Orphans: true,
	}
	planner := NewPlanner(message)
	require.Equal(t, Selection{
		AfterExclusive: plannerID(10),
		UpdatedBefore:  updatedBefore,
		ContentTypes:   message.ContentTypes,
		Orphans:        true,
		OrderBy:        "info_hash_asc",
		Limit:          2,
	}, planner.Selection())

	_, err := planner.AddPage([]protocol.ID{plannerID(11), plannerID(12)})
	require.NoError(t, err)
	require.Equal(t, plannerID(12), planner.Selection().AfterExclusive)
}

func TestPlannerFinalizesOnce(t *testing.T) {
	planner := NewPlanner(MessageParams{BatchSize: 1, ChunkSize: 1})
	_, err := planner.AddPage([]protocol.ID{plannerID(1)})
	require.NoError(t, err)
	_, err = planner.Finalize()
	require.NoError(t, err)
	_, err = planner.Finalize()
	require.ErrorIs(t, err, errPlannerAlreadyFinalized)
	_, err = planner.AddPage(nil)
	require.ErrorIs(t, err, errPlannerAlreadyFinalized)
}

func TestPlannerRejectsPagesOutsideTheOrderedKeysetContract(t *testing.T) {
	for name, page := range map[string][]protocol.ID{
		"cursor regression": {plannerID(9), plannerID(11)},
		"duplicate":         {plannerID(11), plannerID(11)},
		"descending":        {plannerID(12), plannerID(11)},
		"oversized":         {plannerID(11), plannerID(12), plannerID(13)},
	} {
		t.Run(name, func(t *testing.T) {
			planner := NewPlanner(MessageParams{
				InfoHashGreaterThan: plannerID(10),
				BatchSize:           2,
				ChunkSize:           10,
			})
			_, err := planner.AddPage(page)
			require.ErrorIs(t, err, errInvalidPlannerPage)
		})
	}
}
