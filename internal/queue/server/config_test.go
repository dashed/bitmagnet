package server

import (
	"reflect"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/config"
	"github.com/bitmagnet-io/bitmagnet/internal/config/configresolver"
	"github.com/go-playground/validator/v10"
)

func TestDefaultConfigLeavesEveryQueueEnabled(t *testing.T) {
	t.Parallel()

	if got := NewDefaultConfig().DisabledQueues; len(got) != 0 {
		t.Fatalf("default disabled queues = %v, want empty", got)
	}
}

func TestConfigResolvesDisabledQueuesFromEnv(t *testing.T) {
	t.Parallel()

	result, err := config.New(config.Params{
		Specs: []config.Spec{{
			Key:          "queue_server",
			DefaultValue: NewDefaultConfig(),
		}},
		Resolvers: []configresolver.Resolver{
			configresolver.NewEnv(map[string]string{
				"QUEUE_SERVER_DISABLED_QUEUES": "process_torrent_batch,blob_migration",
			}),
		},
		Validate: validator.New(),
	})
	if err != nil {
		t.Fatalf("resolve queue server config: %v", err)
	}

	got, ok := result.Resolved.NodeMap["queue_server"].Value.(Config)
	if !ok {
		t.Fatalf("resolved queue_server value has type %T, want server.Config", result.Resolved.NodeMap["queue_server"].Value)
	}
	want := []string{"process_torrent_batch", "blob_migration"}
	if !reflect.DeepEqual(got.DisabledQueues, want) {
		t.Fatalf("disabled queues = %v, want %v", got.DisabledQueues, want)
	}
}
