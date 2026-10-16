package classifier

import (
	"context"
	"errors"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
)

// Each attach action has guards that can return before any dependency call.
// T1 counts action ENTRY, so the evidence hook must run before those guards,
// not merely beside the local-search/TMDB seam.
func TestAttachActionsRecordEntryBeforeEarlyReturn(t *testing.T) {
	tests := []struct {
		name       string
		definition actionDefinition
	}{
		{attachLocalContentByIDName, attachLocalContentByIDAction{}},
		{attachLocalContentBySearchName, attachLocalContentBySearchAction{}},
		{attachTMDBContentByIDName, attachTMDBContentByIDAction{}},
		{attachTmdbContentBySearchName, attachTmdbContentBySearchAction{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiled, err := test.definition.compileAction(compilerContext{source: test.name})
			if err != nil {
				t.Fatalf("compile: %v", err)
			}

			recorder := tape.NewRecorder("sha256:test", 1, tape.Provenance{})
			ctx := recorder.Begin(
				context.Background(),
				test.name,
				"default",
				nil,
				nil,
				tape.ProcessorState{},
			)
			_, runErr := compiled.run(executionContext{Context: ctx})
			if !errors.Is(runErr, classification.ErrUnmatched) {
				t.Fatalf("early return = %v, want unmatched", runErr)
			}
			tape.EndSession(ctx, tape.RecordOutcome{Kind: tape.RecordUnmatched})

			records, err := recorder.Records()
			if err != nil {
				t.Fatalf("records: %v", err)
			}
			if len(records) != 1 || len(records[0].ActionEntries) != 1 ||
				records[0].ActionEntries[0].Name != test.name {
				t.Fatalf("action entry was not captured before the guard: %+v", records)
			}
		})
	}
}
