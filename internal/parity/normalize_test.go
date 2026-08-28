package parity

import (
	"encoding/json"
	"testing"
)

// The canonicalizer is the foundation every differential comparison rests on:
// a bug here produces FALSE parity passes. These cases mirror the Rust crate's
// (bitmagnet-diff normalize.rs) so both sides pin identical semantics.
func TestCanonicalJSONSortsObjectKeysRecursively(t *testing.T) {
	t.Parallel()

	got, err := CanonicalJSON(json.RawMessage(`{"b":{"z":1,"a":2},"a":[{"y":1,"x":2}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"a":[{"x":2,"y":1}],"b":{"a":2,"z":1}}`; string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSONPreservesArrayOrderAndScalars(t *testing.T) {
	t.Parallel()

	got, err := CanonicalJSON(json.RawMessage(`[3,1,2,{"k":null},"s",true]`))
	if err != nil {
		t.Fatal(err)
	}
	if want := `[3,1,2,{"k":null},"s",true]`; string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}
