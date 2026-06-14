package filesearch

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestEscapeLikePattern(t *testing.T) {
	cases := map[string]string{
		`plain`:      `plain`,
		`50%`:        `50\%`,
		`a_b`:        `a\_b`,
		`back\slash`: `back\\slash`,
		`%_\`:        `\%\_\\`,
		`x\%y`:       `x\\\%y`, // backslash escaped first, then %
		`音楽_test%`:   `音楽\_test\%`,
	}

	for in, want := range cases {
		if got := EscapeLikePattern(in); got != want {
			t.Errorf("EscapeLikePattern(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewPathTypeaheadInput_MinChars(t *testing.T) {
	if _, err := NewPathTypeaheadInput("a", 0); !errors.Is(err, ErrPrefixTooShort) {
		t.Errorf("1-char prefix: err = %v, want ErrPrefixTooShort", err)
	}

	if _, err := NewPathTypeaheadInput("  a  ", 0); !errors.Is(err, ErrPrefixTooShort) {
		t.Errorf("trimmed 1-char prefix: err = %v, want ErrPrefixTooShort", err)
	}

	in, err := NewPathTypeaheadInput("ab", 0)
	if err != nil {
		t.Fatalf("2-char prefix: unexpected err %v", err)
	}

	if in.Limit != DefaultTypeaheadLimit {
		t.Errorf("limit = %d, want default %d", in.Limit, DefaultTypeaheadLimit)
	}
}

func TestNewPathTypeaheadInput_EscapesAndCaps(t *testing.T) {
	in, err := NewPathTypeaheadInput("50%_x", 999)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	if in.PrefixLikePattern != `50\%\_x` {
		t.Errorf("PrefixLikePattern = %q, want %q", in.PrefixLikePattern, `50\%\_x`)
	}

	if in.Limit != MaxTypeaheadLimit {
		t.Errorf("limit = %d, want clamped %d", in.Limit, MaxTypeaheadLimit)
	}

	long := strings.Repeat("a", MaxPrefixLen+50)

	capped, err := NewPathTypeaheadInput(long, 5)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	if len([]rune(capped.Prefix)) != MaxPrefixLen {
		t.Errorf("prefix len = %d, want capped %d", len([]rune(capped.Prefix)), MaxPrefixLen)
	}
}

func TestNewFileSearchInput_RejectsUnconstrained(t *testing.T) {
	if _, err := NewFileSearchInput(FileSearchParams{}); !errors.Is(err, ErrEmptyQuery) {
		t.Errorf("empty params: err = %v, want ErrEmptyQuery", err)
	}

	// A lone extension filter is enough to constrain.
	if _, err := NewFileSearchInput(FileSearchParams{Extensions: []string{"mkv"}}); err != nil {
		t.Errorf("extension-only: unexpected err %v", err)
	}

	// A size bound alone is enough.
	if _, err := NewFileSearchInput(FileSearchParams{MinSize: 1}); err != nil {
		t.Errorf("size-only: unexpected err %v", err)
	}
}

func TestNewFileSearchInput_NormalizesExtensions(t *testing.T) {
	in, err := NewFileSearchInput(FileSearchParams{
		Query:      "  hello  ",
		Extensions: []string{".MKV", "mkv", " MP4 ", "", "."},
	})
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	if in.Query != "hello" {
		t.Errorf("Query = %q, want trimmed %q", in.Query, "hello")
	}

	want := []string{"mkv", "mp4"}
	if len(in.Extensions) != len(want) {
		t.Fatalf("Extensions = %v, want %v", in.Extensions, want)
	}

	for i := range want {
		if in.Extensions[i] != want[i] {
			t.Errorf("Extensions[%d] = %q, want %q", i, in.Extensions[i], want[i])
		}
	}

	if in.Limit != DefaultLimit {
		t.Errorf("limit = %d, want default %d", in.Limit, DefaultLimit)
	}
}

func TestNewFileSearchInput_CapsQueryAndClampsLimit(t *testing.T) {
	long := strings.Repeat("z", MaxQueryLen+100)

	in, err := NewFileSearchInput(FileSearchParams{Query: long, Limit: 10_000})
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	if len([]rune(in.Query)) != MaxQueryLen {
		t.Errorf("query len = %d, want capped %d", len([]rune(in.Query)), MaxQueryLen)
	}

	if in.Limit != MaxLimit {
		t.Errorf("limit = %d, want clamped %d", in.Limit, MaxLimit)
	}
}

func TestDisabledClient(t *testing.T) {
	c := Disabled()

	if _, err := c.FileSearch(context.Background(), FileSearchInput{}); !errors.Is(err, ErrDisabled) {
		t.Errorf("FileSearch err = %v, want ErrDisabled", err)
	}

	if _, err := c.PathTypeahead(context.Background(), PathTypeaheadInput{}); !errors.Is(err, ErrDisabled) {
		t.Errorf("PathTypeahead err = %v, want ErrDisabled", err)
	}
}
