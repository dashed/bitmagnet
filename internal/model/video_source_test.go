package model

import (
	"strings"
	"testing"
)

func TestCreateVideoSourceRegexPrefersSpecificAlias(t *testing.T) {
	t.Parallel()

	for i := 0; i < 128; i++ {
		match := createVideoSourceRegex().FindStringSubmatch(
			"Another Hinted Movie (2005) 720p WEB-DL x265-GRP.mkv",
		)
		if len(match) < 2 {
			t.Fatal("expected video source match")
		}
		if got := strings.ToLower(match[1]); got != "web-dl" {
			t.Fatalf("expected specific web-dl alias, got %q", got)
		}
	}
}
