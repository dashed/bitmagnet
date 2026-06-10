package gqlmodel

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
)

func relevanceOnly() maps.InsertMap[search.TorrentContentOrderBy, search.OrderDirection] {
	m := maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection]()
	m.Set(search.TorrentContentOrderByRelevance, search.OrderDirectionDescending)

	return m
}

func TestFind2PopularitySortDefault(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })

	t.Run("flag off leaves relevance untouched", func(t *testing.T) {
		search.SetFeatureFlags(search.FeatureFlags{PopularitySortDefault: false})

		got := find2PopularitySortDefault(relevanceOnly(), true)
		if !got.Has(search.TorrentContentOrderByRelevance) {
			t.Error("relevance should be preserved when flag is off")
		}
	})

	t.Run("flag on rewrites lone relevance to seeders", func(t *testing.T) {
		search.SetFeatureFlags(search.FeatureFlags{PopularitySortDefault: true})

		got := find2PopularitySortDefault(relevanceOnly(), true)

		if got.Has(search.TorrentContentOrderByRelevance) {
			t.Error("relevance should have been removed")
		}

		dir, ok := got.Get(search.TorrentContentOrderBySeeders)
		if !ok {
			t.Fatal("expected seeders ordering")
		}

		if dir != search.OrderDirectionDescending {
			t.Errorf("seeders direction = %v, want descending", dir)
		}
	})

	t.Run("flag on but no query string is untouched", func(t *testing.T) {
		search.SetFeatureFlags(search.FeatureFlags{PopularitySortDefault: true})

		got := find2PopularitySortDefault(relevanceOnly(), false)
		if !got.Has(search.TorrentContentOrderByRelevance) {
			t.Error("no query string ⇒ relevance must be preserved")
		}
	})

	t.Run("explicit multi-field order is opt-in and preserved", func(t *testing.T) {
		search.SetFeatureFlags(search.FeatureFlags{PopularitySortDefault: true})

		m := relevanceOnly()
		m.Set(search.TorrentContentOrderByPublishedAt, search.OrderDirectionDescending)

		got := find2PopularitySortDefault(m, true)
		if !got.Has(search.TorrentContentOrderByRelevance) {
			t.Error("relevance combined with an explicit field is an opt-in and must be preserved")
		}
	})

	t.Run("non-relevance lone order is untouched", func(t *testing.T) {
		search.SetFeatureFlags(search.FeatureFlags{PopularitySortDefault: true})

		m := maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection]()
		m.Set(search.TorrentContentOrderBySize, search.OrderDirectionDescending)

		got := find2PopularitySortDefault(m, true)
		if !got.Has(search.TorrentContentOrderBySize) {
			t.Error("a user's explicit size order must be preserved")
		}
	})
}
