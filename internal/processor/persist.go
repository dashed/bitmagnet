package processor

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"github.com/bitmagnet-io/bitmagnet/internal/slice"
	"gorm.io/gorm/clause"
)

// searchIndexTimeout bounds a single background dual-write batch to the Tantivy
// sidecar.
const searchIndexTimeout = 30 * time.Second

type persistPayload struct {
	torrentContents  []model.TorrentContent
	deleteIDs        []string
	deleteInfoHashes []protocol.ID
	addTags          map[protocol.ID]map[string]struct{}
}

func (c processor) persist(ctx context.Context, payload persistPayload) error {
	contentsMap := make(map[model.ContentRef]struct{}, len(payload.torrentContents))
	contentsPtr := make([]*model.Content, 0, len(payload.torrentContents))
	torrentContentsPtr := make([]*model.TorrentContent, 0, len(payload.torrentContents))
	torrentTagsPtr := make([]*model.TorrentTag, 0, len(payload.addTags))

	for _, tc := range payload.torrentContents {
		tcCopy := tc
		tcCopy.Torrent = model.Torrent{}

		if tcCopy.ContentID.Valid && tcCopy.Content.CreatedAt.IsZero() {
			contentRef := tcCopy.Content.Ref()
			if _, ok := contentsMap[contentRef]; !ok {
				contentsMap[contentRef] = struct{}{}
				contentCopy := tcCopy.Content
				contentsPtr = append(contentsPtr, &contentCopy)
			}
		}

		tcCopy.Content = model.Content{}
		torrentContentsPtr = append(torrentContentsPtr, &tcCopy)
	}

	for infoHash, tags := range payload.addTags {
		for tag := range tags {
			torrentTagsPtr = append(torrentTagsPtr, &model.TorrentTag{
				InfoHash: infoHash,
				Name:     tag,
			})
		}
	}

	if len(payload.deleteInfoHashes) > 0 {
		if blockErr := c.blockingManager.Block(ctx, payload.deleteInfoHashes, false); blockErr != nil {
			return blockErr
		}
	}

	err := c.dao.Transaction(func(tx *dao.Query) error {
		if len(contentsPtr) > 0 {
			if createContentErr := tx.Content.WithContext(ctx).Clauses(
				clause.OnConflict{
					UpdateAll: true,
				}).CreateInBatches(contentsPtr, 100); createContentErr != nil {
				return createContentErr
			}
		}

		if len(payload.deleteIDs) > 0 {
			if _, deleteErr := tx.TorrentContent.WithContext(ctx).Where(
				c.dao.TorrentContent.ID.In(payload.deleteIDs...),
			).Delete(); deleteErr != nil {
				return deleteErr
			}
		}

		if len(torrentContentsPtr) > 0 {
			if createErr := tx.TorrentContent.WithContext(ctx).Clauses(
				clause.OnConflict{
					UpdateAll: true,
				},
			).CreateInBatches(torrentContentsPtr, 100); createErr != nil {
				return createErr
			}
		}

		if len(torrentTagsPtr) > 0 {
			if createErr := tx.TorrentTag.WithContext(ctx).Clauses(
				clause.OnConflict{
					DoNothing: true,
				},
			).CreateInBatches(torrentTagsPtr, 100); createErr != nil {
				return createErr
			}
		}

		if len(payload.deleteInfoHashes) > 0 {
			valuers := slice.Map(payload.deleteInfoHashes, func(infoHash protocol.ID) driver.Valuer {
				return infoHash
			})

			if _, deleteErr := tx.Torrent.WithContext(ctx).Where(
				c.dao.Torrent.InfoHash.In(valuers...),
			).Delete(); deleteErr != nil {
				return deleteErr
			}
		}

		return nil
	})

	// Dual-write to the search sidecar only after the Postgres transaction has
	// committed, so the index never reflects rows that were rolled back.
	if err == nil {
		c.indexToSearchSidecar(ctx, payload)
	}

	return err
}

// indexToSearchSidecar mirrors a just-committed persist into the Tantivy sidecar:
// it upserts one document per torrent_content and deletes whole torrents by info
// hash. It is fire-and-forget — a no-op when the sidecar is disabled (nil
// client), detached from the request context with its own timeout, and never
// returns an error to the caller (a failed dual-write must never fail crawling;
// the periodic backfill reconciles any drift).
//
// Note: payload.deleteIDs (individual torrent_content removals) are not deleted
// here — the sidecar deletes by info hash only — so a reclassification that drops
// one of a torrent's contents leaves a stale document until the next backfill.
// Mapping those to per-document deletes is a follow-up.
func (c processor) indexToSearchSidecar(ctx context.Context, payload persistPayload) {
	if c.tantivy == nil {
		return
	}

	if len(payload.torrentContents) == 0 && len(payload.deleteInfoHashes) == 0 {
		return
	}

	contents := payload.torrentContents
	deleteInfoHashes := payload.deleteInfoHashes

	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), searchIndexTimeout)
		defer cancel()

		for i := range contents {
			if _, err := c.tantivy.IndexDocument(ctx, tantivy.BuildDocument(contents[i])); err != nil {
				c.logSearchIndexError("index", err)
			}
		}

		for _, infoHash := range deleteInfoHashes {
			if _, err := c.tantivy.DeleteDocument(ctx, infoHash.Bytes()); err != nil {
				c.logSearchIndexError("delete", err)
			}
		}
	}()
}

func (c processor) logSearchIndexError(op string, err error) {
	if c.logger != nil {
		c.logger.Warnw("search sidecar dual-write failed", "op", op, "error", err)
	}
}
