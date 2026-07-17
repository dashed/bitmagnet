package dhtcrawler

import (
	"context"
	"database/sql/driver"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// runPersistTorrents waits on the persistTorrents channel, and persists torrents to the database in batches.
// After persisting each batch it will publish a message to the classifier,
// and forward the hash on the scrape channel to attempt finding the seeders/leechers.
func (c *crawler) runPersistTorrents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case is := <-c.persistTorrents.Out():
			torrentsToPersist := make([]*model.Torrent, 0, len(is))

			var torrentFilesToPersist []*model.TorrentFile

			var torrentFileSummariesToPersist []*model.TorrentFileSummary

			var torrentSourcesToPersist []*model.TorrentsTorrentSource

			var torrentPiecesToPersist []*model.TorrentPieces

			var queueJobsToPersist []*model.QueueJob

			hashMap := make(map[protocol.ID]infoHashWithMetaInfo, len(is))

			var hashesToClassify []protocol.ID

			now := time.Now()

			flushHashesToClassify := func() {
				if len(hashesToClassify) > 0 {
					job, err := processor.NewQueueJob(processor.MessageParams{
						InfoHashes: hashesToClassify,
					},
						// delay the classifier by a minute to allow time for the S/L scrape:
						model.QueueJobDelayBy(time.Minute),
					)
					if err != nil {
						c.logger.Errorf("error creating queue job: %s", err.Error())
					} else {
						queueJobsToPersist = append(queueJobsToPersist, &job)
					}
				}

				hashesToClassify = make([]protocol.ID, 0, classifyBatchSize)
			}
			flushHashesToClassify()

			// Collapse hybrid torrents discovered under BOTH their v1 and truncated-v2
			// infohashes into a single row (first-one-wins). Only hybrids can reach this:
			// pure-v2 re-discoveries share one truncated primary key (handled by the
			// ON CONFLICT upsert) and v1-only torrents carry no v2 hash.
			existingV2 := c.lookupExistingV2(ctx, is)

			kept, droppedV2 := filterV2Duplicates(is, existingV2)
			if droppedV2 > 0 {
				c.torrentsDropped.WithLabelValues("v2_duplicate").Add(float64(droppedV2))
			}

			for _, i := range kept {
				if _, ok := hashMap[i.infoHash]; ok {
					continue
				}

				hashMap[i.infoHash] = i

				if t, err := createTorrentModel(
					i.infoHash, i.metaInfo, c.savePieces, c.saveFilesThreshold); err != nil {
					c.logger.Errorf("error creating torrent model: %s", err.Error())
				} else {
					for _, f := range t.Files {
						fc := f
						torrentFilesToPersist = append(torrentFilesToPersist, &fc)
					}

					if len(t.Files) > 0 {
						// len(t.FilesData) is the exact blob written to torrents.files_data
						// in this same transaction, so compressed_bytes matches octet_length.
						summary := buildTorrentFileSummary(i.infoHash, t.Files, len(t.FilesData), now)
						torrentFileSummariesToPersist = append(torrentFileSummariesToPersist, &summary)
					}

					t.Files = nil
					for _, s := range t.Sources {
						sc := s
						torrentSourcesToPersist = append(torrentSourcesToPersist, &sc)
					}

					t.Sources = nil
					if c.savePieces {
						pc := t.Pieces
						torrentPiecesToPersist = append(torrentPiecesToPersist, &pc)
						t.Pieces = model.TorrentPieces{}
					}

					torrentsToPersist = append(torrentsToPersist, &t)

					hashesToClassify = append(hashesToClassify, i.infoHash)
					if len(hashesToClassify) >= classifyBatchSize {
						flushHashesToClassify()
					}
				}
			}

			flushHashesToClassify()

			if persistErr := c.dao.Transaction(func(tx *dao.Query) error {
				if err := tx.WithContext(ctx).Torrent.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: string(c.dao.Torrent.InfoHash.ColumnName())}},
					DoUpdates: clause.AssignmentColumns([]string{
						string(c.dao.Torrent.Name.ColumnName()),
						string(c.dao.Torrent.FilesStatus.ColumnName()),
						string(c.dao.Torrent.FilesCount.ColumnName()),
						string(c.dao.Torrent.UpdatedAt.ColumnName()),
						"files_data",
						"file_extensions",
					}),
				}).CreateInBatches(torrentsToPersist, 100); err != nil {
					return err
				}
				if len(torrentFilesToPersist) > 0 {
					if err := tx.WithContext(ctx).TorrentFile.Clauses(clause.OnConflict{
						DoNothing: true,
					}).CreateInBatches(torrentFilesToPersist, 100); err != nil {
						return err
					}
				}
				if len(torrentFileSummariesToPersist) > 0 {
					if err := torrentFileSummaryPersistQuery(ctx, tx).
						CreateInBatches(torrentFileSummariesToPersist, 100).Error; err != nil {
						return err
					}
				}
				if err := tx.WithContext(ctx).TorrentsTorrentSource.Clauses(clause.OnConflict{
					DoNothing: true,
				}).CreateInBatches(torrentSourcesToPersist, 100); err != nil {
					return err
				}
				if c.savePieces {
					if err := tx.WithContext(ctx).TorrentPieces.Clauses(clause.OnConflict{
						DoNothing: true,
					}).CreateInBatches(torrentPiecesToPersist, 10); err != nil {
						return err
					}
				}
				return tx.WithContext(ctx).QueueJob.CreateInBatches(queueJobsToPersist, 10)
			}); persistErr != nil {
				c.logger.Errorf("error persisting torrents: %s", persistErr)
			} else {
				c.persistedTotal.With(prometheus.Labels{"entity": "Torrent"}).Add(float64(len(torrentsToPersist)))
				c.logger.Debugw("persisted torrents", "count", len(torrentsToPersist))

				for _, i := range hashMap {
					select {
					case <-ctx.Done():
						return
					case c.scrape.In() <- i.nodeHasPeersForHash:
						continue
					}
				}
			}
		}
	}
}

func torrentFileSummaryPersistQuery(ctx context.Context, tx *dao.Query) *gorm.DB {
	return tx.Torrent.UnderlyingDB().WithContext(ctx).
		Table(model.TableNameTorrentFileSummary).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "info_hash"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"file_count",
				"total_size",
				"largest_file_size",
				"extensions",
				"has_video",
				"has_subtitle",
				"has_audio",
				"compressed_bytes",
				"updated_at",
			}),
		})
}

func createTorrentModel(
	hash protocol.ID,
	parsed metainfo.ParsedInfo,
	savePieces bool,
	saveFilesThreshold uint,
) (model.Torrent, error) {
	info := parsed.Info
	name := info.BestName()

	private := false
	if info.Private != nil {
		private = *info.Private
	}

	var filesCount model.NullUint

	var files []model.TorrentFile

	// Classify single vs multi. For v1, info.IsDir() == (len(info.Files) != 0), so
	// v1 behaviour is unchanged. For v2 (BEP 52) the file tree root is always a
	// directory keyed by the file name, so IsDir() is true even for a single
	// top-level file; treat "exactly one top-level file" as single so v2 / hybrid
	// single-file torrents match v1 single-file behaviour (no file rows; the
	// extension is indexed from the name in the Tantivy document builder).
	filesStatus := model.FilesStatusSingle

	if info.IsDir() {
		// UpvertedFiles yields the file list for both v1 (info.Files) and v2
		// (file tree) torrents.
		upvertedFiles := info.UpvertedFiles()

		isV2Single := info.HasV2() &&
			len(upvertedFiles) == 1 &&
			len(upvertedFiles[0].Path) <= 1

		if !isV2Single {
			filesStatus = model.FilesStatusMulti
			filesCount = model.NewNullUint(uint(len(upvertedFiles)))
			files = make([]model.TorrentFile, 0, min(int(saveFilesThreshold), len(upvertedFiles)))

			for i, file := range upvertedFiles {
				if i >= int(saveFilesThreshold) {
					filesStatus = model.FilesStatusOverThreshold
					break
				}

				files = append(files, model.TorrentFile{
					InfoHash: hash,
					Index:    uint(i),
					Path:     file.DisplayPath(&info),
					Size:     uint(file.Length),
				})
			}
		}
	}

	var filesData []byte

	var fileExts []string

	if len(files) > 0 {
		if blobData, blobErr := blobmigration.SerializeFiles(files); blobErr == nil {
			filesData = blobData
			fileExts = blobmigration.ExtractUniqueExtensions(files)
		}
	}

	var pieces model.TorrentPieces
	if savePieces {
		pieces = model.TorrentPieces{
			InfoHash:    hash,
			PieceLength: info.PieceLength,
			Pieces:      info.Pieces,
		}
	}

	return model.Torrent{
		InfoHash:    hash,
		InfoHashV1:  parsed.InfoHashV1,
		InfoHashV2:  parsed.InfoHashV2,
		MetaVersion: model.NewNullUint16(uint16(parsed.MetaVersion)),
		Name:        name,
		Size:        uint(info.TotalLength()),
		Private:     private,
		Pieces:      pieces,
		Files:       files,
		FilesData:   filesData,
		FileExts:    fileExts,
		FilesStatus: filesStatus,
		FilesCount:  filesCount,
		Sources: []model.TorrentsTorrentSource{
			{
				Source:   "dht",
				InfoHash: hash,
			},
		},
	}, nil
}

func buildTorrentFileSummary(
	infoHash protocol.ID,
	files []model.TorrentFile,
	compressedBytes int,
	now time.Time,
) model.TorrentFileSummary {
	summary := blobmigration.BuildFileSummary(infoHash, files, compressedBytes)
	summary.CreatedAt = now
	summary.UpdatedAt = now

	return summary
}

const classifyBatchSize = 100

// v2LookupChunkSize bounds the number of v2 hashes per dedup lookup query. The
// persist batch is currently capped at 1000 (factory.go), so this is defensive
// insurance should that cap ever be raised.
const v2LookupChunkSize = 1000

// dropV2Duplicate reports whether a discovery should be dropped as a cross-primary-key
// v2 duplicate: a hybrid torrent already represented under a DIFFERENT primary key
// (first-one-wins). The stored != pk check is essential — it preserves a legitimate
// same-primary-key re-discovery (which must upsert) while dropping only true cross-PK
// collisions.
func dropV2Duplicate(
	v2 protocol.InfoHashV2,
	pk protocol.ID,
	existing, batch map[protocol.InfoHashV2]protocol.ID,
) bool {
	if stored, ok := existing[v2]; ok && stored != pk {
		return true
	}

	if stored, ok := batch[v2]; ok && stored != pk {
		return true
	}

	return false
}

// filterV2Duplicates removes hybrid torrents discovered under a second infohash when
// the same full v2 identity is already represented under another primary key — either
// already in the database (existing) or earlier in this batch. It returns the kept
// items and the number dropped.
func filterV2Duplicates(
	is []infoHashWithMetaInfo,
	existing map[protocol.InfoHashV2]protocol.ID,
) (kept []infoHashWithMetaInfo, dropped int) {
	batch := make(map[protocol.InfoHashV2]protocol.ID)
	kept = make([]infoHashWithMetaInfo, 0, len(is))

	for _, i := range is {
		if v2 := i.metaInfo.InfoHashV2; v2 != nil {
			if dropV2Duplicate(*v2, i.infoHash, existing, batch) {
				dropped++

				continue
			}

			batch[*v2] = i.infoHash
		}

		kept = append(kept, i)
	}

	return kept, dropped
}

// lookupExistingV2 returns, for the full v2 hashes present in the batch, the primary
// key of any torrent already stored under each v2 hash. On error it logs and returns
// what it has (fail-open: dedup is skipped for the batch, never blocking persistence).
func (c *crawler) lookupExistingV2(
	ctx context.Context,
	is []infoHashWithMetaInfo,
) map[protocol.InfoHashV2]protocol.ID {
	v2Set := make(map[protocol.InfoHashV2]struct{})

	for _, i := range is {
		if v2 := i.metaInfo.InfoHashV2; v2 != nil {
			v2Set[*v2] = struct{}{}
		}
	}

	existing := make(map[protocol.InfoHashV2]protocol.ID, len(v2Set))
	if len(v2Set) == 0 {
		return existing
	}

	values := make([]driver.Valuer, 0, len(v2Set))
	for v2 := range v2Set {
		values = append(values, v2)
	}

	t := c.dao.Torrent

	for start := 0; start < len(values); start += v2LookupChunkSize {
		end := min(start+v2LookupChunkSize, len(values))

		rows, err := t.WithContext(ctx).
			Select(t.InfoHash, t.InfoHashV2).
			Where(t.InfoHashV2.In(values[start:end]...)).
			Find()
		if err != nil {
			c.logger.Errorf("error looking up existing v2 infohashes: %s", err.Error())

			return existing
		}

		for _, row := range rows {
			if row.InfoHashV2 != nil {
				existing[*row.InfoHashV2] = row.InfoHash
			}
		}
	}

	return existing
}

// runPersistSources waits on the persistSources channel for scraped torrents, and persists sources
// (which includes discovery date, seeders and leechers) to the database in batches.
func (c *crawler) runPersistSources(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case scrapes := <-c.persistSources.Out():
			srcs := make([]*model.TorrentsTorrentSource, 0, len(scrapes))

			hashSet := make(map[protocol.ID]struct{}, len(scrapes))
			for _, s := range scrapes {
				if _, ok := hashSet[s.infoHash]; ok {
					continue
				}

				hashSet[s.infoHash] = struct{}{}

				if src, err := createTorrentSourceModel(s); err != nil {
					c.logger.Errorf("error creating torrent source model: %s", err.Error())
				} else {
					srcs = append(srcs, &src)
				}
			}

			if persistErr := persistScrapedTorrentSources(ctx, c.dao, srcs); persistErr != nil {
				c.logger.Errorf("error persisting torrent sources: %s", persistErr.Error())
			} else {
				c.persistedTotal.With(prometheus.Labels{"entity": "TorrentsTorrentSource"}).Add(float64(len(srcs)))
				c.logger.Debugw("persisted torrent sources", "count", len(srcs))
			}
		}
	}
}

func persistScrapedTorrentSources(
	ctx context.Context,
	q *dao.Query,
	srcs []*model.TorrentsTorrentSource,
) error {
	const batchSize = 100

	now := time.Now()
	db := q.Torrent.UnderlyingDB().WithContext(ctx)

	for start := 0; start < len(srcs); start += batchSize {
		end := min(start+batchSize, len(srcs))
		batch := srcs[start:end]

		b := strings.Builder{}
		args := make([]any, 0, len(batch)*8)

		_, _ = b.WriteString("INSERT INTO torrents_torrent_sources " +
			"(source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) ")
		_, _ = b.WriteString("SELECT v.source, decode(v.info_hash, 'hex'), v.seeders, v.leechers, " +
			"v.published_at, v.seen_count, v.created_at, v.updated_at FROM (VALUES ")

		for i, src := range batch {
			if i > 0 {
				_, _ = b.WriteString(",")
			}

			_, _ = b.WriteString(
				"(?,?,?::integer,?::integer,?::timestamptz,?::integer,?::timestamptz,?::timestamptz)",
			)

			args = append(
				args,
				src.Source,
				src.InfoHash.String(),
				src.Seeders,
				src.Leechers,
				src.PublishedAt,
				src.SeenCount,
				now,
				now,
			)
		}

		_, _ = b.WriteString(
			") AS v(source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) ",
		)
		_, _ = b.WriteString(
			"WHERE EXISTS (SELECT 1 FROM torrents t WHERE t.info_hash = decode(v.info_hash, 'hex')) ",
		)
		_, _ = b.WriteString("ON CONFLICT (info_hash, source) DO UPDATE SET " +
			"seeders = excluded.seeders, " +
			"leechers = excluded.leechers, " +
			// sets to null, fixes torrents indexed before 0.8.0 with published_at
			// 0001-01-01 00:00:00+00:
			"published_at = excluded.published_at, " +
			"updated_at = excluded.updated_at, " +
			"seen_count = torrents_torrent_sources.seen_count + 1")

		if err := db.Exec(b.String(), args...).Error; err != nil {
			return err
		}
	}

	return nil
}

func createTorrentSourceModel(
	result infoHashWithScrape,
) (model.TorrentsTorrentSource, error) {
	seeders := model.NewNullUint(uint(result.bfsd.ApproximatedSize()))
	leechers := model.NewNullUint(uint(result.bfpe.ApproximatedSize()))

	return model.TorrentsTorrentSource{
		Source:    "dht",
		InfoHash:  result.infoHash,
		Seeders:   seeders,
		Leechers:  leechers,
		SeenCount: 1,
	}, nil
}
