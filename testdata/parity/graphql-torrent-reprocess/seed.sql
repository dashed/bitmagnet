-- Baseline database image for the torrent.reprocess Go/Rust parity corpus.
--
-- Every case starts from exactly this image. All seeded timestamps predate
-- 2016 so dump.sql can tell them apart from rows the run under test writes.
--
--   1000..01  movie-like single file, two sources          -> classified
--   2000..02  TV episode, one source                        -> classified
--   3000..03  unparseable name, no file info                -> classified, no content type
--   5000..05  TV episode with a stale `movie` row           -> stale row deleted
--   6000..06  movie with a reusable tmdb-backed association -> reused unless rematch
--   7000..07  audiobook with a type-only explicit hint      -> hint applied
--   8000..08  explicit hint naming content tmdb:42          -> Rust refuses (attach by ID)
--   9000..09  pre-existing tag, re-tagged by nothing        -> tag kept
--   e000..0x  never seeded                                  -> missing
TRUNCATE queue_jobs, torrent_tags, torrent_contents, torrent_hints, torrents_torrent_sources,
  content_collections_content, content_collections, content_attributes, content, torrents,
  deleted_torrents CASCADE;

INSERT INTO torrents (info_hash, name, size, private, files_status, files_count,
                      created_at, updated_at)
VALUES
  (decode('1000000000000000000000000000000000000001', 'hex'),
   'The.Matrix.1999.1080p.BluRay.x264-SPARKS.mkv', 8000000000, false, 'single', NULL,
   '2015-01-01 00:00:00.000001+00', '2015-01-02 00:00:00+00'),
  (decode('2000000000000000000000000000000000000002', 'hex'),
   'Breaking.Bad.S01E02.720p.HDTV.x264-CTU.mkv', 1200000000, false, 'single', NULL,
   '2015-02-01 00:00:00+00', '2015-02-02 00:00:00+00'),
  (decode('3000000000000000000000000000000000000003', 'hex'),
   'random upload 2015', 1234, false, 'no_info', NULL,
   '2015-03-01 00:00:00+00', '2015-03-02 00:00:00+00'),
  (decode('5000000000000000000000000000000000000005', 'hex'),
   'Planet.Earth.S01E01.1080p.BluRay.x264-GRP.mkv', 4000000000, false, 'single', NULL,
   '2015-05-01 00:00:00+00', '2015-05-02 00:00:00+00'),
  (decode('6000000000000000000000000000000000000006', 'hex'),
   'Reusable.Movie.1999.720p.WEB-DL.x264-GRP.mkv', 2000000000, false, 'single', NULL,
   '2015-06-01 00:00:00+00', '2015-06-02 00:00:00+00'),
  (decode('7000000000000000000000000000000000000007', 'hex'),
   'Synthetic Story.m4b', 60000000, false, 'single', NULL,
   '2015-07-01 00:00:00+00', '2015-07-02 00:00:00+00'),
  (decode('8000000000000000000000000000000000000008', 'hex'),
   'Hinted.Movie.1999.mkv', 900000000, false, 'single', NULL,
   '2015-08-01 00:00:00+00', '2015-08-02 00:00:00+00'),
  (decode('9000000000000000000000000000000000000009', 'hex'),
   'Tagged.Thing.2010.mkv', 700000000, false, 'single', NULL,
   '2015-09-01 00:00:00+00', '2015-09-02 00:00:00+00');

INSERT INTO torrents_torrent_sources (source, info_hash, seeders, leechers, published_at,
                                      created_at, updated_at)
VALUES
  ('dht', decode('1000000000000000000000000000000000000001', 'hex'), 25, 3,
   '2015-03-01 12:00:00+00', '2015-01-01 00:00:00+00', '2015-01-02 00:00:00+00'),
  ('rarbg', decode('1000000000000000000000000000000000000001', 'hex'), 40, NULL,
   NULL, '2014-12-01 00:00:00+00', '2015-01-02 00:00:00+00'),
  ('dht', decode('2000000000000000000000000000000000000002', 'hex'), 5, 1,
   NULL, '2015-02-01 00:00:00+00', '2015-02-02 00:00:00+00'),
  ('dht', decode('5000000000000000000000000000000000000005', 'hex'), 0, 0,
   '2015-05-03 00:00:00+00', '2015-05-01 00:00:00+00', '2015-05-02 00:00:00+00');

INSERT INTO torrent_hints (info_hash, content_type, content_source, content_id,
                           created_at, updated_at)
VALUES (decode('7000000000000000000000000000000000000007', 'hex'), 'audiobook', NULL, NULL,
        '2015-07-01 00:00:00+00', '2015-07-01 00:00:00+00'),
       (decode('8000000000000000000000000000000000000008', 'hex'), 'movie', 'tmdb', '42',
        '2015-08-01 00:00:00+00', '2015-08-01 00:00:00+00');

INSERT INTO content (type, source, id, title, release_year, original_language, tsv,
                     created_at, updated_at)
VALUES ('movie', 'tmdb', '42', 'Reusable Movie', 1999, 'fr', ''::tsvector,
        '2015-06-01 00:00:00+00', '2015-06-01 00:00:00+00');
INSERT INTO content_attributes (content_type, content_source, content_id, source, key, value,
                                created_at, updated_at)
VALUES ('movie', 'tmdb', '42', 'imdb', 'id', 'tt0042',
        '2015-06-01 00:00:00+00', '2015-06-01 00:00:00+00');

INSERT INTO torrent_contents (info_hash, content_type, content_source, content_id,
                              languages, episodes, video_resolution, created_at, updated_at,
                              tsv, seeders, leechers, published_at, size, files_count)
VALUES
  (decode('5000000000000000000000000000000000000005', 'hex'), 'movie', NULL, NULL,
   '[]'::jsonb, NULL, 'V1080p', '2015-05-01 00:00:00+00', '2015-05-01 00:00:00+00',
   ''::tsvector, 0, 0, '2015-05-01 00:00:00+00', 4000000000, 1),
  (decode('6000000000000000000000000000000000000006', 'hex'), 'movie', 'tmdb', '42',
   '[]'::jsonb, NULL, 'V720p', '2015-06-01 00:00:00+00', '2015-06-01 00:00:00+00',
   ''::tsvector, NULL, NULL, '2015-06-01 00:00:00+00', 2000000000, 1);

INSERT INTO torrent_tags (info_hash, name, created_at, updated_at)
VALUES (decode('9000000000000000000000000000000000000009', 'hex'), 'kept',
        '2015-09-01 00:00:00+00', '2015-09-01 00:00:00+00');
