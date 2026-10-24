package tantivy

import (
	"encoding/hex"
	"math"
	"sort"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

// BuildDocument maps a live model.TorrentContent onto the proto TorrentDocument
// the Tantivy sidecar indexes. It is the Go twin of the Rust backfill transform
// (bitmagnet-rs/crates/bitmagnet-search/src/transform.rs): a document produced
// here for a row is byte-identical to one the backfill produces from the same
// row, so a shadow / dual-write upsert lands on the exact same Tantivy document
// (same DocID) the backfill would have written — never a duplicate.
//
// One document is produced per torrent_content row (NOT per torrent): a torrent
// classified as several contents yields several documents, each keyed by its
// own DocID, mirroring bitmagnet's `tsv @@ tsquery` Postgres search.
//
// Field sourcing follows the Rust SQL (stream.rs STREAM_FOR_INDEX_SQL) + the
// transform: it reads the *raw stored* values, so callers MUST populate the
// associations the fields come from for full parity:
//   - tc.Content  (title, original_title, release_year, genre collections) — empty
//     for an unclassified row, which is correct (those fields are then "").
//   - tc.Torrent  (name) and tc.Torrent.Files (paths + per-file extensions). Files
//     may come from the torrent_files association or the decoded blob; either way
//     each file's Extension is the value the blob records, which is exactly what
//     the backfill reads. If Files is not loaded, file_paths / file_extensions are
//     empty (a degraded-parity document, but still the same DocID).
//
// Absent optional values become the proto defaults (empty string / 0); the
// sidecar's indexer skips empties and maps absent doc_id segments to "?".
func BuildDocument(tc model.TorrentContent) *pb.TorrentDocument {
	return &pb.TorrentDocument{
		// --- Identity & weight-A text -------------------------------------
		InfoHash:    append([]byte(nil), tc.InfoHash[:]...),
		TorrentName: tc.Torrent.Name,
		// content_title / original_title come from the joined content row;
		// zero-valued (empty) when the torrent_content is unclassified.
		ContentTitle:  tc.Content.Title,
		OriginalTitle: tc.Content.OriginalTitle.String,

		// --- Weight B: release year (from the content row; 0 == absent) ----
		ReleaseYear: uint32(tc.Content.ReleaseYear),

		// --- Weight C: video attributes -----------------------------------
		// video_resolution and video_3d send Go's Label() (the leading "V"
		// stripped: "V1080p" -> "1080p", "V3D" -> "3D"), matching transform.rs's
		// strip_v_prefix so the indexed text / facet / filter all agree with Go's
		// weight-C tsvector + GraphQL facets. source/codec/modifier pass through
		// raw (their Label() == String()).
		VideoResolution: videoResolutionValue(tc.VideoResolution),
		VideoSource:     videoSourceValue(tc.VideoSource),
		VideoCodec:      videoCodecValue(tc.VideoCodec),
		Video_3D:        video3DLabel(tc.Video3D),
		VideoModifier:   videoModifierValue(tc.VideoModifier),
		ReleaseGroup:    tc.ReleaseGroup.String,

		// --- Classification key (forms the DocID; see DocID) --------------
		ContentType:   contentTypeToProtoEnum(tc.ContentType),
		ContentSource: tc.ContentSource.String,
		ContentId:     tc.ContentID.String,

		// --- Weight D relevance / facets ----------------------------------
		Genres:         genres(tc.Content),
		FilePaths:      filePaths(tc.Torrent.Files),
		FileExtensions: fileExtensionsForDoc(tc.Torrent),
		Languages:      languages(tc.Languages),
		// No Postgres source for audio languages (see transform.rs): never set.
		AudioLanguages: nil,

		// --- Numerics: sort / range filter --------------------------------
		Seeders:     nullUintToU32(tc.Seeders),
		Leechers:    nullUintToU32(tc.Leechers),
		FilesCount:  nullUintToU32(tc.FilesCount), // mirrors tc.files_count, NOT len(Files)
		Size:        uint64(tc.Size),
		PublishedAt: publishedAt(tc),
	}
}

// DocID reproduces the composite identity the sidecar's indexer derives from a
// TorrentDocument (bitmagnet-search/src/indexer.rs `doc_id`), which is in turn
// byte-identical to the PostgreSQL `torrent_contents.id` generated column and to
// model.TorrentContent.InferID():
//
//	hex(info_hash):content_type:content_source:content_id
//
// with each missing segment rendered "?". It is the Tantivy upsert key, so a
// dual-written and a backfilled document for the same row collapse onto one
// document. Exposed so the dual-write path can log / key by it and tests can
// assert the cross-system invariant against InferID().
func DocID(doc *pb.TorrentDocument) string {
	contentType := "?"
	if ct, ok := protoToContentType[doc.GetContentType()]; ok {
		contentType = ct.String()
	}

	contentSource := "?"
	if doc.GetContentSource() != "" {
		contentSource = doc.GetContentSource()
	}

	contentID := "?"
	if doc.GetContentId() != "" {
		contentID = doc.GetContentId()
	}

	return strings.Join([]string{
		hex.EncodeToString(doc.GetInfoHash()),
		contentType,
		contentSource,
		contentID,
	}, ":")
}

// contentTypeToProto maps the canonical Go content type to its proto enum,
// mirroring Rust ContentType::to_proto_value. The integer values are the wire
// contract shared with the sidecar (common.proto).
var contentTypeToProto = map[model.ContentType]pb.ContentType{
	model.ContentTypeMovie:     pb.ContentType_CONTENT_TYPE_MOVIE,
	model.ContentTypeTvShow:    pb.ContentType_CONTENT_TYPE_TV_SHOW,
	model.ContentTypeMusic:     pb.ContentType_CONTENT_TYPE_MUSIC,
	model.ContentTypeEbook:     pb.ContentType_CONTENT_TYPE_EBOOK,
	model.ContentTypeComic:     pb.ContentType_CONTENT_TYPE_COMIC,
	model.ContentTypeAudiobook: pb.ContentType_CONTENT_TYPE_AUDIOBOOK,
	model.ContentTypeGame:      pb.ContentType_CONTENT_TYPE_GAME,
	model.ContentTypeSoftware:  pb.ContentType_CONTENT_TYPE_SOFTWARE,
	model.ContentTypeXxx:       pb.ContentType_CONTENT_TYPE_XXX,
}

// protoToContentType is the inverse of contentTypeToProto, used by DocID to
// recover the canonical string (mirrors Rust ContentType::from_proto_value).
var protoToContentType = func() map[pb.ContentType]model.ContentType {
	m := make(map[pb.ContentType]model.ContentType, len(contentTypeToProto))
	for ct, proto := range contentTypeToProto {
		m[proto] = ct
	}
	return m
}()

// contentTypeToProtoEnum maps a NullContentType to the proto enum, defaulting to
// CONTENT_TYPE_UNKNOWN (0) when absent or unrecognised — matching the backfill,
// which yields 0 for an unclassified row, and InferID, which renders "?".
func contentTypeToProtoEnum(ct model.NullContentType) pb.ContentType {
	if !ct.Valid {
		return pb.ContentType_CONTENT_TYPE_UNKNOWN
	}

	return contentTypeToProto[ct.ContentType]
}

// videoResolutionValue returns Go's VideoResolution.Label() ("V1080p" ->
// "1080p"), matching transform.rs's strip_v_prefix so the indexed value agrees
// with Go's weight-C tsvector + GraphQL facet (which both use the label).
func videoResolutionValue(v model.NullVideoResolution) string {
	if !v.Valid {
		return ""
	}

	return v.VideoResolution.Label()
}

func videoSourceValue(v model.NullVideoSource) string {
	if !v.Valid {
		return ""
	}

	return v.VideoSource.String()
}

func videoCodecValue(v model.NullVideoCodec) string {
	if !v.Valid {
		return ""
	}

	return v.VideoCodec.String()
}

// video3DLabel sends Go's Video3D.Label() (V3D -> "3D"); like video_resolution,
// the backfill strips the leading V (transform.rs strip_v_prefix).
func video3DLabel(v model.NullVideo3D) string {
	if !v.Valid {
		return ""
	}

	return v.Video3D.Label()
}

func videoModifierValue(v model.NullVideoModifier) string {
	if !v.Valid {
		return ""
	}

	return v.VideoModifier.String()
}

// genres returns the content's genre collection names, sorted to match the
// backfill SQL's `ORDER BY cc.name`.
func genres(content model.Content) []string {
	var out []string

	for _, c := range content.Collections {
		if c.Type == "genre" && c.Name != "" {
			out = append(out, c.Name)
		}
	}

	sort.Strings(out)

	return out
}

// filePaths returns every non-empty file path, mirroring transform.rs (paths
// feed weight-D relevance and are never stored).
func filePaths(files []model.TorrentFile) []string {
	var out []string

	for _, f := range files {
		if f.Path != "" {
			out = append(out, f.Path)
		}
	}

	return out
}

// fileExtensions returns the distinct, sorted, non-empty per-file extensions —
// the BTreeSet the backfill builds from each blob file's recorded extension.
// It uses the stored Extension (the same value serialized into the blob the
// backfill reads), NOT a re-derivation from the path.
func fileExtensions(files []model.TorrentFile) []string {
	seen := make(map[string]struct{}, len(files))

	for _, f := range files {
		if f.Extension.String != "" {
			seen[f.Extension.String] = struct{}{}
		}
	}

	if len(seen) == 0 {
		return nil
	}

	out := make([]string, 0, len(seen))
	for ext := range seen {
		out = append(out, ext)
	}

	sort.Strings(out)

	return out
}

// fileExtensionsForDoc selects the file_extensions source per FilesStatus,
// mirroring model.Torrent.FileExtensions(). A single-file torrent has no
// per-file rows, so its extension is derived from the torrent name (the
// weight-A field in Postgres); that single name-derived extension is indexed so
// the document is filterable / facetable by extension, matching what the
// Postgres-side model.Torrent.FileExtensions() yields for the same row. The
// multi-file arm is unchanged: it reads the stored per-file Extension via
// fileExtensions, preserving byte-parity with the Rust blob backfill path.
func fileExtensionsForDoc(t model.Torrent) []string {
	if t.SingleFile() {
		if ext := model.FileExtensionFromPath(t.Name); ext.Valid {
			return []string{ext.String}
		}

		return nil
	}

	return fileExtensions(t.Files)
}

// languages returns the content languages as alpha-2 codes in the same order
// the JSONB column stores them: Languages.Slice() sorts by language name via
// natsort, which is exactly the order Languages.Value() wrote, and therefore the
// order the backfill reads back via jsonb_array_elements_text.
func languages(langs model.Languages) []string {
	if len(langs) == 0 {
		return nil
	}

	out := make([]string, 0, len(langs))
	for _, l := range langs.Slice() {
		out = append(out, l.String())
	}

	return out
}

// publishedAt mirrors the backfill's epoch(COALESCE(tc.published_at,
// t.created_at)). The column is NOT NULL (default 1999-01-01), so the coalesce
// is normally a no-op; the zero-time guard defends against an unset value the
// same way the SQL coalesce does.
func publishedAt(tc model.TorrentContent) int64 {
	t := tc.PublishedAt
	if t.IsZero() {
		t = tc.Torrent.CreatedAt
	}

	return t.Unix()
}

// nullUintToU32 mirrors transform.rs's `map_or(0, to_u32)`: absent -> 0, and an
// out-of-range value clamps to 0 (the source columns are small non-negative
// counts, so the clamp is just defence).
func nullUintToU32(n model.NullUint) uint32 {
	if !n.Valid || n.Uint > math.MaxUint32 {
		return 0
	}

	return uint32(n.Uint)
}
