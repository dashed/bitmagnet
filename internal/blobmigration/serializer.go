package blobmigration

import (
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	model.FilesDataDeserializer = DeserializeFiles
}

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	decoder, _ = zstd.NewReader(nil)
)

type compactFile struct {
	Index     int    `msgpack:"i"`
	Path      string `msgpack:"p"`
	Extension string `msgpack:"e"`
	Size      uint   `msgpack:"s"`
}

func SerializeFiles(files []model.TorrentFile) ([]byte, error) {
	compact := make([]compactFile, len(files))
	for i, f := range files {
		compact[i] = compactFile{
			Index: int(f.Index),
			Path:  f.Path,
			// G1: canonicalize the stored `e` from the PATH, ignoring the caller's
			// f.Extension. The crawler dual-write (dhtcrawler/persist.go) builds
			// TorrentFiles with no Extension, which previously persisted an empty
			// `e`; once torrent_files is DROPped the blob is the source of truth, so
			// `e` must equal model.FileExtensionFromPath(path) everywhere. This is a
			// no-op for the blob-migration backfill caller, whose f.Extension already
			// comes from torrent_files.extension — itself the generated column
			// substring(lower(path) from '[^/.]\.([a-z0-9]+)$'), byte-identical to
			// FileExtensionFromPath. ExtractUniqueExtensions (below) already derives
			// from the path; this makes SerializeFiles consistent with it.
			Extension: model.FileExtensionFromPath(f.Path).String,
			Size:      f.Size,
		}
	}

	raw, err := msgpack.Marshal(compact)
	if err != nil {
		return nil, err
	}

	return encoder.EncodeAll(raw, make([]byte, 0, len(raw))), nil
}

func DeserializeFiles(data []byte) ([]model.TorrentFile, error) {
	raw, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}

	var compact []compactFile
	if err := msgpack.Unmarshal(raw, &compact); err != nil {
		return nil, err
	}

	files := make([]model.TorrentFile, len(compact))
	for i, c := range compact {
		files[i] = model.TorrentFile{
			Index: uint(c.Index),
			Path:  c.Path,
			Extension: model.NullString{
				String: c.Extension,
				Valid:  c.Extension != "",
			},
			Size: c.Size,
		}
	}

	return files, nil
}

func ExtractUniqueExtensions(files []model.TorrentFile) []string {
	seen := make(map[string]struct{})

	// Non-nil so it serializes to JSON '[]' (not SQL NULL) for files with no extractable
	// extension: torrents.file_extensions and torrent_file_summary.extensions are JSONB NOT NULL,
	// and the GORM json serializer writes a nil slice as NULL -> constraint violation (which broke
	// the dual-write + backfill on extension-less torrents).
	exts := []string{}

	for _, f := range files {
		ext := model.FileExtensionFromPath(f.Path)
		if !ext.Valid {
			continue
		}

		if _, ok := seen[ext.String]; ok {
			continue
		}

		seen[ext.String] = struct{}{}

		exts = append(exts, ext.String)
	}

	sort.Strings(exts)

	return exts
}

// BuildFileSummary derives the denormalized summary for a torrent. compressedBytes
// is the octet length of the torrent's compressed files_data blob (len(blob)); the
// caller passes the exact bytes it writes to torrents.files_data so the summary's
// compressed_bytes stays consistent with octet_length(files_data).
func BuildFileSummary(
	infoHash protocol.ID,
	files []model.TorrentFile,
	compressedBytes int,
) model.TorrentFileSummary {
	summary := model.TorrentFileSummary{
		InfoHash:        infoHash,
		CompressedBytes: model.NewNullInt(compressedBytes),
	}

	summary.FileCount = len(files)
	exts := ExtractUniqueExtensions(files)
	summary.Extensions = exts

	for _, f := range files {
		size := int64(f.Size)
		summary.TotalSize += size

		if size > summary.LargestFileSize {
			summary.LargestFileSize = size
		}
	}

	for _, ext := range exts {
		ft := model.FileTypeFromExtension(ext)
		if !ft.Valid {
			continue
		}

		switch ft.FileType {
		case model.FileTypeVideo:
			summary.HasVideo = true
		case model.FileTypeAudio:
			summary.HasAudio = true
		case model.FileTypeSubtitles:
			summary.HasSubtitle = true
		}
	}

	return summary
}
