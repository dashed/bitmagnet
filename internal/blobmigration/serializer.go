package blobmigration

import (
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

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
			Index:     int(f.Index),
			Path:      f.Path,
			Extension: f.Extension.String,
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
	var exts []string

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

func BuildFileSummary(infoHash protocol.ID, files []model.TorrentFile) model.TorrentFileSummary {
	summary := model.TorrentFileSummary{
		InfoHash: infoHash,
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
