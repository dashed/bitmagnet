package classifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// DecodeTapeClassifierInput rebuilds the Go classifier input captured at the
// runner boundary. It is intentionally the inverse of newTapeClassifierInput,
// rather than an unmarshal into model.Torrent: the stable tape DTO and the
// database model have deliberately different JSON contracts.
//
// The returned torrent contains only classifier-visible state. Processor-only
// state, such as the IDs of existing torrent_contents rows, belongs to the
// record's processorState field and is consumed by the write-set harness.
func DecodeTapeClassifierInput(raw json.RawMessage) (model.Torrent, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return model.Torrent{}, fmt.Errorf("classifier tape input is absent or null")
	}

	var input tapeClassifierInput
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return model.Torrent{}, fmt.Errorf("decode classifier tape input: %w", err)
	}
	if err := requireTapeInputEOF(decoder); err != nil {
		return model.Torrent{}, err
	}

	infoHash, err := protocol.ParseID(input.ID)
	if err != nil {
		return model.Torrent{}, fmt.Errorf("classifier tape input id %q is not an info hash: %w", input.ID, err)
	}
	filesStatus, err := model.ParseFilesStatus(input.FilesStatus)
	if err != nil {
		return model.Torrent{}, fmt.Errorf("classifier tape input filesStatus %q: %w", input.FilesStatus, err)
	}

	torrent := model.Torrent{
		InfoHash:    infoHash,
		Name:        input.Name,
		Size:        input.Size,
		FilesStatus: filesStatus,
		Files:       make([]model.TorrentFile, 0, len(input.Files)),
		Contents:    make([]model.TorrentContent, 0, len(input.Contents)),
	}
	if input.Extension != nil {
		torrent.Extension = model.NewNullString(*input.Extension)
	}
	if input.FilesCount != nil {
		torrent.FilesCount = model.NewNullUint(*input.FilesCount)
	}

	for _, file := range input.Files {
		rebuilt := model.TorrentFile{
			InfoHash: infoHash,
			Index:    file.Index,
			Path:     file.Path,
			Size:     file.Size,
		}
		if file.Extension != "" {
			rebuilt.Extension = model.NewNullString(file.Extension)
		}
		torrent.Files = append(torrent.Files, rebuilt)
	}

	if input.Hint != nil {
		hint, err := decodeTapeClassifierHint(infoHash, *input.Hint)
		if err != nil {
			return model.Torrent{}, err
		}
		torrent.Hint = hint
	}

	for i, content := range input.Contents {
		rebuilt, err := decodeTapeClassifierContent(infoHash, content)
		if err != nil {
			return model.Torrent{}, fmt.Errorf("classifier tape input content %d: %w", i, err)
		}
		torrent.Contents = append(torrent.Contents, rebuilt)
	}

	return torrent, nil
}

func requireTapeInputEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing classifier tape input: %w", err)
	}
	return fmt.Errorf("classifier tape input contains more than one JSON value")
}

func decodeTapeClassifierHint(infoHash protocol.ID, input tapeClassifierHint) (model.TorrentHint, error) {
	contentType, err := model.ParseContentType(input.ContentType)
	if err != nil {
		return model.TorrentHint{}, fmt.Errorf("classifier tape input hint contentType %q: %w", input.ContentType, err)
	}

	hint := model.TorrentHint{
		InfoHash:    infoHash,
		ContentType: contentType,
		Episodes:    model.ParseEpisodes(input.Episodes),
	}
	if input.ContentSource != "" {
		hint.ContentSource = model.NewNullString(input.ContentSource)
	}
	if input.ContentID != "" {
		hint.ContentID = model.NewNullString(input.ContentID)
	}
	for _, name := range input.Languages {
		language := model.ParseLanguage(name)
		if !language.Valid {
			return model.TorrentHint{}, fmt.Errorf("classifier tape input hint language %q is invalid", name)
		}
		if hint.Languages == nil {
			hint.Languages = make(model.Languages)
		}
		hint.Languages[language.Language] = struct{}{}
	}

	if input.VideoResolution != nil {
		value, err := model.ParseVideoResolution(*input.VideoResolution)
		if err != nil {
			return model.TorrentHint{}, fmt.Errorf("classifier tape input hint videoResolution %q: %w", *input.VideoResolution, err)
		}
		hint.VideoResolution = model.NewNullVideoResolution(value)
	}
	if input.VideoSource != nil {
		value, err := model.ParseVideoSource(*input.VideoSource)
		if err != nil {
			return model.TorrentHint{}, fmt.Errorf("classifier tape input hint videoSource %q: %w", *input.VideoSource, err)
		}
		hint.VideoSource = model.NewNullVideoSource(value)
	}
	if input.VideoCodec != nil {
		value, err := model.ParseVideoCodec(*input.VideoCodec)
		if err != nil {
			return model.TorrentHint{}, fmt.Errorf("classifier tape input hint videoCodec %q: %w", *input.VideoCodec, err)
		}
		hint.VideoCodec = model.NewNullVideoCodec(value)
	}
	if input.Video3D != nil {
		value, err := model.ParseVideo3D(*input.Video3D)
		if err != nil {
			return model.TorrentHint{}, fmt.Errorf("classifier tape input hint video3d %q: %w", *input.Video3D, err)
		}
		hint.Video3D = model.NewNullVideo3D(value)
	}
	if input.VideoModifier != nil {
		value, err := model.ParseVideoModifier(*input.VideoModifier)
		if err != nil {
			return model.TorrentHint{}, fmt.Errorf("classifier tape input hint videoModifier %q: %w", *input.VideoModifier, err)
		}
		hint.VideoModifier = model.NewNullVideoModifier(value)
	}
	if input.ReleaseGroup != nil {
		hint.ReleaseGroup = model.NewNullString(*input.ReleaseGroup)
	}

	return hint, nil
}

func decodeTapeClassifierContent(
	infoHash protocol.ID,
	input tapeClassifierContent,
) (model.TorrentContent, error) {
	content := model.TorrentContent{InfoHash: infoHash}
	if input.ContentType != "" {
		contentType, err := model.ParseContentType(input.ContentType)
		if err != nil {
			return model.TorrentContent{}, fmt.Errorf("contentType %q: %w", input.ContentType, err)
		}
		content.ContentType = model.NewNullContentType(contentType)
	}
	if input.ContentSource != "" {
		content.ContentSource = model.NewNullString(input.ContentSource)
	}
	if input.ContentID != "" {
		content.ContentID = model.NewNullString(input.ContentID)
	}
	if input.Content != nil {
		rebuilt, err := decodeTapeClassifierContentRow(*input.Content)
		if err != nil {
			return model.TorrentContent{}, err
		}
		content.Content = rebuilt
	}

	return content, nil
}

func decodeTapeClassifierContentRow(input tapeClassifierContentRow) (model.Content, error) {
	contentType, err := model.ParseContentType(input.Type)
	if err != nil {
		return model.Content{}, fmt.Errorf("hydrated content type %q: %w", input.Type, err)
	}

	content := model.Content{
		Type:        contentType,
		Source:      input.Source,
		ID:          input.ID,
		Title:       input.Title,
		Collections: make([]model.ContentCollection, 0, len(input.Collections)),
		Attributes:  make([]model.ContentAttribute, 0, len(input.Attributes)),
	}
	if input.ReleaseDate != nil {
		content.ReleaseDate = model.NewDateFromParts(
			model.Year(input.ReleaseDate.Year),
			time.Month(input.ReleaseDate.Month),
			input.ReleaseDate.Day,
		)
		if !content.ReleaseDate.IsValid() {
			return model.Content{}, fmt.Errorf("hydrated content releaseDate is invalid: %+v", *input.ReleaseDate)
		}
	}
	if input.ReleaseYear != nil {
		content.ReleaseYear = model.Year(*input.ReleaseYear)
	}
	if input.Adult != nil {
		content.Adult = model.NewNullBool(*input.Adult)
	}
	if input.OriginalLanguage != nil {
		language := model.ParseLanguage(*input.OriginalLanguage)
		if !language.Valid {
			return model.Content{}, fmt.Errorf("hydrated content originalLanguage %q is invalid", *input.OriginalLanguage)
		}
		content.OriginalLanguage = language
	}
	if input.OriginalTitle != nil {
		content.OriginalTitle = model.NewNullString(*input.OriginalTitle)
	}
	if input.Overview != nil {
		content.Overview = model.NewNullString(*input.Overview)
	}
	if input.Runtime != nil {
		content.Runtime = model.NewNullUint16(*input.Runtime)
	}
	if input.Popularity != nil {
		content.Popularity = model.NewNullFloat32(*input.Popularity)
	}
	if input.VoteAverage != nil {
		content.VoteAverage = model.NewNullFloat32(*input.VoteAverage)
	}
	if input.VoteCount != nil {
		content.VoteCount = model.NewNullUint(*input.VoteCount)
	}
	if input.CreatedAt != nil {
		content.CreatedAt = time.Unix(*input.CreatedAt, 0).UTC()
	}
	if input.UpdatedAt != nil {
		content.UpdatedAt = time.Unix(*input.UpdatedAt, 0).UTC()
	}

	for _, collection := range input.Collections {
		content.Collections = append(content.Collections, model.ContentCollection{
			Type:   collection.Type,
			Source: collection.Source,
			ID:     collection.ID,
			Name:   collection.Name,
		})
	}
	for _, attribute := range input.Attributes {
		attributeType, err := model.ParseContentType(attribute.ContentType)
		if err != nil {
			return model.Content{}, fmt.Errorf("hydrated content attribute contentType %q: %w", attribute.ContentType, err)
		}
		content.Attributes = append(content.Attributes, model.ContentAttribute{
			ContentType:   attributeType,
			ContentSource: attribute.ContentSource,
			ContentID:     attribute.ContentID,
			Source:        attribute.Source,
			Key:           attribute.Key,
			Value:         attribute.Value,
		})
	}

	return content, nil
}
