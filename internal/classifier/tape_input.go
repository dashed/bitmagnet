package classifier

import (
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
)

// tapeClassifierInput is the stable, language-neutral shape embedded in each
// tape record. It is deliberately not model.Torrent: the database model carries
// timestamps, nullable wrappers and nested associations whose default JSON does
// not match the Rust ClassifierInput contract.
//
// The value is built from the Torrent handed to runner.Run. At that point the
// processor has already synthesised/preserved the EFFECTIVE hint, but the
// classifier has not run yet. That exact boundary is what a replay needs.
type tapeClassifierInput struct {
	ID          string                  `json:"id"`
	Name        string                  `json:"name"`
	Size        uint                    `json:"size"`
	FilesStatus string                  `json:"filesStatus"`
	Extension   *string                 `json:"extension,omitempty"`
	FilesCount  *uint                   `json:"filesCount,omitempty"`
	Files       []tapeClassifierFile    `json:"files"`
	Hint        *tapeClassifierHint     `json:"hint,omitempty"`
	Contents    []tapeClassifierContent `json:"contents"`
}

type tapeClassifierFile struct {
	Index     uint   `json:"index"`
	Path      string `json:"path"`
	Extension string `json:"extension"`
	Size      uint   `json:"size"`
}

type tapeClassifierHint struct {
	ContentType     string   `json:"contentType"`
	ContentSource   string   `json:"contentSource,omitempty"`
	ContentID       string   `json:"contentId,omitempty"`
	Episodes        string   `json:"episodes,omitempty"`
	Languages       []string `json:"languages,omitempty"`
	VideoResolution *string  `json:"videoResolution,omitempty"`
	VideoSource     *string  `json:"videoSource,omitempty"`
	VideoCodec      *string  `json:"videoCodec,omitempty"`
	Video3D         *string  `json:"video3d,omitempty"`
	VideoModifier   *string  `json:"videoModifier,omitempty"`
	ReleaseGroup    *string  `json:"releaseGroup,omitempty"`
}

type tapeClassifierContent struct {
	ContentType   string                    `json:"contentType,omitempty"`
	ContentSource string                    `json:"contentSource,omitempty"`
	ContentID     string                    `json:"contentId,omitempty"`
	Content       *tapeClassifierContentRow `json:"content,omitempty"`
}

// tapeClassifierContentRow mirrors the Rust bitmagnet_model::Content JSON
// surface. The tsvector and MetadataSource expansion are deliberately absent:
// neither is read by the classifier, while collections and attributes are
// retained because they are part of the hydrated content row carried through a
// pre-attach.
type tapeClassifierContentRow struct {
	Type             string                            `json:"type"`
	Source           string                            `json:"source"`
	ID               string                            `json:"id"`
	Title            string                            `json:"title"`
	ReleaseDate      *tapeClassifierDate               `json:"releaseDate,omitempty"`
	ReleaseYear      *uint16                           `json:"releaseYear,omitempty"`
	Adult            *bool                             `json:"adult,omitempty"`
	OriginalLanguage *string                           `json:"originalLanguage,omitempty"`
	OriginalTitle    *string                           `json:"originalTitle,omitempty"`
	Overview         *string                           `json:"overview,omitempty"`
	Runtime          *uint16                           `json:"runtime,omitempty"`
	Popularity       *float32                          `json:"popularity,omitempty"`
	VoteAverage      *float32                          `json:"voteAverage,omitempty"`
	VoteCount        *uint                             `json:"voteCount,omitempty"`
	CreatedAt        *int64                            `json:"createdAt,omitempty"`
	UpdatedAt        *int64                            `json:"updatedAt,omitempty"`
	Collections      []tapeClassifierContentCollection `json:"collections,omitempty"`
	Attributes       []tapeClassifierContentAttribute  `json:"attributes,omitempty"`
}

type tapeClassifierDate struct {
	Year  uint16 `json:"year"`
	Month uint8  `json:"month"`
	Day   uint8  `json:"day"`
}

type tapeClassifierContentCollection struct {
	Type   string `json:"type"`
	Source string `json:"source"`
	ID     string `json:"id"`
	Name   string `json:"name"`
}

type tapeClassifierContentAttribute struct {
	ContentType   string `json:"contentType"`
	ContentSource string `json:"contentSource"`
	ContentID     string `json:"contentId"`
	Source        string `json:"source"`
	Key           string `json:"key"`
	Value         string `json:"value"`
}

func newTapeClassifierInput(subject string, torrent model.Torrent) tapeClassifierInput {
	input := tapeClassifierInput{
		ID:          subject,
		Name:        torrent.Name,
		Size:        torrent.Size,
		FilesStatus: torrent.FilesStatus.String(),
		Files:       make([]tapeClassifierFile, 0, len(torrent.Files)),
		Contents:    make([]tapeClassifierContent, 0, len(torrent.Contents)),
	}

	if torrent.Extension.Valid {
		input.Extension = tapeValuePointer(torrent.Extension.String)
	}
	if torrent.FilesCount.Valid {
		input.FilesCount = tapeValuePointer(torrent.FilesCount.Uint)
	}

	for _, file := range torrent.Files {
		recorded := tapeClassifierFile{
			Index: file.Index,
			Path:  file.Path,
			Size:  file.Size,
		}
		if file.Extension.Valid {
			recorded.Extension = file.Extension.String
		}
		input.Files = append(input.Files, recorded)
	}

	if !torrent.Hint.IsNil() {
		input.Hint = newTapeClassifierHint(torrent.Hint)
	}

	for _, content := range torrent.Contents {
		input.Contents = append(input.Contents, newTapeClassifierContent(content))
	}

	return input
}

// newTapeProcessorState captures state that affects processor write-set
// materialization but is deliberately outside the classifier input contract.
// Preserve torrent.Contents order; each side canonicalizes delete IDs only when
// constructing its write set.
func newTapeProcessorState(torrent model.Torrent) tape.ProcessorState {
	state := tape.ProcessorState{
		ExistingContentIDs: make([]string, 0, len(torrent.Contents)),
	}
	for _, content := range torrent.Contents {
		state.ExistingContentIDs = append(state.ExistingContentIDs, content.ID)
	}

	return state
}

func newTapeClassifierHint(hint model.TorrentHint) *tapeClassifierHint {
	recorded := &tapeClassifierHint{
		ContentType: hint.ContentType.String(),
		Episodes:    hint.Episodes.String(),
		Languages:   make([]string, 0, len(hint.Languages)),
	}

	if hint.ContentSource.Valid {
		recorded.ContentSource = hint.ContentSource.String
	}
	if hint.ContentID.Valid {
		recorded.ContentID = hint.ContentID.String
	}
	for _, language := range hint.Languages.Slice() {
		recorded.Languages = append(recorded.Languages, language.String())
	}
	if hint.VideoResolution.Valid {
		recorded.VideoResolution = tapeValuePointer(hint.VideoResolution.VideoResolution.String())
	}
	if hint.VideoSource.Valid {
		recorded.VideoSource = tapeValuePointer(hint.VideoSource.VideoSource.String())
	}
	if hint.VideoCodec.Valid {
		recorded.VideoCodec = tapeValuePointer(hint.VideoCodec.VideoCodec.String())
	}
	if hint.Video3D.Valid {
		recorded.Video3D = tapeValuePointer(hint.Video3D.Video3D.String())
	}
	if hint.VideoModifier.Valid {
		recorded.VideoModifier = tapeValuePointer(hint.VideoModifier.VideoModifier.String())
	}
	if hint.ReleaseGroup.Valid {
		recorded.ReleaseGroup = tapeValuePointer(hint.ReleaseGroup.String)
	}

	return recorded
}

func newTapeClassifierContent(content model.TorrentContent) tapeClassifierContent {
	recorded := tapeClassifierContent{}
	if content.ContentType.Valid {
		recorded.ContentType = content.ContentType.ContentType.String()
	}
	if content.ContentSource.Valid {
		recorded.ContentSource = content.ContentSource.String
	}
	if content.ContentID.Valid {
		recorded.ContentID = content.ContentID.String
	}

	// An unhydrated association carries a zero Content. Do not turn it into a
	// present-but-invalid row: Rust uses absence for the same condition.
	if content.Content.Type != "" || content.Content.Source != "" || content.Content.ID != "" {
		recorded.Content = newTapeClassifierContentRow(content.Content)
	}

	return recorded
}

func newTapeClassifierContentRow(content model.Content) *tapeClassifierContentRow {
	recorded := &tapeClassifierContentRow{
		Type:        content.Type.String(),
		Source:      content.Source,
		ID:          content.ID,
		Title:       content.Title,
		Collections: make([]tapeClassifierContentCollection, 0, len(content.Collections)),
		Attributes:  make([]tapeClassifierContentAttribute, 0, len(content.Attributes)),
	}

	if !content.ReleaseDate.IsNil() {
		recorded.ReleaseDate = &tapeClassifierDate{
			Year:  uint16(content.ReleaseDate.Year),
			Month: uint8(content.ReleaseDate.Month),
			Day:   content.ReleaseDate.Day,
		}
	}
	if !content.ReleaseYear.IsNil() {
		recorded.ReleaseYear = tapeValuePointer(uint16(content.ReleaseYear))
	}
	if content.Adult.Valid {
		recorded.Adult = tapeValuePointer(content.Adult.Bool)
	}
	if content.OriginalLanguage.Valid {
		recorded.OriginalLanguage = tapeValuePointer(content.OriginalLanguage.Language.String())
	}
	if content.OriginalTitle.Valid {
		recorded.OriginalTitle = tapeValuePointer(content.OriginalTitle.String)
	}
	if content.Overview.Valid {
		recorded.Overview = tapeValuePointer(content.Overview.String)
	}
	if content.Runtime.Valid {
		recorded.Runtime = tapeValuePointer(content.Runtime.Uint16)
	}
	if content.Popularity.Valid {
		recorded.Popularity = tapeValuePointer(content.Popularity.Float32)
	}
	if content.VoteAverage.Valid {
		recorded.VoteAverage = tapeValuePointer(content.VoteAverage.Float32)
	}
	if content.VoteCount.Valid {
		recorded.VoteCount = tapeValuePointer(content.VoteCount.Uint)
	}
	if !content.CreatedAt.IsZero() {
		recorded.CreatedAt = tapeValuePointer(content.CreatedAt.Unix())
	}
	if !content.UpdatedAt.IsZero() {
		recorded.UpdatedAt = tapeValuePointer(content.UpdatedAt.Unix())
	}

	for _, collection := range content.Collections {
		recorded.Collections = append(recorded.Collections, tapeClassifierContentCollection{
			Type:   collection.Type,
			Source: collection.Source,
			ID:     collection.ID,
			Name:   collection.Name,
		})
	}
	for _, attribute := range content.Attributes {
		recorded.Attributes = append(recorded.Attributes, tapeClassifierContentAttribute{
			ContentType:   attribute.ContentType.String(),
			ContentSource: attribute.ContentSource,
			ContentID:     attribute.ContentID,
			Source:        attribute.Source,
			Key:           attribute.Key,
			Value:         attribute.Value,
		})
	}

	return recorded
}

func tapeValuePointer[T any](value T) *T { return &value }
