package processor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
)

// TapeRerunSchema identifies the machine-comparable Go/Rust rerun report.
const TapeRerunSchema = "bitmagnet.classifier-tape-rerun/v1"

// TapeRerunReport binds every per-record write set to the exact classifier
// configuration digest of the tape that supplied its input and dependencies.
type TapeRerunReport struct {
	Schema                string            `json:"schema"`
	EffectiveConfigDigest string            `json:"effectiveConfigDigest"`
	AcquisitionPlanDigest string            `json:"acquisitionPlanDigest,omitempty"`
	RecordCount           int               `json:"recordCount"`
	Records               []TapeRerunRecord `json:"records"`
}

// TapeRerunRecord is one exact same-input, same-observation replay result. Go
// verifies the recorded action sequence while running, then emits that verified
// sequence so Rust can emit and compare its own actual sequence byte-for-byte.
type TapeRerunRecord struct {
	Subject          string                 `json:"subject"`
	Attempt          int                    `json:"attempt"`
	Workflow         string                 `json:"workflow"`
	InputSHA256      string                 `json:"inputSha256"`
	ProcessorState   tape.ProcessorState    `json:"processorState"`
	ObservationCount int                    `json:"observationCount"`
	ActionEntries    []tape.ActionEntry     `json:"actionEntries"`
	Outcome          tape.RecordOutcomeKind `json:"outcome"`
	WriteSet         TapeRerunWriteSet      `json:"writeSet"`
}

// ReplayClassifierTape runs Go's real classifier and processor materializer
// over every authoritative record in a traced tape. It fails closed on legacy
// evidence: the both-rerun contract requires embedded input, processor state,
// ordered actions, deterministic outcomes, and a quiescent final generation.
func ReplayClassifierTape(ctx context.Context, replay *tape.Replay) (TapeRerunReport, error) {
	if replay == nil {
		return TapeRerunReport{}, fmt.Errorf("classifier tape replay is nil")
	}

	manifest := replay.Manifest()
	coreDigest, err := classifier.CoreEffectiveConfigDigest()
	if err != nil {
		return TapeRerunReport{}, fmt.Errorf("compute embedded core classifier digest: %w", err)
	}
	if manifest.EffectiveConfigDigest != coreDigest {
		return TapeRerunReport{}, fmt.Errorf(
			"classifier tape digest %s is not the embedded core digest %s",
			manifest.EffectiveConfigDigest,
			coreDigest,
		)
	}
	if manifest.ActionEntryCount == nil {
		return TapeRerunReport{}, fmt.Errorf("classifier tape has no action-entry trace")
	}
	if manifest.IncompleteRecordCount != 0 {
		return TapeRerunReport{}, fmt.Errorf(
			"classifier tape has %d incomplete records; final rerun evidence must be quiescent",
			manifest.IncompleteRecordCount,
		)
	}
	if manifest.AuthoritativeRecordCount != manifest.RecordCount {
		return TapeRerunReport{}, fmt.Errorf(
			"classifier tape has %d authoritative records out of %d",
			manifest.AuthoritativeRecordCount,
			manifest.RecordCount,
		)
	}

	replayer, err := classifier.NewTapeReplayer(replay)
	if err != nil {
		return TapeRerunReport{}, err
	}
	records := replay.Subjects()
	sort.Slice(records, func(i, j int) bool {
		if records[i].Subject != records[j].Subject {
			return records[i].Subject < records[j].Subject
		}
		return records[i].Attempt < records[j].Attempt
	})
	if len(records) != manifest.RecordCount {
		return TapeRerunReport{}, fmt.Errorf(
			"classifier tape exposes %d replayable records, manifest declares %d",
			len(records),
			manifest.RecordCount,
		)
	}

	results := make([]TapeRerunRecord, 0, len(records))
	for _, record := range records {
		if record.ProcessorState == nil {
			return TapeRerunReport{}, fmt.Errorf(
				"classifier tape record %s#%d has no processorState",
				record.Subject,
				record.Attempt,
			)
		}
		replayed, replayErr := replayer.Run(ctx, record)
		if replayErr != nil {
			return TapeRerunReport{}, replayErr
		}
		writeSet, materializeErr := MaterializeTapeRerun(replayed)
		if materializeErr != nil {
			return TapeRerunReport{}, materializeErr
		}
		actions := append([]tape.ActionEntry(nil), record.ActionEntries...)
		if actions == nil {
			actions = make([]tape.ActionEntry, 0)
		}
		processorState := *record.ProcessorState
		processorState.ExistingContentIDs = append(
			[]string{},
			record.ProcessorState.ExistingContentIDs...,
		)
		results = append(results, TapeRerunRecord{
			Subject:          record.Subject,
			Attempt:          record.Attempt,
			Workflow:         record.Workflow,
			InputSHA256:      fmt.Sprintf("sha256:%x", sha256.Sum256(record.Input)),
			ProcessorState:   processorState,
			ObservationCount: len(record.Observations),
			ActionEntries:    actions,
			Outcome:          replayed.Outcome.Kind,
			WriteSet:         writeSet,
		})
	}

	return TapeRerunReport{
		Schema:                TapeRerunSchema,
		EffectiveConfigDigest: manifest.EffectiveConfigDigest,
		AcquisitionPlanDigest: manifest.AcquisitionPlanDigest,
		RecordCount:           len(results),
		Records:               results,
	}, nil
}

// TapeRerunWriteSet is the stable classification-derived processor image used
// by the same-input Go/Rust rerun gate. It mirrors bitmagnet_processor::WriteSet
// and deliberately excludes volatile source snapshots and generated tsv data.
type TapeRerunWriteSet struct {
	Contents         []TapeRerunContentWrite        `json:"contents"`
	TorrentContents  []TapeRerunTorrentContentWrite `json:"torrentContents"`
	DeleteIDs        []string                       `json:"deleteIds"`
	DeleteInfoHashes []string                       `json:"deleteInfoHashes"`
	AddTags          map[string][]string            `json:"addTags"`
	FailedInfoHashes []string                       `json:"failedInfoHashes"`
}

// TapeRerunContentWrite is the stable attached-content projection already
// owned by the Rust processor materializer.
type TapeRerunContentWrite struct {
	ContentType string            `json:"contentType"`
	Source      string            `json:"source"`
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	ReleaseYear *uint16           `json:"releaseYear"`
	Identifiers map[string]string `json:"identifiers"`
}

// TapeRerunTorrentContentWrite is the stable projection of the row Go builds
// in newTorrentContent.
type TapeRerunTorrentContentWrite struct {
	ID              string   `json:"id"`
	InfoHash        string   `json:"infoHash"`
	ContentType     *string  `json:"contentType"`
	ContentSource   *string  `json:"contentSource"`
	ContentID       *string  `json:"contentId"`
	Languages       []string `json:"languages"`
	Episodes        string   `json:"episodes"`
	VideoResolution *string  `json:"videoResolution"`
	VideoSource     *string  `json:"videoSource"`
	VideoCodec      *string  `json:"videoCodec"`
	Video3D         *string  `json:"video3d"`
	VideoModifier   *string  `json:"videoModifier"`
	ReleaseGroup    *string  `json:"releaseGroup"`
	Size            uint     `json:"size"`
	FilesCount      *uint    `json:"filesCount"`
}

// MaterializeTapeRerun projects one successful Go tape replay through the same
// classification-derived rules as processor.Process. ProcessorState is
// required because classifier input intentionally omits the database IDs of
// existing torrent_contents rows, while stale-row deletion depends on them.
func MaterializeTapeRerun(replay classifier.TapeReplayResult) (TapeRerunWriteSet, error) {
	writeSet := emptyTapeRerunWriteSet()
	infoHash := replay.Torrent.InfoHash.String()

	switch replay.Outcome.Kind {
	case tape.RecordDeleted:
		writeSet.DeleteInfoHashes = append(writeSet.DeleteInfoHashes, infoHash)
	case tape.RecordUnmatched:
		writeSet.FailedInfoHashes = append(writeSet.FailedInfoHashes, infoHash)
	case tape.RecordCompleted:
		if replay.Record.ProcessorState == nil {
			return TapeRerunWriteSet{}, fmt.Errorf(
				"classifier tape record %s#%d has no processorState",
				replay.Record.Subject,
				replay.Record.Attempt,
			)
		}

		torrentContent := newTorrentContent(replay.Torrent, replay.Classification)
		writeSet.TorrentContents = append(
			writeSet.TorrentContents,
			newTapeRerunTorrentContentWrite(torrentContent),
		)
		if replay.Classification.Content != nil {
			writeSet.Contents = append(
				writeSet.Contents,
				newTapeRerunContentWrite(*replay.Classification.Content),
			)
		}

		keepID := torrentContent.InferID()
		for _, existingID := range replay.Record.ProcessorState.ExistingContentIDs {
			if existingID != keepID {
				writeSet.DeleteIDs = append(writeSet.DeleteIDs, existingID)
			}
		}
		if len(replay.Classification.Tags) > 0 {
			tags := make([]string, 0, len(replay.Classification.Tags))
			for tag := range replay.Classification.Tags {
				tags = append(tags, tag)
			}
			writeSet.AddTags[infoHash] = tags
		}
	default:
		return TapeRerunWriteSet{}, fmt.Errorf(
			"classifier tape record %s#%d has unsupported authoritative outcome %q",
			replay.Record.Subject,
			replay.Record.Attempt,
			replay.Outcome.Kind,
		)
	}

	writeSet.canonicalize()
	return writeSet, nil
}

func emptyTapeRerunWriteSet() TapeRerunWriteSet {
	return TapeRerunWriteSet{
		Contents:         make([]TapeRerunContentWrite, 0),
		TorrentContents:  make([]TapeRerunTorrentContentWrite, 0),
		DeleteIDs:        make([]string, 0),
		DeleteInfoHashes: make([]string, 0),
		AddTags:          make(map[string][]string),
		FailedInfoHashes: make([]string, 0),
	}
}

func newTapeRerunContentWrite(content model.Content) TapeRerunContentWrite {
	result := TapeRerunContentWrite{
		ContentType: content.Type.String(),
		Source:      content.Source,
		ID:          content.ID,
		Title:       content.Title,
		Identifiers: make(map[string]string),
	}
	if !content.ReleaseYear.IsNil() {
		year := uint16(content.ReleaseYear)
		result.ReleaseYear = &year
	}
	for _, attribute := range content.Attributes {
		if attribute.Key == "id" {
			result.Identifiers[attribute.Source] = attribute.Value
		}
	}
	return result
}

func newTapeRerunTorrentContentWrite(content model.TorrentContent) TapeRerunTorrentContentWrite {
	result := TapeRerunTorrentContentWrite{
		ID:        content.InferID(),
		InfoHash:  content.InfoHash.String(),
		Languages: make([]string, 0, len(content.Languages)),
		Episodes:  content.Episodes.String(),
		Size:      content.Size,
	}
	if content.ContentType.Valid {
		result.ContentType = tapeRerunPointer(content.ContentType.ContentType.String())
	}
	if content.ContentSource.Valid {
		result.ContentSource = tapeRerunPointer(content.ContentSource.String)
	}
	if content.ContentID.Valid {
		result.ContentID = tapeRerunPointer(content.ContentID.String)
	}
	for language := range content.Languages {
		result.Languages = append(result.Languages, language.String())
	}
	sort.Strings(result.Languages)
	if content.VideoResolution.Valid {
		result.VideoResolution = tapeRerunPointer(content.VideoResolution.VideoResolution.String())
	}
	if content.VideoSource.Valid {
		result.VideoSource = tapeRerunPointer(content.VideoSource.VideoSource.String())
	}
	if content.VideoCodec.Valid {
		result.VideoCodec = tapeRerunPointer(content.VideoCodec.VideoCodec.String())
	}
	if content.Video3D.Valid {
		result.Video3D = tapeRerunPointer(content.Video3D.Video3D.String())
	}
	if content.VideoModifier.Valid {
		result.VideoModifier = tapeRerunPointer(content.VideoModifier.VideoModifier.String())
	}
	if content.ReleaseGroup.Valid {
		result.ReleaseGroup = tapeRerunPointer(content.ReleaseGroup.String)
	}
	if content.FilesCount.Valid {
		result.FilesCount = tapeRerunPointer(content.FilesCount.Uint)
	}
	return result
}

func (w *TapeRerunWriteSet) canonicalize() {
	sort.Slice(w.Contents, func(i, j int) bool {
		left, right := w.Contents[i], w.Contents[j]
		if left.ContentType != right.ContentType {
			return left.ContentType < right.ContentType
		}
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		return left.ID < right.ID
	})
	sort.Slice(w.TorrentContents, func(i, j int) bool {
		left, right := w.TorrentContents[i], w.TorrentContents[j]
		if left.InfoHash != right.InfoHash {
			return left.InfoHash < right.InfoHash
		}
		return left.ID < right.ID
	})
	sort.Strings(w.DeleteIDs)
	w.DeleteIDs = dedupeSortedTapeRerunStrings(w.DeleteIDs)
	sort.Strings(w.DeleteInfoHashes)
	w.DeleteInfoHashes = dedupeSortedTapeRerunStrings(w.DeleteInfoHashes)
	sort.Strings(w.FailedInfoHashes)
	w.FailedInfoHashes = dedupeSortedTapeRerunStrings(w.FailedInfoHashes)
	for infoHash, tags := range w.AddTags {
		sort.Strings(tags)
		w.AddTags[infoHash] = dedupeSortedTapeRerunStrings(tags)
	}
}

func dedupeSortedTapeRerunStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}

	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func tapeRerunPointer[T any](value T) *T { return &value }
