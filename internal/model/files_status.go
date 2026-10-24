package model

// FilesStatus represents what we know about the files in a Torrent
// ENUM(no_info, single, multi, over_threshold)
type FilesStatus string

// HasStoredFileList reports whether a torrent with this status is expected to
// have a stored file list. A no_info / over_threshold torrent has none BY
// NATURE rather than through a missing-blob failure: no_info never had one, and
// over_threshold's was too large to store. Classifier rules that evaluate
// torrent.files therefore cannot fire for those torrents at all, which is not
// the same as the rules evaluating and finding nothing.
func (x FilesStatus) HasStoredFileList() bool {
	return x != FilesStatusNoInfo && x != FilesStatusOverThreshold
}
