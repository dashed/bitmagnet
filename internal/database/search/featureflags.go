package search

import (
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrLegacyTorrentFilesReadDisabled = errors.New(
	"legacy torrent_files read is disabled in drop-compatible read mode",
)

// FeatureFlags holds the DROP-gate / search-migration feature toggles for the
// torrent_files → blob + sidecar cutover. Every flag defaults to OFF so the app
// behaves exactly as upstream until an operator explicitly flips one (and only
// after the matching validation passes — see docs/dev/dv4-go-integration-notes.md).
//
// The flags live in this package (rather than being threaded through every pure
// criteria/order function) and are published via a package-level atomic pointer,
// mirroring the established model.FilesDataDeserializer indirection. fx resolves
// the config at startup and calls SetFeatureFlags exactly once; the criteria,
// ordering and GraphQL layers read the live snapshot with FeatureFlagsValue().
type FeatureFlags struct {
	// DropCompatibleReads (D1 proof mode): when ON, all known production read
	// paths that previously depended on torrent_files must use their L1/L2/L3
	// replacements. It currently forces the effective values of
	// GateFileExtensionsJSONB and FileBrowserFromBlob and also tells repair code
	// not to clear files_data for a torrent_files-backed rebuild.
	DropCompatibleReads bool

	// GateFileExtensionsJSONB (DROP-gate flip, measured FB-A1): when ON, the
	// multi-file branch of TorrentFileExtensionCriteria stops doing an
	// EXISTS(torrent_files ...) sub-query and instead matches the denormalised
	// torrents.file_extensions JSONB column with the @> containment operator
	// (jsonb_path_ops GIN supported). The single-file Torrent.Extension.In branch
	// is unchanged. Flip ONLY after DV-1 confirms prod ext-parity (the JSONB
	// column == the per-row EXISTS result for a representative sample).
	GateFileExtensionsJSONB bool

	// PopularitySortDefault (FIND-2, measured 49s → ms): when ON, a query-string
	// search whose order is exactly the web-UI default single `relevance` clause
	// is rewritten server-side to `seeders DESC, info_hash` — avoiding the
	// ts_rank_cd-over-the-whole-match-set wall. True relevance stays opt-in: any
	// order that names a non-relevance field, or relevance plus an explicit extra
	// field, is left untouched. See notes for the UI-side alternative.
	PopularitySortDefault bool

	// FileBrowserFromBlob (G2): when ON, the per-torrent file browser
	// (TorrentQuery.Files) is served from the AfterFind-hydrated blob
	// (torrents.files_data) instead of a residual `SELECT FROM torrent_files`,
	// so the browser keeps working after the torrent_files DROP. Flip only once
	// the blob is proven a faithful source of truth in prod (DV-1 / verify-full).
	FileBrowserFromBlob bool

	// FileSearchEnabled (DV-2/DV-3 wiring): master switch for the GraphQL
	// fileSearch + pathTypeahead resolvers. When OFF the resolvers report the
	// feature as disabled; when ON they delegate to the configured sidecar
	// client. Flip only once the DuckDB/path-FTS sidecar is deployed and proven.
	FileSearchEnabled bool
}

var featureFlags atomic.Pointer[FeatureFlags]

// FeatureFlagsValue returns the live feature-flag snapshot. It never returns a
// nil-deref: before fx wires the config (and in unit tests that don't), all
// flags read as their zero value (OFF), which is the safe upstream behaviour.
func FeatureFlagsValue() FeatureFlags {
	if f := featureFlags.Load(); f != nil {
		return *f
	}

	return FeatureFlags{}
}

// SetFeatureFlags publishes a new flag snapshot. It is safe for concurrent use;
// fx calls it once at startup and tests call it to exercise both code paths.
func SetFeatureFlags(f FeatureFlags) {
	featureFlags.Store(&f)
}

// UseFileExtensionsJSONB is the effective gate for the multi-file extension
// filter. DropCompatibleReads forces it on because the legacy branch queries
// torrent_files.
func (f FeatureFlags) UseFileExtensionsJSONB() bool {
	return f.DropCompatibleReads || f.GateFileExtensionsJSONB
}

// UseFileBrowserFromBlob is the effective gate for TorrentQuery.Files. In
// drop-compatible read mode the per-torrent browser must not query torrent_files.
func (f FeatureFlags) UseFileBrowserFromBlob() bool {
	return f.DropCompatibleReads || f.FileBrowserFromBlob
}

// AllowTorrentFilesRepair reports whether live repair may clear files_data and
// rely on torrent_files for a later rebuild. That repair mode is pre-DROP only.
func (f FeatureFlags) AllowTorrentFilesRepair() bool {
	return !f.DropCompatibleReads
}

// CheckLegacyTorrentFilesReadAllowed fails closed for any served path that still
// has a legacy torrent_files implementation. Pre-DROP verifier/backfill tooling
// should not use this package-level Search API for legacy-table parity.
func (f FeatureFlags) CheckLegacyTorrentFilesReadAllowed(surface string) error {
	if !f.DropCompatibleReads {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrLegacyTorrentFilesReadDisabled, surface)
}
