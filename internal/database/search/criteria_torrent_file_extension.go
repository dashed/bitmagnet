package search

import (
	"encoding/json"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"gorm.io/gen"
)

// TorrentFileExtensionCriteria matches torrents that contain at least one file
// with one of the given extensions.
//
// It is an OR of two branches:
//   - the single-file branch: the torrent's own Torrent.Extension column;
//   - the multi-file branch: by default an EXISTS over torrent_files, but when
//     the GateFileExtensionsJSONB feature flag is ON it switches to matching the
//     denormalised torrents.file_extensions JSONB column with the @> containment
//     operator (the DROP-gate flip — lets the per-file-extension filter survive
//     the torrent_files DROP). The single-file branch is unchanged either way.
func TorrentFileExtensionCriteria(extensions ...string) query.Criteria {
	return query.GenCriteria(func(ctx query.DBContext) (query.Criteria, error) {
		q := ctx.Query()

		torrentJoins := maps.NewInsertMap(
			maps.MapEntry[string, struct{}]{Key: model.TableNameTorrent},
		)

		var multiFileBranch query.Criteria

		if FeatureFlagsValue().UseFileExtensionsJSONB() {
			sql, args := fileExtensionsJSONBContains(extensions)
			multiFileBranch = query.RawCriteria{
				Query: sql,
				Args:  args,
				Joins: torrentJoins,
			}
		} else {
			multiFileBranch = query.RawCriteria{
				Query: gen.Exists(
					q.TorrentFile.Where(
						q.TorrentFile.InfoHash.EqCol(q.Torrent.InfoHash),
						q.TorrentFile.Extension.In(extensions...),
					),
				),
				Joins: torrentJoins,
			}
		}

		return query.OrCriteria{
			Criteria: []query.Criteria{
				query.RawCriteria{
					Query: q.Torrent.Where(
						q.Torrent.Extension.In(extensions...),
					),
					Joins: torrentJoins,
				},
				multiFileBranch,
			},
		}, nil
	})
}

// fileExtensionsJSONBContains builds an OR of jsonb @> containment checks against
// torrents.file_extensions, one per extension. jsonb_path_ops GIN indexes support
// @> (containment) but NOT ?| (key existence), so a per-extension OR-of-@> is the
// index-friendly way to express "contains any of these extensions". Each arg is a
// single-element JSON array, e.g. `["mkv"]`.
func fileExtensionsJSONBContains(extensions []string) (string, []interface{}) {
	if len(extensions) == 0 {
		// No extensions → match nothing (mirrors an empty IN(...)).
		return "FALSE", nil
	}

	clauses := make([]string, 0, len(extensions))
	args := make([]interface{}, 0, len(extensions))

	for _, ext := range extensions {
		clauses = append(clauses, model.TableNameTorrent+".file_extensions @> ?::jsonb")
		// json.Marshal of a []string yields a valid JSON array and safely escapes
		// the extension, so this never produces injectable SQL.
		encoded, _ := json.Marshal([]string{ext})
		args = append(args, string(encoded))
	}

	return "(" + strings.Join(clauses, " OR ") + ")", args
}
