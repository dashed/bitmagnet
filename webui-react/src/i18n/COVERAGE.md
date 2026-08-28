# React i18n Catalog Coverage

Generated from the React catalog keys in `src/i18n/locales/en.ts` and the Angular Transloco catalogs in `../../webui/src/app/i18n/translations/*.json`. Non-English React locale modules only include confident mappings with real translated values; `__missing__` marker values are stripped so i18next falls back to English.

## Source Catalogs

Angular catalogs (14): `ar.json`, `ca.json`, `de.json`, `en.json`, `es.json`, `fr.json`, `hi.json`, `ja.json`, `nl.json`, `pt.json`, `ru.json`, `tr.json`, `uk.json`, `zh.json`.
React English keys: 442. Static inline defaults folded into English: 158.

## P4 React-Only Keys

The `search.modes.*`, `fileSearch.*`, and `paths.*` keys are en-only for the P4
beyond-parity search surface. Angular never exposed file search, path
typeahead, or collapse-path browsing, so there is no Angular Transloco source
catalog to map from.

## Mapping Summary

| Confidence                                    | Keys |
| --------------------------------------------- | ---: |
| Exact English value                           |  176 |
| Clear corresponding key/value                 |   25 |
| No confident Transloco source; fallback to en |  241 |

## Per-Language Coverage

| Language | Keys included | Coverage | English fallbacks | `__missing__` stripped | Placeholder mismatches stripped |
| -------- | ------------: | -------: | ----------------: | ---------------------: | ------------------------------: |
| ar       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| ca       |       151/442 |    34.2% |               291 |                     25 |                               1 |
| de       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| es       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| fr       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| hi       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| ja       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| nl       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| pt       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| ru       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| tr       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| uk       |       176/442 |    39.8% |               266 |                     23 |                               0 |
| zh       |       176/442 |    39.8% |               266 |                     23 |                               0 |

## Mapping Table

| React key                              | English value                                                                            | Transloco key                                             | Confidence    |
| -------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------- | ------------- |
| `actions.bulkLabel`                    | Bulk torrent actions                                                                     | English fallback                                          | fallback      |
| `actions.clearSelection`               | Clear selection                                                                          | English fallback                                          | fallback      |
| `actions.copy.body`                    | Copy selected values to the clipboard.                                                   | English fallback                                          | fallback      |
| `actions.copy.infoHashError`           | Could not copy info hashes                                                               | English fallback                                          | fallback      |
| `actions.copy.infoHashes`              | Info hashes                                                                              | `torrents.info_hashes`                                    | exact         |
| `actions.copy.infoHashSuccess`         | Copied info hash                                                                         | English fallback                                          | fallback      |
| `actions.copy.infoHashSuccess_other`   | Copied {{count}} info hashes                                                             | English fallback                                          | fallback      |
| `actions.copy.magnetError`             | Could not copy magnet links                                                              | English fallback                                          | fallback      |
| `actions.copy.magnetLinks`             | Magnet links                                                                             | `torrents.magnet_links`                                   | exact         |
| `actions.copy.magnetSuccess`           | Copied magnet link                                                                       | English fallback                                          | fallback      |
| `actions.copy.magnetSuccess_other`     | Copied {{count}} magnet links                                                            | English fallback                                          | fallback      |
| `actions.copy.title`                   | Copy                                                                                     | `torrents.copy`                                           | exact         |
| `actions.delete.acknowledge`           | I understand this cannot be undone                                                       | English fallback                                          | fallback      |
| `actions.delete.cancel`                | Cancel                                                                                   | `general.cancel`                                          | exact         |
| `actions.delete.confirm`               | Delete                                                                                   | `torrents.delete`                                         | exact         |
| `actions.delete.dialogBody`            | This will delete {{count}} selected torrent.                                             | English fallback                                          | fallback      |
| `actions.delete.dialogBody_other`      | This will delete {{count}} selected torrents.                                            | English fallback                                          | fallback      |
| `actions.delete.dialogTitle`           | Delete {{count}} torrent?                                                                | English fallback                                          | fallback      |
| `actions.delete.dialogTitle_other`     | Delete {{count}} torrents?                                                               | English fallback                                          | fallback      |
| `actions.delete.error`                 | Error deleting torrents: {{error}}                                                       | English fallback                                          | fallback      |
| `actions.delete.open`                  | Delete                                                                                   | `torrents.delete`                                         | exact         |
| `actions.delete.success`               | Deleted {{count}} torrent                                                                | English fallback                                          | fallback      |
| `actions.delete.success_other`         | Deleted {{count}} torrents                                                               | English fallback                                          | fallback      |
| `actions.delete.title`                 | Delete                                                                                   | `torrents.delete`                                         | exact         |
| `actions.delete.warning`               | This action cannot be undone.                                                            | English fallback                                          | fallback      |
| `actions.deselectPage`                 | Deselect results on this page                                                            | English fallback                                          | fallback      |
| `actions.deselectResult`               | Deselect {{title}}                                                                       | English fallback                                          | fallback      |
| `actions.loading`                      | Loading actions                                                                          | English fallback                                          | fallback      |
| `actions.reprocess.error`              | Error reprocessing torrents: {{error}}                                                   | English fallback                                          | fallback      |
| `actions.reprocess.externalApiSearch`  | Match content by external API search                                                     | `torrents.reprocess.match_content_by_external_api_search` | exact         |
| `actions.reprocess.forceRematch`       | Force rematch of already matched content                                                 | `torrents.reprocess.force_rematch`                        | exact         |
| `actions.reprocess.localSearch`        | Match content by local search                                                            | `torrents.reprocess.match_content_by_local_search`        | exact         |
| `actions.reprocess.options`            | Reprocess options                                                                        | English fallback                                          | fallback      |
| `actions.reprocess.submit`             | Reprocess                                                                                | `torrents.reprocess.reprocess`                            | exact         |
| `actions.reprocess.success`            | Queued {{count}} torrent for reprocessing                                                | English fallback                                          | fallback      |
| `actions.reprocess.success_other`      | Queued {{count}} torrents for reprocessing                                               | English fallback                                          | fallback      |
| `actions.reprocess.title`              | Reprocess                                                                                | `torrents.reprocess.reprocess`                            | exact         |
| `actions.selectedCount`                | {{count}} selected                                                                       | English fallback                                          | fallback      |
| `actions.selectPage`                   | Select page                                                                              | English fallback                                          | fallback      |
| `actions.selectResult`                 | Select {{title}}                                                                         | English fallback                                          | fallback      |
| `actions.tags.delete`                  | Remove from selected                                                                     | English fallback                                          | fallback      |
| `actions.tags.deleteSuccess`           | Removed tags from {{count}} torrent                                                      | English fallback                                          | fallback      |
| `actions.tags.deleteSuccess_other`     | Removed tags from {{count}} torrents                                                     | English fallback                                          | fallback      |
| `actions.tags.error`                   | Error updating tags: {{error}}                                                           | English fallback                                          | fallback      |
| `actions.tags.inputLabel`              | Tags                                                                                     | English fallback                                          | fallback      |
| `actions.tags.placeholder`             | Add a tag                                                                                | English fallback                                          | fallback      |
| `actions.tags.put`                     | Add to selected                                                                          | English fallback                                          | fallback      |
| `actions.tags.putSuccess`              | Added tags to {{count}} torrent                                                          | English fallback                                          | fallback      |
| `actions.tags.putSuccess_other`        | Added tags to {{count}} torrents                                                         | English fallback                                          | fallback      |
| `actions.tags.removeChip`              | Remove {{tagName}}                                                                       | English fallback                                          | fallback      |
| `actions.tags.set`                     | Replace on selected                                                                      | English fallback                                          | fallback      |
| `actions.tags.setSuccess`              | Replaced tags on {{count}} torrent                                                       | English fallback                                          | fallback      |
| `actions.tags.setSuccess_other`        | Replaced tags on {{count}} torrents                                                      | English fallback                                          | fallback      |
| `actions.tags.suggestionError`         | Error loading tag suggestions: {{error}}                                                 | English fallback                                          | fallback      |
| `actions.tags.suggestionsLabel`        | Tag suggestions                                                                          | English fallback                                          | fallback      |
| `actions.tags.title`                   | Tags                                                                                     | English fallback                                          | fallback      |
| `actions.title`                        | Actions                                                                                  | English fallback                                          | fallback      |
| `app.title`                            | bitmagnet                                                                                | English fallback                                          | fallback      |
| `app.version`                          | v0.0.0                                                                                   | English fallback                                          | fallback      |
| `contentTypes.audiobook`               | Audiobook                                                                                | `content_types.singular.audiobook`                        | exact         |
| `contentTypes.comic`                   | Comic                                                                                    | `content_types.singular.comic`                            | exact         |
| `contentTypes.ebook`                   | Ebook                                                                                    | `content_types.singular.ebook`                            | corresponding |
| `contentTypes.game`                    | Game                                                                                     | `content_types.singular.game`                             | exact         |
| `contentTypes.movie`                   | Movie                                                                                    | `content_types.singular.movie`                            | exact         |
| `contentTypes.music`                   | Music                                                                                    | `content_types.singular.music`                            | exact         |
| `contentTypes.software`                | Software                                                                                 | `content_types.singular.software`                         | exact         |
| `contentTypes.tv_show`                 | TV show                                                                                  | `content_types.singular.tv_show`                          | corresponding |
| `contentTypes.unknown`                 | Unknown                                                                                  | `content_types.singular.null`                             | exact         |
| `contentTypes.xxx`                     | XXX                                                                                      | `content_types.singular.xxx`                              | exact         |
| `contentTypesPlural.audiobook`         | Audiobooks                                                                               | `content_types.plural.audiobook`                          | exact         |
| `contentTypesPlural.comic`             | Comics                                                                                   | `content_types.plural.comic`                              | exact         |
| `contentTypesPlural.ebook`             | Ebooks                                                                                   | `content_types.plural.ebook`                              | corresponding |
| `contentTypesPlural.game`              | Games                                                                                    | `content_types.plural.game`                               | exact         |
| `contentTypesPlural.movie`             | Movies                                                                                   | `content_types.plural.movie`                              | exact         |
| `contentTypesPlural.music`             | Music                                                                                    | `content_types.plural.music`                              | exact         |
| `contentTypesPlural.software`          | Software                                                                                 | `content_types.plural.software`                           | exact         |
| `contentTypesPlural.tv_show`           | TV shows                                                                                 | `content_types.plural.tv_show`                            | corresponding |
| `contentTypesPlural.unknown`           | Unknown                                                                                  | `content_types.plural.null`                               | exact         |
| `contentTypesPlural.xxx`               | XXX                                                                                      | `content_types.plural.xxx`                                | exact         |
| `dash.body`                            | At-a-glance torrent, queue, and service health status.                                   | English fallback                                          | fallback      |
| `dash.errorBody`                       | Try again.                                                                               | English fallback                                          | fallback      |
| `dash.eyebrow`                         | Operations                                                                               | English fallback                                          | fallback      |
| `dash.health.checksLabel`              | checks                                                                                   | English fallback                                          | fallback      |
| `dash.health.meta`                     | Health checks and worker status                                                          | English fallback                                          | fallback      |
| `dash.health.status.down`              | Down                                                                                     | `health.statuses.down`                                    | exact         |
| `dash.health.status.inactive`          | Inactive                                                                                 | `health.statuses.inactive`                                | exact         |
| `dash.health.status.unknown`           | Unknown                                                                                  | `content_types.plural.null`                               | exact         |
| `dash.health.status.up`                | Up                                                                                       | `health.statuses.up`                                      | exact         |
| `dash.health.title`                    | Health                                                                                   | English fallback                                          | fallback      |
| `dash.health.workersStartedLabel`      | workers started                                                                          | English fallback                                          | fallback      |
| `dash.links.health.body`               | Open service checks and worker status.                                                   | English fallback                                          | fallback      |
| `dash.links.health.title`              | Health                                                                                   | English fallback                                          | fallback      |
| `dash.links.queue.body`                | Inspect queue jobs and processing state.                                                 | English fallback                                          | fallback      |
| `dash.links.queue.title`               | Queue                                                                                    | `dashboard.queues.queue`                                  | exact         |
| `dash.links.title`                     | Quick links                                                                              | English fallback                                          | fallback      |
| `dash.loading`                         | Loading                                                                                  | English fallback                                          | fallback      |
| `dash.metrics.errorTitle`              | Queue metrics failed                                                                     | English fallback                                          | fallback      |
| `dash.metrics.eventsInRange`           | events in range                                                                          | English fallback                                          | fallback      |
| `dash.metrics.title`                   | Queue throughput                                                                         | English fallback                                          | fallback      |
| `dash.queue.meta`                      | Jobs across all queues                                                                   | English fallback                                          | fallback      |
| `dash.queue.status.failed`             | Failed                                                                                   | `dashboard.event.failed`                                  | exact         |
| `dash.queue.status.pending`            | Pending                                                                                  | `dashboard.queues.pending`                                | exact         |
| `dash.queue.status.processed`          | Processed                                                                                | `dashboard.event.processed`                               | exact         |
| `dash.queue.status.retry`              | Retry                                                                                    | `dashboard.queues.retry`                                  | exact         |
| `dash.queue.statusesLabel`             | Queue status counts                                                                      | English fallback                                          | fallback      |
| `dash.queue.title`                     | Queue jobs                                                                               | `dashboard.home.queue_jobs`                               | exact         |
| `dash.retry`                           | Retry                                                                                    | `dashboard.queues.retry`                                  | exact         |
| `dash.title`                           | Dashboard                                                                                | `routes.dashboard`                                        | exact         |
| `dash.torrentMetrics.errorTitle`       | Torrent metrics failed                                                                   | English fallback                                          | fallback      |
| `dash.torrentMetrics.eventsInRange`    | {{count}} events in range                                                                | English fallback                                          | fallback      |
| `dash.torrentMetrics.loadingCharts`    | Loading charts                                                                           | English fallback                                          | fallback      |
| `dash.torrentMetrics.title`            | Torrent throughput                                                                       | English fallback                                          | fallback      |
| `dash.torrents.estimateMeta`           | Estimated indexed torrent records                                                        | English fallback                                          | fallback      |
| `dash.torrents.meta`                   | Indexed torrent records                                                                  | English fallback                                          | fallback      |
| `dash.torrents.title`                  | Torrents                                                                                 | `routes.torrents`                                         | exact         |
| `dash.unavailable`                     | Unavailable                                                                              | English fallback                                          | fallback      |
| `dashboard.body`                       | No dashboard data yet.                                                                   | English fallback                                          | fallback      |
| `dashboard.title`                      | Dashboard                                                                                | `routes.dashboard`                                        | exact         |
| `detail.content`                       | Content                                                                                  | English fallback                                          | fallback      |
| `detail.copyInfoHash`                  | Copy hash                                                                                | English fallback                                          | fallback      |
| `detail.dhtFirstSeen`                  | DHT first seen                                                                           | `torrents.dht_first_seen`                                 | exact         |
| `detail.dhtLastSeen`                   | DHT last seen                                                                            | `torrents.dht_last_seen`                                  | exact         |
| `detail.dhtSeen`                       | DHT seen                                                                                 | `torrents.dht_seen`                                       | exact         |
| `detail.dhtSeenCount`                  | DHT crawl count                                                                          | `torrents.dht_seen_count`                                 | exact         |
| `detail.dhtSeenSummary`                | seen {{time}} · {{seenCount}}×                                                           | English fallback                                          | fallback      |
| `detail.episodes`                      | Episodes                                                                                 | `torrents.episodes`                                       | exact         |
| `detail.externalLinks`                 | External links                                                                           | `torrents.external_links`                                 | exact         |
| `detail.fileFilterLabel`               | Filter files                                                                             | English fallback                                          | fallback      |
| `detail.fileFilterPlaceholder`         | Filter files...                                                                          | English fallback                                          | fallback      |
| `detail.fileIndex`                     | Index                                                                                    | `torrents.file_index`                                     | corresponding |
| `detail.fileIndexValue`                | #{{index}}                                                                               | English fallback                                          | fallback      |
| `detail.filePath`                      | Path / Name                                                                              | `torrents.file_path`                                      | corresponding |
| `detail.files`                         | Files                                                                                    | `torrents.files`                                          | exact         |
| `detail.filesCount`                    | {{count}} file                                                                           | English fallback                                          | fallback      |
| `detail.filesCount_other`              | {{count}} files                                                                          | `torrents.files_count_n`                                  | exact         |
| `detail.filesEmpty`                    | No file rows are available.                                                              | English fallback                                          | fallback      |
| `detail.filesFilterEmpty`              | No files match this filter.                                                              | English fallback                                          | fallback      |
| `detail.fileSize`                      | Size                                                                                     | `torrents.file_size`                                      | corresponding |
| `detail.filesLimitedWindow`            | Sorting and search cover the first {{shown}} of {{total}} files                          | English fallback                                          | fallback      |
| `detail.filesLoading`                  | Loading files                                                                            | English fallback                                          | fallback      |
| `detail.filesMatchCount`               | {{count}} of {{total}} files match                                                       | English fallback                                          | fallback      |
| `detail.filesNoInfo`                   | No file information is available for this torrent.                                       | `torrents.files_no_info`                                  | corresponding |
| `detail.fileSortAscending`             | Asc                                                                                      | English fallback                                          | fallback      |
| `detail.fileSortDescending`            | Desc                                                                                     | English fallback                                          | fallback      |
| `detail.filesPage`                     | Page {{page}} of {{totalPages}}                                                          | English fallback                                          | fallback      |
| `detail.filesShowingCount`             | Showing {{shown}} of {{total}} files                                                     | English fallback                                          | fallback      |
| `detail.fileType`                      | Type                                                                                     | `torrents.file_type`                                      | corresponding |
| `detail.firstSeen`                     | First seen                                                                               | English fallback                                          | fallback      |
| `detail.genres`                        | Genres                                                                                   | `torrents.genres`                                         | exact         |
| `detail.infoHash`                      | Info hash                                                                                | `torrents.info_hash`                                      | exact         |
| `detail.languages`                     | Languages                                                                                | `torrents.languages`                                      | exact         |
| `detail.lastSeen`                      | Last seen                                                                                | English fallback                                          | fallback      |
| `detail.loading`                       | Loading torrent details                                                                  | English fallback                                          | fallback      |
| `detail.notFoundBody`                  | No torrent matched this info hash.                                                       | English fallback                                          | fallback      |
| `detail.notFoundTitle`                 | Torrent not found                                                                        | English fallback                                          | fallback      |
| `detail.originalMarker`                | (original)                                                                               | English fallback                                          | fallback      |
| `detail.originalTitle`                 | Original title                                                                           | English fallback                                          | fallback      |
| `detail.overview`                      | Overview                                                                                 | English fallback                                          | fallback      |
| `detail.peers`                         | Seeders / Leechers                                                                       | English fallback                                          | fallback      |
| `detail.posterAlt`                     | Poster for {{title}}                                                                     | English fallback                                          | fallback      |
| `detail.published`                     | Published                                                                                | `torrents.published`                                      | exact         |
| `detail.rating`                        | Rating                                                                                   | `torrents.rating`                                         | exact         |
| `detail.ratingVotes`                   | {{count}} vote                                                                           | English fallback                                          | fallback      |
| `detail.ratingVotes_other`             | {{count}} votes                                                                          | `torrents.votes_count_n`                                  | exact         |
| `detail.releaseDate`                   | Release date                                                                             | English fallback                                          | fallback      |
| `detail.returnToSearch`                | Return to torrents                                                                       | English fallback                                          | fallback      |
| `detail.seen`                          | Seen                                                                                     | English fallback                                          | fallback      |
| `detail.size`                          | Size                                                                                     | `torrents.size`                                           | exact         |
| `detail.sources`                       | Sources                                                                                  | English fallback                                          | fallback      |
| `detail.sourceSeenCount`               | {{count}} time                                                                           | English fallback                                          | fallback      |
| `detail.sourceSeenCount_other`         | {{count}} times                                                                          | English fallback                                          | fallback      |
| `detail.unknown`                       | Unknown                                                                                  | `content_types.plural.null`                               | exact         |
| `error.empty`                          | Nothing to show.                                                                         | English fallback                                          | fallback      |
| `error.loading`                        | Loading...                                                                               | English fallback                                          | fallback      |
| `error.notFound`                       | Not found                                                                                | English fallback                                          | fallback      |
| `error.retry`                          | Retry                                                                                    | `dashboard.queues.retry`                                  | exact         |
| `error.title`                          | Something went wrong                                                                     | English fallback                                          | fallback      |
| `facets.clear`                         | Clear                                                                                    | English fallback                                          | fallback      |
| `facets.file_type`                     | File type                                                                                | `facets.file_type`                                        | corresponding |
| `facets.genre`                         | Genre                                                                                    | `facets.genre`                                            | exact         |
| `facets.language`                      | Language                                                                                 | `facets.language`                                         | exact         |
| `facets.none`                          | No values                                                                                | English fallback                                          | fallback      |
| `facets.reset`                         | Reset all filters                                                                        | English fallback                                          | fallback      |
| `facets.torrent_source`                | Torrent source                                                                           | `facets.torrent_source`                                   | corresponding |
| `facets.torrent_tag`                   | Torrent tag                                                                              | `facets.torrent_tag`                                      | corresponding |
| `facets.unknown`                       | Unknown                                                                                  | `content_types.plural.null`                               | exact         |
| `facets.video_resolution`              | Video resolution                                                                         | English fallback                                          | fallback      |
| `facets.video_source`                  | Video source                                                                             | English fallback                                          | fallback      |
| `fileTypes.archive`                    | Archive                                                                                  | `file_types.archive`                                      | exact         |
| `fileTypes.audio`                      | Audio                                                                                    | `file_types.audio`                                        | exact         |
| `fileTypes.data`                       | Data                                                                                     | `file_types.data`                                         | exact         |
| `fileTypes.document`                   | Document                                                                                 | `file_types.document`                                     | exact         |
| `fileTypes.image`                      | Image                                                                                    | `file_types.image`                                        | exact         |
| `fileTypes.software`                   | Software                                                                                 | `file_types.software`                                     | exact         |
| `fileTypes.subtitles`                  | Subtitles                                                                                | `file_types.subtitles`                                    | exact         |
| `fileTypes.unknown`                    | Unknown                                                                                  | `file_types.unknown`                                      | exact         |
| `fileTypes.video`                      | Video                                                                                    | `file_types.video`                                        | exact         |
| `health.bitmagnet_is_status`           | bitmagnet is {{status}}                                                                  | `health.bitmagnet_is_status`                              | exact         |
| `health.checks`                        | Checks                                                                                   | English fallback                                          | fallback      |
| `health.description`                   | Service checks and worker state.                                                         | English fallback                                          | fallback      |
| `health.error`                         | Error                                                                                    | `health.error`                                            | exact         |
| `health.errorFallback`                 | Unknown error                                                                            | English fallback                                          | fallback      |
| `health.key`                           | Key                                                                                      | English fallback                                          | fallback      |
| `health.lastChecked`                   | Last checked                                                                             | English fallback                                          | fallback      |
| `health.lastUpdated`                   | Updated {{time}}                                                                         | English fallback                                          | fallback      |
| `health.loadFailed`                    | Health check failed                                                                      | English fallback                                          | fallback      |
| `health.loading`                       | Loading health status                                                                    | English fallback                                          | fallback      |
| `health.noChecks`                      | No checks reported.                                                                      | English fallback                                          | fallback      |
| `health.noError`                       | None                                                                                     | `general.none`                                            | exact         |
| `health.noWorkers`                     | No workers reported.                                                                     | English fallback                                          | fallback      |
| `health.overallStatus`                 | Overall status                                                                           | English fallback                                          | fallback      |
| `health.overallStatuses.degraded`      | Degraded                                                                                 | `health.statuses.degraded`                                | exact         |
| `health.overallStatuses.ok`            | OK                                                                                       | English fallback                                          | fallback      |
| `health.refresh`                       | Refresh                                                                                  | `general.refresh`                                         | exact         |
| `health.refreshing`                    | Refreshing...                                                                            | English fallback                                          | fallback      |
| `health.retry`                         | Retry                                                                                    | `dashboard.queues.retry`                                  | exact         |
| `health.status`                        | Status                                                                                   | `health.status`                                           | exact         |
| `health.statuses.down`                 | Down                                                                                     | `health.statuses.down`                                    | exact         |
| `health.statuses.inactive`             | Inactive                                                                                 | `health.statuses.inactive`                                | exact         |
| `health.statuses.unknown`              | Pending                                                                                  | `health.statuses.unknown`                                 | exact         |
| `health.statuses.up`                   | Up                                                                                       | `health.statuses.up`                                      | exact         |
| `health.title`                         | Health                                                                                   | English fallback                                          | fallback      |
| `health.workers`                       | Workers                                                                                  | English fallback                                          | fallback      |
| `health.workerStates.started`          | Started                                                                                  | `health.statuses.started`                                 | exact         |
| `health.workerStates.stopped`          | Stopped                                                                                  | English fallback                                          | fallback      |
| `language.label`                       | Language                                                                                 | `facets.language`                                         | exact         |
| `metrics.autoRefresh.minutes_1`        | 1m                                                                                       | English fallback                                          | fallback      |
| `metrics.autoRefresh.minutes_5`        | 5m                                                                                       | English fallback                                          | fallback      |
| `metrics.autoRefresh.off`              | Off                                                                                      | `dashboard.interval.off`                                  | exact         |
| `metrics.autoRefresh.seconds_10`       | 10s                                                                                      | English fallback                                          | fallback      |
| `metrics.autoRefresh.seconds_30`       | 30s                                                                                      | English fallback                                          | fallback      |
| `metrics.bucketDurations.day`          | Days                                                                                     | `dashboard.interval.days`                                 | exact         |
| `metrics.bucketDurations.hour`         | Hours                                                                                    | `dashboard.interval.hours`                                | exact         |
| `metrics.bucketDurations.minute`       | Minutes                                                                                  | `dashboard.interval.minutes`                              | exact         |
| `metrics.charts.empty`                 | No metric buckets to show.                                                               | English fallback                                          | fallback      |
| `metrics.charts.seconds`               | {{value}}s                                                                               | English fallback                                          | fallback      |
| `metrics.controls.allEvents`           | All events                                                                               | English fallback                                          | fallback      |
| `metrics.controls.allQueues`           | All queues                                                                               | English fallback                                          | fallback      |
| `metrics.controls.allSources`          | All sources                                                                              | English fallback                                          | fallback      |
| `metrics.controls.bucketDuration`      | Bucket                                                                                   | English fallback                                          | fallback      |
| `metrics.controls.bucketMultiplier`    | Multiplier                                                                               | English fallback                                          | fallback      |
| `metrics.controls.event`               | Event                                                                                    | `dashboard.metrics.event`                                 | exact         |
| `metrics.controls.lastUpdated`         | Updated {{time}}                                                                         | English fallback                                          | fallback      |
| `metrics.controls.loading`             | Loading metrics                                                                          | English fallback                                          | fallback      |
| `metrics.controls.queue`               | Queue                                                                                    | `dashboard.queues.queue`                                  | exact         |
| `metrics.controls.refresh`             | Refresh                                                                                  | `general.refresh`                                         | exact         |
| `metrics.controls.source`              | Source                                                                                   | English fallback                                          | fallback      |
| `metrics.controls.timeframe`           | Timeframe                                                                                | `dashboard.metrics.timeframe`                             | exact         |
| `metrics.controls.waiting`             | Waiting for data                                                                         | `dashboard.live.waiting_for_data`                         | exact         |
| `metrics.events.created`               | Created                                                                                  | `dashboard.event.created`                                 | exact         |
| `metrics.events.failed`                | Failed                                                                                   | `dashboard.event.failed`                                  | exact         |
| `metrics.events.processed`             | Processed                                                                                | `dashboard.event.processed`                               | exact         |
| `metrics.events.updated`               | Updated                                                                                  | `dashboard.event.updated`                                 | exact         |
| `metrics.statuses.failed`              | Failed                                                                                   | `dashboard.queues.failed`                                 | exact         |
| `metrics.statuses.pending`             | Pending                                                                                  | `dashboard.queues.pending`                                | exact         |
| `metrics.statuses.processed`           | Processed                                                                                | `dashboard.queues.processed`                              | exact         |
| `metrics.statuses.retry`               | Retry                                                                                    | `dashboard.queues.retry`                                  | exact         |
| `metrics.timeframes.all`               | All time                                                                                 | `dashboard.interval.all`                                  | corresponding |
| `metrics.timeframes.days_1`            | 1 day                                                                                    | `dashboard.interval.days_1`                               | exact         |
| `metrics.timeframes.hours_1`           | 1 hour                                                                                   | `dashboard.interval.hours_1`                              | exact         |
| `metrics.timeframes.hours_12`          | 12 hours                                                                                 | `dashboard.interval.hours_12`                             | exact         |
| `metrics.timeframes.hours_6`           | 6 hours                                                                                  | `dashboard.interval.hours_6`                              | exact         |
| `metrics.timeframes.minutes_15`        | 15 minutes                                                                               | `dashboard.interval.minutes_15`                           | exact         |
| `metrics.timeframes.minutes_30`        | 30 minutes                                                                               | `dashboard.interval.minutes_30`                           | exact         |
| `metrics.timeframes.weeks_1`           | 1 week                                                                                   | `dashboard.interval.weeks_1`                              | exact         |
| `nav.classicUi`                        | Classic UI                                                                               | English fallback                                          | fallback      |
| `nav.dashboard`                        | Dashboard                                                                                | `routes.dashboard`                                        | exact         |
| `nav.torrents`                         | Torrents                                                                                 | `routes.torrents`                                         | exact         |
| `queue.admin.acknowledge`              | I understand this permanently deletes the selected queue jobs.                           | English fallback                                          | fallback      |
| `queue.admin.allQueues`                | all queues                                                                               | English fallback                                          | fallback      |
| `queue.admin.allStatuses`              | all statuses                                                                             | English fallback                                          | fallback      |
| `queue.admin.body`                     | Purge queue jobs by queue and status, or enqueue a scoped torrent reprocess batch.       | English fallback                                          | fallback      |
| `queue.admin.cancel`                   | Cancel                                                                                   | `general.cancel`                                          | exact         |
| `queue.admin.confirmPurge`             | Purge jobs                                                                               | `dashboard.queues.purge_jobs`                             | exact         |
| `queue.admin.dialogBody`               | This will delete jobs in {{queueScope}} with {{statusScope}}.                            | English fallback                                          | fallback      |
| `queue.admin.dialogError`              | Purge failed: {{error}}                                                                  | English fallback                                          | fallback      |
| `queue.admin.dialogTitle`              | Purge queue jobs                                                                         | `dashboard.queues.purge_queue_jobs`                       | exact         |
| `queue.admin.fullPurgeWarning`         | No queue or status scope is selected, so this will purge the entire queue table.         | English fallback                                          | fallback      |
| `queue.admin.openPurge`                | Purge jobs                                                                               | `dashboard.queues.purge_jobs`                             | exact         |
| `queue.admin.openReprocess`            | Enqueue reprocess batch                                                                  | English fallback                                          | fallback      |
| `queue.admin.purgeError`               | Failed to purge queue jobs: {{error}}                                                    | English fallback                                          | fallback      |
| `queue.admin.purgeSuccess`             | Queue jobs purged                                                                        | `dashboard.queues.queue_purged`                           | corresponding |
| `queue.admin.purging`                  | Purging                                                                                  | English fallback                                          | fallback      |
| `queue.admin.queueScope`               | Queue scope                                                                              | English fallback                                          | fallback      |
| `queue.admin.reprocessAcknowledge`     | I understand this will enqueue jobs for the selected torrent scope.                      | English fallback                                          | fallback      |
| `queue.admin.reprocessAllContentTypes` | All                                                                                      | `dashboard.interval.all`                                  | exact         |
| `queue.admin.reprocessApiSearch`       | Match content by external API search                                                     | `torrents.reprocess.match_content_by_external_api_search` | exact         |
| `queue.admin.reprocessConfirm`         | Enqueue jobs                                                                             | `dashboard.queues.enqueue_jobs`                           | exact         |
| `queue.admin.reprocessContentTypes`    | Content types                                                                            | English fallback                                          | fallback      |
| `queue.admin.reprocessDialogBody`      | This will enqueue a batch reprocess job using the selected classifier and content scope. | English fallback                                          | fallback      |
| `queue.admin.reprocessDialogError`     | Enqueue failed: {{error}}                                                                | English fallback                                          | fallback      |
| `queue.admin.reprocessDialogTitle`     | Enqueue torrent processing batch                                                         | `dashboard.queues.enqueue_torrent_processing_batch`       | corresponding |
| `queue.admin.reprocessEnqueuing`       | Enqueuing                                                                                | English fallback                                          | fallback      |
| `queue.admin.reprocessError`           | Failed to enqueue jobs: {{error}}                                                        | English fallback                                          | fallback      |
| `queue.admin.reprocessForceRematch`    | Force rematch                                                                            | English fallback                                          | fallback      |
| `queue.admin.reprocessLocalSearch`     | Match content by local search                                                            | `torrents.reprocess.match_content_by_local_search`        | exact         |
| `queue.admin.reprocessOrphansOnly`     | Process orphaned torrents only                                                           | `dashboard.queues.process_orphaned_torrents_only`         | exact         |
| `queue.admin.reprocessPending`         | Enqueuing jobs                                                                           | `dashboard.queues.jobs_enqueued`                          | corresponding |
| `queue.admin.reprocessPurge`           | Purge queue jobs                                                                         | `dashboard.queues.purge_queue_jobs`                       | exact         |
| `queue.admin.reprocessSuccess`         | Torrent processing batch enqueued                                                        | English fallback                                          | fallback      |
| `queue.admin.statusScope`              | Status scope                                                                             | English fallback                                          | fallback      |
| `queue.admin.title`                    | Admin                                                                                    | `routes.admin`                                            | exact         |
| `queue.admin.warning`                  | Purge is destructive. Review the scope in the confirmation dialog before continuing.     | English fallback                                          | fallback      |
| `queue.body`                           | Monitor queue throughput, inspect jobs, and run scoped purge actions.                    | English fallback                                          | fallback      |
| `queue.facets.allQueues`               | All queues                                                                               | `dashboard.queues.queues`                                 | corresponding |
| `queue.facets.allStatuses`             | All statuses                                                                             | English fallback                                          | fallback      |
| `queue.facets.queue`                   | Queue                                                                                    | `facets.queue`                                            | exact         |
| `queue.facets.status`                  | Status                                                                                   | `facets.status`                                           | exact         |
| `queue.jobs.ascending`                 | Ascending                                                                                | English fallback                                          | fallback      |
| `queue.jobs.body`                      | Inspect queued work by queue, status, priority, timing, payload, and error.              | English fallback                                          | fallback      |
| `queue.jobs.collapseJob`               | Collapse job                                                                             | English fallback                                          | fallback      |
| `queue.jobs.count`                     | {{count}} jobs                                                                           | English fallback                                          | fallback      |
| `queue.jobs.createdAt`                 | Created                                                                                  | `dashboard.queues.created_at`                             | corresponding |
| `queue.jobs.descending`                | Descending                                                                               | English fallback                                          | fallback      |
| `queue.jobs.emptyBody`                 | Adjust facets or refresh the queue.                                                      | English fallback                                          | fallback      |
| `queue.jobs.emptyTitle`                | No jobs found                                                                            | English fallback                                          | fallback      |
| `queue.jobs.error`                     | Error                                                                                    | `health.error`                                            | exact         |
| `queue.jobs.expand`                    | Expand                                                                                   | English fallback                                          | fallback      |
| `queue.jobs.expandJob`                 | Expand job                                                                               | English fallback                                          | fallback      |
| `queue.jobs.id`                        | ID                                                                                       | English fallback                                          | fallback      |
| `queue.jobs.loading`                   | Loading queue jobs                                                                       | English fallback                                          | fallback      |
| `queue.jobs.next`                      | Next                                                                                     | English fallback                                          | fallback      |
| `queue.jobs.notRun`                    | Not run                                                                                  | English fallback                                          | fallback      |
| `queue.jobs.orderBy`                   | Order by                                                                                 | `torrents.order_by`                                       | exact         |
| `queue.jobs.pageSize`                  | Page size                                                                                | English fallback                                          | fallback      |
| `queue.jobs.pageStatus`                | Page {{page}} of {{totalPages}}                                                          | English fallback                                          | fallback      |
| `queue.jobs.payload`                   | Payload                                                                                  | `dashboard.queues.payload`                                | exact         |
| `queue.jobs.previous`                  | Previous                                                                                 | English fallback                                          | fallback      |
| `queue.jobs.priority`                  | Priority                                                                                 | `dashboard.queues.priority`                               | exact         |
| `queue.jobs.queue`                     | Queue                                                                                    | `dashboard.queues.queue`                                  | exact         |
| `queue.jobs.ranAt`                     | Ran                                                                                      | `dashboard.queues.ran_at`                                 | corresponding |
| `queue.jobs.refresh`                   | Refresh                                                                                  | `general.refresh`                                         | exact         |
| `queue.jobs.retries`                   | Retries                                                                                  | English fallback                                          | fallback      |
| `queue.jobs.runAfter`                  | Run after                                                                                | English fallback                                          | fallback      |
| `queue.jobs.status`                    | Status                                                                                   | `general.status`                                          | exact         |
| `queue.jobs.title`                     | Jobs                                                                                     | `routes.jobs`                                             | exact         |
| `queue.nav.admin`                      | Admin                                                                                    | `routes.admin`                                            | exact         |
| `queue.nav.jobs`                       | Jobs                                                                                     | `routes.jobs`                                             | exact         |
| `queue.nav.visualize`                  | Visualize                                                                                | `routes.visualize`                                        | exact         |
| `queue.order.created_at`               | Created at                                                                               | `dashboard.queues.created_at`                             | exact         |
| `queue.order.priority`                 | Priority                                                                                 | `dashboard.queues.priority`                               | exact         |
| `queue.order.ran_at`                   | Ran at                                                                                   | `dashboard.queues.ran_at`                                 | exact         |
| `queue.sections`                       | Queue sections                                                                           | English fallback                                          | fallback      |
| `queue.status.failed`                  | Failed                                                                                   | `dashboard.queues.failed`                                 | exact         |
| `queue.status.pending`                 | Pending                                                                                  | `dashboard.queues.pending`                                | exact         |
| `queue.status.processed`               | Processed                                                                                | `dashboard.queues.processed`                              | exact         |
| `queue.status.retry`                   | Retry                                                                                    | `dashboard.queues.retry`                                  | exact         |
| `queue.title`                          | Queue                                                                                    | `dashboard.queues.queue`                                  | exact         |
| `queue.visualize.body`                 | Queue throughput, job status, and latency over the selected window.                      | English fallback                                          | fallback      |
| `queue.visualize.eventsTitle`          | Events and latency                                                                       | English fallback                                          | fallback      |
| `queue.visualize.loadingCharts`        | Loading charts                                                                           | English fallback                                          | fallback      |
| `queue.visualize.statusTitle`          | Statuses                                                                                 | English fallback                                          | fallback      |
| `queue.visualize.title`                | Visualize                                                                                | `routes.visualize`                                        | exact         |
| `queue.visualize.total`                | {{count}} jobs                                                                           | English fallback                                          | fallback      |
| `queue.visualize.totalsTitle`          | Totals by queue                                                                          | English fallback                                          | fallback      |
| `search.appLoadedIn`                   | app loaded in {{ms}} ms                                                                  | English fallback                                          | fallback      |
| `search.apply`                         | Apply                                                                                    | `general.apply`                                           | exact         |
| `search.ascending`                     | Ascending                                                                                | English fallback                                          | fallback      |
| `search.browseEyebrow`                 | Newest torrents                                                                          | English fallback                                          | fallback      |
| `search.clear`                         | Clear                                                                                    | English fallback                                          | fallback      |
| `search.closeFilters`                  | Close filters                                                                            | English fallback                                          | fallback      |
| `search.contentType`                   | Content type                                                                             | English fallback                                          | fallback      |
| `search.contentTypeAll`                | All                                                                                      | `content_types.plural.all`                                | exact         |
| `search.copyMagnet`                    | Copy                                                                                     | `torrents.copy`                                           | exact         |
| `search.copyMagnetLink`                | Copy magnet link for {{title}}                                                           | English fallback                                          | fallback      |
| `search.descending`                    | Descending                                                                               | English fallback                                          | fallback      |
| `search.dhtFirstSeen`                  | DHT first seen                                                                           | `torrents.dht_first_seen`                                 | exact         |
| `search.dhtLastSeen`                   | DHT last seen                                                                            | `torrents.dht_last_seen`                                  | exact         |
| `search.dhtSeen`                       | DHT seen                                                                                 | `torrents.dht_seen`                                       | exact         |
| `search.dhtSeenCount`                  | DHT crawl count                                                                          | `torrents.dht_seen_count`                                 | exact         |
| `search.dhtSeenSummary`                | seen {{time}} · {{seenCount}}×                                                           | English fallback                                          | fallback      |
| `search.emptyBody`                     | No torrents to show.                                                                     | English fallback                                          | fallback      |
| `search.emptyTitle`                    | No torrents yet                                                                          | English fallback                                          | fallback      |
| `search.fetchedIn`                     | fetched in {{ms}} ms                                                                     | English fallback                                          | fallback      |
| `search.files`                         | Files                                                                                    | `torrents.files`                                          | exact         |
| `search.filtersSummary`                | Filters                                                                                  | English fallback                                          | fallback      |
| `search.filtersSummaryActive`          | Filters, {{count}} active                                                                | English fallback                                          | fallback      |
| `search.infoHash`                      | Info hash                                                                                | `torrents.info_hash`                                      | exact         |
| `search.inputLabel`                    | Search torrents                                                                          | English fallback                                          | fallback      |
| `search.leechers`                      | Leechers                                                                                 | `torrents.leechers`                                       | exact         |
| `search.loading`                       | Loading search results                                                                   | English fallback                                          | fallback      |
| `search.magnet`                        | Magnet                                                                                   | `torrents.magnet`                                         | exact         |
| `search.maxSize`                       | Max size                                                                                 | `torrents.max_size`                                       | corresponding |
| `search.maxSizeUnit`                   | Max unit                                                                                 | English fallback                                          | fallback      |
| `search.minSize`                       | Min size                                                                                 | `torrents.min_size`                                       | corresponding |
| `search.minSizeUnit`                   | Min unit                                                                                 | English fallback                                          | fallback      |
| `search.nextPage`                      | Next                                                                                     | English fallback                                          | fallback      |
| `search.noResultsBody`                 | Try another query.                                                                       | English fallback                                          | fallback      |
| `search.noResultsTitle`                | No matching torrents                                                                     | English fallback                                          | fallback      |
| `search.openMagnetLink`                | Open magnet link for {{title}}                                                           | English fallback                                          | fallback      |
| `search.orderBy`                       | Order by                                                                                 | `torrents.order_by`                                       | exact         |
| `search.ordering.files_count`          | Files count                                                                              | `torrents.ordering.files_count`                           | exact         |
| `search.ordering.info_hash`            | Info hash                                                                                | `torrents.ordering.info_hash`                             | exact         |
| `search.ordering.leechers`             | Leechers                                                                                 | `torrents.ordering.leechers`                              | exact         |
| `search.ordering.name`                 | Name                                                                                     | `torrents.ordering.name`                                  | exact         |
| `search.ordering.published_at`         | Published at                                                                             | `torrents.ordering.published_at`                          | exact         |
| `search.ordering.relevance`            | Relevance                                                                                | `torrents.ordering.relevance`                             | exact         |
| `search.ordering.seeders`              | Seeders                                                                                  | `torrents.ordering.seeders`                               | exact         |
| `search.ordering.size`                 | Size                                                                                     | `torrents.ordering.size`                                  | exact         |
| `search.ordering.updated_at`           | Updated at                                                                               | `torrents.ordering.updated_at`                            | exact         |
| `search.page`                          | Page {{page}}                                                                            | English fallback                                          | fallback      |
| `search.pageTitle`                     | Torrent search                                                                           | English fallback                                          | fallback      |
| `search.peers`                         | Seeders / Leechers                                                                       | English fallback                                          | fallback      |
| `search.placeholder`                   | Search torrents by name or hash                                                          | English fallback                                          | fallback      |
| `search.previousPage`                  | Previous                                                                                 | English fallback                                          | fallback      |
| `search.published`                     | Published                                                                                | `torrents.published`                                      | exact         |
| `search.publishedAny`                  | Any time                                                                                 | English fallback                                          | fallback      |
| `search.publishedFilter`               | Published date                                                                           | `torrents.published_date_filter`                          | corresponding |
| `search.publishedLastDay`              | Last day                                                                                 | English fallback                                          | fallback      |
| `search.publishedLastMonth`            | Last month                                                                               | English fallback                                          | fallback      |
| `search.publishedLastThreeMonths`      | Last 3 months                                                                            | English fallback                                          | fallback      |
| `search.publishedLastWeek`             | Last week                                                                                | English fallback                                          | fallback      |
| `search.publishedLastYear`             | Last year                                                                                | English fallback                                          | fallback      |
| `search.refresh`                       | Refresh                                                                                  | `torrents.refresh`                                        | corresponding |
| `search.resultsCount`                  | {{count}} result                                                                         | English fallback                                          | fallback      |
| `search.resultsCount_other`            | {{count}} results                                                                        | English fallback                                          | fallback      |
| `search.resultsCountEstimate`          | About {{count}} result                                                                   | English fallback                                          | fallback      |
| `search.resultsCountEstimate_other`    | About {{count}} results                                                                  | English fallback                                          | fallback      |
| `search.seeders`                       | Seeders                                                                                  | `torrents.seeders`                                        | exact         |
| `search.size`                          | Size                                                                                     | `torrents.size`                                           | exact         |
| `search.sizeFilter`                    | Size                                                                                     | `torrents.size_filter`                                    | corresponding |
| `search.sizeUnits.GB`                  | GB                                                                                       | `torrents.size_units.gb`                                  | exact         |
| `search.sizeUnits.GiB`                 | GiB                                                                                      | `torrents.size_units.gib`                                 | exact         |
| `search.sizeUnits.KB`                  | KB                                                                                       | `torrents.size_units.kb`                                  | exact         |
| `search.sizeUnits.KiB`                 | KiB                                                                                      | `torrents.size_units.kib`                                 | exact         |
| `search.sizeUnits.MB`                  | MB                                                                                       | `torrents.size_units.mb`                                  | exact         |
| `search.sizeUnits.MiB`                 | MiB                                                                                      | `torrents.size_units.mib`                                 | exact         |
| `search.sizeUnits.TB`                  | TB                                                                                       | `torrents.size_units.tb`                                  | exact         |
| `search.sizeUnits.TiB`                 | TiB                                                                                      | `torrents.size_units.tib`                                 | exact         |
| `search.sort`                          | Sort                                                                                     | English fallback                                          | fallback      |
| `search.submit`                        | Search                                                                                   | `torrents.search`                                         | exact         |
| `search.toggleSortDirection`           | Toggle sort direction                                                                    | `torrents.order_direction_toggle`                         | corresponding |
| `theme.switchToDark`                   | Switch to dark theme                                                                     | English fallback                                          | fallback      |
| `theme.switchToLight`                  | Switch to light theme                                                                    | English fallback                                          | fallback      |
| `toast.dismiss`                        | Dismiss notification                                                                     | English fallback                                          | fallback      |
| `toast.hashCopied`                     | Info hash copied                                                                         | English fallback                                          | fallback      |
| `toast.hashCopyFailed`                 | Could not copy the info hash                                                             | English fallback                                          | fallback      |
| `toast.infoHashCopied`                 | Info hash copied                                                                         | English fallback                                          | fallback      |
| `toast.infoHashCopyFailed`             | Could not copy info hash                                                                 | English fallback                                          | fallback      |
| `toast.magnetCopied`                   | Magnet link copied                                                                       | English fallback                                          | fallback      |
| `toast.magnetCopyFailed`               | Could not copy magnet link                                                               | English fallback                                          | fallback      |
| `toast.searchSubmitted`                | Search submitted                                                                         | English fallback                                          | fallback      |

## Notes

- React keys remain independent of Transloco keys; the table records source-key provenance only.
- Corresponding mappings are limited to obvious label-shape differences such as `Ebook` -> `E-Book`, `TV show` -> `TV Show`, plural content-type labels, and React queue/status labels that map to Angular queue/status labels.
- Marker values equal to `__missing__` are never emitted into React locale modules; those keys intentionally fall back to `en`.
- Translations with interpolation placeholders that do not match the React English key are also omitted so runtime interpolation cannot leak raw marker names.
- No machine translation was used.
