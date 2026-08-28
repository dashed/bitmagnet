//! Pure projection of one processor row's volatile persistence fields.
//!
//! Go derives these values in `newTorrentContent` and `TorrentContent.UpdateTsv`
//! before opening the persistence transaction. Keeping the projection pure makes
//! it independently testable without attaching it to the loader, runtime, or
//! writer until the parity boundary is accepted.

use bitmagnet_classifier::ClassifierInput;
use bitmagnet_fts::{Tsvector, TsvectorWeight, MAX_TSVECTOR_BYTES};
use bitmagnet_release::goclass;

use super::{TorrentContentPersistence, TorrentContentWrite};

/// Go's strict `time.After` cutoff for accepting a source `published_at`.
const PUBLISHED_AT_CUTOFF_MICROS: i64 = 946_684_800_000_000;

/// Volatile torrent fields read by the single-row writer projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TorrentSnapshot {
    /// Torrent creation time as microseconds since the Unix epoch.
    pub created_at_micros: i64,
}

/// Volatile source fields read by the single-row writer projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TorrentSourceSnapshot {
    /// Source-reported seeders; `Some(0)` is distinct from no report.
    pub seeders: Option<u64>,
    /// Source-reported leechers; `Some(0)` is distinct from no report.
    pub leechers: Option<u64>,
    /// Source publication time as microseconds since the Unix epoch.
    pub published_at_micros: Option<i64>,
    /// Source row creation time as microseconds since the Unix epoch.
    pub created_at_micros: i64,
}

/// Refusal modes for the persistence projection.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WriterProjectionError {
    /// An attached row needs the attached content's existing TSV as its base.
    #[error("attached torrent_content projection requires the content TSV")]
    AttachedContentUnsupported,
    /// The attached row must carry both halves of its content foreign key.
    #[error("torrent_content has a partial attached content reference")]
    PartialContentReference,
    /// The classifier snapshot and write row must describe the same torrent.
    #[error(
        "torrent_content info hash '{row_info_hash}' does not match classifier input '{classifier_info_hash}'"
    )]
    InfoHashMismatch {
        row_info_hash: String,
        classifier_info_hash: String,
    },
    /// Classification rows may only carry canonical `V...` resolution values.
    #[error("invalid canonical video resolution '{0}'")]
    InvalidVideoResolution(String),
    /// Classification rows may only carry one of Go's three valid 3D values.
    #[error("invalid canonical video 3D value '{0}'")]
    InvalidVideo3D(String),
}

/// Project one unattached `torrent_contents` row's source maxima, publication
/// time, and TSV exactly as Go does before persistence.
///
/// The function refuses attached rows because Go starts their vector from
/// `Content.Tsv.Copy()`, which is intentionally outside this minimal API.
/// Inputs are borrowed and file paths are sorted in a borrowed view, so the
/// caller's classifier input is never reordered or mutated.
pub fn project_unattached_persistence(
    row: &TorrentContentWrite,
    classifier_input: &ClassifierInput,
    torrent: TorrentSnapshot,
    sources: &[TorrentSourceSnapshot],
) -> Result<TorrentContentPersistence, WriterProjectionError> {
    if row.content_source.is_some() || row.content_id.is_some() {
        return Err(WriterProjectionError::AttachedContentUnsupported);
    }
    project_torrent_persistence(row, classifier_input, torrent, sources, None)
}

/// Project one `torrent_contents` row with the exact content TSV base Go gives
/// `TorrentContent.UpdateTsv`.
///
/// An unattached row requires `None`; an attached row requires `Some` and both
/// foreign-key components. Keeping the base vector as an explicit input lets
/// the flags-off production shadow retain its current read-only ACL while the
/// disconnected writer loader supplies complete existing content and
/// associations without changing the classifier's null resolver.
pub fn project_torrent_persistence(
    row: &TorrentContentWrite,
    classifier_input: &ClassifierInput,
    torrent: TorrentSnapshot,
    sources: &[TorrentSourceSnapshot],
    content_tsv: Option<&Tsvector>,
) -> Result<TorrentContentPersistence, WriterProjectionError> {
    let base_tsv = match (
        row.content_type.as_ref(),
        row.content_source.as_ref(),
        row.content_id.as_ref(),
        content_tsv,
    ) {
        (_, None, None, None) => Tsvector::new(),
        (Some(_), Some(_), Some(_), Some(tsv)) => tsv.clone(),
        (Some(_), Some(_), Some(_), None) => {
            return Err(WriterProjectionError::AttachedContentUnsupported)
        }
        _ => return Err(WriterProjectionError::PartialContentReference),
    };
    if row.info_hash != classifier_input.id {
        return Err(WriterProjectionError::InfoHashMismatch {
            row_info_hash: row.info_hash.clone(),
            classifier_info_hash: classifier_input.id.clone(),
        });
    }

    let seeders = sources.iter().filter_map(|source| source.seeders).max();
    let leechers = sources.iter().filter_map(|source| source.leechers).max();
    let published_at_micros =
        sources
            .iter()
            .fold(torrent.created_at_micros, |published_at, source| {
                let source_published_at = source
                    .published_at_micros
                    .filter(|value| *value > PUBLISHED_AT_CUTOFF_MICROS)
                    .unwrap_or(source.created_at_micros);
                published_at.min(source_published_at)
            });

    let tsv = torrent_content_tsv(row, classifier_input, base_tsv)?;
    Ok(TorrentContentPersistence {
        seeders,
        leechers,
        published_at_micros,
        tsv: tsv.to_string(),
    })
}

fn torrent_content_tsv(
    row: &TorrentContentWrite,
    classifier_input: &ClassifierInput,
    mut tsv: Tsvector,
) -> Result<Tsvector, WriterProjectionError> {
    if let Some(value) = row.video_resolution.as_deref() {
        let label = value
            .strip_prefix('V')
            .filter(|label| !label.is_empty())
            .ok_or_else(|| WriterProjectionError::InvalidVideoResolution(value.to_owned()))?;
        tsv.add_text(label, TsvectorWeight::C);
    }
    if let Some(value) = row.video_source.as_deref() {
        tsv.add_text(value, TsvectorWeight::C);
    }
    if let Some(value) = row.video_codec.as_deref() {
        tsv.add_text(value, TsvectorWeight::C);
    }
    if let Some(value) = row.video_3d.as_deref() {
        if !matches!(value, "V3D" | "V3DSBS" | "V3DOU") {
            return Err(WriterProjectionError::InvalidVideo3D(value.to_owned()));
        }
        tsv.add_text("3D", TsvectorWeight::C);
    }
    if let Some(value) = row.video_modifier.as_deref() {
        tsv.add_text(value, TsvectorWeight::C);
    }
    if let Some(value) = row.release_group.as_deref() {
        tsv.add_text(value, TsvectorWeight::C);
    }

    // High-priority fields deliberately remain unbounded, matching Go.
    tsv.add_text(&row.info_hash, TsvectorWeight::A);
    tsv.add_text(&classifier_input.name, TsvectorWeight::A);

    let serialized_len =
        isize::try_from(tsv.to_string().len()).expect("an allocated Rust String length fits isize");
    let max_bytes = isize::try_from(MAX_TSVECTOR_BYTES).expect("TSV budget fits isize");
    let mut budget = max_bytes - serialized_len;
    for path in file_search_strings(classifier_input) {
        if budget <= 0 {
            break;
        }
        budget = tsv.add_text_bounded(&path, TsvectorWeight::D, budget);
    }

    Ok(tsv)
}

/// Go `Torrent.fileSearchStrings`, including its byte-wise prefix/suffix walks.
fn file_search_strings(classifier_input: &ClassifierInput) -> Vec<String> {
    let mut paths = classifier_input
        .files
        .iter()
        .map(|file| file.path.as_bytes())
        .collect::<Vec<_>>();
    paths.sort_unstable();

    let mut first_pass = Vec::with_capacity(paths.len());
    let mut previous: &[u8] = &[];
    'outer: for path in paths {
        let mut index = 0;
        loop {
            if index >= path.len() {
                continue 'outer;
            }
            if index >= previous.len() || previous[index] != path[index] {
                break;
            }
            index += 1;
        }
        while index != 0 && is_go_word_byte(path[index]) {
            index -= 1;
        }
        first_pass.push(&path[index..]);
        previous = path;
    }

    let mut search_strings = Vec::with_capacity(first_pass.len());
    for (index, path) in first_pass.iter().enumerate() {
        let mut longest_suffix_length = 0;
        for previous in &first_pass[..index] {
            let mut suffix_length = 0;
            while suffix_length < path.len()
                && suffix_length < previous.len()
                && path[path.len() - suffix_length - 1]
                    == previous[previous.len() - suffix_length - 1]
            {
                suffix_length += 1;
            }
            longest_suffix_length = longest_suffix_length.max(suffix_length);
        }

        while longest_suffix_length != 0
            && is_go_word_byte(path[path.len() - longest_suffix_length])
        {
            longest_suffix_length -= 1;
        }

        let raw = &path[..path.len() - longest_suffix_length];
        let decoded = decode_like_go_string(raw);
        let trimmed = decoded.trim_matches(is_go_space);
        if !trimmed.is_empty() {
            search_strings.push(trimmed.to_owned());
        }
    }

    search_strings
}

fn is_go_word_byte(byte: u8) -> bool {
    goclass::is_word_char(char::from(byte))
}

/// Make Go's possibly-invalid byte substring representable without changing
/// tokenizer behavior. `bufio.Reader.ReadRune` consumes one invalid byte and
/// returns U+FFFD; this does the same. U+FFFD is a non-word separator in both
/// implementations, so the persisted TSV remains exact even when the raw Go
/// intermediate cannot be represented by a Rust `String`.
fn decode_like_go_string(bytes: &[u8]) -> String {
    let mut decoded = String::with_capacity(bytes.len());
    let mut offset = 0;
    while offset < bytes.len() {
        let first = bytes[offset];
        let width = match first {
            0x00..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 0,
        };
        if width != 0 && offset + width <= bytes.len() {
            if let Ok(value) = std::str::from_utf8(&bytes[offset..offset + width]) {
                if let Some(ch) = value.chars().next() {
                    decoded.push(ch);
                    offset += width;
                    continue;
                }
            }
        }
        decoded.push(char::REPLACEMENT_CHARACTER);
        offset += 1;
    }
    decoded
}

/// Go `unicode.IsSpace`, used by `strings.TrimSpace` for non-ASCII strings.
fn is_go_space(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{0085}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
    )
}

#[cfg(test)]
mod tests {
    use bitmagnet_classifier::{ClassifierInput, InputFile};
    use bitmagnet_fts::{TsvectorWeight, MAX_TSVECTOR_BYTES};

    use super::*;

    const INFO_HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    fn row() -> TorrentContentWrite {
        TorrentContentWrite {
            id: format!("{INFO_HASH}:movie:?:?"),
            info_hash: INFO_HASH.to_owned(),
            content_type: Some("movie".to_owned()),
            content_source: None,
            content_id: None,
            languages: Vec::new(),
            episodes: "[]".to_owned(),
            video_resolution: None,
            video_source: None,
            video_codec: None,
            video_3d: None,
            video_modifier: None,
            release_group: None,
            size: 1,
            files_count: None,
        }
    }

    fn input(name: &str, paths: &[&str]) -> ClassifierInput {
        ClassifierInput {
            id: INFO_HASH.to_owned(),
            name: name.to_owned(),
            size: 1,
            files_status: "multi".to_owned(),
            extension: None,
            files_count: Some(paths.len().try_into().expect("test path count fits u32")),
            files: paths
                .iter()
                .enumerate()
                .map(|(index, path)| InputFile {
                    index: index.try_into().expect("test path index fits u32"),
                    path: (*path).to_owned(),
                    extension: String::new(),
                    size: 1,
                })
                .collect(),
            hint: None,
            contents: Vec::new(),
        }
    }

    fn project(
        row: &TorrentContentWrite,
        input: &ClassifierInput,
        torrent_created: i64,
        sources: &[TorrentSourceSnapshot],
    ) -> TorrentContentPersistence {
        project_unattached_persistence(
            row,
            input,
            TorrentSnapshot {
                created_at_micros: torrent_created,
            },
            sources,
        )
        .expect("unattached test row projects")
    }

    #[test]
    fn no_sources_preserves_torrent_creation_and_null_counts() {
        let persistence = project(&row(), &input("Example", &[]), 1_700_000_000_000_000, &[]);

        assert_eq!(persistence.seeders, None);
        assert_eq!(persistence.leechers, None);
        assert_eq!(persistence.published_at_micros, 1_700_000_000_000_000);
        assert_eq!(persistence.tsv, format!("'{INFO_HASH}':1A 'example':3A"));
    }

    #[test]
    fn source_maxima_are_independent_and_preserve_some_zero() {
        let sources = [
            TorrentSourceSnapshot {
                seeders: Some(0),
                leechers: None,
                published_at_micros: None,
                created_at_micros: 30,
            },
            TorrentSourceSnapshot {
                seeders: Some(7),
                leechers: Some(0),
                published_at_micros: None,
                created_at_micros: 40,
            },
            TorrentSourceSnapshot {
                seeders: None,
                leechers: Some(11),
                published_at_micros: None,
                created_at_micros: 50,
            },
        ];
        let projected = project(&row(), &input("Example", &[]), 100, &sources);
        assert_eq!(projected.seeders, Some(7));
        assert_eq!(projected.leechers, Some(11));

        let zeros = project(
            &row(),
            &input("Example", &[]),
            100,
            &[TorrentSourceSnapshot {
                seeders: Some(0),
                leechers: Some(0),
                published_at_micros: None,
                created_at_micros: 50,
            }],
        );
        assert_eq!(zeros.seeders, Some(0));
        assert_eq!(zeros.leechers, Some(0));
    }

    #[test]
    fn source_permutation_does_not_change_projection() {
        let mut sources = vec![
            TorrentSourceSnapshot {
                seeders: Some(2),
                leechers: Some(9),
                published_at_micros: Some(PUBLISHED_AT_CUTOFF_MICROS + 10),
                created_at_micros: 30,
            },
            TorrentSourceSnapshot {
                seeders: Some(8),
                leechers: Some(1),
                published_at_micros: None,
                created_at_micros: 20,
            },
        ];
        let first = project(&row(), &input("Example", &[]), 100, &sources);
        sources.reverse();
        let second = project(&row(), &input("Example", &[]), 100, &sources);
        assert_eq!(first, second);
    }

    #[test]
    fn published_at_cutoff_is_strict_and_invalid_values_fall_back_to_created() {
        let source = |published_at_micros, created_at_micros| TorrentSourceSnapshot {
            seeders: None,
            leechers: None,
            published_at_micros,
            created_at_micros,
        };

        let exact = project(
            &row(),
            &input("Example", &[]),
            2_000_000_000_000_000,
            &[source(
                Some(PUBLISHED_AT_CUTOFF_MICROS),
                1_500_000_000_000_000,
            )],
        );
        assert_eq!(exact.published_at_micros, 1_500_000_000_000_000);

        let after = project(
            &row(),
            &input("Example", &[]),
            2_000_000_000_000_000,
            &[source(
                Some(PUBLISHED_AT_CUTOFF_MICROS + 1),
                1_500_000_000_000_000,
            )],
        );
        assert_eq!(after.published_at_micros, PUBLISHED_AT_CUTOFF_MICROS + 1);

        let before = project(
            &row(),
            &input("Example", &[]),
            2_000_000_000_000_000,
            &[source(
                Some(PUBLISHED_AT_CUTOFF_MICROS - 1),
                1_400_000_000_000_000,
            )],
        );
        assert_eq!(before.published_at_micros, 1_400_000_000_000_000);
    }

    #[test]
    fn older_valid_source_publication_is_the_projected_insert_value() {
        let torrent_created_at = 1_738_368_000_000_000;
        let older_source_published_at = 1_609_459_200_000_000;
        let projected = project(
            &row(),
            &input("Campaign-shaped fixture", &[]),
            torrent_created_at,
            &[TorrentSourceSnapshot {
                seeders: None,
                leechers: None,
                published_at_micros: Some(older_source_published_at),
                created_at_micros: 1_740_960_000_000_000,
            }],
        );

        assert_eq!(projected.published_at_micros, older_source_published_at);
    }

    #[test]
    fn high_priority_tsv_fields_follow_go_order_and_labels() {
        let mut row = row();
        row.video_resolution = Some("V1080p".to_owned());
        row.video_source = Some("BluRay".to_owned());
        row.video_codec = Some("x264".to_owned());
        row.video_3d = Some("V3DSBS".to_owned());
        row.video_modifier = Some("Remux".to_owned());
        row.release_group = Some("GROUP".to_owned());

        let persistence = project(&row, &input("Example Movie", &[]), 1, &[]);
        assert_eq!(
            persistence.tsv,
            format!(
                "'{INFO_HASH}':13A '1080p':1C '3d':7C 'bluray':3C \
                 'example':15A 'group':11C 'movie':16A 'remux':9C 'x264':5C"
            )
        );
    }

    #[test]
    fn every_valid_video_3d_value_adds_only_the_literal_3d_lexeme() {
        for value in ["V3D", "V3DSBS", "V3DOU"] {
            let mut row = row();
            row.video_3d = Some(value.to_owned());
            let persistence = project(&row, &input("Example", &[]), 1, &[]);
            assert!(persistence.tsv.contains("'3d':1C"));
            assert!(!persistence.tsv.contains("3dsbs"));
            assert!(!persistence.tsv.contains("3dou"));
        }
    }

    #[test]
    fn file_reduction_sorts_and_skips_duplicates_and_full_prefixes() {
        let input = input("Example", &["root/file.txt", "root", "root"]);
        assert_eq!(file_search_strings(&input), ["root", "/file.txt"]);
    }

    #[test]
    fn file_reduction_matches_prefix_suffix_and_whitespace_boundaries() {
        let input = input(
            "Example",
            &[
                "Album/Disc 2/Track 03.flac",
                "Album/Disc 1/Track 02.flac",
                "Album/Disc 1/Track 01.flac",
                "Album/Disc 1/Track 02.flac",
            ],
        );
        assert_eq!(
            file_search_strings(&input),
            ["Album/Disc 1/Track 01.flac", "02", "2/Track 03",]
        );
    }

    #[test]
    fn byte_splits_use_go_replacement_rune_semantics_safely() {
        let prefix_split = input("Example", &["一alpha", "丁bravo"]);
        assert_eq!(file_search_strings(&prefix_split), ["一alpha", "�bravo"]);

        let suffix_split = input("Example", &["Aé", "BΩ"]);
        assert_eq!(file_search_strings(&suffix_split), ["Aé", "B�"]);
    }

    #[test]
    fn low_priority_path_tokens_stop_at_the_remaining_budget() {
        let repeated = "pathword ".repeat(60_000);
        let input_with_paths = input("Primary", &[&repeated]);
        let without_paths = project(&row(), &input("Primary", &[]), 1, &[]);
        let persistence = project(&row(), &input_with_paths, 1, &[]);

        let budget = MAX_TSVECTOR_BYTES - without_paths.tsv.len();
        let expected_positions = budget / ("pathword".len() + 12);
        let labels = persistence
            .tsv
            .split("'pathword':")
            .nth(1)
            .expect("pathword lexeme exists")
            .split(' ')
            .next()
            .expect("pathword labels exist");
        assert_eq!(labels.split(',').count(), expected_positions);
        assert!(expected_positions < 60_000);
        assert!(persistence.tsv.contains(&format!("'{INFO_HASH}':1A")));
        assert!(persistence.tsv.contains("'primary':3A"));
        assert!(persistence.tsv.len() <= MAX_TSVECTOR_BYTES);
    }

    #[test]
    fn attached_rows_fail_closed_without_a_content_tsv() {
        let mut row = row();
        row.content_source = Some("tmdb".to_owned());
        row.content_id = Some("42".to_owned());
        assert_eq!(
            project_unattached_persistence(
                &row,
                &input("Example", &[]),
                TorrentSnapshot {
                    created_at_micros: 1,
                },
                &[],
            ),
            Err(WriterProjectionError::AttachedContentUnsupported)
        );
    }

    #[test]
    fn attached_rows_extend_the_rebuilt_content_tsv() {
        let mut row = row();
        row.id = format!("{INFO_HASH}:movie:tmdb:42");
        row.content_source = Some("tmdb".to_owned());
        row.content_id = Some("42".to_owned());
        row.video_resolution = Some("V1080p".to_owned());

        let mut content_tsv = Tsvector::new();
        content_tsv.add_text("Attached Title", TsvectorWeight::A);
        let persistence = project_torrent_persistence(
            &row,
            &input("Release Name", &[]),
            TorrentSnapshot {
                created_at_micros: 1,
            },
            &[],
            Some(&content_tsv),
        )
        .expect("project attached row from its content base");

        assert!(persistence.tsv.contains("'attached':1A"));
        assert!(persistence.tsv.contains("'title':2A"));
        assert!(persistence.tsv.contains("'1080p':4C"));
        assert!(persistence.tsv.contains(&format!("'{INFO_HASH}':6A")));
        assert!(persistence.tsv.contains("'release':8A"));
        assert!(persistence.tsv.contains("'name':9A"));
    }

    #[test]
    fn partial_attached_reference_fails_closed() {
        for (content_type, source, id) in [
            (Some("movie"), Some("tmdb"), None),
            (Some("movie"), None, Some("42")),
            (None, Some("tmdb"), Some("42")),
            (None, Some("tmdb"), None),
            (None, None, Some("42")),
        ] {
            let mut row = row();
            row.content_type = content_type.map(str::to_owned);
            row.content_source = source.map(str::to_owned);
            row.content_id = id.map(str::to_owned);
            assert_eq!(
                project_torrent_persistence(
                    &row,
                    &input("Example", &[]),
                    TorrentSnapshot {
                        created_at_micros: 1,
                    },
                    &[],
                    None,
                ),
                Err(WriterProjectionError::PartialContentReference)
            );
        }
    }

    #[test]
    fn mismatched_classifier_snapshot_fails_closed() {
        let mut classifier_input = input("Example", &[]);
        classifier_input.id = "fedcba9876543210fedcba9876543210fedcba98".to_owned();

        assert_eq!(
            project_unattached_persistence(
                &row(),
                &classifier_input,
                TorrentSnapshot {
                    created_at_micros: 1,
                },
                &[],
            ),
            Err(WriterProjectionError::InfoHashMismatch {
                row_info_hash: INFO_HASH.to_owned(),
                classifier_info_hash: classifier_input.id,
            })
        );
    }
}
