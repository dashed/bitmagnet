//! Torznab query-string parsing and profile-path resolution.

/// Torznab function selector.
pub const PARAM_TYPE: &str = "t";
/// Free-text search query.
pub const PARAM_QUERY: &str = "q";
/// Repeatable and comma-separated category IDs.
pub const PARAM_CAT: &str = "cat";
/// IMDb identifier.
pub const PARAM_IMDB_ID: &str = "imdbid";
/// TMDB identifier.
pub const PARAM_TMDB_ID: &str = "tmdbid";
/// TV season number.
pub const PARAM_SEASON: &str = "season";
/// TV episode number.
pub const PARAM_EPISODE: &str = "ep";
/// Page-size override.
pub const PARAM_LIMIT: &str = "limit";
/// Result offset.
pub const PARAM_OFFSET: &str = "offset";

/// Capabilities function.
pub const FUNCTION_CAPS: &str = "caps";
/// General search function.
pub const FUNCTION_SEARCH: &str = "search";
/// Movie search function.
pub const FUNCTION_MOVIE: &str = "movie";
/// TV search function.
pub const FUNCTION_TV_SEARCH: &str = "tvsearch";
/// Music search function.
pub const FUNCTION_MUSIC: &str = "music";
/// Book search function.
pub const FUNCTION_BOOK: &str = "book";

/// Parsed HTTP request before Torznab-domain criteria mapping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TorznabRequest {
    pub type_: String,
    pub query: String,
    pub cats: Vec<i32>,
    pub imdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// Parses decoded query-string pairs using the same first-value and validity
/// rules as Gin's `Query`/`QueryArray` accessors in the Go handler.
#[must_use]
pub fn parse<I, K, V>(query_pairs: I) -> TorznabRequest
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut type_value = None;
    let mut query = None;
    let mut cats = Vec::new();
    let mut imdb_id = None;
    let mut tmdb_id = None;
    let mut season = None;
    let mut episode = None;
    let mut limit = None;
    let mut offset = None;

    for (key, value) in query_pairs {
        let key = key.as_ref();
        let value = value.as_ref();

        match key {
            PARAM_TYPE if type_value.is_none() => type_value = Some(value.to_owned()),
            PARAM_QUERY if query.is_none() => query = Some(value.to_owned()),
            PARAM_CAT => {
                cats.extend(
                    value
                        .split(',')
                        .filter_map(|token| token.parse::<i32>().ok()),
                );
            }
            PARAM_IMDB_ID if imdb_id.is_none() => imdb_id = Some(value.to_owned()),
            PARAM_TMDB_ID if tmdb_id.is_none() => tmdb_id = Some(value.to_owned()),
            PARAM_SEASON if season.is_none() => season = Some(value.to_owned()),
            PARAM_EPISODE if episode.is_none() => episode = Some(value.to_owned()),
            PARAM_LIMIT if limit.is_none() => limit = Some(value.to_owned()),
            PARAM_OFFSET if offset.is_none() => offset = Some(value.to_owned()),
            _ => {}
        }
    }

    let season = season
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<i32>().ok());
    let episode = season.and_then(|_| {
        episode
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<i32>().ok())
    });
    let limit = limit
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        // Any positive Go `int` above Rust's u32 search-limit domain will be
        // clamped by the profile, so saturating here preserves that outcome.
        .map(|value| u32::try_from(value).unwrap_or(u32::MAX));
    // Go parses an `int` and casts it to an unsigned value. Parsing through
    // i64 and using `as u32` intentionally preserves negative wrap behavior.
    let offset = offset
        .and_then(|value| value.parse::<i64>().ok())
        .map(|value| value as u32);

    TorznabRequest {
        type_: type_value.unwrap_or_default(),
        query: query.unwrap_or_default(),
        cats,
        imdb_id: imdb_id.filter(|value| !value.is_empty()),
        tmdb_id: tmdb_id.filter(|value| !value.is_empty()),
        season,
        episode,
        limit,
        offset,
    }
}

/// Resolves the first Torznab path segment to its lowercase profile name.
#[must_use]
pub fn profile_name(path: &str) -> String {
    path.trim_matches('/')
        .split('/')
        .next()
        .unwrap_or_default()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{parse, profile_name, TorznabRequest};

    fn pairs(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn parses_repeated_and_csv_categories_and_drops_invalid_tokens() {
        let request = parse(pairs(&[
            ("t", "movie"),
            ("cat", "2000,not-a-number,2030"),
            ("cat", "2040"),
            ("unknown", "ignored"),
        ]));

        assert_eq!(request.type_, "movie");
        assert_eq!(request.cats, [2000, 2030, 2040]);
    }

    #[test]
    fn episode_requires_a_valid_season() {
        for values in [
            pairs(&[("ep", "3")]),
            pairs(&[("season", "invalid"), ("ep", "3")]),
        ] {
            let request = parse(values);
            assert_eq!(request.season, None);
            assert_eq!(request.episode, None);
        }

        let request = parse(pairs(&[("season", "2"), ("ep", "3")]));
        assert_eq!(request.season, Some(2));
        assert_eq!(request.episode, Some(3));
    }

    #[test]
    fn scalar_params_use_the_first_value_and_go_validity_rules() {
        let request = parse(pairs(&[
            ("q", "first"),
            ("q", "second"),
            ("imdbid", ""),
            ("imdbid", "123"),
            ("limit", "0"),
            ("limit", "10"),
            ("offset", "-1"),
        ]));

        assert_eq!(
            request,
            TorznabRequest {
                query: "first".to_owned(),
                offset: Some(u32::MAX),
                ..TorznabRequest::default()
            }
        );
    }

    #[test]
    fn profile_path_uses_the_first_trimmed_lowercase_segment() {
        assert_eq!(profile_name("/"), "");
        assert_eq!(profile_name("/Test/API/"), "test");
        assert_eq!(profile_name("DEFAULT"), "default");
    }
}
