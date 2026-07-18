//! Torznab request-to-search-domain mapping.

use bitmagnet_search_query::{
    ContentRef, ContentType, Criteria, Episodes, TorrentContentAttribute, TorrentContentOrder,
    TorznabSearchParams, Video3D, VideoResolution,
};

use crate::categories::{
    category_by_id, CATEGORY_AUDIO, CATEGORY_AUDIO_AUDIOBOOK, CATEGORY_BOOKS,
    CATEGORY_BOOKS_COMICS, CATEGORY_MOVIES, CATEGORY_MOVIES_3D, CATEGORY_MOVIES_HD,
    CATEGORY_MOVIES_SD, CATEGORY_MOVIES_UHD, CATEGORY_PC, CATEGORY_TV, CATEGORY_TV_HD,
    CATEGORY_TV_SD, CATEGORY_TV_UHD, CATEGORY_XXX,
};
use crate::config::Profile;
use crate::request::{
    TorznabRequest, FUNCTION_BOOK, FUNCTION_MOVIE, FUNCTION_MUSIC, FUNCTION_SEARCH,
    FUNCTION_TV_SEARCH,
};
use crate::response::TorznabError;

/// Resolves a parsed Torznab request into Lane Q's search-domain input.
pub fn to_search_params(
    request: &TorznabRequest,
    profile: &Profile,
) -> Result<TorznabSearchParams, TorznabError> {
    let mut criteria = Vec::new();

    match request.type_.as_str() {
        FUNCTION_SEARCH => {}
        FUNCTION_MOVIE => {
            criteria.push(content_type_or_null([ContentType::Movie]));
        }
        FUNCTION_TV_SEARCH => {
            criteria.push(content_type_or_null([ContentType::TvShow]));
            if let Some(season) = request.season {
                let episodes = match request.episode {
                    Some(episode) => Episodes::new().add_episode(season, episode),
                    None => Episodes::new().add_season(season),
                };
                criteria.push(Criteria::episodes(episodes));
            }
        }
        FUNCTION_MUSIC => {
            criteria.push(content_type_or_null([ContentType::Music]));
        }
        FUNCTION_BOOK => {
            criteria.push(book_content_types());
        }
        other => {
            return Err(TorznabError {
                code: 202,
                description: format!("no such function ({other})"),
            });
        }
    }

    let mut category_criteria = Vec::new();
    for &category_id in &request.cats {
        let mut per_category = Vec::new();

        if category_has(CATEGORY_MOVIES, category_id) {
            if request.type_ != FUNCTION_MOVIE || category_id == CATEGORY_MOVIES {
                per_category.push(content_type_or_null([ContentType::Movie]));
            }
            match category_id {
                CATEGORY_MOVIES_SD => {
                    per_category.push(Criteria::video_resolution_in([VideoResolution::V480p]))
                }
                CATEGORY_MOVIES_HD => per_category.push(Criteria::video_resolution_in([
                    VideoResolution::V720p,
                    VideoResolution::V1080p,
                    VideoResolution::V1440p,
                    VideoResolution::V2160p,
                ])),
                CATEGORY_MOVIES_UHD => {
                    per_category.push(Criteria::video_resolution_in([VideoResolution::V2160p]))
                }
                CATEGORY_MOVIES_3D => per_category.push(Criteria::video_3d_in([
                    Video3D::V3D,
                    Video3D::V3DSBS,
                    Video3D::V3DOU,
                ])),
                _ => {}
            }
        } else if category_has(CATEGORY_TV, category_id) {
            if request.type_ != FUNCTION_TV_SEARCH || category_id == CATEGORY_TV {
                per_category.push(content_type_or_null([ContentType::TvShow]));
            }
            match category_id {
                CATEGORY_TV_SD => {
                    per_category.push(Criteria::video_resolution_in([VideoResolution::V480p]))
                }
                CATEGORY_TV_HD => per_category.push(Criteria::video_resolution_in([
                    VideoResolution::V720p,
                    VideoResolution::V1080p,
                    VideoResolution::V1440p,
                    VideoResolution::V2160p,
                ])),
                CATEGORY_TV_UHD => {
                    per_category.push(Criteria::video_resolution_in([VideoResolution::V2160p]))
                }
                _ => {}
            }
        } else if category_has(CATEGORY_XXX, category_id) {
            per_category.push(content_type_or_null([ContentType::Xxx]));
        } else if category_has(CATEGORY_PC, category_id) {
            per_category.push(content_type_or_null([
                ContentType::Software,
                ContentType::Game,
            ]));
        } else if category_id == CATEGORY_AUDIO_AUDIOBOOK {
            per_category.push(content_type_or_null([ContentType::Audiobook]));
        } else if category_has(CATEGORY_AUDIO, category_id) {
            per_category.push(content_type_or_null([ContentType::Music]));
        } else if category_id == CATEGORY_BOOKS_COMICS {
            per_category.push(content_type_or_null([ContentType::Comic]));
        } else if category_has(CATEGORY_BOOKS, category_id) {
            // Go appends this directly to `options`, outside `catsCriteria`.
            criteria.push(book_content_types());
        }

        if !per_category.is_empty() {
            category_criteria.push(Criteria::and(per_category));
        }
    }

    if !category_criteria.is_empty() {
        criteria.push(Criteria::or(category_criteria));
    }

    if let Some(imdb_id) = &request.imdb_id {
        let imdb_id = if imdb_id.starts_with("tt") {
            imdb_id.clone()
        } else {
            format!("tt{imdb_id}")
        };
        criteria.push(Criteria::alternative_identifier([ContentRef {
            content_type: identifier_content_type(&request.type_),
            source: "imdb".to_owned(),
            id: imdb_id,
        }]));
    }

    if let Some(tmdb_id) = &request.tmdb_id {
        criteria.push(Criteria::canonical_identifier([ContentRef {
            content_type: identifier_content_type(&request.type_),
            source: "tmdb".to_owned(),
            id: tmdb_id.clone(),
        }]));
    }

    if !profile.tags.is_empty() {
        criteria.push(Criteria::torrent_tag(profile.tags.iter().cloned()));
    }

    let mut limit = profile.default_limit;
    if let Some(requested_limit) = request.limit {
        limit = requested_limit;
        if limit > profile.max_limit {
            limit = profile.max_limit;
        }
    }

    let mut params = TorznabSearchParams::new(limit);
    if !request.query.is_empty() {
        params.query = Some(request.query.clone());
        params.order = Some(if profile.disable_order_by_relevance {
            TorrentContentOrder::published_at_desc()
        } else {
            TorrentContentOrder::relevance_desc()
        });
    }
    if !criteria.is_empty() {
        params.filter = Some(Criteria::and(criteria));
    }
    params.offset = request.offset;

    Ok(params)
}

fn category_has(category_id: i32, requested_id: i32) -> bool {
    category_by_id(category_id).is_some_and(|category| category.has(requested_id))
}

fn book_content_types() -> Criteria {
    content_type_or_null([
        ContentType::Ebook,
        ContentType::Comic,
        ContentType::Audiobook,
    ])
}

/// `content_type IN (types) OR content_type IS NULL` — the widened content-type
/// criterion Torznab typed/category searches use so the ~64% of the corpus with a
/// NULL content_type is not hidden on an exact title match (F3). The general
/// search / GraphQL API keeps the strict `Criteria::content_type_in`.
fn content_type_or_null(types: impl IntoIterator<Item = ContentType>) -> Criteria {
    Criteria::or([
        Criteria::content_type_in(types),
        Criteria::is_null(TorrentContentAttribute::ContentType),
    ])
}

fn identifier_content_type(function: &str) -> Option<ContentType> {
    // Keep the Go `if type != TV { Movie } else if type != Movie { TV }`
    // branches literally: notably, `t=search` is scoped to Movie here.
    if function != FUNCTION_TV_SEARCH {
        Some(ContentType::Movie)
    } else if function != FUNCTION_MOVIE {
        Some(ContentType::TvShow)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use bitmagnet_search_query::{
        ContentRef, ContentType, Criteria, Episodes, TorrentContentAttribute, TorrentContentOrder,
        TorznabSearchParams, Video3D, VideoResolution,
    };

    use super::{content_type_or_null, to_search_params};
    use crate::categories::{
        CATEGORY_AUDIO, CATEGORY_AUDIO_AUDIOBOOK, CATEGORY_BOOKS, CATEGORY_BOOKS_COMICS,
        CATEGORY_BOOKS_EBOOK, CATEGORY_MOVIES, CATEGORY_MOVIES_3D, CATEGORY_MOVIES_HD,
        CATEGORY_MOVIES_SD, CATEGORY_MOVIES_UHD, CATEGORY_OTHER, CATEGORY_PC, CATEGORY_PC_GAMES,
        CATEGORY_TV, CATEGORY_TV_HD, CATEGORY_TV_SD, CATEGORY_TV_UHD, CATEGORY_XXX,
        CATEGORY_XXX_OTHER,
    };
    use crate::config::Profile;
    use crate::request::TorznabRequest;

    #[test]
    fn movie_categories_limit_and_offset_match_the_go_option_shape() {
        let request = TorznabRequest {
            type_: "movie".to_owned(),
            cats: vec![CATEGORY_MOVIES, CATEGORY_MOVIES_SD],
            limit: Some(10),
            offset: Some(100),
            ..TorznabRequest::default()
        };

        let actual =
            to_search_params(&request, &Profile::default_profile()).expect("movie request maps");
        let expected = TorznabSearchParams {
            query: None,
            filter: Some(Criteria::and([
                content_type_or_null([ContentType::Movie]),
                Criteria::or([
                    Criteria::and([content_type_or_null([ContentType::Movie])]),
                    Criteria::and([Criteria::video_resolution_in([VideoResolution::V480p])]),
                ]),
            ])),
            order: None,
            limit: 10,
            offset: Some(100),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn tv_imdb_and_season_match_the_go_option_shape() {
        let request = TorznabRequest {
            type_: "tvsearch".to_owned(),
            imdb_id: Some("123".to_owned()),
            season: Some(1),
            ..TorznabRequest::default()
        };

        let actual =
            to_search_params(&request, &Profile::default_profile()).expect("TV IMDb request maps");
        assert_eq!(actual.limit, 100);
        assert_eq!(actual.offset, None);
        assert_eq!(
            actual.filter,
            Some(Criteria::and([
                content_type_or_null([ContentType::TvShow]),
                Criteria::episodes(Episodes::new().add_season(1)),
                Criteria::alternative_identifier([ContentRef {
                    content_type: Some(ContentType::TvShow),
                    source: "imdb".to_owned(),
                    id: "tt123".to_owned(),
                }]),
            ]))
        );
    }

    #[test]
    fn tv_tmdb_and_episode_match_the_go_option_shape() {
        let request = TorznabRequest {
            type_: "tvsearch".to_owned(),
            tmdb_id: Some("123".to_owned()),
            season: Some(2),
            episode: Some(3),
            ..TorznabRequest::default()
        };

        let actual =
            to_search_params(&request, &Profile::default_profile()).expect("TV TMDB request maps");
        assert_eq!(
            actual.filter,
            Some(Criteria::and([
                content_type_or_null([ContentType::TvShow]),
                Criteria::episodes(Episodes::new().add_episode(2, 3)),
                Criteria::canonical_identifier([ContentRef {
                    content_type: Some(ContentType::TvShow),
                    source: "tmdb".to_owned(),
                    id: "123".to_owned(),
                }]),
            ]))
        );
    }

    #[test]
    fn query_order_tags_and_clamped_limit_preserve_accumulation_order() {
        let profile = Profile {
            disable_order_by_relevance: true,
            default_limit: 25,
            max_limit: 50,
            tags: vec!["one".to_owned(), "two".to_owned()],
            ..Profile::default_profile()
        };
        let request = TorznabRequest {
            type_: "search".to_owned(),
            query: "needle".to_owned(),
            imdb_id: Some("tt42".to_owned()),
            tmdb_id: Some("7".to_owned()),
            limit: Some(500),
            ..TorznabRequest::default()
        };

        let actual = to_search_params(&request, &profile).expect("search request maps");
        assert_eq!(actual.query.as_deref(), Some("needle"));
        assert_eq!(actual.order, Some(TorrentContentOrder::published_at_desc()));
        assert_eq!(actual.limit, 50);
        assert_eq!(
            actual.filter,
            Some(Criteria::and([
                Criteria::alternative_identifier([ContentRef {
                    content_type: Some(ContentType::Movie),
                    source: "imdb".to_owned(),
                    id: "tt42".to_owned(),
                }]),
                Criteria::canonical_identifier([ContentRef {
                    content_type: Some(ContentType::Movie),
                    source: "tmdb".to_owned(),
                    id: "7".to_owned(),
                }]),
                Criteria::torrent_tag(["one".to_owned(), "two".to_owned()]),
            ]))
        );
    }

    #[test]
    fn books_category_is_a_top_level_and_criterion_not_a_category_or() {
        let request = TorznabRequest {
            type_: "search".to_owned(),
            cats: vec![7000, 2030],
            ..TorznabRequest::default()
        };

        let actual = to_search_params(&request, &Profile::default_profile())
            .expect("book and movie categories map");
        assert_eq!(
            actual.filter,
            Some(Criteria::and([
                content_type_or_null([
                    ContentType::Ebook,
                    ContentType::Comic,
                    ContentType::Audiobook,
                ]),
                Criteria::or([Criteria::and([
                    content_type_or_null([ContentType::Movie]),
                    Criteria::video_resolution_in([VideoResolution::V480p]),
                ])]),
            ]))
        );
    }

    #[test]
    fn every_go_category_arm_maps_to_the_expected_criteria() {
        let hd = || {
            Criteria::video_resolution_in([
                VideoResolution::V720p,
                VideoResolution::V1080p,
                VideoResolution::V1440p,
                VideoResolution::V2160p,
            ])
        };
        let books = || {
            vec![content_type_or_null([
                ContentType::Ebook,
                ContentType::Comic,
                ContentType::Audiobook,
            ])]
        };
        let cases = [
            (
                CATEGORY_MOVIES,
                vec![content_type_or_null([ContentType::Movie])],
                false,
            ),
            (
                CATEGORY_MOVIES_SD,
                vec![
                    content_type_or_null([ContentType::Movie]),
                    Criteria::video_resolution_in([VideoResolution::V480p]),
                ],
                false,
            ),
            (
                CATEGORY_MOVIES_HD,
                vec![content_type_or_null([ContentType::Movie]), hd()],
                false,
            ),
            (
                CATEGORY_MOVIES_UHD,
                vec![
                    content_type_or_null([ContentType::Movie]),
                    Criteria::video_resolution_in([VideoResolution::V2160p]),
                ],
                false,
            ),
            (
                CATEGORY_MOVIES_3D,
                vec![
                    content_type_or_null([ContentType::Movie]),
                    Criteria::video_3d_in([Video3D::V3D, Video3D::V3DSBS, Video3D::V3DOU]),
                ],
                false,
            ),
            (
                CATEGORY_TV,
                vec![content_type_or_null([ContentType::TvShow])],
                false,
            ),
            (
                CATEGORY_TV_SD,
                vec![
                    content_type_or_null([ContentType::TvShow]),
                    Criteria::video_resolution_in([VideoResolution::V480p]),
                ],
                false,
            ),
            (
                CATEGORY_TV_HD,
                vec![content_type_or_null([ContentType::TvShow]), hd()],
                false,
            ),
            (
                CATEGORY_TV_UHD,
                vec![
                    content_type_or_null([ContentType::TvShow]),
                    Criteria::video_resolution_in([VideoResolution::V2160p]),
                ],
                false,
            ),
            (
                CATEGORY_XXX,
                vec![content_type_or_null([ContentType::Xxx])],
                false,
            ),
            (
                CATEGORY_XXX_OTHER,
                vec![content_type_or_null([ContentType::Xxx])],
                false,
            ),
            (
                CATEGORY_PC,
                vec![content_type_or_null([
                    ContentType::Software,
                    ContentType::Game,
                ])],
                false,
            ),
            (
                CATEGORY_PC_GAMES,
                vec![content_type_or_null([
                    ContentType::Software,
                    ContentType::Game,
                ])],
                false,
            ),
            (
                CATEGORY_AUDIO_AUDIOBOOK,
                vec![content_type_or_null([ContentType::Audiobook])],
                false,
            ),
            (
                CATEGORY_AUDIO,
                vec![content_type_or_null([ContentType::Music])],
                false,
            ),
            (
                CATEGORY_BOOKS_COMICS,
                vec![content_type_or_null([ContentType::Comic])],
                false,
            ),
            (CATEGORY_BOOKS, books(), true),
            (CATEGORY_BOOKS_EBOOK, books(), true),
            (CATEGORY_OTHER, Vec::new(), false),
            (9999, Vec::new(), false),
        ];

        for (category_id, leaves, top_level) in cases {
            let request = TorznabRequest {
                type_: "search".to_owned(),
                cats: vec![category_id],
                ..TorznabRequest::default()
            };
            let actual = to_search_params(&request, &Profile::default_profile())
                .expect("category request maps");
            let expected = if leaves.is_empty() {
                None
            } else if top_level {
                Some(Criteria::and(leaves))
            } else {
                Some(Criteria::and([Criteria::or([Criteria::and(leaves)])]))
            };

            assert_eq!(actual.filter, expected, "category {category_id}");
        }
    }

    #[test]
    fn unknown_function_is_torznab_error_202() {
        let request = TorznabRequest {
            type_: "foo".to_owned(),
            ..TorznabRequest::default()
        };

        let error = to_search_params(&request, &Profile::default_profile())
            .expect_err("unknown function is rejected");
        assert_eq!(error.code, 202);
        assert_eq!(error.description, "no such function (foo)");
    }

    #[test]
    fn query_uses_relevance_order_by_default() {
        let request = TorznabRequest {
            type_: "search".to_owned(),
            query: "needle".to_owned(),
            ..TorznabRequest::default()
        };

        let actual =
            to_search_params(&request, &Profile::default_profile()).expect("query request maps");
        assert_eq!(actual.order, Some(TorrentContentOrder::relevance_desc()));
    }

    // ---- F3: typed/category content-type widens to `IN (...) OR IS NULL` ------

    #[test]
    fn content_type_or_null_is_a_disjunction_with_the_is_null_branch() {
        // The widened criterion admits the requested types OR an unclassified
        // (NULL content_type) row, while still excluding a row classified as a
        // *different* type (it satisfies neither the IN nor the IS NULL branch).
        assert_eq!(
            content_type_or_null([ContentType::Movie]),
            Criteria::or([
                Criteria::content_type_in([ContentType::Movie]),
                Criteria::is_null(TorrentContentAttribute::ContentType),
            ])
        );
    }

    #[test]
    fn typed_movie_search_admits_null_content_type() {
        let request = TorznabRequest {
            type_: "movie".to_owned(),
            ..TorznabRequest::default()
        };

        let actual =
            to_search_params(&request, &Profile::default_profile()).expect("movie request maps");
        assert_eq!(
            actual.filter,
            Some(Criteria::and([content_type_or_null([ContentType::Movie])]))
        );
    }

    #[test]
    fn category_movies_admits_null_content_type() {
        let request = TorznabRequest {
            type_: "search".to_owned(),
            cats: vec![CATEGORY_MOVIES],
            ..TorznabRequest::default()
        };

        let actual = to_search_params(&request, &Profile::default_profile())
            .expect("movie category request maps");
        assert_eq!(
            actual.filter,
            Some(Criteria::and([Criteria::or([Criteria::and([
                content_type_or_null([ContentType::Movie])
            ])])]))
        );
    }
}
