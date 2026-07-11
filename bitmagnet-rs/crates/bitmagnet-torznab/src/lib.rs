//! Phase-1 Torznab/Newznab read adapter (roadmap 05 §Phase 1): axum handler +
//! quick-xml response structs (caps/categories/search/tv/movie/music/book),
//! translating Torznab params → search options → results via
//! `bitmagnet-search-query`. Reads blob/sidecars only — NEVER torrent_files.
//! Serves on the Phase-0 bitmagnet-common bootstrap (serve/metrics/config).
//!
//! Lane contract (phase1-tasks.md): this crate owns HTTP/XML/params/categories.
//! Query construction lives in bitmagnet-search-query.

pub mod categories;
pub mod config;
pub mod mapping;
pub mod request;
pub mod response;
pub mod result_map;
pub mod service;
mod xml;

pub use categories::{
    category_by_id, top_level_categories, CATEGORY_AUDIO, CATEGORY_AUDIO_AUDIOBOOK, CATEGORY_BOOKS,
    CATEGORY_BOOKS_COMICS, CATEGORY_BOOKS_EBOOK, CATEGORY_MOVIES, CATEGORY_MOVIES_3D,
    CATEGORY_MOVIES_HD, CATEGORY_MOVIES_SD, CATEGORY_MOVIES_UHD, CATEGORY_OTHER, CATEGORY_PC,
    CATEGORY_PC_GAMES, CATEGORY_TV, CATEGORY_TV_HD, CATEGORY_TV_SD, CATEGORY_TV_UHD, CATEGORY_XXX,
    CATEGORY_XXX_OTHER,
};
pub use config::{Config, Profile};
pub use mapping::to_search_params;
pub use request::{
    parse, profile_name, TorznabRequest, FUNCTION_BOOK, FUNCTION_CAPS, FUNCTION_MOVIE,
    FUNCTION_MUSIC, FUNCTION_SEARCH, FUNCTION_TV_SEARCH, PARAM_CAT, PARAM_EPISODE, PARAM_IMDB_ID,
    PARAM_LIMIT, PARAM_OFFSET, PARAM_QUERY, PARAM_SEASON, PARAM_TMDB_ID, PARAM_TYPE,
};
pub use response::{
    caps, Caps, CapsLimits, CapsSearch, CapsSearching, CapsServer, Category, Channel, Enclosure,
    Item, Response, RssDate, SearchResult, Subcategory, TorznabAttr, TorznabError, RSS_DATE_FORMAT,
};
pub use result_map::{
    content_type_category_id, item_from_fixture_fields, magnet, to_item, to_search_result,
    FixtureItemFields,
};
pub use service::{router, SearchClient, SearchError};
pub use xml::XmlError;
