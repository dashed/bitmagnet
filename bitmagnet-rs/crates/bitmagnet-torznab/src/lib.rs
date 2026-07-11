//! Phase-1 Torznab/Newznab read adapter (roadmap 05 §Phase 1): axum handler +
//! quick-xml response structs (caps/categories/search/tv/movie/music/book),
//! translating Torznab params → search options → results via
//! `bitmagnet-search-query`. Reads blob/sidecars only — NEVER torrent_files.
//! Serves on the Phase-0 bitmagnet-common bootstrap (serve/metrics/config).
//!
//! Lane contract (phase1-tasks.md): this crate owns HTTP/XML/params/categories.
//! Query construction lives in bitmagnet-search-query.

pub mod categories;
pub mod response;
mod xml;

pub use categories::{
    category_by_id, top_level_categories, CATEGORY_AUDIO, CATEGORY_AUDIO_AUDIOBOOK, CATEGORY_BOOKS,
    CATEGORY_BOOKS_COMICS, CATEGORY_BOOKS_EBOOK, CATEGORY_MOVIES, CATEGORY_MOVIES_3D,
    CATEGORY_MOVIES_HD, CATEGORY_MOVIES_SD, CATEGORY_MOVIES_UHD, CATEGORY_OTHER, CATEGORY_PC,
    CATEGORY_PC_GAMES, CATEGORY_TV, CATEGORY_TV_HD, CATEGORY_TV_SD, CATEGORY_TV_UHD, CATEGORY_XXX,
    CATEGORY_XXX_OTHER,
};
pub use response::{
    caps, Caps, CapsLimits, CapsSearch, CapsSearching, CapsServer, Category, Channel, Enclosure,
    Item, Response, RssDate, SearchResult, Subcategory, TorznabAttr, TorznabError, RSS_DATE_FORMAT,
};
pub use xml::XmlError;
