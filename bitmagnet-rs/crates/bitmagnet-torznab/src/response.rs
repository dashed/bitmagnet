//! Torznab caps, RSS feed, and error response value types.

use chrono::{DateTime, FixedOffset, NaiveDate};
use thiserror::Error;

use crate::categories::top_level_categories;

/// Chrono format equivalent to Go's `Mon, 02 Jan 2006 15:04:05 -0700` layout.
pub const RSS_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S %z";

/// A Torznab capabilities document.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Caps {
    pub server: CapsServer,
    pub limits: CapsLimits,
    pub searching: CapsSearching,
    pub categories: Vec<Category>,
    pub tags: String,
}

/// Server metadata rendered as attributes on the caps `server` element.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapsServer {
    pub title: String,
}

/// Search limits rendered as attributes on the caps `limits` element.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CapsLimits {
    pub max: u32,
    pub default: u32,
}

/// The six Torznab search modes, in their wire-order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapsSearching {
    pub search: CapsSearch,
    pub tv_search: CapsSearch,
    pub movie_search: CapsSearch,
    pub music_search: CapsSearch,
    pub audio_search: CapsSearch,
    pub book_search: CapsSearch,
}

/// Availability and supported parameters for one Torznab search mode.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapsSearch {
    pub available: String,
    pub supported_params: String,
}

/// A top-level Torznab category and its subcategories.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Category {
    pub id: i32,
    pub name: String,
    pub subcat: Vec<Subcategory>,
}

impl Category {
    /// Returns true when `id` identifies this category or one of its subcategories.
    #[must_use]
    pub fn has(&self, id: i32) -> bool {
        self.id == id || self.subcat.iter().any(|subcategory| subcategory.id == id)
    }
}

/// A leaf Torznab category.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Subcategory {
    pub id: i32,
    pub name: String,
}

/// A complete Torznab RSS search result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SearchResult {
    pub channel: Channel,
}

/// RSS channel metadata and search result items.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Channel {
    pub title: Option<String>,
    pub link: Option<String>,
    pub description: Option<String>,
    pub language: Option<String>,
    pub pub_date: RssDate,
    pub last_build_date: RssDate,
    pub docs: Option<String>,
    pub generator: Option<String>,
    pub response: Response,
    pub items: Vec<Item>,
}

/// Newznab paging metadata.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Response {
    pub offset: u32,
    pub total: u32,
}

/// One Torznab RSS result item.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Item {
    pub title: String,
    pub guid: Option<String>,
    pub pub_date: RssDate,
    pub category: Option<String>,
    pub link: Option<String>,
    pub size: u64,
    pub description: Option<String>,
    pub comments: Option<String>,
    pub enclosure: Enclosure,
    pub torznab_attrs: Vec<TorznabAttr>,
}

/// RSS enclosure attributes for a result's magnet URI.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Enclosure {
    pub url: String,
    pub length: String,
    pub type_: String,
}

/// A literal `torznab:attr` name/value pair.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TorznabAttr {
    pub name: String,
    pub value: String,
}

/// A Torznab error response.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{description}")]
pub struct TorznabError {
    pub code: i32,
    pub description: String,
}

/// An RSS date with the same formatting behavior as the Go adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RssDate(pub DateTime<FixedOffset>);

impl RssDate {
    /// Formats this date using the RSS layout shared by elements and Torznab attributes.
    #[must_use]
    pub fn format(&self) -> String {
        self.0.format(RSS_DATE_FORMAT).to_string()
    }
}

impl Default for RssDate {
    fn default() -> Self {
        let date = NaiveDate::from_ymd_opt(1, 1, 1)
            .and_then(|value| value.and_hms_opt(0, 0, 0))
            .expect("year 1 at midnight is a valid chrono date");
        let offset = FixedOffset::east_opt(0).expect("UTC is a valid fixed offset");

        Self(DateTime::from_naive_utc_and_offset(date, offset))
    }
}

impl From<DateTime<FixedOffset>> for RssDate {
    fn from(value: DateTime<FixedOffset>) -> Self {
        Self(value)
    }
}

/// Builds the fixed caps profile exposed by the Go Torznab adapter.
#[must_use]
pub fn caps(title: &str, max_limit: u32, default_limit: u32) -> Caps {
    Caps {
        server: CapsServer {
            title: title.to_owned(),
        },
        limits: CapsLimits {
            max: max_limit,
            default: default_limit,
        },
        searching: CapsSearching {
            search: CapsSearch {
                available: "yes".to_owned(),
                supported_params: "q,imdbid,tmdbid".to_owned(),
            },
            tv_search: CapsSearch {
                available: "yes".to_owned(),
                supported_params: "q,imdbid,tmdbid,season,ep".to_owned(),
            },
            movie_search: CapsSearch {
                available: "yes".to_owned(),
                supported_params: "q,imdbid,tmdbid".to_owned(),
            },
            music_search: CapsSearch {
                available: "yes".to_owned(),
                supported_params: "q".to_owned(),
            },
            audio_search: CapsSearch {
                available: "no".to_owned(),
                supported_params: String::new(),
            },
            book_search: CapsSearch {
                available: "yes".to_owned(),
                supported_params: "q".to_owned(),
            },
        },
        categories: top_level_categories(),
        tags: String::new(),
    }
}
