//! Minimal code-first schema covering the SDL fidelity edge cases.

use async_graphql::{
    EmptyMutation, EmptySubscription, Enum, InputObject, MaybeUndefined, Object, Schema,
};
use serde::{Deserialize, Serialize};

macro_rules! string_scalar {
    ($name:ident, $sdl_name:literal) => {
        #[derive(Clone, Debug, Deserialize, Serialize)]
        struct $name(String);

        async_graphql::scalar!($name, $sdl_name);
    };
}

string_scalar!(Hash20, "Hash20");
string_scalar!(Hash32, "Hash32");
string_scalar!(Date, "Date");
string_scalar!(DateTime, "DateTime");
string_scalar!(Duration, "Duration");
string_scalar!(Year, "Year");

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Void(());

async_graphql::scalar!(Void, "Void");

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
enum ContentType {
    #[graphql(name = "movie")]
    Movie,
    #[graphql(name = "tv_show")]
    TvShow,
    #[graphql(name = "music")]
    Music,
    #[graphql(name = "ebook")]
    Ebook,
    #[graphql(name = "comic")]
    Comic,
    #[graphql(name = "audiobook")]
    Audiobook,
    #[graphql(name = "game")]
    Game,
    #[graphql(name = "software")]
    Software,
    #[graphql(name = "xxx")]
    Xxx,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
enum FacetLogic {
    #[graphql(name = "and")]
    And,
    #[graphql(name = "or")]
    Or,
}

#[derive(InputObject)]
struct ContentTypeFacetInput {
    aggregate: Option<bool>,
    filter: Option<Vec<Option<ContentType>>>,
}

#[derive(InputObject)]
struct SizeRangeInput {
    max: Option<i32>,
    min: Option<i32>,
}

#[derive(InputObject)]
struct TorrentReprocessInput {
    apis_disabled: Option<bool>,
    classifier_rematch: Option<bool>,
    classifier_workflow: Option<String>,
    info_hashes: Vec<Hash20>,
    local_search_disabled: Option<bool>,
}

#[derive(InputObject)]
struct WrapperPinInput {
    via_option: Option<bool>,
    via_maybe_undefined: MaybeUndefined<bool>,
    via_option_option: Option<Option<bool>>,
}

/// Scaffolding query root that makes every spike type reachable.
pub struct Query;

#[Object]
impl Query {
    async fn input_cases(
        &self,
        content_type_facet: Option<ContentTypeFacetInput>,
        size_range: Option<SizeRangeInput>,
        torrent_reprocess: Option<TorrentReprocessInput>,
        wrapper_pin: Option<WrapperPinInput>,
        hashes: Option<Vec<Hash20>>,
    ) -> bool {
        drop((
            content_type_facet,
            size_range,
            torrent_reprocess,
            wrapper_pin,
            hashes,
        ));
        true
    }

    async fn hash32(&self) -> Option<Hash32> {
        None
    }

    async fn date(&self) -> Option<Date> {
        None
    }

    async fn date_time(&self) -> Option<DateTime> {
        None
    }

    async fn duration(&self) -> Option<Duration> {
        None
    }

    async fn year(&self) -> Option<Year> {
        None
    }

    async fn void(&self) -> Option<Void> {
        None
    }

    async fn content_type(&self) -> Option<ContentType> {
        None
    }

    async fn facet_logic(&self) -> Option<FacetLogic> {
        None
    }
}

/// Build the code-first schema used by the fidelity spike.
pub fn spike_schema() -> Schema<Query, EmptyMutation, EmptySubscription> {
    Schema::build(Query, EmptyMutation, EmptySubscription).finish()
}

/// Export the code-first spike schema as SDL.
pub fn spike_schema_sdl() -> String {
    spike_schema().sdl()
}
