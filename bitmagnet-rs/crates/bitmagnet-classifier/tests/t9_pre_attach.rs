//! T9 — the pre-attach path and the full `ApplyHint`.
//!
//! Go's `runner.Run` attaches an already-known content row **before** the
//! workflow runs, and `classifier.core.yml:92` gates the whole enrichment branch
//! on `!result.hasAttachedContent`. So this is not an optimisation: a torrent
//! whose content is already attached makes NO dependency calls, while one
//! without makes the full local-then-TMDB chain. Both facts are asserted here
//! against a resolver that records what it was asked.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bitmagnet_classifier::resolver::{tmdb, ContentResolver, ContentResultItem, ResolveError};
use bitmagnet_classifier::{Classifier, ClassifierInput, InputContent, InputHint};
use bitmagnet_model::{Content, ContentType};

/// Records every question asked, and answers nothing — so a call is visible
/// without changing the outcome.
#[derive(Default)]
struct RecordingResolver {
    calls: Mutex<Vec<String>>,
}

impl RecordingResolver {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("not poisoned").clone()
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("not poisoned").push(call);
    }
}

#[async_trait]
impl ContentResolver for RecordingResolver {
    async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        self.record(format!(
            "content_by_id({}, {source}, {id})",
            content_type.as_str()
        ));
        Ok(None)
    }

    async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        self.record(format!(
            "content_by_search({}, {base_title:?}, {year:?})",
            content_type.as_str()
        ));
        Ok(Vec::new())
    }

    async fn tmdb_find_by_external_id(
        &self,
        _request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError> {
        self.record("tmdb_find_by_external_id".to_owned());
        Ok(tmdb::FindByIdResponse::default())
    }

    async fn tmdb_movie_details(
        &self,
        _request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
        self.record("tmdb_movie_details".to_owned());
        Ok(None)
    }

    async fn tmdb_tv_details(
        &self,
        _request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
        self.record("tmdb_tv_details".to_owned());
        Ok(None)
    }

    async fn tmdb_search_movie(
        &self,
        _request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
        self.record("tmdb_search_movie".to_owned());
        Ok(tmdb::SearchMovieResponse::default())
    }

    async fn tmdb_search_tv(
        &self,
        _request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError> {
        self.record("tmdb_search_tv".to_owned());
        Ok(tmdb::SearchTvResponse::default())
    }
}

/// The real subject from the production corpus that exposed this gap.
fn sunny(hint: InputHint, contents: Vec<InputContent>) -> ClassifierInput {
    ClassifierInput {
        id: "b78a66755eb9c4c0deaf88eae082e2e53683e4f9".to_owned(),
        name: "Sunny (2011) DC BluRay 1080p 5.1CH x264 SmallAndHD".to_owned(),
        size: 2_276_985_820,
        files_status: "multi".to_owned(),
        extension: None,
        files_count: Some(1),
        files: vec![bitmagnet_classifier::InputFile {
            index: 0,
            path: "Sunny.2011.DC.BluRay.1080p.5.1CH.x264-SmallAndHD.mkv".to_owned(),
            extension: "mkv".to_owned(),
            size: 2_276_985_820,
        }],
        hint: Some(hint),
        contents,
    }
}

fn attached_movie() -> InputContent {
    InputContent {
        content_type: "movie".to_owned(),
        content_source: "tmdb".to_owned(),
        content_id: "77117".to_owned(),
        content: Some(Content {
            content_type: ContentType::Movie,
            source: "tmdb".to_owned(),
            id: "77117".to_owned(),
            title: "Sunny".to_owned(),
            release_date: None,
            release_year: Some(2011),
            adult: None,
            original_language: None,
            original_title: None,
            overview: None,
            runtime: None,
            popularity: None,
            vote_average: None,
            vote_count: None,
            created_at: None,
            updated_at: None,
            tsv: bitmagnet_fts::Tsvector::default(),
            collections: Vec::new(),
            attributes: Vec::new(),
        }),
    }
}

fn flags_on() -> bitmagnet_classifier::Flags {
    use bitmagnet_classifier::FlagValue;
    [
        ("local_search_enabled", true),
        ("apis_enabled", true),
        ("tmdb_enabled", true),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), FlagValue::Bool(value)))
    .collect()
}

async fn run(input: &ClassifierInput) -> Vec<String> {
    let resolver = Arc::new(RecordingResolver::default());
    let classifier =
        Classifier::from_core_with(Arc::clone(&resolver) as Arc<_>).expect("classifier compiles");
    let _ = classifier.run("default", &flags_on(), input).await;
    resolver.calls()
}

/// The synthesised hint (content source present) plus the matching association
/// means the workflow asks NOTHING.
#[tokio::test]
async fn a_pre_attached_torrent_makes_no_dependency_calls() {
    let calls = run(&sunny(
        InputHint {
            content_type: "movie".to_owned(),
            content_source: "tmdb".to_owned(),
            content_id: "77117".to_owned(),
            ..Default::default()
        },
        vec![attached_movie()],
    ))
    .await;

    assert!(
        calls.is_empty(),
        "an already-attached torrent must not be re-derived, got: {calls:?}"
    );
}

/// The control: the SAME torrent without the pre-attach runs the full chain.
/// This is the pair that makes the point — the only difference is the input Go
/// had and Rust previously could not represent.
#[tokio::test]
async fn the_same_torrent_without_the_association_searches() {
    let calls = run(&sunny(
        InputHint {
            content_type: "movie".to_owned(),
            ..Default::default()
        },
        Vec::new(),
    ))
    .await;

    assert_eq!(
        calls,
        vec![
            "content_by_search(movie, \"Sunny\", Some(2011))".to_owned(),
            "tmdb_search_movie".to_owned(),
        ],
        "without the association the enrichment branch must run"
    );
}

/// Go requires the hint to carry a SOURCE. A bare content type is the
/// `attach_local_content_by_id` case, not this one.
#[tokio::test]
async fn a_source_less_hint_does_not_pre_attach() {
    let calls = run(&sunny(
        InputHint {
            content_type: "movie".to_owned(),
            ..Default::default()
        },
        vec![attached_movie()],
    ))
    .await;

    assert!(
        !calls.is_empty(),
        "without a hint source Go does not pre-attach, so the search must still run"
    );
}

/// The association has to match on id, not merely on type and source.
#[tokio::test]
async fn a_different_content_id_does_not_pre_attach() {
    let calls = run(&sunny(
        InputHint {
            content_type: "movie".to_owned(),
            content_source: "tmdb".to_owned(),
            content_id: "999999".to_owned(),
            ..Default::default()
        },
        vec![attached_movie()],
    ))
    .await;

    assert!(!calls.is_empty(), "a non-matching association is not a hit");
}

/// Go guards on `tc.Content.Source == tc.ContentSource`, which is how it detects
/// an association whose content was never loaded. Attaching an unhydrated row
/// would blank the result rather than enrich it.
#[tokio::test]
async fn an_unhydrated_association_does_not_pre_attach() {
    let mut association = attached_movie();
    association.content = None;

    let calls = run(&sunny(
        InputHint {
            content_type: "movie".to_owned(),
            content_source: "tmdb".to_owned(),
            content_id: "77117".to_owned(),
            ..Default::default()
        },
        vec![association],
    ))
    .await;

    assert!(
        !calls.is_empty(),
        "an unloaded association must not be mistaken for an attachment"
    );
}
