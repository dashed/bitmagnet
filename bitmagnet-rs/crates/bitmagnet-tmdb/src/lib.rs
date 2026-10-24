//! The **live** TMDB client for the B′ enrichment-parity lanes — a port of Go's
//! `internal/tmdb`.
//!
//! [`TmdbClient`] issues the five calls the classifier makes:
//!
//! | endpoint | Go | seam method |
//! |---|---|---|
//! | `GET /search/movie` | `client.SearchMovie` | [`TmdbClient::search_movie`] |
//! | `GET /search/tv` | `client.SearchTv` | [`TmdbClient::search_tv`] |
//! | `GET /movie/{id}` | `client.MovieDetails` | [`TmdbClient::movie_details`] |
//! | `GET /tv/{series_id}` | `client.TvDetails` | [`TmdbClient::tv_details`] |
//! | `GET /find/{external_id}` | `client.FindByID` | [`TmdbClient::find_by_external_id`] |
//!
//! Their signatures and error type are the TMDB half of
//! `bitmagnet_classifier::ContentResolver`, so a composite resolver — the local
//! PostgreSQL search plus this — delegates without adapting anything. The trait
//! itself is deliberately not implemented here: its other two methods belong to
//! `bitmagnet-content-search`.
//!
//! # What this crate is *for*
//!
//! Not "an HTTP client for TMDB" — those are a weekend's work. This is the live
//! half of a **parity oracle**. The replay half already exists
//! (`bitmagnet_classifier::resolver::tape::TapeContentResolver`) and is pinned
//! against a production tape recorded from Go; the numbers in that gate are only
//! meaningful if the live client asks the *same questions* the recording did.
//!
//! So the contract is not "TMDB accepts our requests" but "our requests are Go's
//! requests, byte for byte". Three things follow, and they are the reason the
//! code is shaped the way it is:
//!
//! * The request is a **value** ([`request::TmdbRequestSpec`]) built with no
//!   I/O, so `tests/tape_conformance.rs` can rebuild all 48 TMDB calls in the
//!   production corpus and compare them to what Go recorded.
//! * The failure classes are **not** flattened ([`error::TmdbError`]). A 404 on
//!   a details lookup is an *absence* the classifier falls through on; a 401
//!   latches; anything else aborts the classification. One error type would
//!   silently change control flow.
//! * The middleware layers reproduce Go's **order**, including where that order
//!   costs something — see [`TmdbClient`]'s module docs.
//!
//! # 🚨 Credentials
//!
//! The API key is set once on the client, never per request — Go does the same
//! (`requester_lazy.go:66`), and it is why recorded tapes contain no secret.
//! [`ApiKey`] has a redacted `Debug` and no `Display`, the logging layer prints
//! the path and parameters rather than the URL, and transport errors are
//! stripped of their URL before they are rendered.
//!
//! # Not covered here
//!
//! `MovieDetailsResponse::into_content` / `TvDetailsResponse::into_content` —
//! Go's `transformers.go` — live with the DTOs in
//! `bitmagnet_classifier::resolver::tmdb`, because the classifier's `attach_*`
//! actions need them at attach time and a dependency this way round would be a
//! cycle. This crate is the transport only.

pub mod error;
pub mod request;
pub mod transport;

mod client;
mod limiter;

pub use client::{ApiKey, TmdbClient, TmdbConfig};
pub use error::TmdbError;
pub use request::TmdbRequestSpec;
pub use transport::{HttpResponse, ReqwestTransport, RetryPolicy, Transport};
