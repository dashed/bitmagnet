//! TMDB API client for the B′ enrichment-parity lanes (Go `internal/tmdb`).
//!
//! Placeholder registered by the B′-0 classifier-dependency-seam lane so the
//! follow-on lane can land the `reqwest` client (workspace dependency already
//! promoted) plus `MovieDetailsToMovieModel` / `TvShowDetailsToTvShowModel`
//! without editing the workspace manifest.
//!
//! The request/response DTOs the client returns already exist, in
//! `bitmagnet_classifier::resolver::tmdb` — they live with the
//! `ContentResolver` trait they are the vocabulary of, so that this crate can
//! depend on `bitmagnet-classifier` to implement the trait without a
//! dependency cycle.
