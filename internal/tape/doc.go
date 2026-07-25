// Package tape records and replays the observations a classification makes
// against its impure dependencies (the local content search and the TMDB HTTP
// API).
//
// # Why a tape and not a database snapshot
//
// The Go classifier is not a pure function of (torrent, database snapshot).
// The local content search orders candidates by ts_rank_cd, which is degenerate
// for the phrase queries the classifier issues: a real production query can
// return dozens of rows all ranked exactly 1.0. The candidate window (LIMIT 10)
// and the order within it are then decided by the query plan, and the
// levenshtein selection that follows is strictly first-wins with an early exit
// on an exact match. Re-running the same query against a frozen snapshot
// re-rolls that dice.
//
// The only replayable artifact is therefore the ordered candidate list that was
// actually observed. This package records it, together with the request that
// produced it.
//
// # Requests are recorded, not just responses
//
// Every observation stores the request as well as the response. On replay the
// incoming request is compared against the recorded one and a mismatch is a
// hard error ([ErrDesync]). This catches a port asking a different question --
// a different search string, a missing year filter, a dropped query parameter
// -- even when the answers happen to coincide.
//
// # Empty is not missing
//
// A recorded observation with outcome "ok" and an empty response is a genuine
// empty answer from the dependency. An observation that was never recorded is
// [ErrMiss]. The two are distinct in the format and distinct on replay; a
// replay must never read a gap in the tape as a legitimate empty answer.
//
// # Off by default
//
// Recording is driven entirely by a [Session] carried on the context. Code
// paths that observe dependencies call [SessionFrom]; when no session is
// present -- which is every code path in a normally configured process -- they
// do nothing beyond a context lookup and a nil check.
package tape
