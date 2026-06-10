//! Domain query types — the validated, engine-agnostic intent.
//!
//! Proto requests are mapped to these (see [`crate::service`]); the DuckDB
//! engine turns them into [`crate::sql::SafeQuery`]s, while the
//! [`crate::engine::InMemoryEngine`] evaluates them directly (so the service is
//! testable without DuckDB). Keeping this layer separate is what makes "no user
//! value ever reaches SQL text" checkable in isolation.

/// Structured file filters (all optional; empty = unconstrained).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    /// `extension IN (...)`. An empty-string entry selects the NULL-extension
    /// (no path-derived extension) bucket.
    pub extensions: Vec<String>,
    pub size_min: Option<u64>,
    pub size_max: Option<u64>,
    /// Case-insensitive path substring (escaped before ILIKE).
    pub path_query: Option<String>,
}

impl Filters {
    /// Evaluate the filter against one in-memory file row (InMemoryEngine).
    pub fn matches(&self, ext: &Option<String>, size: u64, path: &str) -> bool {
        if !self.extensions.is_empty() {
            let hit = self.extensions.iter().any(|e| match (e.is_empty(), ext) {
                (true, None) => true,             // '' selects NULL ext
                (false, Some(x)) => x == e,
                _ => false,
            });
            if !hit {
                return false;
            }
        }
        if let Some(min) = self.size_min {
            if size < min {
                return false;
            }
        }
        if let Some(max) = self.size_max {
            if size > max {
                return false;
            }
        }
        if let Some(q) = &self.path_query {
            if !path.to_lowercase().contains(&q.to_lowercase()) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortField {
    Size,
    Path,
    InfoHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sort {
    pub field: SortField,
    pub dir: SortDir,
}

impl Default for Sort {
    fn default() -> Self {
        Self {
            field: SortField::Size,
            dir: SortDir::Desc,
        }
    }
}

impl Sort {
    /// Parse a proto sort field name; unknown names fall back to the default.
    pub fn from_proto(field: &str, descending: bool) -> Self {
        let field = match field {
            "path" => SortField::Path,
            "info_hash" => SortField::InfoHash,
            _ => SortField::Size,
        };
        Self {
            field,
            dir: if descending { SortDir::Desc } else { SortDir::Asc },
        }
    }
}

/// A file-rows / collapse search.
#[derive(Debug, Clone)]
pub struct FileQuery {
    pub filters: Filters,
    pub sort: Sort,
    pub limit: u32,
    pub collapse_to_torrent: bool,
    pub preview_limit: u32,
}

/// A count request.
#[derive(Debug, Clone)]
pub struct CountQuery {
    pub filters: Filters,
    pub collapse_to_torrent: bool,
}

/// Default/maximum page sizes (defensive caps — a broad path-ILIKE must always
/// be bounded; CB campaign / ARCH-C).
pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 500;
pub const DEFAULT_PREVIEW: u32 = 5;
pub const MAX_PREVIEW: u32 = 50;

/// Clamp a requested limit into `[1, MAX_LIMIT]`, defaulting 0 to `DEFAULT_LIMIT`.
pub fn clamp_limit(requested: u32) -> u32 {
    match requested {
        0 => DEFAULT_LIMIT,
        n => n.min(MAX_LIMIT),
    }
}

pub fn clamp_preview(requested: u32) -> u32 {
    match requested {
        0 => DEFAULT_PREVIEW,
        n => n.min(MAX_PREVIEW),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_matches_ext_size_path() {
        let f = Filters {
            extensions: vec!["mkv".into()],
            size_min: Some(10),
            size_max: Some(100),
            path_query: Some("Movie".into()),
        };
        assert!(f.matches(&Some("mkv".into()), 50, "Movie/a.mkv"));
        assert!(!f.matches(&Some("avi".into()), 50, "Movie/a.avi")); // ext
        assert!(!f.matches(&Some("mkv".into()), 5, "Movie/a.mkv")); // size
        assert!(!f.matches(&Some("mkv".into()), 50, "Show/a.mkv")); // path
    }

    #[test]
    fn empty_ext_filter_matches_null_bucket() {
        let f = Filters {
            extensions: vec!["".into()],
            ..Default::default()
        };
        assert!(f.matches(&None, 1, "readme"));
        assert!(!f.matches(&Some("mkv".into()), 1, "a.mkv"));
    }

    #[test]
    fn clamp_limit_bounds() {
        assert_eq!(clamp_limit(0), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(10), 10);
        assert_eq!(clamp_limit(99999), MAX_LIMIT);
    }
}
