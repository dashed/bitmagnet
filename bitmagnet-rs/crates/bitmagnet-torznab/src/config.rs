//! Torznab profiles and profile-list configuration.

use serde::{Deserialize, Serialize};

use crate::response::{caps, Caps};

/// Per-path Torznab behavior and search limits.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Profile {
    pub id: String,
    pub title: String,
    pub disable_order_by_relevance: bool,
    pub default_limit: u32,
    pub max_limit: u32,
    pub tags: Vec<String>,
}

impl Profile {
    /// The built-in profile selected by empty, `api`, and `default` paths.
    #[must_use]
    pub fn default_profile() -> Self {
        Self {
            id: "default".to_owned(),
            title: "bitmagnet".to_owned(),
            disable_order_by_relevance: false,
            default_limit: 100,
            max_limit: 100,
            tags: Vec::new(),
        }
    }

    /// Fills the fields inherited from the built-in profile and clamps the
    /// default page size to the configured maximum.
    #[must_use]
    pub fn merge_defaults(mut self) -> Self {
        let defaults = Self::default_profile();

        if self.title.is_empty() {
            self.title = defaults.title;
        }
        if self.default_limit == 0 {
            self.default_limit = defaults.default_limit;
        }
        if self.max_limit == 0 {
            self.max_limit = defaults.max_limit;
        }
        if self.default_limit > self.max_limit {
            self.default_limit = self.max_limit;
        }

        self
    }

    /// Builds the capabilities document advertised for this profile.
    #[must_use]
    pub fn caps(&self) -> Caps {
        caps(&self.title, self.max_limit, self.default_limit)
    }
}

/// User-defined Torznab profiles.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub profiles: Vec<Profile>,
}

impl Config {
    /// Applies [`Profile::merge_defaults`] to every configured profile.
    #[must_use]
    pub fn merge_defaults(mut self) -> Self {
        self.profiles = self
            .profiles
            .into_iter()
            .map(Profile::merge_defaults)
            .collect();
        self
    }

    /// Finds a profile by ID using the case-insensitive profile lookup used by
    /// the Go server.
    #[must_use]
    pub fn get_profile(&self, name: &str) -> Option<&Profile> {
        self.profiles
            .iter()
            .find(|profile| equal_fold(&profile.id, name))
    }
}

fn equal_fold(left: &str, right: &str) -> bool {
    if left.is_ascii() && right.is_ascii() {
        return left.eq_ignore_ascii_case(right);
    }

    let mut left = left.chars();
    let mut right = right.chars();
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right))
                if left == right
                    || left.to_lowercase().eq(right.to_lowercase())
                    || left.to_uppercase().eq(right.to_uppercase()) => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Profile};

    #[test]
    fn profile_defaults_and_limit_clamp_match_go() {
        let profile = Profile {
            id: "test".to_owned(),
            default_limit: 250,
            max_limit: 200,
            ..Profile::default()
        }
        .merge_defaults();

        assert_eq!(profile.title, "bitmagnet");
        assert_eq!(profile.default_limit, 200);
        assert_eq!(profile.max_limit, 200);
    }

    #[test]
    fn config_merges_profiles_and_looks_them_up_case_insensitively() {
        let config = Config {
            profiles: vec![
                Profile {
                    id: "MixedCase".to_owned(),
                    ..Profile::default()
                },
                Profile {
                    id: "Σ".to_owned(),
                    ..Profile::default()
                },
            ],
        }
        .merge_defaults();

        let profile = config
            .get_profile("mixedcase")
            .expect("configured profile is found");
        assert_eq!(profile.title, "bitmagnet");
        assert!(config.get_profile("ς").is_some());
        assert!(config.get_profile("missing").is_none());
    }

    #[test]
    fn built_in_profile_caps_use_profile_limits_and_title() {
        let profile = Profile::default_profile();
        let caps = profile.caps();

        assert_eq!(caps.server.title, "bitmagnet");
        assert_eq!(caps.limits.max, 100);
        assert_eq!(caps.limits.default, 100);
    }

    #[test]
    fn serde_accepts_profiles_with_fields_that_merge_from_defaults() {
        let config: Config = serde_json::from_str(r#"{"profiles":[{"id":"test"}]}"#)
            .expect("partial profile config deserializes");
        let profile = config
            .merge_defaults()
            .profiles
            .into_iter()
            .next()
            .expect("profile is present");

        assert_eq!(profile.title, "bitmagnet");
        assert_eq!(profile.default_limit, 100);
        assert_eq!(profile.max_limit, 100);
    }
}
