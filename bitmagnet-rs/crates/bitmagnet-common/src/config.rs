//! Layered configuration loading with Go-compatible environment key lookup.
//!
//! Environment overrides use Figment's value syntax. Full Go value-coercion
//! parity, including comma-separated lists, is outside Phase 0's scope; the
//! environment key mapping is the compatibility contract established here.

use std::path::Path;

use figment::providers::{Format, Serialized, Yaml};
use figment::value::Value;
use figment::Figment;

use crate::{Error, Result};

/// The environment-variable name for a dotted config path, matching Go's env
/// resolver (`strings.ToUpper(strings.Join(path, "_"))`). Segments must already
/// be `snake_case`.
#[must_use]
pub fn env_key(path: &[&str]) -> String {
    path.join("_").to_uppercase()
}

/// Return the environment-variable name for a dotted configuration path.
///
/// For example, `blob_migration.consistency.enabled` maps to
/// `BLOB_MIGRATION_CONSISTENCY_ENABLED`.
#[must_use]
pub fn env_key_for_dotpath(dotpath: &str) -> String {
    env_key(&dotpath.split('.').collect::<Vec<_>>())
}

/// A configuration provider stack with defaults, optional YAML, and forward
/// environment overrides, in increasing order of priority.
#[must_use = "the layered configuration must be extracted or further configured"]
pub struct Layered {
    figment: Figment,
}

impl Layered {
    /// Seed the provider stack from a serializable defaults value.
    pub fn defaults<T: serde::Serialize>(defaults: &T) -> Result<Self> {
        let figment = Figment::from(Serialized::defaults(defaults));
        figment.extract::<Value>().map_err(figment_error)?;
        Ok(Self { figment })
    }

    /// Merge a YAML file above the defaults when the path exists.
    ///
    /// A missing path is a no-op. Errors from an existing file are reported by
    /// [`Self::merge_env`] or [`Self::extract`].
    pub fn merge_yaml_file(mut self, path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        if path.exists() {
            self.figment = self.figment.merge(Yaml::file(path));
        }
        self
    }

    /// Apply environment overrides for every known leaf path in the current
    /// provider stack, using [`env_key`] for forward key resolution.
    ///
    /// Overrides have the highest priority. Non-Unicode environment values are
    /// rejected as configuration errors.
    pub fn merge_env(self) -> Result<Self> {
        let value = self.figment.extract::<Value>().map_err(figment_error)?;
        let mut leaf_paths = Vec::new();
        collect_leaf_paths(&value, &mut Vec::new(), &mut leaf_paths);

        let mut figment = self.figment;
        for path in leaf_paths {
            let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
            let variable = env_key(&segments);
            match std::env::var(&variable) {
                Ok(raw_string) => {
                    let dotpath = path.join(".");
                    // Match Figment's Env provider value parsing without
                    // reverse-splitting the environment-variable name.
                    let parsed = raw_string
                        .parse::<Value>()
                        .expect("Figment value parsing is infallible");
                    figment = figment.merge(Serialized::default(&dotpath, parsed));
                }
                Err(std::env::VarError::NotPresent) => {}
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(Error::Config(format!(
                        "{variable} must contain valid Unicode"
                    )));
                }
            }
        }

        Ok(Self { figment })
    }

    /// Extract the final typed configuration from the provider stack.
    pub fn extract<T: serde::de::DeserializeOwned>(self) -> Result<T> {
        self.figment.extract().map_err(figment_error)
    }
}

fn collect_leaf_paths(value: &Value, path: &mut Vec<String>, output: &mut Vec<Vec<String>>) {
    if let Some(dict) = value.as_dict() {
        for (key, child) in dict {
            path.push(key.clone());
            collect_leaf_paths(child, path, output);
            path.pop();
        }
    } else if !path.is_empty() {
        output.push(path.clone());
    }
}

fn figment_error(error: figment::Error) -> Error {
    Error::Config(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Mutex;

    use serde::{Deserialize, Serialize};

    use super::{env_key, Layered};

    const COUNT_PATH: &[&str] = &["bitmagnet_common_layered_test", "count"];
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        bitmagnet_common_layered_test: Nested,
        scalar: String,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nested {
        count: u32,
        name: String,
    }

    struct EnvRestore {
        name: String,
        original: Option<OsString>,
    }

    impl EnvRestore {
        fn set(name: String, value: &str) -> Self {
            let original = std::env::var_os(&name);
            std::env::set_var(&name, value);
            Self { name, original }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => std::env::set_var(&self.name, value),
                None => std::env::remove_var(&self.name),
            }
        }
    }

    fn sample_defaults() -> Sample {
        Sample {
            bitmagnet_common_layered_test: Nested {
                count: 1,
                name: "default name".to_owned(),
            },
            scalar: "default scalar".to_owned(),
        }
    }

    fn yaml_override(count: u32) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().expect("temporary YAML file is created");
        std::fs::write(
            file.path(),
            format!("bitmagnet_common_layered_test:\n  count: {count}\n"),
        )
        .expect("temporary YAML file is written");
        file
    }

    #[test]
    fn defaults_only_returns_seed_values() {
        let defaults = sample_defaults();
        let loaded = Layered::defaults(&defaults)
            .expect("defaults are valid")
            .extract::<Sample>()
            .expect("defaults extract");

        assert_eq!(loaded, defaults);
    }

    #[test]
    fn yaml_file_overrides_a_default_leaf() {
        let file = yaml_override(3);
        let loaded = Layered::defaults(&sample_defaults())
            .expect("defaults are valid")
            .merge_yaml_file(file.path())
            .extract::<Sample>()
            .expect("YAML override extracts");

        assert_eq!(loaded.bitmagnet_common_layered_test.count, 3);
        assert_eq!(loaded.bitmagnet_common_layered_test.name, "default name");
        assert_eq!(loaded.scalar, "default scalar");
    }

    #[test]
    fn env_key_from_the_walked_path_overrides_a_default_leaf() {
        let _env_lock = ENV_LOCK.lock().expect("config env lock is not poisoned");
        let _restore = EnvRestore::set(env_key(COUNT_PATH), "5");
        let loaded = Layered::defaults(&sample_defaults())
            .expect("defaults are valid")
            .merge_env()
            .expect("environment override is valid")
            .extract::<Sample>()
            .expect("environment override extracts");

        assert_eq!(loaded.bitmagnet_common_layered_test.count, 5);
    }

    #[test]
    fn env_beats_yaml_which_beats_defaults() {
        let _env_lock = ENV_LOCK.lock().expect("config env lock is not poisoned");
        let _restore = EnvRestore::set(env_key(COUNT_PATH), "5");
        let file = yaml_override(3);
        let loaded = Layered::defaults(&sample_defaults())
            .expect("defaults are valid")
            .merge_yaml_file(file.path())
            .merge_env()
            .expect("environment override is valid")
            .extract::<Sample>()
            .expect("layered configuration extracts");

        assert_eq!(loaded.bitmagnet_common_layered_test.count, 5);
        assert_eq!(loaded.bitmagnet_common_layered_test.name, "default name");
        assert_eq!(loaded.scalar, "default scalar");
    }
}
