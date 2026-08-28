//! Connection configuration, mirroring Go's
//! `internal/database/postgres.Config`.

use std::env;
use std::fmt;
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgSslMode};
use url::Url;

use crate::error::DbError;

/// PostgreSQL connection settings.
///
/// When [`Self::dsn`] is non-empty it is used verbatim (parsed by SQLx as a
/// `postgres://` URL); otherwise a connection is built from the individual
/// fields. Defaults mirror Go's `NewDefaultConfig` (`localhost`, user
/// `postgres`, port `5432`, database `bitmagnet`).
#[derive(Clone)]
pub struct DbConfig {
    /// Full connection string. If set, takes precedence over the fields below.
    /// SQLx expects the `postgres://user:pass@host:port/db?sslmode=…` URL form
    /// (note: unlike Go's libpq default, the space-separated keyword/value form
    /// is not accepted here).
    pub dsn: String,
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Login role.
    pub user: String,
    /// Login password (empty for none).
    pub password: String,
    /// Database name.
    pub dbname: String,
    /// SSL mode, e.g. `disable`, `prefer`, `require` (empty = SQLx default).
    pub sslmode: String,
    /// Upper bound on pooled connections.
    pub max_connections: u32,
}

impl fmt::Debug for DbConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DbConfig")
            .field("dsn", &"<redacted>")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("dbname", &self.dbname)
            .field("sslmode", &self.sslmode)
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            host: "localhost".to_owned(),
            port: 5432,
            user: "postgres".to_owned(),
            password: String::new(),
            dbname: "bitmagnet".to_owned(),
            sslmode: String::new(),
            max_connections: 10,
        }
    }
}

impl DbConfig {
    /// Builds the config from `BITMAGNET_POSTGRES_*` environment variables,
    /// falling back to [`Default`] for any that are unset. Returns
    /// [`DbError::Config`] if `PORT` or `MAX_CONNECTIONS` are set but unparsable.
    pub fn from_env() -> Result<Self, DbError> {
        let defaults = Self::default();

        let port = parse_env("BITMAGNET_POSTGRES_PORT", defaults.port)?;
        let max_connections = parse_env(
            "BITMAGNET_POSTGRES_MAX_CONNECTIONS",
            defaults.max_connections,
        )?;

        Ok(Self {
            dsn: env::var("BITMAGNET_POSTGRES_DSN").unwrap_or(defaults.dsn),
            host: env::var("BITMAGNET_POSTGRES_HOST").unwrap_or(defaults.host),
            port,
            user: env::var("BITMAGNET_POSTGRES_USER").unwrap_or(defaults.user),
            password: env::var("BITMAGNET_POSTGRES_PASSWORD").unwrap_or(defaults.password),
            dbname: env::var("BITMAGNET_POSTGRES_NAME").unwrap_or(defaults.dbname),
            sslmode: env::var("BITMAGNET_POSTGRES_SSLMODE").unwrap_or(defaults.sslmode),
            max_connections,
        })
    }

    /// Builds a production-compatible config from the process environment.
    ///
    /// The Rust-native `BITMAGNET_POSTGRES_*` names take precedence over the
    /// Go application's deployed `POSTGRES_*` names. In particular, the
    /// existing Rust spellings are retained for the two names that are not a
    /// direct prefix substitution:
    ///
    /// * `BITMAGNET_POSTGRES_MAX_CONNECTIONS` over `POSTGRES_POOL_MAX_CONNS`
    /// * `BITMAGNET_POSTGRES_SSLMODE` over `POSTGRES_SSL_MODE`
    ///
    /// Missing and explicit-zero pool caps follow pgx's effective default of
    /// `max(4, runtime.NumCPU())`, approximated with Rust's available
    /// parallelism. If the operating system cannot report parallelism, the
    /// pgx floor of four is used. Non-Unicode values fail closed and errors
    /// identify only the affected key.
    ///
    /// [`Self::from_env`] intentionally remains the Rust-only loader for
    /// existing binaries and tests.
    pub fn from_compatible_env() -> Result<Self, DbError> {
        Self::from_compatible_fallible_lookup(compatibility_env_value, available_parallelism())
    }

    /// Builds a production-compatible config using an injected environment
    /// lookup.
    ///
    /// This is the injected-lookup form of [`Self::from_compatible_env`]. An
    /// empty value is still a supplied value and therefore wins precedence.
    /// Go settings that SQLx cannot faithfully implement are rejected when
    /// non-empty instead of being silently ignored. When no positive pool cap
    /// is supplied, the effective default remains intentionally dependent on
    /// the host's available parallelism.
    pub fn from_compatible_lookup<F>(mut lookup: F) -> Result<Self, DbError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        Self::from_compatible_fallible_lookup(|key| Ok(lookup(key)), available_parallelism())
    }

    fn from_compatible_fallible_lookup<F>(
        mut lookup: F,
        available_parallelism: Option<usize>,
    ) -> Result<Self, DbError>
    where
        F: FnMut(&str) -> Result<Option<String>, DbError>,
    {
        const UNSUPPORTED_KEYS: [&str; 8] = [
            "BITMAGNET_POSTGRES_CONNECTION_TIMEOUT",
            "BITMAGNET_POSTGRES_SSL_CERT_PATH",
            "BITMAGNET_POSTGRES_SSL_KEY_PATH",
            "BITMAGNET_POSTGRES_SSL_ROOT_CERT_PATH",
            "POSTGRES_CONNECTION_TIMEOUT",
            "POSTGRES_SSL_CERT_PATH",
            "POSTGRES_SSL_KEY_PATH",
            "POSTGRES_SSL_ROOT_CERT_PATH",
        ];

        for key in UNSUPPORTED_KEYS {
            if lookup(key)?.is_some_and(|value| !value.is_empty()) {
                return Err(DbError::Config(format!(
                    "unsupported non-empty compatibility setting: {key}"
                )));
            }
        }

        let defaults = Self::default();
        let dsn = preferred_value(&mut lookup, "BITMAGNET_POSTGRES_DSN", "POSTGRES_DSN")
            .map(|value| value.unwrap_or(defaults.dsn))?;
        let host = preferred_value(&mut lookup, "BITMAGNET_POSTGRES_HOST", "POSTGRES_HOST")
            .map(|value| value.unwrap_or(defaults.host))?;
        let port = preferred_parsed(&mut lookup, "BITMAGNET_POSTGRES_PORT", "POSTGRES_PORT")?
            .unwrap_or(defaults.port);
        let user = preferred_value(&mut lookup, "BITMAGNET_POSTGRES_USER", "POSTGRES_USER")
            .map(|value| value.unwrap_or(defaults.user))?;
        let password = preferred_value(
            &mut lookup,
            "BITMAGNET_POSTGRES_PASSWORD",
            "POSTGRES_PASSWORD",
        )
        .map(|value| value.unwrap_or(defaults.password))?;
        let dbname = preferred_value(&mut lookup, "BITMAGNET_POSTGRES_NAME", "POSTGRES_NAME")
            .map(|value| value.unwrap_or(defaults.dbname))?;
        let sslmode = preferred_value(
            &mut lookup,
            "BITMAGNET_POSTGRES_SSLMODE",
            "POSTGRES_SSL_MODE",
        )
        .map(|value| value.unwrap_or(defaults.sslmode))?;
        let configured_max_connections = preferred_parsed(
            &mut lookup,
            "BITMAGNET_POSTGRES_MAX_CONNECTIONS",
            "POSTGRES_POOL_MAX_CONNS",
        )?;
        let max_connections =
            effective_go_max_connections(configured_max_connections, available_parallelism);

        Ok(Self {
            dsn,
            host,
            port,
            user,
            password,
            dbname,
            sslmode,
            max_connections,
        })
    }

    /// Derives SQLx [`PgConnectOptions`] from this config.
    pub(crate) fn connect_options(&self) -> Result<PgConnectOptions, DbError> {
        if !self.dsn.is_empty() {
            validate_dsn_query_parameters(&self.dsn)?;
            return PgConnectOptions::from_str(&self.dsn)
                .map_err(|_| DbError::Config("invalid PostgreSQL DSN".to_owned()));
        }

        let mut options = PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(&self.user)
            .database(&self.dbname);
        if !self.password.is_empty() {
            options = options.password(&self.password);
        }
        if !self.sslmode.is_empty() {
            let mode = PgSslMode::from_str(&self.sslmode)
                .map_err(|_| DbError::Config(format!("invalid sslmode: {}", self.sslmode)))?;
            options = options.ssl_mode(mode);
        }
        Ok(options)
    }

    /// A password-free description of the connection target, for logs.
    pub(crate) fn log_target(&self) -> String {
        if self.dsn.is_empty() {
            format!("{}@{}:{}/{}", self.user, self.host, self.port, self.dbname)
        } else {
            "<dsn>".to_owned()
        }
    }
}

fn validate_dsn_query_parameters(dsn: &str) -> Result<(), DbError> {
    let url =
        Url::parse(dsn).map_err(|_| DbError::Config("invalid PostgreSQL DSN URL".to_owned()))?;
    let all_supported = url.query_pairs().all(|(key, _)| {
        matches!(
            key.as_ref(),
            "sslmode"
                | "ssl-mode"
                | "sslrootcert"
                | "ssl-root-cert"
                | "ssl-ca"
                | "sslcert"
                | "ssl-cert"
                | "sslkey"
                | "ssl-key"
                | "statement-cache-capacity"
                | "host"
                | "hostaddr"
                | "port"
                | "dbname"
                | "user"
                | "password"
                | "application_name"
                | "options"
        ) || key
            .strip_prefix("options[")
            .is_some_and(|key| key.strip_suffix(']').is_some())
    });
    if !all_supported {
        return Err(DbError::Config(
            "PostgreSQL DSN contains an unsupported query parameter".to_owned(),
        ));
    }
    Ok(())
}

fn compatibility_env_value(key: &str) -> Result<Option<String>, DbError> {
    match env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(DbError::Config(format!(
            "non-Unicode compatibility setting: {key}"
        ))),
    }
}

fn available_parallelism() -> Option<usize> {
    std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZeroUsize::get)
}

fn effective_go_max_connections(
    configured: Option<u32>,
    available_parallelism: Option<usize>,
) -> u32 {
    if let Some(configured) = configured.filter(|configured| *configured > 0) {
        return configured;
    }

    let inferred = available_parallelism.unwrap_or(4).max(4);
    u32::try_from(inferred).unwrap_or(u32::MAX)
}

fn preferred_value<F>(
    lookup: &mut F,
    preferred: &str,
    fallback: &str,
) -> Result<Option<String>, DbError>
where
    F: FnMut(&str) -> Result<Option<String>, DbError>,
{
    match lookup(preferred)? {
        Some(value) => Ok(Some(value)),
        None => lookup(fallback),
    }
}

fn preferred_parsed<T, F>(
    lookup: &mut F,
    preferred: &str,
    fallback: &str,
) -> Result<Option<T>, DbError>
where
    T: FromStr,
    F: FnMut(&str) -> Result<Option<String>, DbError>,
{
    let selected = match lookup(preferred)? {
        Some(value) => Some((preferred, value)),
        None => lookup(fallback)?.map(|value| (fallback, value)),
    };
    selected
        .map(|(key, value)| {
            value
                .parse()
                .map_err(|_| DbError::Config(format!("invalid compatibility setting: {key}")))
        })
        .transpose()
}

/// Parses an environment variable into `T`, returning `default` when unset.
fn parse_env<T>(key: &str, default: T) -> Result<T, DbError>
where
    T: FromStr,
{
    match env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|_| DbError::Config(format!("invalid {key}: {value:?}"))),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);

    struct LogCaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogCaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(bytes)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for LogCapture {
        type Writer = LogCaptureWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            LogCaptureWriter(self.0.clone())
        }
    }

    #[test]
    fn defaults_match_go() {
        let cfg = DbConfig::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.user, "postgres");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.dbname, "bitmagnet");
        assert!(cfg.dsn.is_empty());
    }

    #[test]
    fn connect_options_from_components() {
        let cfg = DbConfig {
            password: "secret".to_owned(),
            ..DbConfig::default()
        };
        assert!(cfg.connect_options().is_ok());
        // The password must never leak into the log target.
        assert_eq!(cfg.log_target(), "postgres@localhost:5432/bitmagnet");
    }

    #[test]
    fn connect_options_from_dsn_url() {
        let cfg = DbConfig {
            dsn: "postgres://u:p@db.example:6432/mydb?sslmode=require&application_name=bitmagnet&options%5Bsearch_path%5D=public".to_owned(),
            ..DbConfig::default()
        };
        assert!(cfg.connect_options().is_ok());
        assert_eq!(cfg.log_target(), "<dsn>");
    }

    #[test]
    fn dsn_rejects_unknown_query_keys_without_parsing_or_exposing_values() {
        let secret = "sentinel-secret-that-must-not-be-logged";
        for dsn in [
            format!("postgres://u:p@db.example/mydb?typo={secret}"),
            format!("postgres://u:p@db.example/mydb?ty%70o={secret}"),
        ] {
            let cfg = DbConfig {
                dsn,
                ..DbConfig::default()
            };
            let rendered = cfg.connect_options().unwrap_err().to_string();
            assert_eq!(
                rendered,
                "configuration error: PostgreSQL DSN contains an unsupported query parameter"
            );
            assert!(!rendered.contains(secret));
            assert!(!rendered.contains("typo"));
        }
    }

    #[test]
    fn unknown_dsn_value_never_reaches_tracing() {
        let secret = "sentinel-secret-that-must-not-reach-tracing";
        let cfg = DbConfig {
            dsn: format!("postgres://u:p@db.example/mydb?typo={secret}"),
            ..DbConfig::default()
        };
        let capture = LogCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(capture.clone())
            .finish();

        let rendered = tracing::subscriber::with_default(subscriber, || {
            cfg.connect_options().unwrap_err().to_string()
        });
        let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();

        assert!(!rendered.contains(secret));
        assert!(!logs.contains(secret));
        assert!(
            logs.is_empty(),
            "unknown DSN must fail before SQLx logs: {logs}"
        );
    }

    #[test]
    fn dsn_parse_errors_are_redacted() {
        let secret = "sentinel-secret-that-must-not-be-exposed";
        let cfg = DbConfig {
            dsn: format!("postgres://u:p@db.example/mydb?sslmode={secret}"),
            ..DbConfig::default()
        };
        let rendered = cfg.connect_options().unwrap_err().to_string();
        assert_eq!(rendered, "configuration error: invalid PostgreSQL DSN");
        assert!(!rendered.contains(secret));
    }

    #[test]
    fn debug_redacts_dsn_and_password() {
        let cfg = DbConfig {
            dsn: "postgres://dsn-user:dsn-secret@example.invalid/database".to_owned(),
            password: "component-secret".to_owned(),
            ..DbConfig::default()
        };

        let debug = format!("{cfg:?}");
        assert!(!debug.contains("dsn-secret"));
        assert!(!debug.contains("component-secret"));
        assert!(!debug.contains("postgres://"));
        assert_eq!(debug.matches("<redacted>").count(), 2);
    }

    #[test]
    fn compatible_lookup_maps_go_postgres_environment() {
        let values = HashMap::from([
            ("POSTGRES_DSN", "postgres://go-dsn"),
            ("POSTGRES_HOST", "go-host"),
            ("POSTGRES_PORT", "6543"),
            ("POSTGRES_USER", "go-user"),
            ("POSTGRES_PASSWORD", "go-password"),
            ("POSTGRES_NAME", "go-name"),
            ("POSTGRES_POOL_MAX_CONNS", "27"),
            ("POSTGRES_SSL_MODE", "require"),
        ]);

        let cfg = DbConfig::from_compatible_lookup(|key| {
            values.get(key).map(|value| (*value).to_owned())
        })
        .unwrap();

        assert_eq!(cfg.dsn, "postgres://go-dsn");
        assert_eq!(cfg.host, "go-host");
        assert_eq!(cfg.port, 6543);
        assert_eq!(cfg.user, "go-user");
        assert_eq!(cfg.password, "go-password");
        assert_eq!(cfg.dbname, "go-name");
        assert_eq!(cfg.max_connections, 27);
        assert_eq!(cfg.sslmode, "require");
    }

    #[test]
    fn compatible_lookup_prefers_existing_rust_names_even_when_empty() {
        let values = HashMap::from([
            ("BITMAGNET_POSTGRES_DSN", ""),
            ("POSTGRES_DSN", "postgres://legacy-dsn"),
            ("BITMAGNET_POSTGRES_HOST", "rust-host"),
            ("POSTGRES_HOST", "go-host"),
            ("BITMAGNET_POSTGRES_PORT", "7654"),
            ("POSTGRES_PORT", "6543"),
            ("BITMAGNET_POSTGRES_USER", "rust-user"),
            ("POSTGRES_USER", "go-user"),
            ("BITMAGNET_POSTGRES_PASSWORD", "rust-password"),
            ("POSTGRES_PASSWORD", "go-password"),
            ("BITMAGNET_POSTGRES_NAME", "rust-name"),
            ("POSTGRES_NAME", "go-name"),
            ("BITMAGNET_POSTGRES_MAX_CONNECTIONS", "31"),
            ("POSTGRES_POOL_MAX_CONNS", "27"),
            ("BITMAGNET_POSTGRES_SSLMODE", "verify-full"),
            ("POSTGRES_SSL_MODE", "require"),
        ]);

        let cfg = DbConfig::from_compatible_lookup(|key| {
            values.get(key).map(|value| (*value).to_owned())
        })
        .unwrap();

        assert!(cfg.dsn.is_empty());
        assert_eq!(cfg.host, "rust-host");
        assert_eq!(cfg.port, 7654);
        assert_eq!(cfg.user, "rust-user");
        assert_eq!(cfg.password, "rust-password");
        assert_eq!(cfg.dbname, "rust-name");
        assert_eq!(cfg.max_connections, 31);
        assert_eq!(cfg.sslmode, "verify-full");
    }

    #[test]
    fn compatible_lookup_uses_pgxs_effective_pool_default_for_absent_and_zero_caps() {
        let parallelism = available_parallelism();
        let expected = effective_go_max_connections(None, parallelism);
        assert!(expected >= 4);

        let absent = DbConfig::from_compatible_lookup(|_| None).unwrap();
        assert_eq!(absent.max_connections, expected);

        let go_zero = DbConfig::from_compatible_lookup(|key| {
            (key == "POSTGRES_POOL_MAX_CONNS").then(|| "0".to_owned())
        })
        .unwrap();
        assert_eq!(go_zero.max_connections, expected);

        let prefixed_zero = DbConfig::from_compatible_lookup(|key| match key {
            "BITMAGNET_POSTGRES_MAX_CONNECTIONS" => Some("0".to_owned()),
            "POSTGRES_POOL_MAX_CONNS" => Some("27".to_owned()),
            _ => None,
        })
        .unwrap();
        assert_eq!(prefixed_zero.max_connections, expected);
    }

    #[test]
    fn go_pool_default_inference_has_a_four_connection_fallback_and_floor() {
        assert_eq!(effective_go_max_connections(None, None), 4);
        assert_eq!(effective_go_max_connections(Some(0), None), 4);
        assert_eq!(effective_go_max_connections(None, Some(1)), 4);
        assert_eq!(effective_go_max_connections(Some(0), Some(3)), 4);
        assert_eq!(effective_go_max_connections(None, Some(12)), 12);
        assert_eq!(effective_go_max_connections(Some(27), Some(12)), 27);
    }

    #[test]
    fn compatible_lookup_rejects_unsupported_nonempty_settings_in_both_namespaces() {
        for key in [
            "BITMAGNET_POSTGRES_CONNECTION_TIMEOUT",
            "BITMAGNET_POSTGRES_SSL_CERT_PATH",
            "BITMAGNET_POSTGRES_SSL_KEY_PATH",
            "BITMAGNET_POSTGRES_SSL_ROOT_CERT_PATH",
            "POSTGRES_CONNECTION_TIMEOUT",
            "POSTGRES_SSL_CERT_PATH",
            "POSTGRES_SSL_KEY_PATH",
            "POSTGRES_SSL_ROOT_CERT_PATH",
        ] {
            let secret_value = format!("secret-value-for-{key}");
            let error = DbConfig::from_compatible_lookup(|candidate| {
                (candidate == key).then(|| secret_value.clone())
            })
            .unwrap_err();
            let rendered = error.to_string();

            assert!(rendered.contains(key));
            assert!(!rendered.contains(&secret_value));
        }

        let cfg = DbConfig::from_compatible_lookup(|key| match key {
            "BITMAGNET_POSTGRES_CONNECTION_TIMEOUT"
            | "BITMAGNET_POSTGRES_SSL_CERT_PATH"
            | "BITMAGNET_POSTGRES_SSL_KEY_PATH"
            | "BITMAGNET_POSTGRES_SSL_ROOT_CERT_PATH"
            | "POSTGRES_CONNECTION_TIMEOUT"
            | "POSTGRES_SSL_CERT_PATH"
            | "POSTGRES_SSL_KEY_PATH"
            | "POSTGRES_SSL_ROOT_CERT_PATH" => Some(String::new()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.host, DbConfig::default().host);
    }

    #[cfg(unix)]
    #[test]
    fn compatible_env_rejects_non_unicode_without_value_leakage() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let _env_guard = ENV_LOCK.lock().unwrap();
        let key = "BITMAGNET_POSTGRES_CONNECTION_TIMEOUT";
        let original = env::var_os(key);
        env::set_var(
            key,
            OsString::from_vec(b"super-secret-invalid-\xff".to_vec()),
        );

        let result = DbConfig::from_compatible_env();
        match original {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }

        let rendered = result.unwrap_err().to_string();
        assert!(rendered.contains(key));
        assert!(rendered.contains("non-Unicode"));
        assert!(!rendered.contains("super-secret"));
    }

    #[test]
    fn compatible_lookup_reports_the_preferred_numeric_name_without_the_value() {
        let secret_value = "not-a-number-that-must-not-be-echoed";
        let error = DbConfig::from_compatible_lookup(|key| {
            (key == "POSTGRES_POOL_MAX_CONNS").then(|| secret_value.to_owned())
        })
        .unwrap_err();
        let rendered = error.to_string();

        assert!(rendered.contains("POSTGRES_POOL_MAX_CONNS"));
        assert!(!rendered.contains(secret_value));
    }

    #[test]
    fn invalid_sslmode_is_rejected() {
        let cfg = DbConfig {
            sslmode: "definitely-not-a-mode".to_owned(),
            ..DbConfig::default()
        };
        assert!(matches!(cfg.connect_options(), Err(DbError::Config(_))));
    }

    #[test]
    fn from_env_reads_dsn_and_defaults() {
        let _env_guard = ENV_LOCK.lock().unwrap();
        let original_dsn = env::var_os("BITMAGNET_POSTGRES_DSN");
        let original_port = env::var_os("BITMAGNET_POSTGRES_PORT");
        env::set_var("BITMAGNET_POSTGRES_DSN", "postgres://localhost/x");
        env::set_var("BITMAGNET_POSTGRES_PORT", "6543");
        let cfg = DbConfig::from_env().unwrap();
        assert_eq!(cfg.dsn, "postgres://localhost/x");
        assert_eq!(cfg.port, 6543);
        assert_eq!(cfg.dbname, "bitmagnet"); // untouched default

        env::set_var("BITMAGNET_POSTGRES_PORT", "not-a-number");
        assert!(matches!(DbConfig::from_env(), Err(DbError::Config(_))));

        match original_dsn {
            Some(value) => env::set_var("BITMAGNET_POSTGRES_DSN", value),
            None => env::remove_var("BITMAGNET_POSTGRES_DSN"),
        }
        match original_port {
            Some(value) => env::set_var("BITMAGNET_POSTGRES_PORT", value),
            None => env::remove_var("BITMAGNET_POSTGRES_PORT"),
        }
    }
}
