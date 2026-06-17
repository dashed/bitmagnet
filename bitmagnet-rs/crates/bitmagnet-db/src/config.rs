//! Connection configuration, mirroring Go's
//! `internal/database/postgres.Config`.

use std::env;
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgSslMode};

use crate::error::DbError;

/// PostgreSQL connection settings.
///
/// When [`Self::dsn`] is non-empty it is used verbatim (parsed by SQLx as a
/// `postgres://` URL); otherwise a connection is built from the individual
/// fields. Defaults mirror Go's `NewDefaultConfig` (`localhost`, user
/// `postgres`, port `5432`, database `bitmagnet`).
#[derive(Debug, Clone)]
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

    /// Derives SQLx [`PgConnectOptions`] from this config.
    pub(crate) fn connect_options(&self) -> Result<PgConnectOptions, DbError> {
        if !self.dsn.is_empty() {
            return PgConnectOptions::from_str(&self.dsn).map_err(DbError::from);
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
            dsn: "postgres://u:p@db.example:6432/mydb".to_owned(),
            ..DbConfig::default()
        };
        assert!(cfg.connect_options().is_ok());
        assert_eq!(cfg.log_target(), "<dsn>");
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
        // Single test owning all env access in this crate, to avoid races.
        env::set_var("BITMAGNET_POSTGRES_DSN", "postgres://localhost/x");
        env::set_var("BITMAGNET_POSTGRES_PORT", "6543");
        let cfg = DbConfig::from_env().unwrap();
        assert_eq!(cfg.dsn, "postgres://localhost/x");
        assert_eq!(cfg.port, 6543);
        assert_eq!(cfg.dbname, "bitmagnet"); // untouched default

        env::set_var("BITMAGNET_POSTGRES_PORT", "not-a-number");
        assert!(matches!(DbConfig::from_env(), Err(DbError::Config(_))));

        env::remove_var("BITMAGNET_POSTGRES_DSN");
        env::remove_var("BITMAGNET_POSTGRES_PORT");
    }
}
