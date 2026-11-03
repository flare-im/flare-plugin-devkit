//! The plugin's PostgreSQL pool. Every connection is bound to the plugin's own
//! schema and carries statement and lock timeouts, so no query waits
//! unbounded on a lock held by another replica.
use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct PoolSettings<'a> {
    /// The plugin's schema: `[a-z_][a-z0-9_]{0,62}`.
    pub schema: &'a str,
    pub max_connections: u32,
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
}
impl<'a> PoolSettings<'a> {
    pub fn new(schema: &'a str) -> Self {
        Self {
            schema,
            max_connections: 16,
            statement_timeout: Duration::from_secs(5),
            lock_timeout: Duration::from_secs(2),
        }
    }
}

/// A lowercase SQL identifier that needs no quoting.
pub fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Connection options for `url` bound to the settings' schema and timeouts.
pub fn connect_options(
    url: &str,
    settings: PoolSettings<'_>,
) -> Result<PgConnectOptions, sqlx::Error> {
    bind_options(url.parse()?, settings)
}

/// `options` bound to the settings' schema and timeouts.
pub fn bind_options(
    options: PgConnectOptions,
    settings: PoolSettings<'_>,
) -> Result<PgConnectOptions, sqlx::Error> {
    if !valid_identifier(settings.schema) {
        return Err(sqlx::Error::Configuration(
            "invalid plugin schema name".into(),
        ));
    }
    Ok(options.options([
        ("search_path", settings.schema.to_owned()),
        (
            "statement_timeout",
            settings.statement_timeout.as_millis().to_string(),
        ),
        (
            "lock_timeout",
            settings.lock_timeout.as_millis().to_string(),
        ),
    ]))
}

pub async fn connect(url: &str, settings: PoolSettings<'_>) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .acquire_timeout(Duration::from_secs(2))
        .connect_with(connect_options(url, settings)?)
        .await
}

/// The database clock in epoch milliseconds: one clock for every replica.
pub async fn db_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
        .fetch_one(&mut **tx)
        .await
}

/// [`db_now`] outside a transaction.
pub async fn pool_now(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
        .fetch_one(pool)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_names_are_plain_identifiers() {
        assert!(valid_identifier("flare_poll"));
        assert!(valid_identifier("_x1"));
        for bad in ["", "Poll", "1poll", "poll;drop", "a-b", &"a".repeat(64)] {
            assert!(!valid_identifier(bad), "{bad}");
        }
        assert!(connect_options("postgres://u@h/db", PoolSettings::new("bad name")).is_err());
        assert!(connect_options("postgres://u@h/db", PoolSettings::new("flare_poll")).is_ok());
    }
}
