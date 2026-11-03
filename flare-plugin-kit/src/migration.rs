//! Reviewed, versioned schema steps of one plugin, applied by the schema owner.
//!
//! The runtime role never runs DDL: it only checks that the schema is at the
//! version its binary was built for ([`Migrations::check`]). The owner applies
//! the missing steps in one transaction, serialized across processes by an
//! advisory lock, and grants the runtime role its least privileges.
use crate::{KitError, database::valid_identifier};
use serde_json::{Value, json};
use sqlx::PgPool;

/// DDL of the kit's own tables (commands, outbox, entity states); include it in
/// the plugin's first step.
pub const KIT_SQL: &str = include_str!("../initial/kit.sql");
/// The runtime role's privileges on the kit tables (`:"runtime_role"`).
pub const KIT_GRANTS_SQL: &str = include_str!("../initial/kit-grants.sql");

/// One reviewed step. Fragments run in order in the step's transaction.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static [&'static str],
}

#[derive(Debug, Clone)]
pub struct Migrations {
    schema: String,
    steps: &'static [Migration],
    grants: &'static [&'static str],
}
impl Migrations {
    /// `steps` have strictly increasing versions from 1. `grants` are the
    /// runtime role's privileges, with the role written `:"runtime_role"`.
    pub fn new(
        schema: impl Into<String>,
        steps: &'static [Migration],
        grants: &'static [&'static str],
    ) -> Result<Self, KitError> {
        let schema = schema.into();
        if !valid_identifier(&schema)
            || steps.is_empty()
            || steps
                .iter()
                .enumerate()
                .any(|(i, step)| step.version != i as i32 + 1 || step.name.is_empty())
        {
            return Err(KitError::Invalid);
        }
        Ok(Self {
            schema,
            steps,
            grants,
        })
    }
    pub fn schema(&self) -> &str {
        &self.schema
    }
    /// The version this binary was built for.
    pub fn current(&self) -> i32 {
        self.steps.len() as i32
    }

    /// Runtime startup check; never changes anything.
    pub async fn check(&self, pool: &PgPool) -> Result<(), KitError> {
        let version: Option<i32> = sqlx::query_scalar(&format!(
            "SELECT schema_version FROM {}.plugin_schema_meta WHERE singleton",
            self.schema
        ))
        .fetch_optional(pool)
        .await
        .map_err(|_| KitError::IncompatibleSchema)?;
        if version != Some(self.current()) {
            return Err(KitError::IncompatibleSchema);
        }
        Ok(())
    }

    /// Schema owner operation: create the schema when absent and apply the
    /// missing steps. Returns `{schema, fromVersion, toVersion, applied}`. A
    /// schema newer than this binary is refused, never downgraded.
    pub async fn apply(&self, owner: &PgPool) -> Result<Value, KitError> {
        let mut tx = owner.begin().await?;
        sqlx::query("SET LOCAL lock_timeout='5s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!("flare-plugin-kit:migrate:{}", self.schema))
            .execute(&mut *tx)
            .await?;
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             SET LOCAL search_path TO {schema};
             CREATE TABLE IF NOT EXISTS plugin_schema_meta (
               singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
               schema_version integer NOT NULL CHECK (schema_version >= 0),
               applied jsonb NOT NULL DEFAULT '[]'::jsonb,
               updated_at_ms bigint NOT NULL
             );
             REVOKE ALL ON plugin_schema_meta FROM PUBLIC;
             INSERT INTO plugin_schema_meta(singleton, schema_version, updated_at_ms)
               VALUES (true, 0, (extract(epoch FROM clock_timestamp())*1000)::bigint)
               ON CONFLICT DO NOTHING;",
            schema = self.schema
        ))
        .execute(&mut *tx)
        .await?;
        let from: i32 = sqlx::query_scalar(
            "SELECT schema_version FROM plugin_schema_meta WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if from > self.current() {
            return Err(KitError::IncompatibleSchema);
        }
        let mut applied = Vec::new();
        for step in self.steps.iter().filter(|step| step.version > from) {
            for fragment in step.sql {
                sqlx::raw_sql(fragment).execute(&mut *tx).await?;
            }
            sqlx::query(
                "UPDATE plugin_schema_meta SET schema_version=$1, applied=applied||jsonb_build_array($2::text), updated_at_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE singleton",
            )
            .bind(step.version)
            .bind(step.name)
            .execute(&mut *tx)
            .await?;
            applied.push(step.name);
        }
        tx.commit().await?;
        Ok(json!({
            "schema": self.schema,
            "fromVersion": from,
            "toVersion": self.current(),
            "applied": applied,
        }))
    }

    /// Schema owner operation: grant `role` (an existing login of this plugin
    /// alone) usage of the schema and the declared privileges, and read access
    /// to the version marker. Never ownership or DDL.
    pub async fn grant(&self, owner: &PgPool, role: &str) -> Result<(), KitError> {
        if !valid_identifier(role) {
            return Err(KitError::Invalid);
        }
        let quoted = format!("\"{role}\"");
        let mut tx = owner.begin().await?;
        sqlx::raw_sql(&format!(
            "SET LOCAL search_path TO {schema};
             GRANT USAGE ON SCHEMA {schema} TO {quoted};
             GRANT SELECT ON plugin_schema_meta TO {quoted};",
            schema = self.schema
        ))
        .execute(&mut *tx)
        .await?;
        for grants in self.grants {
            sqlx::raw_sql(&grants.replace(":\"runtime_role\"", &quoted))
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    static STEPS: &[Migration] = &[
        Migration {
            version: 1,
            name: "one",
            sql: &[KIT_SQL],
        },
        Migration {
            version: 2,
            name: "two",
            sql: &["SELECT 1"],
        },
    ];
    #[test]
    fn steps_are_dense_and_schema_is_an_identifier() {
        assert_eq!(Migrations::new("p", STEPS, &[]).unwrap().current(), 2);
        assert!(Migrations::new("P;", STEPS, &[]).is_err());
        assert!(Migrations::new("p", &STEPS[1..], &[]).is_err());
        assert!(Migrations::new("p", &[], &[]).is_err());
    }
}
