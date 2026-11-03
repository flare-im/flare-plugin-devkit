//! Idempotent commands keyed by `(tenant, actor, commandId)`.
//!
//! The first committed result of a command is replayed for the same request
//! and a different request under the same key is refused (`COMMAND_CONFLICT`).
//! A key is the client's `commandId` when it sends one, otherwise Core's
//! request id, which Core keeps across its own retries of one call.
//!
//! Key spaces. The command part is `command:<commandId>`, `request:<requestId>`
//! or, for a key the plugin fixes itself ([`CommandKey::fixed`]),
//! `<scope>:<id>`; none can take another's keys. The actor part keeps members,
//! staff and the platform apart ([`Actor`]): a member is keyed by their user
//! id, staff by `$staff:<id>` and system actors by `$<name>`. User tickets only
//! admit resource ids (`[A-Za-z0-9_:-]`), so no user id, whatever the tenant
//! names its users, starts with `$`.
//!
//! ```ignore
//! let mut tx = pool.begin().await?;
//! if let Command::Replay(result) = commands::begin(&mut tx, &key, OP, &digest).await? {
//!     tx.commit().await?;
//!     return Ok(result);
//! }
//! // ... the business change, in the same transaction ...
//! commands::record(&mut tx, &key, OP, &digest, Some(&entity), &result, now).await?;
//! tx.commit().await?;
//! ```
use crate::KitError;
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};

/// How the plugin's records and command keys name a staff member: `$staff:<id>`
/// (`StaffCaller::actor`). Members are named by their user id, a resource id
/// (`[A-Za-z0-9_:-]`) that never starts with `$`, so a member whose id reads
/// `staff:<x>` can never pass for staff in a `closed_by` or `deleted_by`.
pub fn staff_actor(staff: &str) -> String {
    format!("$staff:{staff}")
}

/// Who a command is keyed under. Each kind has its own key space, so a member
/// whose user id happens to read `platform` or `staff:<id>` can never take the
/// platform's or a staff member's keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Actor<'a> {
    /// A conversation member: their user id, a resource id.
    Member(&'a str),
    /// Tenant staff (`StaffCaller::staff`): keyed `$staff:<id>`.
    Staff(&'a str),
    /// The platform or a plugin job, by a fixed lowercase name: keyed
    /// `$<name>`.
    System(&'a str),
}
impl Actor<'_> {
    fn key(self) -> Result<String, KitError> {
        match self {
            Self::Member(user)
                if flare_plugin_host::protocol::resource::valid_resource_id(user) =>
            {
                Ok(user.into())
            }
            Self::Staff(staff)
                if !staff.is_empty()
                    && staff.len() <= 128
                    && !staff.chars().any(char::is_control) =>
            {
                Ok(staff_actor(staff))
            }
            Self::System(name)
                if !name.is_empty()
                    && name.len() <= 64
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || matches!(b, b'-' | b'.')) =>
            {
                Ok(format!("${name}"))
            }
            _ => Err(KitError::Invalid),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandKey {
    tenant: String,
    actor: String,
    command: String,
}
impl CommandKey {
    /// `explicit` is the client's `commandId` (`[A-Za-z0-9_:-]{1,128}`);
    /// without one the key is `request:<requestId>`. `actor` is stored as
    /// given; it may not start with `$`, which [`CommandKey::of`] keeps for
    /// staff and system actors.
    pub fn new(
        tenant: &str,
        actor: &str,
        explicit: Option<&str>,
        request_id: &str,
    ) -> Result<Self, KitError> {
        if actor.starts_with('$') {
            return Err(KitError::Invalid);
        }
        Self::keyed(tenant, actor.into(), explicit, request_id)
    }
    /// A key of `actor`'s own key space; otherwise as [`CommandKey::new`].
    pub fn of(
        tenant: &str,
        actor: Actor<'_>,
        explicit: Option<&str>,
        request_id: &str,
    ) -> Result<Self, KitError> {
        Self::keyed(tenant, actor.key()?, explicit, request_id)
    }
    /// A key the plugin fixes, whatever command or request id the call
    /// carries: `<scope>:<id>`, one command per actor and `id` (every vote of
    /// one member on one poll, every notice about one entity). `scope` is
    /// lowercase letters and never `command` or `request`, so these keys stay
    /// apart from the client's and Core's.
    pub fn fixed(tenant: &str, actor: Actor<'_>, scope: &str, id: &str) -> Result<Self, KitError> {
        if scope.is_empty()
            || scope.len() > 32
            || !scope.bytes().all(|b| b.is_ascii_lowercase())
            || matches!(scope, "command" | "request")
            || !valid_command_id(id)
        {
            return Err(KitError::Invalid);
        }
        let actor = actor.key()?;
        Self::checked(tenant, actor, format!("{scope}:{id}"))
    }
    fn keyed(
        tenant: &str,
        actor: String,
        explicit: Option<&str>,
        request_id: &str,
    ) -> Result<Self, KitError> {
        let command = match explicit {
            Some(id) if valid_command_id(id) => format!("command:{id}"),
            Some(_) => return Err(KitError::Invalid),
            None if !request_id.is_empty()
                && request_id.len() <= 192
                && !request_id.chars().any(char::is_control) =>
            {
                format!("request:{request_id}")
            }
            None => return Err(KitError::Invalid),
        };
        Self::checked(tenant, actor, command)
    }
    fn checked(tenant: &str, actor: String, command: String) -> Result<Self, KitError> {
        if tenant.is_empty() || actor.is_empty() || actor.len() > 160 {
            return Err(KitError::Invalid);
        }
        Ok(Self {
            tenant: tenant.into(),
            actor,
            command,
        })
    }
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// The actor column: a member's user id, `$staff:<id>` or `$<name>`.
    pub fn actor(&self) -> &str {
        &self.actor
    }
    /// The command column: `command:<id>`, `request:<id>` or `<scope>:<id>`.
    pub fn command(&self) -> &str {
        &self.command
    }
}

pub fn valid_command_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':'))
}

/// `sha256:<hex>` of the canonical JSON of a request.
pub fn digest(value: &impl Serialize) -> Result<String, KitError> {
    let value = serde_json::to_value(value).map_err(|_| KitError::Invalid)?;
    flare_plugin_host::protocol::runtime::configuration_digest(&value)
        .map_err(|_| KitError::Invalid)
}

pub enum Command {
    /// Committed before with this exact request: its original result.
    Replay(Value),
    /// Not committed yet; the key is locked until the transaction ends.
    Fresh,
}

/// Lock the key for this transaction and look the command up.
pub async fn begin(
    tx: &mut Transaction<'_, Postgres>,
    key: &CommandKey,
    operation: &str,
    request_digest: &str,
) -> Result<Command, KitError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!(
            "flare-plugin-kit:command:{}:{}:{}",
            key.tenant, key.actor, key.command
        ))
        .execute(&mut **tx)
        .await?;
    let row = sqlx::query(
        "SELECT operation, request_digest, result FROM plugin_commands WHERE app_id=$1 AND actor_id=$2 AND command_id=$3",
    )
    .bind(&key.tenant)
    .bind(&key.actor)
    .bind(&key.command)
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        None => Ok(Command::Fresh),
        Some(row)
            if row.get::<String, _>("operation") == operation
                && row.get::<String, _>("request_digest") == request_digest =>
        {
            Ok(Command::Replay(row.get("result")))
        }
        Some(_) => Err(KitError::Conflict),
    }
}

/// Look a command up without keeping the lock: a replay answers before any
/// host read or current limit applies.
pub async fn replay(
    pool: &PgPool,
    key: &CommandKey,
    operation: &str,
    request_digest: &str,
) -> Result<Option<Value>, KitError> {
    let mut tx = pool.begin().await?;
    let result = match begin(&mut tx, key, operation, request_digest).await? {
        Command::Replay(value) => Some(value),
        Command::Fresh => None,
    };
    tx.commit().await?;
    Ok(result)
}

/// Record the result in the command's transaction. `entity` lets
/// [`forget_entity`] drop the stored results of a deleted entity.
pub async fn record(
    tx: &mut Transaction<'_, Postgres>,
    key: &CommandKey,
    operation: &str,
    request_digest: &str,
    entity: Option<&str>,
    result: &Value,
    now_ms: i64,
) -> Result<(), KitError> {
    sqlx::query(
        "INSERT INTO plugin_commands(app_id,actor_id,command_id,operation,request_digest,entity,result,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(&key.tenant)
    .bind(&key.actor)
    .bind(&key.command)
    .bind(operation)
    .bind(request_digest)
    .bind(entity)
    .bind(result)
    .bind(now_ms)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Drop the stored results that carry a deleted entity's content.
pub async fn forget_entity(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &str,
    entity: &str,
) -> Result<u64, KitError> {
    Ok(
        sqlx::query("DELETE FROM plugin_commands WHERE app_id=$1 AND entity=$2")
            .bind(tenant)
            .bind(entity)
            .execute(&mut **tx)
            .await?
            .rows_affected(),
    )
}

/// Drop the stored results of `operations` on one entity, keeping its other
/// commands (a poll's vote records when its voter list goes).
pub async fn forget_entity_operations(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &str,
    entity: &str,
    operations: &[&str],
) -> Result<u64, KitError> {
    Ok(sqlx::query(
        "DELETE FROM plugin_commands WHERE app_id=$1 AND entity=$2 AND operation=ANY($3)",
    )
    .bind(tenant)
    .bind(entity)
    .bind(operations)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

/// Retention of one operation shorter than the plugin's default: drop up to
/// `limit` of its commands created before `before_ms`.
pub async fn prune_operation(
    pool: &PgPool,
    operation: &str,
    before_ms: i64,
    limit: i64,
) -> Result<u64, KitError> {
    Ok(sqlx::query(
        "DELETE FROM plugin_commands WHERE ctid IN (SELECT ctid FROM plugin_commands WHERE operation=$1 AND created_at_ms<$2 ORDER BY created_at_ms LIMIT $3)",
    )
    .bind(operation)
    .bind(before_ms)
    .bind(limit.clamp(1, 10_000))
    .execute(pool)
    .await?
    .rows_affected())
}

/// Retention: drop up to `limit` commands created before `before_ms`. A retry
/// arriving later than the retention window is a new command.
pub async fn prune(pool: &PgPool, before_ms: i64, limit: i64) -> Result<u64, KitError> {
    Ok(sqlx::query(
        "DELETE FROM plugin_commands WHERE ctid IN (SELECT ctid FROM plugin_commands WHERE created_at_ms<$1 ORDER BY created_at_ms LIMIT $2)",
    )
    .bind(before_ms)
    .bind(limit.clamp(1, 10_000))
    .execute(pool)
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_prefer_the_command_id_and_fall_back_to_the_request() {
        let explicit = CommandKey::new("201", "alice", Some("c-1"), "r-1").unwrap();
        let fallback = CommandKey::new("201", "alice", None, "r-1").unwrap();
        assert_ne!(explicit, fallback);
        assert_eq!(fallback.command, "request:r-1");
        assert!(CommandKey::new("201", "alice", Some("bad id"), "r-1").is_err());
        assert!(CommandKey::new("201", "alice", None, "").is_err());
        assert!(CommandKey::new("", "alice", None, "r").is_err());
        assert_eq!(
            digest(&serde_json::json!({"b": 1, "a": 2})).unwrap(),
            digest(&serde_json::json!({"a": 2, "b": 1})).unwrap()
        );
    }

    #[test]
    fn members_staff_and_the_platform_never_share_a_key() {
        // A member named after the platform or a staff actor, sending the
        // command ids those actors use.
        let platform =
            CommandKey::fixed("201", Actor::System("platform"), "unbound", "p1").unwrap();
        let squatter =
            CommandKey::of("201", Actor::Member("platform"), Some("unbound:p1"), "r").unwrap();
        assert_ne!(platform, squatter);
        assert_eq!(
            (platform.actor(), platform.command()),
            ("$platform", "unbound:p1")
        );
        assert_eq!(squatter.command(), "command:unbound:p1");
        let staff = CommandKey::of("201", Actor::Staff("bob"), Some("c-1"), "r").unwrap();
        let named = CommandKey::of("201", Actor::Member("staff:bob"), Some("c-1"), "r").unwrap();
        assert_ne!(staff, named);
        assert_eq!(staff.actor(), "$staff:bob");
        // Business records name staff the same way, so a member whose id reads
        // `staff:bob` never passes for that staff member in a `closed_by`.
        let caller = crate::StaffCaller::unverified("201", "bob", "r");
        assert_eq!(caller.actor(), staff.actor());
        assert_ne!(caller.actor(), "staff:bob");
        assert!(!flare_plugin_host::protocol::resource::valid_resource_id(
            &caller.actor()
        ));
        // `$` is never a member's, and the plain constructor keeps it for the
        // typed actors.
        assert!(CommandKey::of("201", Actor::Member("$platform"), None, "r").is_err());
        assert!(CommandKey::new("201", "$platform", None, "r").is_err());
        assert!(CommandKey::of("201", Actor::System("Platform"), None, "r").is_err());
        // A fixed key is one per actor and id, apart from client and request
        // keys.
        let vote = CommandKey::fixed("201", Actor::Member("alice"), "anonymous", "p1").unwrap();
        assert_eq!(vote.command(), "anonymous:p1");
        assert_ne!(
            vote,
            CommandKey::of("201", Actor::Member("alice"), Some("anonymous:p1"), "r").unwrap()
        );
        for (scope, id) in [
            ("command", "p1"),
            ("request", "p1"),
            ("Anon", "p1"),
            ("anonymous", "bad id"),
        ] {
            assert!(CommandKey::fixed("201", Actor::Member("alice"), scope, id).is_err());
        }
    }
}
