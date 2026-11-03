//! The shared server skeleton of Flare business plugins (poll, solitaire,
//! task, schedule, announcement): the common part of the plugin DDD + CQRS
//! standard (`flare_plugin_host::docs::plugin_ddd_cqrs`). A plugin keeps its
//! domain, use cases and repositories; the kit provides the rest, built on
//! `flare-plugin-host`:
//!
//! - [`boot`]: runtime host from the operator environment, the release check
//!   against the signed manifest, bounded serving and background workers;
//! - [`database`]: the pool bound to the plugin's schema with statement and
//!   lock timeouts, and the database clock;
//! - [`migration`]: reviewed schema steps applied by the schema owner, the
//!   runtime role's grants and the startup version check;
//! - [`commands`]: idempotent commands by `commandId` (or Core's request id);
//! - [`outbox`] and [`events`]: audit events to NATS JetStream, or off;
//! - [`caller`] and [`membership`]: verified user and staff callers, the
//!   ticket-bound conversation confirmed with `im.conversation.read`, group
//!   roles from `social.group.member.read`;
//! - [`entity_state`]: shared state in call results, staged in the business
//!   transaction and published with the runtime credential;
//! - [`error`]: definite rejection codes and the `GenericResponse`.
//!
//! Every plugin owns one PostgreSQL schema holding the host's tenant state
//! table, the kit's tables ([`migration::KIT_SQL`]) and its own.
pub mod boot;
pub mod caller;
pub mod commands;
pub mod database;
pub mod entity_state;
pub mod error;
pub mod events;
pub mod membership;
pub mod migration;
pub mod outbox;

pub use caller::{StaffCaller, UserCaller};
pub use error::{KitError, Rejection};
pub use flare_plugin_host as host;
