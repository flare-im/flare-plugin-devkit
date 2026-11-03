-- Tables every plugin built on flare-plugin-kit owns in its own schema. Applied
-- by the schema owner through the plugin's migration, never by the runtime.

-- Idempotent commands: the first result of (tenant, actor, command id) is
-- replayed for the same request and refuses a different one.
CREATE TABLE plugin_commands (
 app_id text NOT NULL CHECK (app_id ~ '^[a-zA-Z0-9_-]{1,32}$'),
 actor_id text NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 160),
 command_id text NOT NULL CHECK (length(command_id) BETWEEN 1 AND 200),
 operation text NOT NULL CHECK (length(operation) BETWEEN 1 AND 128),
 request_digest text NOT NULL,
 entity text CHECK (entity IS NULL OR length(entity) BETWEEN 1 AND 128),
 result jsonb NOT NULL CHECK (pg_column_size(result) <= 65536),
 created_at_ms bigint NOT NULL,
 PRIMARY KEY (app_id, actor_id, command_id)
);
CREATE INDEX plugin_commands_created ON plugin_commands(created_at_ms);
CREATE INDEX plugin_commands_entity ON plugin_commands(app_id, entity) WHERE entity IS NOT NULL;

-- Domain events for audit consumers, committed with the business change and
-- delivered at least once by leased workers.
CREATE TABLE plugin_outbox (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 app_id text NOT NULL CHECK (app_id ~ '^[a-zA-Z0-9_-]{1,32}$'),
 event_key text NOT NULL CHECK (length(event_key) BETWEEN 1 AND 256),
 event_type text NOT NULL CHECK (event_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$' AND length(event_type) <= 64),
 payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object' AND pg_column_size(payload) <= 65536),
 created_at_ms bigint NOT NULL,
 delivered_at_ms bigint,
 lease_owner text,
 lease_until_ms bigint NOT NULL DEFAULT 0,
 fence bigint NOT NULL DEFAULT 0,
 UNIQUE (app_id, event_key)
);
CREATE INDEX plugin_outbox_pending ON plugin_outbox(id) WHERE delivered_at_ms IS NULL;

-- The latest shared state of each entity and how far it reached Core. The
-- publisher sends what is newer than `published_version` with the
-- deployment's runtime credential; Core keeps the highest version.
CREATE TABLE plugin_entity_states (
 app_id text NOT NULL CHECK (app_id ~ '^[a-zA-Z0-9_-]{1,32}$'),
 entity text NOT NULL CHECK (entity ~ '^[A-Za-z0-9._:-]{1,128}$'),
 grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
 version bigint NOT NULL CHECK (version > 0),
 values_json jsonb NOT NULL CHECK (jsonb_typeof(values_json) = 'object' AND pg_column_size(values_json) <= 16384),
 published_version bigint NOT NULL DEFAULT 0 CHECK (published_version >= 0),
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 next_attempt_at_ms bigint NOT NULL,
 updated_at_ms bigint NOT NULL,
 PRIMARY KEY (app_id, entity)
);
CREATE INDEX plugin_entity_states_due ON plugin_entity_states(next_attempt_at_ms) WHERE published_version < version;
