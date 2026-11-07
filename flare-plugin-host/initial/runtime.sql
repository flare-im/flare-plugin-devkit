-- Provision explicitly with the plugin deployment's own database owner.
-- Runtime credentials need SELECT/INSERT/UPDATE only; never DELETE or DDL.
CREATE TABLE plugin_runtime_tenants (
    deployment_id text NOT NULL CHECK (length(deployment_id) BETWEEN 1 AND 128),
    app_id text NOT NULL CHECK (app_id ~ '^[a-zA-Z0-9_-]{1,32}$'),
    organization_id text NOT NULL CHECK (organization_id ~ '^[a-zA-Z0-9_-]{1,128}$'),
    grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
    revision bigint NOT NULL CHECK (revision > 0),
    manifest_digest text NOT NULL,
    configuration_json bytea NOT NULL CHECK (octet_length(configuration_json) <= 16384),
    configuration_digest text NOT NULL,
    enabled boolean NOT NULL,
    valid_until_ms bigint NOT NULL,
    approved_operations text[] NOT NULL DEFAULT '{}' CHECK (cardinality(approved_operations) <= 512),
    permissions text[] NOT NULL DEFAULT '{}' CHECK (cardinality(permissions) <= 512),
    data_scopes text[] NOT NULL DEFAULT '{}' CHECK (cardinality(data_scopes) <= 512),
    PRIMARY KEY (deployment_id, app_id, grant_id)
);
-- Keep expired rows: they are the durable revision tombstones.
