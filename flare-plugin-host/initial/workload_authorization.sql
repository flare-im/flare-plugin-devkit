-- Explicit schema-owner upgrade for existing plugin-runtime stores.
-- No business grants are inferred/backfilled: a fresh signed ApplyTenant
-- projection from the platform must authorize workload operations.
ALTER TABLE plugin_runtime_tenants
  ADD COLUMN approved_operations text[] NOT NULL DEFAULT '{}' CHECK (cardinality(approved_operations) <= 512),
  ADD COLUMN permissions text[] NOT NULL DEFAULT '{}' CHECK (cardinality(permissions) <= 512),
  ADD COLUMN data_scopes text[] NOT NULL DEFAULT '{}' CHECK (cardinality(data_scopes) <= 512);
