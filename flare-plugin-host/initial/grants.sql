-- Explicit provisioning by the schema owner; use a role unique to this plugin.
GRANT SELECT,INSERT,UPDATE ON plugin_runtime_tenants TO :"runtime_role";
REVOKE ALL ON plugin_runtime_tenants FROM PUBLIC;
-- Never grant DELETE: expired rows are durable revocation/revision tombstones.
