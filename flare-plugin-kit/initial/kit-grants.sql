-- Least privileges of the plugin's runtime role on the kit tables. The role
-- never owns the schema and cannot run DDL.
REVOKE ALL ON plugin_commands, plugin_outbox, plugin_entity_states FROM PUBLIC;
GRANT SELECT, INSERT, DELETE ON plugin_commands TO :"runtime_role";
GRANT SELECT, INSERT, DELETE ON plugin_outbox TO :"runtime_role";
GRANT UPDATE (delivered_at_ms, lease_owner, lease_until_ms, fence) ON plugin_outbox TO :"runtime_role";
GRANT USAGE, SELECT ON SEQUENCE plugin_outbox_id_seq TO :"runtime_role";
GRANT SELECT, INSERT ON plugin_entity_states TO :"runtime_role";
GRANT UPDATE (grant_id, version, values_json, published_version, attempts, next_attempt_at_ms, updated_at_ms) ON plugin_entity_states TO :"runtime_role";
