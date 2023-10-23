-- create analytics results table if it doesn't already exist
CREATE TABLE IF NOT EXISTS mitre_labels(
    -- value of pid_hash
    entity VARCHAR,
    -- unique id for the analytic that matched, e.g. CAR-2013-02-003
    analytic_id VARCHAR,
    -- timestamp for when this analytic matched
    time TIMESTAMP,
    -- pid_hash
    entity_type VARCHAR,
)
