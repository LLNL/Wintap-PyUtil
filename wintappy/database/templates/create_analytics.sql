-- create analytics results table if it doesn't already exist
CREATE TABLE IF NOT EXISTS analytics(
    -- unique id for the analytic that matched, e.g. CAR-2013-02-003
    analytic_id VARCHAR,
    technique_id VARCHAR,
    technique_stix_type VARCHAR,
    tactic_id VARCHAR,
    tactic_stix_type VARCHAR
)
