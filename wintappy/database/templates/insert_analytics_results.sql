INSERT INTO mitre_labels(
    entity,
    analytic_id,
    time,
    entity_type
)
VALUES (
    '{{entity}}',
    '{{analytic_id}}',
    to_timestamp({{time}}),
    '{{entity_type}}'
)
