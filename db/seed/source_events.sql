-- Sample outbox rows. psql variables: :events (rows to insert), :entities
-- (distinct entity ids to spread them over). Several events per entity is the
-- interesting case: it exercises per-key ordering on a partition.

INSERT INTO source_events (entity_id, event_type, payload, occurred_at)
SELECT
    'person-' || lpad(entity::text, 5, '0'),
    CASE WHEN seq <= :entities THEN 'record.created' ELSE 'record.updated' END,
    jsonb_build_object(
        'source_system',   'erp',
        'document_number', 'DOC' || lpad(entity::text, 8, '0'),
        'birth_date',      (DATE '1990-01-01' + entity)::text,
        'full_name',       'Person ' || entity,
        'email',           'person' || entity || '@example.com',
        'status',          (ARRAY['active', 'inactive', 'pending'])[1 + (seq % 3)],
        'version',         1 + (seq - 1) / :entities
    ),
    now() - make_interval(secs => :events - seq)
FROM (
    SELECT seq, 1 + (seq - 1) % :entities AS entity
    FROM generate_series(1, :events) AS seq
) AS s
ORDER BY seq;
