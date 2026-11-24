-- Destination side of the reconciliation.

-- Idempotency ledger. Kafka delivers at least once, so the consumer records
-- every event_id it has handled in the same transaction as the state change.
-- A redelivered event hits the primary key and is skipped.
CREATE TABLE processed_events (
    event_id          UUID PRIMARY KEY,
    entity_id         TEXT        NOT NULL,
    -- applied: changed destination state. stale: older than what was already there.
    outcome           TEXT        NOT NULL CHECK (outcome IN ('applied', 'stale')),
    source_topic      TEXT        NOT NULL,
    source_partition  INT         NOT NULL,
    source_offset     BIGINT      NOT NULL,
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Destination system's copy of each record. It has its own primary key and
-- does not know the source's entity_id as an identifier, so records are
-- matched on a natural key: document number plus birth date.
CREATE TABLE reconciled_records (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_number   TEXT        NOT NULL,
    birth_date        DATE        NOT NULL,
    source_system     TEXT        NOT NULL,
    source_entity_id  TEXT        NOT NULL,
    full_name         TEXT        NOT NULL,
    email             TEXT,
    status            TEXT        NOT NULL,
    -- The newest source change applied so far; older events must not overwrite it.
    last_event_id     UUID        NOT NULL,
    last_event_at     TIMESTAMPTZ NOT NULL,
    -- How many events changed this row. Lets tests prove exactly-once effects.
    applied_count     INT         NOT NULL DEFAULT 1,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (document_number, birth_date)
);

-- Audit trail of DLQ messages sent back into the pipeline.
CREATE TABLE dlq_replays (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dlq_topic      TEXT        NOT NULL,
    dlq_partition  INT         NOT NULL,
    dlq_offset     BIGINT      NOT NULL,
    event_id       TEXT,
    error_type     TEXT,
    target_topic   TEXT        NOT NULL,
    patched        BOOLEAN     NOT NULL,
    replayed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX dlq_replays_source_idx ON dlq_replays (dlq_topic, dlq_partition, dlq_offset);
