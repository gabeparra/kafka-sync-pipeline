-- Transactional outbox for the source system.
--
-- The application writes a row here in the same transaction as its business
-- change. A separate poller publishes unpublished rows to Kafka and then marks
-- them, so an event is never lost if the broker is down and never published
-- for a transaction that rolled back. A crash between publish and mark causes
-- a re-publish, which is why delivery is at-least-once and consumers dedupe on
-- event_id.

CREATE TABLE source_events (
    -- Monotonic sequence: the poller publishes in this order.
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Globally unique identity of the event; the consumer's idempotency key.
    event_id      UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    -- Kafka message key. All events for one entity land on one partition.
    entity_id     TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL until the poller has a broker acknowledgement for this row.
    published_at  TIMESTAMPTZ
);

-- The poller only ever scans the unpublished tail, so index just that.
CREATE INDEX source_events_unpublished_idx
    ON source_events (id)
    WHERE published_at IS NULL;
