-- Let the publisher park an outbox row it can never publish.
--
-- A row that violates the event contract would otherwise sit at the head of
-- the unpublished queue forever and be retried on every poll. Recording the
-- reason takes it out of the queue while keeping it visible for a fix.

ALTER TABLE source_events ADD COLUMN publish_error TEXT;

DROP INDEX source_events_unpublished_idx;

CREATE INDEX source_events_unpublished_idx
    ON source_events (id)
    WHERE published_at IS NULL AND publish_error IS NULL;
