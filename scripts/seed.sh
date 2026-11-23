#!/usr/bin/env bash
# Insert sample change events into the source_events outbox table.
#
# Usage: scripts/seed.sh [event_count] [entity_count]
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

events="${1:-100}"
entities="${2:-20}"

for value in "${events}" "${entities}"; do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || { echo "error: counts must be positive integers" >&2; exit 1; }
done

psql_exec -v events="${events}" -v entities="${entities}" < "${REPO_ROOT}/db/seed/source_events.sql"
log "seeded ${events} events across up to ${entities} entities"
psql_exec -At -c "SELECT 'source_events: ' || count(*) || ' total, ' || count(*) FILTER (WHERE published_at IS NULL) || ' unpublished' FROM source_events" >&2
