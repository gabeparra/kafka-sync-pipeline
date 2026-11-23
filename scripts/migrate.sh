#!/usr/bin/env bash
# Apply db/migrations/*.sql in filename order, each exactly once.
#
# Applied versions are recorded in schema_migrations. Each file runs in a
# single transaction together with its bookkeeping row, so a failed migration
# leaves nothing behind and is retried on the next run.
#
# Usage: scripts/migrate.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

psql_exec -c "CREATE TABLE IF NOT EXISTS schema_migrations (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)"

shopt -s nullglob
for file in "${REPO_ROOT}"/db/migrations/*.sql; do
  version="$(basename "${file}" .sql)"
  applied="$(psql_exec -At -v version="${version}" <<< "SELECT 1 FROM schema_migrations WHERE version = :'version'")"
  if [[ "${applied}" == "1" ]]; then
    continue
  fi
  log "applying ${version}"
  {
    cat "${file}"
    echo "INSERT INTO schema_migrations (version) VALUES (:'version');"
  } | psql_exec --single-transaction -v version="${version}"
done
log "migrations up to date"
