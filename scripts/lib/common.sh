# shellcheck shell=bash
# Shared helpers for the lab scripts. Source this file; do not execute it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Load .env the same way Compose does, without overriding the caller's
# environment (make exports its own values first).
if [[ -f "${REPO_ROOT}/.env" ]]; then
  while IFS='=' read -r key value; do
    [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    [[ -v "${key}" ]] || export "${key}=${value}"
  done < "${REPO_ROOT}/.env"
fi

# Resolve the Compose command once: an explicit $COMPOSE wins, then Docker,
# then Podman. Stored as an array so "podman compose" survives word splitting.
if [[ -n "${COMPOSE:-}" ]]; then
  read -r -a COMPOSE_CMD <<< "${COMPOSE}"
elif docker compose version > /dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif podman compose version > /dev/null 2>&1; then
  COMPOSE_CMD=(podman compose)
else
  echo "error: neither 'docker compose' nor 'podman compose' is available" >&2
  exit 1
fi

# podman prints a banner on every call when it delegates to an external compose provider.
export PODMAN_COMPOSE_WARNING_LOGS=false

# Kafka's address as seen from inside the kafka container.
readonly KAFKA_INTERNAL_BOOTSTRAP="localhost:19092"

log() { printf '==> %s\n' "$*" >&2; }

compose() { (cd "${REPO_ROOT}" && "${COMPOSE_CMD[@]}" "$@"); }

# Run one of the CLI tools shipped in the broker image, e.g. `kafka_cli kafka-topics --list`.
kafka_cli() {
  local tool="$1"
  shift
  compose exec -T kafka "/opt/kafka/bin/${tool}.sh" --bootstrap-server "${KAFKA_INTERNAL_BOOTSTRAP}" "$@"
}

# Run psql inside the postgres container as the lab user; SQL comes from stdin or -c.
psql_exec() {
  compose exec -T postgres sh -c 'PGOPTIONS="-c client_min_messages=warning" psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' psql "$@"
}
