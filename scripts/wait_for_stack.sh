#!/usr/bin/env bash
# Block until Kafka, Schema Registry and Postgres all answer real requests.
#
# Probes each service directly instead of reading container health state, so it
# behaves the same under Docker and Podman.
#
# Usage: scripts/wait_for_stack.sh [timeout_seconds]
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

timeout="${1:-120}"
deadline=$((SECONDS + timeout))

wait_for() {
  local name="$1"
  shift
  until "$@" > /dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "error: ${name} not ready after ${timeout}s (try: make logs)" >&2
      exit 1
    fi
    sleep 2
  done
  log "${name} is ready"
}

wait_for "kafka" kafka_cli kafka-broker-api-versions
wait_for "postgres" psql_exec -c 'SELECT 1'
# The registry image has no curl; python3 is what its own healthcheck uses too.
wait_for "schema-registry" compose exec -T schema-registry python3 -c \
  "import urllib.request; urllib.request.urlopen('http://localhost:8081/subjects', timeout=3)"
