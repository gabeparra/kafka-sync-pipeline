#!/usr/bin/env bash
# Create the pipeline's topics and converge their configuration.
#
# Idempotent: safe to re-run. Creation is skipped for topics that exist, and
# topic-level configs are re-applied every time so the script is the single
# source of truth for them. Partition counts are only set at creation; an
# existing topic with a different count is reported, never silently changed
# (growing a keyed topic remaps keys to partitions and breaks per-key ordering).
#
# Usage: scripts/init_topics.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

# Single broker, so every topic has exactly one replica. In a real cluster this
# would be 3 with min.insync.replicas=2 to make acks=all meaningful.
readonly REPLICATION_FACTOR=1

readonly DAY_MS=$((24 * 60 * 60 * 1000))

# ensure_topic <name> <partitions> <config>...
ensure_topic() {
  local name="$1" partitions="$2"
  shift 2
  local configs
  configs="$(IFS=,; echo "$*")"

  log "${name}: ${partitions} partitions, ${configs}"
  kafka_cli kafka-topics --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION_FACTOR}" > /dev/null

  kafka_cli kafka-configs --alter \
    --entity-type topics --entity-name "${name}" \
    --add-config "${configs}" > /dev/null

  local actual
  actual="$(kafka_cli kafka-topics --describe --topic "${name}" | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  if [[ "${actual}" != "${partitions}" ]]; then
    echo "error: ${name} has ${actual} partitions, expected ${partitions}." \
      "Recreate it (make clean) rather than repartitioning a keyed topic." >&2
    exit 1
  fi
}

# Inbound change events, keyed by entity_id. Six partitions give a consumer
# group room to scale out while every event for one entity stays in order.
ensure_topic source.records.raw 6 \
  "cleanup.policy=delete" \
  "retention.ms=$((7 * DAY_MS))"

# Latest reconciled state per entity. Compaction keeps the newest value for
# each key; time-based deletion bounds entities that stop changing. The short
# segment roll lets compaction be observed in a lab, since the active segment
# is never compacted.
ensure_topic records.reconciled 6 \
  "cleanup.policy=[compact,delete]" \
  "retention.ms=$((30 * DAY_MS))" \
  "min.cleanable.dirty.ratio=0.1" \
  "segment.ms=$((60 * 60 * 1000))"

# Transient failures parked for a delayed second attempt.
ensure_topic records.retry.1m 3 \
  "cleanup.policy=delete" \
  "retention.ms=${DAY_MS}"

# Permanently failed or malformed records, kept long enough to inspect and replay.
ensure_topic records.dlq 3 \
  "cleanup.policy=delete" \
  "retention.ms=$((30 * DAY_MS))"

log "topics:"
kafka_cli kafka-topics --describe --exclude-internal | grep -E '^Topic: [^_]' >&2
