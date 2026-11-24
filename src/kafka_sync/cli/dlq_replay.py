"""Inspect the dead letter queue and replay messages back into the pipeline.

    python -m kafka_sync.cli.dlq_replay list [--error-type TYPE]
    python -m kafka_sync.cli.dlq_replay show --partition 0 --offset 12
    python -m kafka_sync.cli.dlq_replay replay --error-type retries_exhausted
    python -m kafka_sync.cli.dlq_replay replay --partition 0 --offset 12 \\
        --patch '{"payload": {"status": "active"}}'

Replaying never removes anything from the DLQ topic (Kafka logs are append
only). Each replay is recorded in the `dlq_replays` table instead, and a
message that was already replayed is skipped unless --force is given.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any

import psycopg
from confluent_kafka import Producer
from pydantic import ValidationError

from kafka_sync.clients import producer_config
from kafka_sync.config import Settings
from kafka_sync.consumer import routing
from kafka_sync.contract import ContractError, EventCodec, loose_json
from kafka_sync.models import RecordEvent
from kafka_sync.snapshot import TopicRecord, read_topic


def error_type_of(record: TopicRecord) -> str:
    return record.headers.get(routing.H_ERROR_TYPE, "unknown")


def origin_of(record: TopicRecord) -> str:
    """Where the message first failed, as topic[partition]@offset."""
    h = record.headers
    return (
        f"{h.get(routing.H_ORIGINAL_TOPIC, '?')}"
        f"[{h.get(routing.H_ORIGINAL_PARTITION, '?')}]"
        f"@{h.get(routing.H_ORIGINAL_OFFSET, '?')}"
    )


@dataclass(frozen=True)
class ReplayResult:
    record: TopicRecord
    status: str  # replayed | would-replay | skipped | failed
    detail: str = ""


# --------------------------------------------------------------------- reading


def read_dlq(settings: Settings) -> list[TopicRecord]:
    """Every message currently retained in the DLQ, in partition/offset order."""
    return read_topic(settings, settings.topics.dlq)


def select(
    records: list[TopicRecord],
    *,
    error_type: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
    limit: int | None = None,
) -> list[TopicRecord]:
    chosen = [
        r
        for r in records
        if (error_type is None or error_type_of(r) == error_type)
        and (partition is None or r.partition == partition)
        and (offset is None or r.offset == offset)
    ]
    return chosen[:limit] if limit else chosen


# -------------------------------------------------------------------- replaying


def merge_patch(target: Any, patch: Any) -> Any:
    """Apply a JSON Merge Patch (RFC 7396): objects merge, null deletes, the rest replaces."""
    if not isinstance(patch, dict):
        return patch
    result = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = merge_patch(result.get(key), value)
    return result


def repaired_value(codec: EventCodec, record: TopicRecord, patch: dict | None) -> bytes:
    """Bytes fit to re-enter the pipeline, or ContractError explaining why not.

    The (optionally patched) event must pass the same contract a fresh event
    would, so a replay cannot smuggle a bad message past validation.
    """
    try:
        event = codec.decode(record.value)
    except ContractError:
        event = loose_json(record.value)
    if patch:
        event = merge_patch(event, patch)
    value = codec.encode(event)
    try:
        RecordEvent.model_validate(event)
    except ValidationError as exc:
        first = exc.errors()[0]
        raise ContractError(f"{'.'.join(map(str, first['loc']))}: {first['msg']}") from exc
    return value


def replay(
    settings: Settings,
    records: list[TopicRecord],
    *,
    patch: dict | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> list[ReplayResult]:
    """Send `records` back to the raw topic and record each replay."""
    codec = EventCodec(settings)
    producer = Producer(producer_config(settings))
    topics = settings.topics
    results: list[ReplayResult] = []

    with psycopg.connect(settings.postgres_dsn) as conn:
        already = replayed_offsets(conn, topics.dlq)
        sent: list[tuple[TopicRecord, str | None]] = []
        failures: dict[tuple[int, int], str] = {}

        for record in records:
            source = (record.partition, record.offset)
            if source in already and not force:
                results.append(ReplayResult(record, "skipped", "already replayed (use --force)"))
                continue
            try:
                value = repaired_value(codec, record, patch)
            except ContractError as exc:
                results.append(ReplayResult(record, "failed", str(exc)))
                continue
            if dry_run:
                results.append(ReplayResult(record, "would-replay"))
                continue

            headers = [(k, v) for k, v in record.raw_headers if k not in routing.PIPELINE_HEADERS]
            headers.append(
                (
                    routing.H_REPLAYED_FROM,
                    f"{topics.dlq}[{record.partition}]@{record.offset}".encode(),
                )
            )

            def on_delivery(err: Any, _msg: Any, source: tuple[int, int] = source) -> None:
                if err is not None:
                    failures[source] = str(err)

            producer.produce(
                topics.raw, key=record.key, value=value, headers=headers, on_delivery=on_delivery
            )
            sent.append((record, loose_json(value).get("event_id")))

        if producer.flush(settings.delivery_timeout_ms / 1000 + 5) > 0:
            raise RuntimeError("timed out waiting for the broker to acknowledge replays")

        # Audit only what the broker acknowledged.
        for record, event_id in sent:
            source = (record.partition, record.offset)
            if source in failures:
                results.append(ReplayResult(record, "failed", failures[source]))
                continue
            conn.execute(
                """
                INSERT INTO dlq_replays
                    (dlq_topic, dlq_partition, dlq_offset, event_id, error_type,
                     target_topic, patched)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    topics.dlq,
                    record.partition,
                    record.offset,
                    event_id,
                    error_type_of(record),
                    topics.raw,
                    patch is not None,
                ),
            )
            results.append(ReplayResult(record, "replayed"))
    return sorted(results, key=lambda r: (r.record.partition, r.record.offset))


def replayed_offsets(conn: psycopg.Connection, dlq_topic: str) -> set[tuple[int, int]]:
    rows = conn.execute(
        "SELECT dlq_partition, dlq_offset FROM dlq_replays WHERE dlq_topic = %s", (dlq_topic,)
    ).fetchall()
    return {(partition, offset) for partition, offset in rows}


# -------------------------------------------------------------------------- CLI


def _text(data: bytes | None) -> str:
    return "" if data is None else data.decode("utf-8", "replace")


def _cmd_list(settings: Settings, args: argparse.Namespace) -> int:
    records = _selected(settings, args)
    with psycopg.connect(settings.postgres_dsn) as conn:
        already = replayed_offsets(conn, settings.topics.dlq)
    row = "{:>4} {:>7}  {:<24} {:>5}  {:<8}  {}"
    print(row.format("PART", "OFFSET", "ERROR TYPE", "TRIES", "REPLAYED", "ORIGIN / REASON"))
    for r in records:
        h = r.headers
        replayed = "yes" if (r.partition, r.offset) in already else "no"
        tries = routing.retry_attempt(h) + 1
        print(row.format(r.partition, r.offset, error_type_of(r), tries, replayed, origin_of(r)))
        print(f"{'':>46}  key={_text(r.key)}  {h.get(routing.H_ERROR_REASON, '')[:100]}")
    print(f"{len(records)} message(s)")
    return 0


def _cmd_show(settings: Settings, args: argparse.Namespace) -> int:
    records = _selected(settings, args)
    if not records:
        print("no matching message", file=sys.stderr)
        return 1
    for r in records:
        print(f"--- {settings.topics.dlq}[{r.partition}]@{r.offset}  key={_text(r.key)}")
        for name, value in r.headers.items():
            print(f"{name}: {value}")
        try:
            print(json.dumps(loose_json(r.value), indent=2))
        except ContractError:
            print(f"(not JSON) {r.value!r}")
    return 0


def _cmd_replay(settings: Settings, args: argparse.Namespace) -> int:
    patch = None
    if args.patch:
        patch = json.loads(args.patch)
        if not isinstance(patch, dict):
            print("--patch must be a JSON object", file=sys.stderr)
            return 2
    results = replay(
        settings, _selected(settings, args), patch=patch, force=args.force, dry_run=args.dry_run
    )
    for result in results:
        r = result.record
        print(
            f"{r.partition:>4} {r.offset:>7}  {result.status:<12} "
            f"{error_type_of(r):<24} {result.detail}"
        )
    counts = {s: sum(1 for r in results if r.status == s) for s in {r.status for r in results}}
    print(", ".join(f"{n} {s}" for s, n in sorted(counts.items())) or "nothing to replay")
    return 1 if counts.get("failed") else 0


def _selected(settings: Settings, args: argparse.Namespace) -> list[TopicRecord]:
    return select(
        read_dlq(settings),
        error_type=args.error_type,
        partition=args.partition,
        offset=args.offset,
        limit=args.limit,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dlq_replay", description="Inspect and replay dead letter queue messages."
    )
    filters = argparse.ArgumentParser(add_help=False)
    filters.add_argument("--error-type", help="only messages with this x-error-type header")
    filters.add_argument("--partition", type=int, help="only this DLQ partition")
    filters.add_argument("--offset", type=int, help="only this DLQ offset")
    filters.add_argument("--limit", type=int, help="at most this many messages")

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", parents=[filters], help="one line per DLQ message").set_defaults(
        run=_cmd_list
    )
    commands.add_parser("show", parents=[filters], help="headers and payload").set_defaults(
        run=_cmd_show
    )
    replay_cmd = commands.add_parser(
        "replay", parents=[filters], help="send messages back to the raw topic"
    )
    replay_cmd.add_argument("--patch", help="JSON Merge Patch applied to each event before replay")
    replay_cmd.add_argument("--force", action="store_true", help="replay even if already replayed")
    replay_cmd.add_argument("--dry-run", action="store_true", help="validate only, send nothing")
    replay_cmd.set_defaults(run=_cmd_replay)
    return parser


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.run(settings or Settings.from_env(), args)


if __name__ == "__main__":
    sys.exit(main())
